#!/usr/bin/env python3
"""
Build statutes SQLite database from Fedlex Akoma Ntoso XML files.

Reads downloaded XML from output/fedlex/xml/{sr_number}/{lang}.xml,
parses article-level text, and builds a searchable SQLite DB with FTS5.

Output: output/statutes.db

Schema:
    laws        — one row per law (SR number, titles, abbreviations)
    articles    — one row per article per language
    articles_fts — FTS5 virtual table over article text

Usage:
    python -m search_stack.build_statutes_db
    python -m search_stack.build_statutes_db --fedlex-dir output/fedlex
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_statutes")

FEDLEX_DIR = Path(os.environ.get("FEDLEX_OUTPUT", "output/fedlex"))
OUTPUT_DB = Path(os.environ.get("STATUTES_DB", "output/statutes.db"))

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"
NS = {"akn": AKN_NS}
# Serialize article subtrees back to clean Akoma Ntoso XML (declare akn as the
# default namespace so fragments read <article xmlns="...">…</article> rather
# than with ElementTree's ns0: prefixes). Issue #22.
ET.register_namespace("", AKN_NS)


def create_schema(conn: sqlite3.Connection):
    """Create database schema."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS laws (
            sr_number TEXT PRIMARY KEY,
            title_de TEXT,
            title_fr TEXT,
            title_it TEXT,
            abbr_de TEXT,
            abbr_fr TEXT,
            abbr_it TEXT,
            consolidation_date TEXT,
            work_uri TEXT
        );

        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sr_number TEXT NOT NULL,
            article_num TEXT NOT NULL,
            heading TEXT,
            footnote TEXT,
            text TEXT NOT NULL,
            xml TEXT,
            lang TEXT NOT NULL,
            FOREIGN KEY (sr_number) REFERENCES laws(sr_number)
        );

        CREATE INDEX IF NOT EXISTS idx_articles_sr_art
            ON articles(sr_number, article_num);
        CREATE INDEX IF NOT EXISTS idx_articles_sr_lang
            ON articles(sr_number, lang);

        CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
            sr_number,
            article_num,
            heading,
            text,
            lang,
            content='articles',
            content_rowid='id',
            tokenize='unicode61 remove_diacritics 2'
        );

        CREATE TABLE IF NOT EXISTS amendment_refs (
            ref_type TEXT NOT NULL,
            year INTEGER NOT NULL,
            page_num INTEGER NOT NULL,
            eli_uri TEXT NOT NULL,
            PRIMARY KEY (ref_type, year, page_num)
        );

        CREATE INDEX IF NOT EXISTS idx_amendment_refs_eli
            ON amendment_refs(eli_uri);
    """)


def extract_text(element, skip_tags: set[str] | None = None) -> str:
    """Recursively extract all text content from an XML element."""
    parts = []
    if element.text:
        parts.append(element.text.strip())
    for child in element:
        # Strip namespace for tag comparison
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if skip_tags and tag in skip_tags:
            # Still include tail text (text after the skipped element)
            if child.tail:
                parts.append(child.tail.strip())
            continue
        parts.append(extract_text(child, skip_tags))
        if child.tail:
            parts.append(child.tail.strip())
    return " ".join(p for p in parts if p)


def parse_article(article_elem) -> tuple[str, str | None, str, str | None]:
    """Parse an article element, return (article_num, heading, full_text, footnote).

    The footnote field captures `<authorialNote>` elements from the article's
    `<num>` and `<heading>` — these contain amendment references like
    "Eingefügt durch... ( AS 2020 4525 ; BBl 2019 4747)" for inserted articles.
    """
    # Extract article number (skip authorialNote footnotes embedded in <num>)
    num_elem = article_elem.find("akn:num", NS)
    if num_elem is None:
        num_elem = article_elem.find(f"{{{AKN_NS}}}num")
    article_num = extract_text(num_elem, skip_tags={"authorialNote"}) if num_elem is not None else ""

    # Extract authorialNote footnotes from <num> and <heading> elements.
    # These contain the amendment references for INSERTED articles.
    footnote_parts: list[str] = []
    for parent in [num_elem, article_elem.find("akn:heading", NS),
                    article_elem.find(f"{{{AKN_NS}}}heading")]:
        if parent is None:
            continue
        for note in parent.findall("akn:authorialNote", NS):
            note_text = extract_text(note)
            if note_text:
                footnote_parts.append(note_text)
        for note in parent.findall(f"{{{AKN_NS}}}authorialNote"):
            note_text = extract_text(note)
            if note_text and note_text not in footnote_parts:
                footnote_parts.append(note_text)
    # Also check direct children of the article element
    for note in article_elem.findall("akn:authorialNote", NS):
        note_text = extract_text(note)
        if note_text and note_text not in footnote_parts:
            footnote_parts.append(note_text)
    for note in article_elem.findall(f"{{{AKN_NS}}}authorialNote"):
        note_text = extract_text(note)
        if note_text and note_text not in footnote_parts:
            footnote_parts.append(note_text)
    footnote = " ".join(footnote_parts).strip() if footnote_parts else None
    # Clean article number: "Art. 41" -> "41", "Art. 41a" -> "41a"
    article_num = re.sub(r"^Art\.?\s*", "", article_num).strip()
    # Strip any remaining footnote text after the article number
    # e.g. "5 a Angenommen in der..." -> "5a"
    m = re.match(r"(\d+)\s*((?:bis|ter|quater|quinquies|sexies|septies|octies|novies)|[a-z])?", article_num)
    if m:
        article_num = m.group(1) + (m.group(2) or "")
    if not article_num:
        # Try eId attribute: "art_41" -> "41"
        eid = article_elem.get("eId", "")
        m = re.search(r"art_(\w+)", eid)
        if m:
            article_num = m.group(1)

    # Extract heading (marginal note / Randtitel)
    heading = None
    heading_elem = article_elem.find("akn:heading", NS)
    if heading_elem is None:
        heading_elem = article_elem.find(f"{{{AKN_NS}}}heading")
    if heading_elem is not None:
        heading = extract_text(heading_elem)

    # Extract paragraphs
    paragraphs = []
    for para in article_elem.findall(".//akn:paragraph", NS):
        para_text = extract_text(para)
        if para_text:
            paragraphs.append(para_text)

    if not paragraphs:
        for para in article_elem.findall(f".//{{{AKN_NS}}}paragraph"):
            para_text = extract_text(para)
            if para_text:
                paragraphs.append(para_text)

    # If no paragraphs found, extract all content
    if not paragraphs:
        content = article_elem.find("akn:content", NS)
        if content is None:
            content = article_elem.find(f"{{{AKN_NS}}}content")
        if content is not None:
            text = extract_text(content)
            if text:
                paragraphs.append(text)

    # If still nothing, get all text from the article
    if not paragraphs:
        text = extract_text(article_elem)
        # Remove the article number and heading from the full text
        if article_num:
            text = text.replace(f"Art. {article_num}", "", 1).strip()
        if heading:
            text = text.replace(heading, "", 1).strip()
        if text:
            paragraphs.append(text)

    full_text = "\n".join(paragraphs)
    return article_num, heading, full_text, footnote


def parse_xml(xml_path: Path) -> list[dict]:
    """Parse an Akoma Ntoso XML file and extract all articles."""
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as e:
        log.warning("XML parse error in %s: %s", xml_path, e)
        return []

    root = tree.getroot()
    articles = []

    # Find all article elements (try both namespaced and non-namespaced)
    article_elems = root.findall(f".//{{{AKN_NS}}}article")
    if not article_elems:
        article_elems = root.findall(".//article")

    for art_elem in article_elems:
        article_num, heading, text, footnote = parse_article(art_elem)
        if not article_num or not text:
            continue

        articles.append({
            "article_num": article_num,
            "heading": heading,
            "text": text,
            "footnote": footnote,
            # Issue #22: verbatim AN XML subtree (enumerations, footnotes,
            # sub-paragraphs) for structured rendering, alongside the text.
            "xml": ET.tostring(art_elem, encoding="unicode"),
        })

    return articles


def build_db():
    """Main build pipeline."""
    xml_dir = FEDLEX_DIR / "xml"
    laws_index_path = FEDLEX_DIR / "laws.json"

    if not xml_dir.exists():
        log.error("XML directory not found: %s — run scrapers/fedlex.py first", xml_dir)
        return

    # Load law index
    law_index = {}
    if laws_index_path.exists():
        with open(laws_index_path, encoding="utf-8") as f:
            for entry in json.load(f):
                law_index[entry["sr_number"]] = entry

    # Prepare output — resolve symlinks so temp file is on same filesystem (atomic rename)
    resolved_db = OUTPUT_DB.resolve()
    tmp_db = resolved_db.with_suffix(".tmp")
    tmp_db.unlink(missing_ok=True)

    conn = sqlite3.connect(str(tmp_db))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -256000")  # 256MB
    create_schema(conn)

    total_laws = 0
    total_articles = 0

    # Iterate over laws.json (authoritative SPARQL discovery) rather than
    # over xml_dir contents. SPARQL drops abrogated laws from the index
    # the moment they're repealed; iterating xml_dir.iterdir() would keep
    # ingesting their stale on-disk XML and silently insert articles
    # for repealed laws into statutes.db with NULL metadata. By driving
    # off the index, abrogated SRs whose XML still happens to be on disk
    # are quietly skipped — the on-disk leftover is harmless until a
    # separate cleanup pass runs.
    sr_dirs_present = (
        {p.name for p in xml_dir.iterdir() if p.is_dir()}
        if xml_dir.exists() else set()
    )
    log.info(
        "Processing %d laws from index (xml_dir has %d directories on disk)",
        len(law_index), len(sr_dirs_present),
    )

    for sr_number in sorted(law_index.keys()):
        sr_dir_name = sr_number.replace(".", "_")
        sr_dir = xml_dir / sr_dir_name
        if not sr_dir.is_dir():
            log.debug("SR %s in index but no XML on disk yet, skipping", sr_number)
            continue

        meta = law_index.get(sr_number, {})

        # Insert law metadata
        conn.execute(
            """INSERT OR REPLACE INTO laws
               (sr_number, title_de, title_fr, title_it,
                abbr_de, abbr_fr, abbr_it, consolidation_date, work_uri)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                sr_number,
                meta.get("title_de"),
                meta.get("title_fr"),
                meta.get("title_it"),
                meta.get("abbr_de"),
                meta.get("abbr_fr"),
                meta.get("abbr_it"),
                meta.get("consolidation_date"),
                meta.get("work_uri"),
            ),
        )

        # Parse articles for each language
        law_article_count = 0
        for lang in ["de", "fr", "it"]:
            xml_path = sr_dir / f"{lang}.xml"
            if not xml_path.exists():
                continue

            articles = parse_xml(xml_path)
            for art in articles:
                conn.execute(
                    """INSERT INTO articles (sr_number, article_num, heading, footnote, text, xml, lang)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (sr_number, art["article_num"], art["heading"], art.get("footnote"),
                     art["text"], art.get("xml"), lang),
                )
                law_article_count += 1

        if law_article_count > 0:
            total_laws += 1
            total_articles += law_article_count

        if total_laws % 100 == 0 and total_laws > 0:
            conn.commit()
            log.info("Progress: %d laws, %d articles", total_laws, total_articles)

    conn.commit()

    # Populate FTS5 index
    log.info("Building FTS5 index...")
    conn.execute("""
        INSERT INTO articles_fts(rowid, sr_number, article_num, heading, text, lang)
        SELECT id, sr_number, article_num, heading, text, lang FROM articles
    """)
    conn.commit()

    # Optimize
    log.info("Optimizing FTS5...")
    conn.execute("INSERT INTO articles_fts(articles_fts) VALUES('optimize')")
    conn.commit()

    # Stats
    law_count = conn.execute("SELECT COUNT(*) FROM laws").fetchone()[0]
    art_count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    log.info("Built statutes DB: %d laws, %d articles", law_count, art_count)

    # Print top laws by article count
    top = conn.execute("""
        SELECT a.sr_number, l.abbr_de, COUNT(*) as cnt
        FROM articles a
        LEFT JOIN laws l ON a.sr_number = l.sr_number
        WHERE a.lang = 'de'
        GROUP BY a.sr_number
        ORDER BY cnt DESC
        LIMIT 15
    """).fetchall()
    log.info("Top laws by article count:")
    for sr, abbr, cnt in top:
        log.info("  SR %s (%s): %d articles", sr, abbr or "?", cnt)

    # Switch to DELETE journal mode for immutable=1 compatibility with MCP workers
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.close()

    # Atomic swap
    os.replace(str(tmp_db), str(resolved_db))
    log.info("Saved to %s (%.1f MB)", resolved_db, resolved_db.stat().st_size / 1e6)


def main():
    global FEDLEX_DIR, OUTPUT_DB

    parser = argparse.ArgumentParser(description="Build statutes DB from Fedlex XML")
    parser.add_argument("--fedlex-dir", type=Path, default=FEDLEX_DIR)
    parser.add_argument("--output", type=Path, default=OUTPUT_DB)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    FEDLEX_DIR = args.fedlex_dir
    OUTPUT_DB = args.output

    t0 = time.time()
    build_db()
    log.info("Total time: %.1f seconds", time.time() - t0)


if __name__ == "__main__":
    main()
