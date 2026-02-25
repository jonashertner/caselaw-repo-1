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
            text TEXT NOT NULL,
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
    """)


def extract_text(element) -> str:
    """Recursively extract all text content from an XML element."""
    parts = []
    if element.text:
        parts.append(element.text.strip())
    for child in element:
        parts.append(extract_text(child))
        if child.tail:
            parts.append(child.tail.strip())
    return " ".join(p for p in parts if p)


def parse_article(article_elem) -> tuple[str, str | None, str]:
    """Parse an article element, return (article_num, heading, full_text)."""
    # Extract article number
    num_elem = article_elem.find("akn:num", NS)
    if num_elem is None:
        num_elem = article_elem.find(f"{{{AKN_NS}}}num")
    article_num = extract_text(num_elem) if num_elem is not None else ""
    # Clean article number: "Art. 41" -> "41", "Art. 41a" -> "41a"
    article_num = re.sub(r"^Art\.?\s*", "", article_num).strip()
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
    return article_num, heading, full_text


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
        article_num, heading, text = parse_article(art_elem)
        if not article_num or not text:
            continue

        articles.append({
            "article_num": article_num,
            "heading": heading,
            "text": text,
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

    # Prepare output
    tmp_db = OUTPUT_DB.with_suffix(".tmp")
    tmp_db.unlink(missing_ok=True)

    conn = sqlite3.connect(str(tmp_db))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -256000")  # 256MB
    create_schema(conn)

    total_laws = 0
    total_articles = 0

    # Iterate over downloaded XML directories
    sr_dirs = sorted(xml_dir.iterdir()) if xml_dir.exists() else []
    log.info("Processing %d law directories...", len(sr_dirs))

    for sr_dir in sr_dirs:
        if not sr_dir.is_dir():
            continue

        # Reconstruct SR number from directory name
        sr_number = sr_dir.name.replace("_", ".")
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
                    """INSERT INTO articles (sr_number, article_num, heading, text, lang)
                       VALUES (?, ?, ?, ?, ?)""",
                    (sr_number, art["article_num"], art["heading"], art["text"], lang),
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

    conn.close()

    # Atomic rename
    if OUTPUT_DB.exists():
        OUTPUT_DB.unlink()
    tmp_db.rename(OUTPUT_DB)
    log.info("Saved to %s (%.1f MB)", OUTPUT_DB, OUTPUT_DB.stat().st_size / 1e6)


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
