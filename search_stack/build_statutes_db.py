#!/usr/bin/env python3
"""
Build statutes SQLite database from Fedlex Akoma Ntoso XML files.

Reads downloaded XML from output/fedlex/xml/{sr_number}/{lang}.xml,
parses article-level text, and builds a searchable SQLite DB with FTS5.

Output: output/statutes.db

Schema:
    laws        — one row per law (SR number, titles, abbreviations)
    articles    — one row per article per language per block (main body or a
                  transitional/final-provisions block, see `section`)
    articles_fts — FTS5 virtual table over article text

Usage:
    python -m search_stack.build_statutes_db
    python -m search_stack.build_statutes_db --fedlex-dir output/fedlex
    python -m search_stack.build_statutes_db --output scratch/statutes.new.db --baseline output/statutes.db

The build refuses to replace the live DB when the result is implausibly
smaller (or larger) than the previous one; see check_floor().
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from collections import Counter
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
    """Create database schema.

    `section` is '' for the main body and the eId prefix of the enclosing
    block for transitional / final-provisions articles ("disp_u2" for eId
    "disp_u2/art_1"); `section_heading` is that block's heading. Without the
    column, OR Art. 1 (de) had 14 indistinguishable rows: the main body plus
    one "Art. 1" from each of 13 amendment blocks.

    idx_articles_key is deliberately non-unique: duplicates are resolved (keep
    first, WARNING) in parse_root so an unattended run never aborts on a
    Fedlex quirk.

    articles_fts is unchanged (5 columns): mcp_server's snippet() call depends
    on column 3 being `text`. Footnotes are not indexed; amendment notes used
    to be searchable only because they were spliced into `text`.
    """
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
            section TEXT NOT NULL DEFAULT '',
            section_heading TEXT,
            eid TEXT,
            FOREIGN KEY (sr_number) REFERENCES laws(sr_number)
        );

        CREATE INDEX IF NOT EXISTS idx_articles_sr_art
            ON articles(sr_number, article_num);
        CREATE INDEX IF NOT EXISTS idx_articles_sr_lang
            ON articles(sr_number, lang);
        CREATE INDEX IF NOT EXISTS idx_articles_key
            ON articles(sr_number, lang, section, article_num);

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


# Tags whose content runs on with the surrounding text without a word
# boundary. Fedlex marks up article-number suffixes as <i>a</i> / <sup>bis</sup>
# and splits words across <inline> ("Auf<inline>wertung</inline>"), so a space
# at these boundaries corrupts the text: "28a" became "28 a", "1bis" became
# "1 bis", and 10 French StGB articles were stored under ". 264 a" and were
# unreachable. Every other element (p, paragraph, item, num, td, br, ...) is
# a block and gets one space at each side.
_INLINE_TAGS = frozenset({
    "b", "i", "u", "s", "sub", "ref", "inline", "span", "a", "abbr", "term",
    "def", "docTitle", "docNumber", "shortTitle", "date", "placeholder",
    "noteRef", "marker", "ins", "del",
})


def _local(tag: str) -> str:
    """Strip the namespace from an ElementTree tag."""
    return tag.split("}")[-1] if "}" in tag else tag


def _is_inline(child) -> bool:
    tag = _local(child.tag)
    if tag in _INLINE_TAGS:
        return True
    # <sup> is an ordinal suffix ("305<sup>bis</sup>", French "1<sup>er</sup>")
    # only when purely alphabetic. Numeric <sup> are paragraph markers
    # ("<sup>1</sup> Die ...") or footnote markers and must stay separated:
    # "3953<sup>3957</sup>" is two numbers, not 39533957.
    if tag == "sup":
        return "".join(child.itertext()).strip().isalpha()
    return False


def _collect_text(element, skip_tags, parts: list[str]) -> None:
    if element.text:
        parts.append(element.text)
    for child in element:
        tag = _local(child.tag)
        inline = _is_inline(child)
        if skip_tags and tag in skip_tags:
            tail = child.tail or ""
            if tag == "authorialNote":
                # BV Art. 10a: <heading>Verbot ...<authorialNote><p><sup>*</sup>
                # Mit Übergangsbestimmung.</p></authorialNote>*</heading>. The
                # trailing "*" is the marker that pairs with the note's own
                # <sup>*</sup>; it belongs to the footnote, not the heading.
                tail = tail.lstrip().removeprefix("*")
            if not inline:
                parts.append(" ")
            parts.append(tail)
            continue
        if not inline:
            parts.append(" ")
        _collect_text(child, skip_tags, parts)
        if not inline:
            parts.append(" ")
        if child.tail:
            parts.append(child.tail)


def extract_text(element, skip_tags: set[str] | None = None) -> str:
    """Recursively extract the text of an XML element.

    Raw text and tail fragments are concatenated as they stand (no per-fragment
    strip); inline tags add nothing at their boundaries, every other child adds
    one space at each side. U+00A0 is normalised to a plain space (U+2011, the
    non-breaking hyphen, is a real glyph and is kept). Whitespace is collapsed
    once at the end.
    """
    parts: list[str] = []
    _collect_text(element, skip_tags, parts)
    text = "".join(parts).replace(" ", " ")
    return re.sub(r"\s+", " ", text).strip()


# Latin ordinal suffixes used in Fedlex article numbers, longest first so the
# alternation can never settle for a prefix of a longer ordinal ("decies" inside
# "duodecies"). Kept in one named place because a missing entry does not fail
# loudly: the "[a-z]" branch below swallows the first letter and drops the rest.
# That is how Art. 322decies and Art. 179decies StGB came to be stored as "322d"
# and "179d" (GitHub #87) — present in the corpus, unreachable by their real
# number.
_ORDINAL_SUFFIXES = (
    "duodecies", "undecies", "decies", "novies", "octies", "septies",
    "sexies", "quinquies", "quater", "ter", "bis",
)

# Article number plus optional letter plus optional ordinal, all attached:
# "41", "41a", "305bis", "268abis". With extract_text no longer splitting at
# inline tags the suffixes arrive joined, so the regex must not skip
# whitespace before them any more: an Italian range "Art. 135 a 149" used to
# yield "135a" (31 rows on the dev slice) and "Art. 50 e 51" yielded "50e".
#
# The trailing lookahead is the load-bearing part: an unrecognised ordinal fails
# the match outright instead of truncating, so parse_article keeps the raw
# article number. A raw "322tredecies" is findable; a corrupted "322t" is not.
#
# The lookahead has to exclude digits as well as letters. With (?![a-z]) alone,
# "322tredecies" does not fail — the engine backtracks \d+ down to "32" and the
# lookahead then passes on the leftover "2", yielding "32". Excluding [0-9] too
# closes that path without disturbing the split-number case ("16 8" -> "16",
# where the lookahead passes on the space).
_ARTICLE_NUM_RE = re.compile(
    r"(\d+)([a-z])?(" + "|".join(_ORDINAL_SUFFIXES) + r")?(?![a-z0-9])"
)

# Single-article eId, last path segment only: "art_41", "art_38_a",
# "art_268_a_bis", "art_322decies". Ranges ("art_135_149", "art_663_a_663_b")
# do not match: their concatenation ("135149") is not an article number.
_EID_ART_RE = re.compile(r"^art_(\d+)((?:_[a-z]+)*)$")


def _eid_article_num(eid: str) -> str | None:
    """Article number encoded in a single-article eId, else None.

    Operates on the last path segment so a transitional-block prefix
    ("disp_u12/art_2_4") never leaks into the number: that prefix plus the
    old `art_(\\w+)` search is how OR Art. 2-4 of an amendment block was
    stored as "24" and polluted OR Art. 24.
    """
    last = (eid or "").rsplit("/", 1)[-1]
    m = _EID_ART_RE.match(last)
    if not m:
        return None
    return m.group(1) + m.group(2).replace("_", "")


def article_section(article_elem) -> str:
    """Block prefix of the article's eId: "disp_u2" for "disp_u2/art_1",
    "" for a main-body article."""
    eid = article_elem.get("eId", "") or ""
    return eid.rpartition("/")[0]


def _child(elem, name: str):
    """Direct child by local name, namespaced or not."""
    found = elem.find(f"{{{AKN_NS}}}{name}")
    if found is None:
        found = elem.find(name)
    return found


def _descendants(elem, name: str) -> list:
    """Descendants by local name, namespaced or not (never the element itself)."""
    found = elem.findall(f".//{{{AKN_NS}}}{name}")
    if not found:
        found = elem.findall(f".//{name}")
    return found


_SKIP_NOTES = frozenset({"authorialNote"})


def parse_article(article_elem) -> tuple[str, str | None, str, str | None]:
    """Parse an article element, return (article_num, heading, full_text, footnote).

    `footnote` collects every <authorialNote> in the article in document
    order, deduplicated: the amendment references Fedlex attaches to the
    number ("Eingefügt durch ..."), to the heading, and to individual
    paragraphs ("Fassung gemäss ..."). None of it is left in `heading` or
    `text`; before this, 23.5 % of rows on the dev slice carried a footnote
    spliced into the body.
    """
    num_elem = _child(article_elem, "num")
    article_num = extract_text(num_elem, skip_tags=_SKIP_NOTES) if num_elem is not None else ""

    footnote_parts: list[str] = []
    for note in _descendants(article_elem, "authorialNote"):
        note_text = extract_text(note)
        if note_text and note_text not in footnote_parts:
            footnote_parts.append(note_text)
    footnote = " ".join(footnote_parts) if footnote_parts else None

    # Clean article number: "Art. 41" -> "41", "Art. 41a" -> "41a"
    article_num = re.sub(r"^Art\.?\s*", "", article_num).strip()
    m = _ARTICLE_NUM_RE.match(article_num)
    if m:
        article_num = m.group(1) + (m.group(2) or "") + (m.group(3) or "")
    elif " " in article_num:
        # Multi-article <num> whose first token the strict regex refuses,
        # e.g. StGB fr "<b>Art. 355</b><i>f</i>et <b>355</b><i>g</i>" ->
        # "355fet 355g": file it under the first article.
        m = re.match(r"(\d+[a-z]?)", article_num)
        if m:
            article_num = m.group(1)
    eid = article_elem.get("eId", "") or ""
    eid_num = _eid_article_num(eid)
    if not article_num and eid_num:
        article_num = eid_num
    # Issue #32: some Fedlex <num> elements split the number across sibling <b>
    # tags, e.g. "<b>Art. 16</b><b>8</b>" -> extract_text "Art. 16 8", so the
    # regex above captures only the leading digits ("16" for Art. 168) and the
    # article gets stored under the wrong number. When the parsed value is pure
    # digits AND the eId names a single article whose digits strictly extend
    # the parsed ones, the <num> was truncated: trust the eId. Never for range
    # eIds (art_135_149 is not Art. 135149), and never against a <num> the eId
    # merely disagrees with (OR fr eId art_221 carries "Art. 220": keep 220).
    if article_num.isdigit() and eid_num:
        eid_digits = re.match(r"\d+", eid_num).group(0)
        if eid_digits != article_num and eid_digits.startswith(article_num):
            article_num = eid_num

    # Heading (marginal note / Randtitel)
    heading = None
    heading_elem = _child(article_elem, "heading")
    if heading_elem is not None:
        heading = extract_text(heading_elem, skip_tags=_SKIP_NOTES) or None

    # Paragraphs
    paragraphs = []
    for para in _descendants(article_elem, "paragraph"):
        para_text = extract_text(para, skip_tags=_SKIP_NOTES)
        if para_text:
            paragraphs.append(para_text)

    # No paragraphs: a bare <content>
    if not paragraphs:
        content = _child(article_elem, "content")
        if content is not None:
            text = extract_text(content, skip_tags=_SKIP_NOTES)
            if text:
                paragraphs.append(text)

    # Still nothing: everything that is not the number, heading or a note
    if not paragraphs:
        rest = [
            extract_text(child, skip_tags=_SKIP_NOTES)
            for child in article_elem
            if _local(child.tag) not in ("num", "heading", "authorialNote")
        ]
        text = " ".join(t for t in rest if t)
        if text:
            paragraphs.append(text)

    full_text = "\n".join(paragraphs)
    # A repealed article is a number plus a note ("Aufgehoben durch ...") and
    # nothing else; some carry a body of just "…". That note is the only text
    # the article has, and it is exactly what these rows contained before the
    # notes were routed out of the body, so keep serving it as the body rather
    # than dropping the article (ZGB Art. 10, 135 keep rendering and matching).
    if footnote and not re.search(r"\w", full_text):
        full_text = footnote
    elif heading and not re.search(r"\w", full_text):
        # A deleted rule of a treaty regulation is a number plus the heading
        # "[Gelöscht]" and nothing else (77 rows corpus-wide). The heading is
        # the only content there is; the old fallback served the number.
        full_text = heading
    return article_num, heading, full_text, footnote


def parse_root(root, stats: Counter | None = None, source: str = "") -> list[dict]:
    """Extract all articles from a parsed Akoma Ntoso document.

    Every drop is counted in `stats` and logged at WARNING; the old silent
    `continue` hid 251 empties on the dev slice. Rows sharing a
    (section, article_num) key are all kept and only counted: on the
    production corpus 1,190 main-body keys collide, and in 1,189 of them the
    rows are different articles (free-trade agreements numbered Art. 7.1 ...
    7.31 all parse as "7"; treaty protocols reuse eIds) — the read side
    already serves several rows per number.
    """
    if stats is None:
        stats = Counter()
    where = f"{source}: " if source else ""

    # Headings of the blocks that hold transitional / final provisions:
    # <transitional eId="disp_u1">, <proviso eId="disp_u2"><heading>Schluss-
    # bestimmungen der Änderung vom 23. März 1962 ...</heading>. Block eIds
    # are slash-free and not article eIds.
    block_headings: dict[str, str | None] = {}
    for elem in root.iter():
        eid = elem.get("eId")
        if not eid or "/" in eid or eid.startswith("art_"):
            continue
        heading_elem = _child(elem, "heading")
        if heading_elem is not None:
            block_headings[eid] = extract_text(heading_elem, skip_tags=_SKIP_NOTES) or None

    articles = []
    seen: set[tuple[str, str]] = set()
    for art_elem in _descendants(root, "article"):
        article_num, heading, text, footnote = parse_article(art_elem)
        eid = art_elem.get("eId", "") or ""
        section = article_section(art_elem)
        if not article_num:
            num_elem = _child(art_elem, "num")
            raw = extract_text(num_elem) if num_elem is not None else ""
            stats["dropped_no_num"] += 1
            log.warning("%sdrop (no article number) eId=%r num=%r", where, eid, raw)
            continue
        if not text:
            stats["dropped_no_text"] += 1
            log.warning("%sdrop (no text) eId=%r article_num=%r", where, eid, article_num)
            continue
        # Never drop a row for sharing its key. Fedlex reuses eIds inside
        # the declaration blocks of treaties (SR 0.131.1: seven
        # `decl_u2/art_1`, one per declaring state) and even in main bodies,
        # and chapter-dotted numbering ("Art. 7.1") collapses to "7" for
        # every article of the chapter: 5,918 main-body rows on the
        # production corpus, one of them an actual duplicate. Count them so
        # the build log shows the shape; the read side serves all rows.
        key = (section, article_num)
        if key in seen:
            stats["duplicate_key"] += 1
            log.info("%sshared key (section=%r, article_num=%r) eId=%r, keeping both",
                     where, section, article_num, eid)
        seen.add(key)
        if footnote and text == footnote:
            stats["footnote_as_body"] += 1

        stats["articles"] += 1
        articles.append({
            "article_num": article_num,
            "heading": heading,
            "text": text,
            "footnote": footnote,
            "section": section,
            "section_heading": block_headings.get(section) or block_headings.get(section.split("/")[0]),
            "eid": eid,
            # Issue #22: verbatim AN XML subtree (enumerations, footnotes,
            # sub-paragraphs) for structured rendering, alongside the text.
            "xml": ET.tostring(art_elem, encoding="unicode"),
        })

    return articles


def parse_xml(xml_path: Path, stats: Counter | None = None) -> list[dict]:
    """Parse an Akoma Ntoso XML file and extract all articles."""
    if stats is None:
        stats = Counter()
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as e:
        stats["parse_error"] += 1
        log.warning("XML parse error in %s: %s", xml_path, e)
        return []
    return parse_root(tree.getroot(), stats, source=str(xml_path))


def _db_stats(conn: sqlite3.Connection) -> dict:
    """Law / article counts plus the SRs that actually have articles."""
    return {
        "laws": conn.execute("SELECT COUNT(*) FROM laws").fetchone()[0],
        "articles": conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0],
        "srs_with_articles": {
            r[0] for r in conn.execute("SELECT DISTINCT sr_number FROM articles")
        },
    }


def check_floor(old_stats: dict | None, new_stats: dict, new_index_srs: set[str], *,
                allow_shrink: bool = False, max_lost: int = 5) -> dict:
    """Decide whether the freshly built DB may replace the previous one.

    Pure. Returns {"ok", "problems", "waived", "lost"}. Conditions:
      - laws and articles not below 90 % of the previous DB;
      - laws not above 110 % + 50 of the previous DB (a wrong --fedlex-dir);
      - at most `max_lost` SRs that had articles, are still in laws.json, and
        now have none. SRs that left laws.json are abrogations, not losses.
    `allow_shrink` waives the shrink and lost-SR conditions (they stay in
    "waived" for the log); the growth guard is not a shrink and stays.
    """
    if old_stats is None:
        return {"ok": True, "problems": [], "waived": [], "lost": []}
    shrink: list[str] = []
    problems: list[str] = []
    if new_stats["laws"] < 0.9 * old_stats["laws"]:
        shrink.append(f"laws {new_stats['laws']} < 90 % of previous {old_stats['laws']}")
    if new_stats["articles"] < 0.9 * old_stats["articles"]:
        shrink.append(f"articles {new_stats['articles']} < 90 % of previous {old_stats['articles']}")
    if new_stats["laws"] > 1.1 * old_stats["laws"] + 50:
        problems.append(f"laws {new_stats['laws']} > 110 % + 50 of previous {old_stats['laws']} "
                        "(wrong --fedlex-dir?)")
    lost = sorted(
        (old_stats["srs_with_articles"] & new_index_srs) - new_stats["srs_with_articles"]
    )
    if len(lost) > max_lost:
        shrink.append(f"{len(lost)} SRs still indexed but now without articles "
                      f"(limit {max_lost}): {', '.join(lost[:20])}"
                      + (" ..." if len(lost) > 20 else ""))
    waived = shrink if allow_shrink else []
    if not allow_shrink:
        problems = shrink + problems
    return {"ok": not problems, "problems": problems, "waived": waived, "lost": lost}


def build_db(*, allow_shrink: bool | None = None, baseline: Path | None = None):
    """Main build pipeline.

    Writes statutes.tmp next to the output, checks it against the previous
    DB (or `baseline`), then swaps it in with os.replace(), keeping the
    previous file as statutes.db.prev. On a floor-check failure the tmp file
    is left in place for inspection and the process exits 2, so the
    service's `&&` chain stops before the worker restart.
    """
    if allow_shrink is None:
        allow_shrink = os.environ.get("STATUTES_ALLOW_SHRINK") == "1"
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
    for leftover in (tmp_db, Path(str(tmp_db) + "-wal"), Path(str(tmp_db) + "-shm")):
        leftover.unlink(missing_ok=True)

    conn = sqlite3.connect(str(tmp_db))
    # MEMORY while building: nobody reads the tmp file, and no -wal/-shm
    # sidecars can be left behind. Switched to DELETE before the swap for
    # the immutable=1 readers.
    conn.execute("PRAGMA journal_mode = MEMORY")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -256000")  # 256MB
    create_schema(conn)

    total_laws = 0
    total_articles = 0
    stats: Counter = Counter()

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

            articles = parse_xml(xml_path, stats)
            for art in articles:
                conn.execute(
                    """INSERT INTO articles (sr_number, article_num, heading, footnote, text, xml,
                                             lang, section, section_heading, eid)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (sr_number, art["article_num"], art["heading"], art.get("footnote"),
                     art["text"], art.get("xml"), lang,
                     art.get("section", ""), art.get("section_heading"), art.get("eid")),
                )
                law_article_count += 1

        if law_article_count > 0:
            total_laws += 1
            total_articles += law_article_count

        if total_laws % 100 == 0 and total_laws > 0:
            conn.commit()
            log.info("Progress: %d laws, %d articles", total_laws, total_articles)

    conn.commit()
    log.info("Parse summary: %s",
             ", ".join(f"{k}={v}" for k, v in sorted(stats.items())) or "nothing parsed")

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
    new_stats = _db_stats(conn)
    log.info("Built statutes DB: %d laws, %d articles", new_stats["laws"], new_stats["articles"])

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

    # Floor check against the previous DB (or an explicit baseline when
    # validating on a copy) before anything touches the live file.
    baseline_path = Path(baseline).resolve() if baseline else resolved_db
    old_stats = None
    if baseline_path.exists():
        try:
            old_conn = sqlite3.connect(f"file:{baseline_path}?mode=ro", uri=True)
            try:
                old_stats = _db_stats(old_conn)
            finally:
                old_conn.close()
        except sqlite3.Error as e:
            log.warning("Cannot read baseline %s (%s), floor check skipped", baseline_path, e)
    else:
        log.info("No previous DB at %s, floor check skipped", baseline_path)
    verdict = check_floor(old_stats, new_stats, set(law_index), allow_shrink=allow_shrink)
    if old_stats is not None:
        log.info("Floor check vs %s: laws %d -> %d, articles %d -> %d, lost SRs: %s",
                 baseline_path, old_stats["laws"], new_stats["laws"],
                 old_stats["articles"], new_stats["articles"],
                 ", ".join(verdict["lost"]) or "none")
    for waived in verdict["waived"]:
        log.warning("Floor check waived (--allow-shrink): %s", waived)
    if not verdict["ok"]:
        for problem in verdict["problems"]:
            log.error("Floor check FAILED: %s", problem)
        log.error("Not replacing %s; inspect %s (rerun with --allow-shrink to override)",
                  resolved_db, tmp_db)
        sys.exit(2)

    # Retain the previous DB, then swap atomically
    if resolved_db.exists():
        prev_db = resolved_db.with_name(resolved_db.name + ".prev")
        prev_db.unlink(missing_ok=True)
        os.link(str(resolved_db), str(prev_db))
        log.info("Previous DB kept as %s", prev_db)
    os.replace(str(tmp_db), str(resolved_db))
    log.info("Saved to %s (%.1f MB)", resolved_db, resolved_db.stat().st_size / 1e6)


def main():
    global FEDLEX_DIR, OUTPUT_DB

    parser = argparse.ArgumentParser(description="Build statutes DB from Fedlex XML")
    parser.add_argument("--fedlex-dir", type=Path, default=FEDLEX_DIR)
    parser.add_argument("--output", type=Path, default=OUTPUT_DB)
    parser.add_argument(
        "--baseline", type=Path, default=None,
        help="Previous DB to floor-check against (default: --output itself); "
             "use when building to a scratch path for validation",
    )
    parser.add_argument(
        "--allow-shrink", action="store_true",
        default=os.environ.get("STATUTES_ALLOW_SHRINK") == "1",
        help="Replace the DB even if it has < 90 %% of the previous laws/articles "
             "or drops > 5 indexed SRs (also STATUTES_ALLOW_SHRINK=1)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    FEDLEX_DIR = args.fedlex_dir
    OUTPUT_DB = args.output

    t0 = time.time()
    build_db(allow_shrink=args.allow_shrink, baseline=args.baseline)
    log.info("Total time: %.1f seconds", time.time() - t0)


if __name__ == "__main__":
    main()
