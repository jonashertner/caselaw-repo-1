"""Verbatim Botschaft corpus — Phase 2 of the Materialien commitment.

Per ``MEMORY.md → materialien_build_commitment``:

    verbatim text + FTS5 + ELI URI links (NOT Sonnet digests);
    ~3 wks, $0 LLM. Fedlex SPARQL serves Acts/Consultations as
    first-class data; only AmtlBull needs parlament.ch separately.

This module extends ``output/materialien.db`` with a verbatim, paragraph-
level corpus of Federal Council Botschaften. The downstream MCP tool
``get_article_purpose(sr_number, article)`` joins:

    article_botschaft_links.sr_number/article
       → botschaft_documents
       → botschaft_paragraphs (where article_anchor matches)

…and returns verbatim paragraphs the LLM can quote with a
verifiable citation.

Pipeline
--------
    amendment_refs (existing)        → unique (year, page) tuples
                                     ↓
    fetch_bbl_pdf(year, page)        → Fedlex PDF URL → raw bytes
                                     ↓
    parse_botschaft(pdf_bytes)       → paragraph dicts with article anchors
                                     ↓
    upsert_paragraphs                → botschaft_paragraphs + FTS5
                                     ↓
    upsert_links                     → article_botschaft_links

Usage::

    # Schema migration (idempotent, safe to re-run)
    python3 -m search_stack.build_botschaft_corpus --schema-only

    # Ingest a single Botschaft (PoC validation)
    python3 -m search_stack.build_botschaft_corpus --ingest-one \\
        --year 1999 --page 6013 --sr 935.61

    # Ingest all unique BBl refs from amendment_refs (run nightly)
    python3 -m search_stack.build_botschaft_corpus --ingest-all
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_botschaft_corpus")


# ── Schema ────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS botschaft_documents (
    botschaft_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    bbl_year        INTEGER NOT NULL,
    bbl_page        INTEGER NOT NULL,
    bbl_citation    TEXT NOT NULL,            -- "BBl 1999 6013"
    eli_uri         TEXT,                     -- https://fedlex.data.admin.ch/eli/fga/1999/6013
    title           TEXT,
    publication_date TEXT,
    source_url      TEXT NOT NULL,
    format          TEXT NOT NULL CHECK(format IN ('akoma-ntoso-xml','pdf')),
    language        TEXT NOT NULL,
    page_count      INTEGER,
    text_hash       TEXT,                     -- sha-256 of full text, for delta detection
    ingested_at     TEXT NOT NULL,
    UNIQUE(bbl_year, bbl_page, language)
);

CREATE INDEX IF NOT EXISTS ix_bd_eli ON botschaft_documents(eli_uri);

CREATE TABLE IF NOT EXISTS botschaft_paragraphs (
    paragraph_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    botschaft_id    INTEGER NOT NULL REFERENCES botschaft_documents(botschaft_id) ON DELETE CASCADE,
    para_order      INTEGER NOT NULL,
    page_number     INTEGER,
    section_path    TEXT,                     -- "Besonderer Teil > Zu Artikel 1"
    article_anchor  TEXT,                     -- "1" if paragraph belongs to a specific article
    text            TEXT NOT NULL,
    text_length     INTEGER NOT NULL,
    UNIQUE(botschaft_id, para_order)
);

CREATE INDEX IF NOT EXISTS ix_bp_anchor ON botschaft_paragraphs(article_anchor);
CREATE INDEX IF NOT EXISTS ix_bp_doc ON botschaft_paragraphs(botschaft_id);

CREATE VIRTUAL TABLE IF NOT EXISTS botschaft_paragraphs_fts USING fts5(
    text,
    section_path,
    article_anchor UNINDEXED,
    content='botschaft_paragraphs',
    content_rowid='paragraph_id',
    tokenize='unicode61 remove_diacritics 1'
);

-- Keep FTS5 in lockstep with the parent table.
CREATE TRIGGER IF NOT EXISTS botschaft_paragraphs_ai AFTER INSERT ON botschaft_paragraphs BEGIN
    INSERT INTO botschaft_paragraphs_fts(rowid, text, section_path, article_anchor)
    VALUES (new.paragraph_id, new.text, new.section_path, new.article_anchor);
END;
CREATE TRIGGER IF NOT EXISTS botschaft_paragraphs_ad AFTER DELETE ON botschaft_paragraphs BEGIN
    INSERT INTO botschaft_paragraphs_fts(botschaft_paragraphs_fts, rowid, text, section_path, article_anchor)
    VALUES ('delete', old.paragraph_id, old.text, old.section_path, old.article_anchor);
END;

CREATE TABLE IF NOT EXISTS article_botschaft_links (
    sr_number       TEXT NOT NULL,
    article         TEXT NOT NULL,
    botschaft_id    INTEGER NOT NULL REFERENCES botschaft_documents(botschaft_id) ON DELETE CASCADE,
    relation        TEXT NOT NULL CHECK(relation IN ('enacted','amended','considered')),
    evidence        TEXT,                     -- e.g. "amendment_refs row 12345"
    PRIMARY KEY (sr_number, article, botschaft_id, relation)
);

CREATE INDEX IF NOT EXISTS ix_abl_lookup ON article_botschaft_links(sr_number, article);
CREATE INDEX IF NOT EXISTS ix_abl_botschaft ON article_botschaft_links(botschaft_id);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


# ── Fedlex BBl URL helpers ────────────────────────────────────────────

# Canonical user-facing URL pattern for a Federal Gazette publication.
#   year ≥ 1998 → eli/fga path (electronic publication)
#   pre-1998   → eli/oc/cantons reference; few public scans, low priority
def bbl_pdf_url(year: int, page: int, language: str = "de") -> str:
    return (
        f"https://www.fedlex.admin.ch/eli/fga/{year}/{page}/"
        f"{language}/pdf-a"
    )


def bbl_eli_uri(year: int, page: int) -> str:
    return f"https://fedlex.data.admin.ch/eli/fga/{year}/{page}"


def bbl_citation(year: int, page: int) -> str:
    return f"BBl {year} {page}"


# ── Article-anchor parsing ────────────────────────────────────────────

# Header patterns Botschaften use for per-article comments. Captures
# the article number (with optional letter suffix like "41a"). Designed
# to match a line that starts with the marker after possible leading
# whitespace and a chapter indent — but NOT a mid-sentence reference
# like "wie in Artikel 41 vorgesehen".
ARTICLE_HEADER_RE = re.compile(
    r"""^\s{0,8}                       # optional indent
        (?:Zu\s+|Ad\s+|All['’]\s*)?    # German "Zu", French "Ad", Italian "All'"
        (?:Art(?:ikel|\.|icolo)?\.?)   # Art / Artikel / Articolo / Art.
        \s+
        (\d+[a-z]?)                    # captured article number, e.g. "41" or "41a"
        \b
        (?:.{0,80}?)?                  # optional title text on the same line
        $""",
    re.IGNORECASE | re.VERBOSE | re.MULTILINE,
)

# A "section header" = capitalised line of <= 80 chars surrounded by
# blank lines. Used to track section_path while walking the PDF.
SECTION_HEADER_RE = re.compile(
    r"^\s{0,8}([A-ZÄÖÜ][^a-z\n]{2,80}|\d+(?:\.\d+)*\s+[A-ZÄÖÜ].{0,80})\s*$",
    re.MULTILINE,
)


def parse_botschaft_text(
    pages: list[str],
    language: str = "de",
) -> Iterator[dict]:
    """Yield paragraph dicts (page_number, section_path, article_anchor, text).

    Input ``pages`` is a list of plain-text page strings (one per PDF page).
    Each non-empty paragraph (split on double newline) becomes one dict.
    The article anchor is set to the most recent ``Zu Art. N`` header
    above this paragraph, until the next such header is seen. The
    section path is a slash-joined trail of the recent section headers.
    """
    section_path_stack: list[str] = []
    current_anchor: str | None = None
    para_order = 0

    for page_idx, page_text in enumerate(pages, start=1):
        # Split on 2+ newlines to approximate paragraph boundaries.
        for chunk in re.split(r"\n\s*\n", page_text):
            chunk = chunk.strip()
            if len(chunk) < 20:
                continue

            # Update section trail if this chunk is itself a header.
            sh = SECTION_HEADER_RE.match(chunk)
            if sh and len(chunk.split("\n")) == 1:
                # Treat as section header — push and don't emit as paragraph.
                section_path_stack = [chunk]
                continue

            # Detect article anchor on the FIRST line of the chunk.
            first_line = chunk.split("\n", 1)[0]
            ah = ARTICLE_HEADER_RE.match(first_line)
            if ah:
                current_anchor = ah.group(1)

            para_order += 1
            yield {
                "para_order": para_order,
                "page_number": page_idx,
                "section_path": " > ".join(section_path_stack) or None,
                "article_anchor": current_anchor,
                "text": chunk,
                "text_length": len(chunk),
            }


# ── Ingest one Botschaft ──────────────────────────────────────────────


def fetch_pdf_bytes(url: str, timeout: int = 60) -> bytes:
    import urllib.request
    log.info(f"  fetching {url}")
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "opencaselaw-materialien/0.1"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def extract_pdf_pages(pdf_bytes: bytes) -> list[str]:
    """Extract text per page using pdfplumber. Falls back to empty
    string for pages that fail. Returns one entry per page."""
    import io
    try:
        import pdfplumber
    except ImportError as e:
        raise SystemExit(
            "pdfplumber not installed. Run: pip install pdfplumber"
        ) from e

    pages_out: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            try:
                pages_out.append(page.extract_text() or "")
            except Exception as e:
                log.warning(f"    page extract failed: {e}")
                pages_out.append("")
    return pages_out


def ingest_one(
    conn: sqlite3.Connection,
    year: int,
    page: int,
    language: str = "de",
    sr_number: str | None = None,
    article: str | None = None,
    relation: str = "considered",
) -> int:
    """Fetch, parse, and store one Botschaft. Returns botschaft_id.

    Idempotent: if (year, page, language) already exists, the existing
    document is returned and paragraphs are re-inserted only if the
    text_hash differs.
    """
    ensure_schema(conn)

    citation = bbl_citation(year, page)
    eli = bbl_eli_uri(year, page)
    url = bbl_pdf_url(year, page, language)
    log.info(f"Ingesting {citation} ({language}) for {sr_number}/{article}")

    # Check existing
    row = conn.execute(
        "SELECT botschaft_id, text_hash FROM botschaft_documents "
        "WHERE bbl_year=? AND bbl_page=? AND language=?",
        (year, page, language),
    ).fetchone()

    pdf_bytes = fetch_pdf_bytes(url)
    text_hash = hashlib.sha256(pdf_bytes).hexdigest()

    if row and row[1] == text_hash:
        log.info(f"  unchanged ({citation}) — keeping existing rows")
        botschaft_id = row[0]
    else:
        pages = extract_pdf_pages(pdf_bytes)
        log.info(f"  extracted {len(pages)} pages")

        if row:
            # Replace: delete existing paragraphs (cascade through trigger)
            conn.execute(
                "DELETE FROM botschaft_paragraphs WHERE botschaft_id = ?",
                (row[0],),
            )
            conn.execute(
                "UPDATE botschaft_documents SET text_hash=?, page_count=?, ingested_at=? "
                "WHERE botschaft_id=?",
                (text_hash, len(pages),
                 datetime.now(timezone.utc).isoformat(), row[0]),
            )
            botschaft_id = row[0]
        else:
            cur = conn.execute(
                """
                INSERT INTO botschaft_documents
                (bbl_year, bbl_page, bbl_citation, eli_uri, source_url,
                 format, language, page_count, text_hash, ingested_at)
                VALUES (?, ?, ?, ?, ?, 'pdf', ?, ?, ?, ?)
                """,
                (year, page, citation, eli, url, language,
                 len(pages), text_hash,
                 datetime.now(timezone.utc).isoformat()),
            )
            botschaft_id = cur.lastrowid

        # Insert paragraphs
        n_paras = 0
        for para in parse_botschaft_text(pages, language=language):
            conn.execute(
                """
                INSERT INTO botschaft_paragraphs
                (botschaft_id, para_order, page_number, section_path,
                 article_anchor, text, text_length)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (botschaft_id, para["para_order"], para["page_number"],
                 para["section_path"], para["article_anchor"],
                 para["text"], para["text_length"]),
            )
            n_paras += 1
        log.info(f"  inserted {n_paras} paragraphs")

    if sr_number and article:
        conn.execute(
            """
            INSERT OR IGNORE INTO article_botschaft_links
            (sr_number, article, botschaft_id, relation, evidence)
            VALUES (?, ?, ?, ?, ?)
            """,
            (sr_number, article, botschaft_id, relation,
             "build_botschaft_corpus.ingest_one"),
        )
    conn.commit()
    return botschaft_id


# ── CLI ────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--db",
        default=str(Path("output/materialien.db")),
        help="materialien.db path",
    )
    parser.add_argument(
        "--schema-only", action="store_true",
        help="just create the new tables and exit",
    )
    parser.add_argument(
        "--ingest-one", action="store_true",
        help="ingest a single Botschaft (PoC); requires --year + --page",
    )
    parser.add_argument("--year", type=int)
    parser.add_argument("--page", type=int)
    parser.add_argument("--sr", help="SR number to link the article to")
    parser.add_argument("--article", help="article number (links to the Botschaft)")
    parser.add_argument(
        "--language", default="de", choices=["de", "fr", "it"],
    )
    parser.add_argument(
        "--ingest-all", action="store_true",
        help="discover BBl refs from amendment_refs and ingest all",
    )
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        log.error(f"DB not found: {db_path}")
        return 1

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    if args.schema_only:
        ensure_schema(conn)
        log.info("schema migrated")
        return 0

    if args.ingest_one:
        if not args.year or not args.page:
            log.error("--ingest-one requires --year and --page")
            return 2
        bid = ingest_one(
            conn,
            year=args.year, page=args.page,
            language=args.language,
            sr_number=args.sr,
            article=args.article,
            relation="enacted" if args.article else "considered",
        )
        log.info(f"botschaft_id = {bid}")
        return 0

    if args.ingest_all:
        # v0.1: pull unique (year, page) tuples whose ref_type='BBl'
        rows = conn.execute(
            """
            SELECT DISTINCT year, page
            FROM amendment_refs
            WHERE ref_type='BBl'
              AND year IS NOT NULL AND page IS NOT NULL
              AND year >= 1998
            ORDER BY year DESC, page DESC
            """
        ).fetchall()
        log.info(f"discovered {len(rows)} unique BBl refs since 1998")
        # v0.1: dry-run by default — log sample and exit until we batch-fetch
        for year, page in rows[:20]:
            log.info(f"  candidate: BBl {year} {page} → {bbl_pdf_url(year, page)}")
        log.info(
            "v0.1: --ingest-all is in dry-run mode; "
            "use --ingest-one for a real fetch+parse cycle"
        )
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
