#!/usr/bin/env python3
"""
Build OnlineKommentar commentaries SQLite database from scraped JSON.

Reads output/onlinekommentar/commentaries.json and builds a searchable
SQLite DB with FTS5 for commentary text.

Output: output/ok_commentaries.db

Usage:
    python -m search_stack.build_ok_commentaries_db
    python -m search_stack.build_ok_commentaries_db --input output/onlinekommentar/commentaries.json
"""

import argparse
import json
import logging
import os
import sqlite3
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_ok_commentaries")

INPUT_FILE = Path(os.environ.get("OK_INPUT", "output/onlinekommentar/commentaries.json"))
OUTPUT_DB = Path(os.environ.get("OK_DB", "output/ok_commentaries.db"))


def create_schema(conn: sqlite3.Connection):
    """Create database schema."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS legislative_acts (
            ok_uuid TEXT PRIMARY KEY,
            sr_number TEXT,
            abbr TEXT,
            title_en TEXT
        );

        CREATE TABLE IF NOT EXISTS commentaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ok_uuid TEXT NOT NULL,
            legislative_act_uuid TEXT NOT NULL,
            sr_number TEXT,
            abbr TEXT,
            article_num TEXT NOT NULL,
            title TEXT NOT NULL,
            language TEXT NOT NULL,
            date TEXT,
            authors TEXT,
            editors TEXT,
            suggested_citation TEXT,
            html_link TEXT,
            pdf_link TEXT,
            content_html TEXT,
            content_text TEXT NOT NULL,
            legal_text TEXT,
            FOREIGN KEY (legislative_act_uuid) REFERENCES legislative_acts(ok_uuid)
        );

        CREATE INDEX IF NOT EXISTS idx_commentaries_sr_art
            ON commentaries(sr_number, article_num);
        CREATE INDEX IF NOT EXISTS idx_commentaries_sr_art_lang
            ON commentaries(sr_number, article_num, language);
        CREATE INDEX IF NOT EXISTS idx_commentaries_abbr
            ON commentaries(abbr);

        CREATE VIRTUAL TABLE IF NOT EXISTS commentaries_fts USING fts5(
            sr_number, abbr, article_num, title, content_text, language,
            content='commentaries', content_rowid='id',
            tokenize='unicode61 remove_diacritics 2'
        );
    """)


def build_db():
    """Main build pipeline."""
    if not INPUT_FILE.exists():
        log.error("Input file not found: %s — run scrapers.onlinekommentar first", INPUT_FILE)
        return

    with open(INPUT_FILE, encoding="utf-8") as f:
        commentaries = json.load(f)

    log.info("Loaded %d commentaries from %s", len(commentaries), INPUT_FILE)

    # Prepare output
    tmp_db = OUTPUT_DB.with_suffix(".tmp")
    tmp_db.unlink(missing_ok=True)
    OUTPUT_DB.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(tmp_db))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -64000")  # 64MB
    create_schema(conn)

    # Collect unique legislative acts
    acts_seen: set[str] = set()
    for c in commentaries:
        act_uuid = c.get("legislative_act_uuid", "")
        if act_uuid and act_uuid not in acts_seen:
            acts_seen.add(act_uuid)
            conn.execute(
                "INSERT OR IGNORE INTO legislative_acts (ok_uuid, sr_number, abbr) VALUES (?, ?, ?)",
                (act_uuid, c.get("sr_number", ""), c.get("abbr", "")),
            )

    # Insert commentaries
    for c in commentaries:
        conn.execute(
            """INSERT INTO commentaries
               (ok_uuid, legislative_act_uuid, sr_number, abbr, article_num,
                title, language, date, authors, editors, suggested_citation,
                html_link, pdf_link, content_html, content_text, legal_text)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                c.get("ok_uuid", ""),
                c.get("legislative_act_uuid", ""),
                c.get("sr_number", ""),
                c.get("abbr", ""),
                c.get("article_num", ""),
                c.get("title", ""),
                c.get("language", "de"),
                c.get("date", ""),
                json.dumps(c.get("authors", []), ensure_ascii=False),
                json.dumps(c.get("editors", []), ensure_ascii=False),
                c.get("suggested_citation", ""),
                c.get("html_link", ""),
                c.get("pdf_link", ""),
                c.get("content_html", ""),
                c.get("content_text", ""),
                c.get("legal_text", ""),
            ),
        )

    conn.commit()

    # Populate FTS5 index
    log.info("Building FTS5 index...")
    conn.execute("""
        INSERT INTO commentaries_fts(rowid, sr_number, abbr, article_num, title, content_text, language)
        SELECT id, sr_number, abbr, article_num, title, content_text, language FROM commentaries
    """)
    conn.commit()

    # Optimize
    log.info("Optimizing FTS5...")
    conn.execute("INSERT INTO commentaries_fts(commentaries_fts) VALUES('optimize')")
    conn.commit()

    # Stats
    total = conn.execute("SELECT COUNT(*) FROM commentaries").fetchone()[0]
    acts = conn.execute("SELECT COUNT(*) FROM legislative_acts").fetchone()[0]
    log.info("Built OK commentaries DB: %d commentaries, %d legislative acts", total, acts)

    # Per-law counts
    top = conn.execute("""
        SELECT abbr, sr_number, COUNT(*) as cnt
        FROM commentaries
        WHERE abbr != ''
        GROUP BY abbr
        ORDER BY cnt DESC
    """).fetchall()
    log.info("Commentaries per law:")
    for abbr, sr, cnt in top:
        log.info("  %s (SR %s): %d", abbr, sr, cnt)

    # Language breakdown
    langs = conn.execute("""
        SELECT language, COUNT(*) FROM commentaries GROUP BY language ORDER BY COUNT(*) DESC
    """).fetchall()
    log.info("Languages: %s", ", ".join(f"{lang}={cnt}" for lang, cnt in langs))

    conn.close()

    # Atomic rename
    if OUTPUT_DB.exists():
        OUTPUT_DB.unlink()
    tmp_db.rename(OUTPUT_DB)
    log.info("Saved to %s (%.1f KB)", OUTPUT_DB, OUTPUT_DB.stat().st_size / 1024)


def main():
    global INPUT_FILE, OUTPUT_DB

    parser = argparse.ArgumentParser(description="Build OK commentaries DB from JSON")
    parser.add_argument("--input", type=Path, default=INPUT_FILE)
    parser.add_argument("--output", type=Path, default=OUTPUT_DB)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    INPUT_FILE = args.input
    OUTPUT_DB = args.output

    t0 = time.time()
    build_db()
    log.info("Total time: %.1f seconds", time.time() - t0)


if __name__ == "__main__":
    main()
