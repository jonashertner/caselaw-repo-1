#!/usr/bin/env python3
"""
Build cantonal laws SQLite database from LexFind cantonal JSONL dumps.

Reads output/lexfind_cantonal/{canton}.jsonl (one law per line), writes
output/cantonal_laws.db with FTS5 over titles + article text.

Schema:
    laws             — one row per (lexfind_id, language) pair
    articles         — one row per article
    articles_fts     — FTS5 virtual table over title + heading + text

Usage:
    python -m search_stack.build_cantonal_laws_db
    python -m search_stack.build_cantonal_laws_db --input output/lexfind_cantonal
"""

from __future__ import annotations

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
log = logging.getLogger("build_cantonal_laws")

INPUT_DIR = Path(os.environ.get("LEXFIND_CANTONAL_OUTPUT", "output/lexfind_cantonal"))
OUTPUT_DB = Path(os.environ.get("CANTONAL_LAWS_DB", "output/cantonal_laws.db"))


SCHEMA = """
CREATE TABLE IF NOT EXISTS laws (
    lexfind_id             INTEGER NOT NULL,
    language               TEXT NOT NULL,
    canton                 TEXT NOT NULL,
    sr_number              TEXT,
    title                  TEXT NOT NULL,
    category               TEXT,
    is_active              INTEGER DEFAULT 1,
    original_url           TEXT,
    version_active_since   TEXT,
    text_length            INTEGER,
    article_count          INTEGER,
    text_source            TEXT,
    full_text              TEXT,
    fetched_at             TEXT,
    PRIMARY KEY (lexfind_id, language)
);

CREATE INDEX IF NOT EXISTS idx_laws_canton       ON laws(canton);
CREATE INDEX IF NOT EXISTS idx_laws_canton_lang  ON laws(canton, language);
CREATE INDEX IF NOT EXISTS idx_laws_sr           ON laws(sr_number);

CREATE TABLE IF NOT EXISTS articles (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    lexfind_id     INTEGER NOT NULL,
    language       TEXT NOT NULL,
    canton         TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    article_num    TEXT,
    heading        TEXT,
    text           TEXT NOT NULL,
    FOREIGN KEY (lexfind_id, language) REFERENCES laws(lexfind_id, language)
);

CREATE INDEX IF NOT EXISTS idx_articles_law
    ON articles(lexfind_id, language);
CREATE INDEX IF NOT EXISTS idx_articles_canton
    ON articles(canton);

CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
    title,
    heading,
    article_num,
    text,
    lexfind_id UNINDEXED,
    canton UNINDEXED,
    language UNINDEXED,
    tokenize='unicode61 remove_diacritics 2'
);
"""


def build(input_dir: Path, output_db: Path) -> None:
    if not input_dir.exists():
        log.error("Input directory not found: %s", input_dir)
        raise SystemExit(1)

    # Resolve symlinks so the tmp file lives on the same filesystem for os.replace
    output_db = Path(os.path.realpath(output_db))
    output_db.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_db.with_suffix(output_db.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    log.info("Building %s from %s", output_db, input_dir)
    conn = sqlite3.connect(tmp_path)
    conn.executescript(SCHEMA)
    conn.execute("PRAGMA journal_mode = MEMORY")
    conn.execute("PRAGMA synchronous = OFF")

    jsonl_files = sorted(input_dir.glob("*.jsonl"))
    log.info("Found %d canton JSONL files", len(jsonl_files))

    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    total_laws = 0
    total_articles = 0
    for path in jsonl_files:
        canton = path.stem.upper()
        c_laws = 0
        c_arts = 0
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as e:
                    log.warning("[%s] bad JSON: %s", canton, e)
                    continue

                full_text = row.get("full_text") or ""
                articles = row.get("articles") or []
                lexfind_id = row.get("lexfind_id")
                if lexfind_id is None:
                    continue
                language = row.get("language") or "de"

                conn.execute(
                    """INSERT OR REPLACE INTO laws
                    (lexfind_id, language, canton, sr_number, title, category,
                     is_active, original_url, version_active_since, text_length,
                     article_count, text_source, full_text, fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        lexfind_id,
                        language,
                        row.get("canton") or canton,
                        row.get("sr_number"),
                        row.get("title") or "",
                        row.get("category"),
                        1 if row.get("is_active", True) else 0,
                        row.get("original_url"),
                        row.get("version_active_since"),
                        len(full_text),
                        len(articles),
                        row.get("text_source"),
                        full_text,
                        now_iso,
                    ),
                )

                title = row.get("title") or ""
                for seq, art in enumerate(articles):
                    art_num = art.get("article_num")
                    heading = art.get("heading")
                    text = art.get("text") or ""
                    if not text and not heading:
                        continue
                    conn.execute(
                        """INSERT INTO articles
                        (lexfind_id, language, canton, seq, article_num, heading, text)
                        VALUES (?,?,?,?,?,?,?)""",
                        (lexfind_id, language, row.get("canton") or canton,
                         seq, art_num, heading, text),
                    )
                    conn.execute(
                        """INSERT INTO articles_fts
                        (title, heading, article_num, text, lexfind_id, canton, language)
                        VALUES (?,?,?,?,?,?,?)""",
                        (title, heading or "", art_num or "", text,
                         lexfind_id, row.get("canton") or canton, language),
                    )
                    c_arts += 1
                c_laws += 1

        log.info("[%s] %d laws, %d articles", canton, c_laws, c_arts)
        total_laws += c_laws
        total_articles += c_arts

    log.info("Optimising FTS5 index...")
    conn.execute("INSERT INTO articles_fts(articles_fts) VALUES ('optimize')")
    conn.commit()
    conn.close()

    log.info("Atomic swap: %s → %s", tmp_path, output_db)
    os.replace(tmp_path, output_db)

    log.info(
        "DONE — %d laws, %d articles indexed. DB size: %.1f MB",
        total_laws, total_articles, output_db.stat().st_size / (1024 * 1024),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_DIR))
    parser.add_argument("--output", default=str(OUTPUT_DB))
    args = parser.parse_args()
    build(Path(args.input), Path(args.output))


if __name__ == "__main__":
    main()
