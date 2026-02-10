#!/usr/bin/env python3
"""
Build/update SQLite FTS5 search database from scraped decisions.

Reads from:
  - output/decisions/*.jsonl  (from run_scraper.py — full Decision objects)
  - output/data/daily/*.parquet (from pipeline.py — Parquet shards)

Produces:
  - output/decisions.db (SQLite with FTS5 full-text search)

The DB schema matches what mcp_server.py expects.

Usage:
    python3 build_fts5.py                          # default: ./output
    python3 build_fts5.py --output /opt/caselaw/repo/output
    python3 build_fts5.py --output ./output --db ~/.swiss-caselaw/decisions.db
    python3 build_fts5.py --watch 60               # rebuild every 60 seconds
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

logger = logging.getLogger("build_fts5")


SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS decisions (
        decision_id TEXT PRIMARY KEY,
        court TEXT NOT NULL,
        canton TEXT NOT NULL,
        chamber TEXT,
        docket_number TEXT NOT NULL,
        decision_date TEXT NOT NULL,
        publication_date TEXT,
        language TEXT NOT NULL,
        title TEXT,
        legal_area TEXT,
        regeste TEXT,
        full_text TEXT,
        decision_type TEXT,
        outcome TEXT,
        source_url TEXT,
        pdf_url TEXT,
        cited_decisions TEXT,
        scraped_at TEXT,
        json_data TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_decisions_court ON decisions(court);
    CREATE INDEX IF NOT EXISTS idx_decisions_canton ON decisions(canton);
    CREATE INDEX IF NOT EXISTS idx_decisions_date ON decisions(decision_date);
    CREATE INDEX IF NOT EXISTS idx_decisions_language ON decisions(language);
    CREATE INDEX IF NOT EXISTS idx_decisions_docket ON decisions(docket_number);

    CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
        decision_id UNINDEXED,
        court,
        canton,
        docket_number,
        language,
        title,
        regeste,
        full_text,
        content=decisions,
        content_rowid=rowid,
        tokenize='unicode61 remove_diacritics 2'
    );

    -- Triggers to keep FTS in sync
    CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
        INSERT INTO decisions_fts(rowid, decision_id, court, canton,
            docket_number, language, title, regeste, full_text)
        VALUES (new.rowid, new.decision_id, new.court, new.canton,
            new.docket_number, new.language, new.title, new.regeste,
            new.full_text);
    END;

    CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
        INSERT INTO decisions_fts(decisions_fts, rowid, decision_id, court,
            canton, docket_number, language, title, regeste, full_text)
        VALUES ('delete', old.rowid, old.decision_id, old.court, old.canton,
            old.docket_number, old.language, old.title, old.regeste,
            old.full_text);
    END;

    CREATE TRIGGER IF NOT EXISTS decisions_au AFTER UPDATE ON decisions BEGIN
        INSERT INTO decisions_fts(decisions_fts, rowid, decision_id, court,
            canton, docket_number, language, title, regeste, full_text)
        VALUES ('delete', old.rowid, old.decision_id, old.court, old.canton,
            old.docket_number, old.language, old.title, old.regeste,
            old.full_text);
        INSERT INTO decisions_fts(rowid, decision_id, court, canton,
            docket_number, language, title, regeste, full_text)
        VALUES (new.rowid, new.decision_id, new.court, new.canton,
            new.docket_number, new.language, new.title, new.regeste,
            new.full_text);
    END;
"""


def insert_decision(conn: sqlite3.Connection, row: dict) -> bool:
    """Insert a single decision. Returns True if inserted, False if skipped."""
    try:
        exists = conn.execute(
            "SELECT 1 FROM decisions WHERE decision_id = ?",
            (row["decision_id"],),
        ).fetchone()
        if exists:
            return False

        # Handle cited_decisions — could be list or JSON string
        cited = row.get("cited_decisions", [])
        if isinstance(cited, list):
            cited = json.dumps(cited)

        conn.execute(
            """INSERT INTO decisions
            (decision_id, court, canton, chamber, docket_number,
             decision_date, publication_date, language, title,
             legal_area, regeste, full_text, decision_type,
             outcome, source_url, pdf_url, cited_decisions,
             scraped_at, json_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                row["decision_id"],
                row["court"],
                row["canton"],
                row.get("chamber"),
                row["docket_number"],
                str(row["decision_date"]),
                str(row["publication_date"]) if row.get("publication_date") else None,
                row["language"],
                row.get("title"),
                row.get("legal_area"),
                row.get("regeste"),
                row.get("full_text"),
                row.get("decision_type"),
                row.get("outcome"),
                row.get("source_url", ""),
                row.get("pdf_url"),
                cited,
                str(row.get("scraped_at", "")),
                json.dumps(row, default=str),
            ),
        )
        return True
    except Exception as e:
        logger.warning(f"Failed to import {row.get('decision_id', '?')}: {e}")
        return False


def import_jsonl(conn: sqlite3.Connection, jsonl_dir: Path) -> tuple[int, int]:
    """Import decisions from JSONL files. Returns (imported, skipped)."""
    imported = 0
    skipped = 0

    for jsonl_file in sorted(jsonl_dir.glob("*.jsonl")):
        file_imported = 0
        with open(jsonl_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    if insert_decision(conn, row):
                        imported += 1
                        file_imported += 1
                    else:
                        skipped += 1
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON in {jsonl_file}: {e}")
                except Exception as e:
                    logger.warning(f"Error importing from {jsonl_file}: {e}")

        if file_imported:
            conn.commit()
            logger.info(f"  {jsonl_file.name}: +{file_imported} decisions")

    return imported, skipped


def import_parquet(conn: sqlite3.Connection, parquet_dir: Path) -> tuple[int, int]:
    """Import decisions from Parquet shards. Returns (imported, skipped)."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        logger.info("pyarrow not installed, skipping Parquet import")
        return 0, 0

    if not parquet_dir.exists():
        return 0, 0

    imported = 0
    skipped = 0

    for pf in sorted(parquet_dir.glob("*.parquet")):
        try:
            table = pq.read_table(pf)
            file_imported = 0
            for batch in table.to_batches():
                for row in batch.to_pylist():
                    if insert_decision(conn, row):
                        imported += 1
                        file_imported += 1
                    else:
                        skipped += 1
            conn.commit()
            if file_imported:
                logger.info(f"  {pf.name}: +{file_imported} decisions")
        except Exception as e:
            logger.warning(f"Failed to read {pf}: {e}")

    return imported, skipped


def build_database(output_dir: Path, db_path: Path | None = None) -> Path:
    """
    Build/update the FTS5 database from all available sources.

    Returns the path to the database.
    """
    db_path = db_path or output_dir / "decisions.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA_SQL)

    # Count existing
    existing = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    # Import from JSONL (run_scraper.py output)
    jsonl_dir = output_dir / "decisions"
    jsonl_imported, jsonl_skipped = 0, 0
    if jsonl_dir.exists():
        logger.info(f"Importing from JSONL: {jsonl_dir}")
        jsonl_imported, jsonl_skipped = import_jsonl(conn, jsonl_dir)

    # Import from Parquet (pipeline.py output)
    parquet_dir = output_dir / "data" / "daily"
    pq_imported, pq_skipped = 0, 0
    if parquet_dir.exists():
        logger.info(f"Importing from Parquet: {parquet_dir}")
        pq_imported, pq_skipped = import_parquet(conn, parquet_dir)

    total_imported = jsonl_imported + pq_imported
    total_skipped = jsonl_skipped + pq_skipped

    if total_imported > 0:
        # Optimize FTS index
        conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('optimize')")
        conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    # Court breakdown
    courts = conn.execute(
        "SELECT court, COUNT(*) as n FROM decisions GROUP BY court ORDER BY n DESC"
    ).fetchall()

    conn.close()

    logger.info(f"Database: {db_path} ({db_path.stat().st_size / 1024 / 1024:.1f} MB)")
    logger.info(f"  Existing: {existing}, New: {total_imported}, Skipped: {total_skipped}")
    logger.info(f"  Total decisions: {total}")
    for court, n in courts:
        logger.info(f"    {court}: {n}")

    return db_path


def main():
    parser = argparse.ArgumentParser(description="Build FTS5 search database")
    parser.add_argument(
        "--output", type=str, default="output",
        help="Output directory containing decisions/ and data/ subdirs"
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Database path (default: {output}/decisions.db)"
    )
    parser.add_argument(
        "--watch", type=int, default=None,
        help="Rebuild every N seconds (for use alongside running scrapers)"
    )
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    output_dir = Path(args.output)
    db_path = Path(args.db) if args.db else None

    if args.watch:
        logger.info(f"Watch mode: rebuilding every {args.watch}s")
        while True:
            try:
                build_database(output_dir, db_path)
            except Exception as e:
                logger.error(f"Build failed: {e}", exc_info=True)
            time.sleep(args.watch)
    else:
        build_database(output_dir, db_path)


if __name__ == "__main__":
    main()