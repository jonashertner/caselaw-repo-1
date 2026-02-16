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
import time
from pathlib import Path

from db_schema import INSERT_COLUMNS, INSERT_OR_IGNORE_SQL, SCHEMA_SQL

logger = logging.getLogger("build_fts5")


def insert_decision(conn: sqlite3.Connection, row: dict) -> bool:
    """Insert a single decision. Returns True if inserted, False if skipped (duplicate)."""
    try:
        # Handle cited_decisions — could be list or JSON string
        cited = row.get("cited_decisions", [])
        if isinstance(cited, list):
            cited = json.dumps(cited)
        row["cited_decisions"] = cited

        # json_data: full row as JSON blob
        row["json_data"] = json.dumps(row, default=str)

        # Build values tuple matching INSERT_COLUMNS order.
        # Convert None-like values properly (avoid storing literal "None" strings).
        def _val(col: str):
            v = row.get(col)
            if v is None or v == "None":
                return None
            if col in ("decision_date", "publication_date", "scraped_at") and v:
                return str(v) if v else None
            return v

        values = tuple(_val(col) for col in INSERT_COLUMNS)

        cursor = conn.execute(INSERT_OR_IGNORE_SQL, values)
        return cursor.rowcount > 0
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
