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
from datetime import datetime, timezone
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


def import_jsonl(
    conn: sqlite3.Connection, jsonl_dir: Path, checkpoint: dict | None = None,
) -> tuple[int, int, dict]:
    """Import decisions from JSONL files.

    Args:
        conn: SQLite connection.
        jsonl_dir: Directory containing .jsonl files.
        checkpoint: If provided, a dict mapping filename -> {"size": int, "imported": int}.
            Files whose size matches the checkpoint are skipped entirely; files that grew
            are read starting from the checkpoint byte offset (JSONL files are append-only).

    Returns:
        (imported, skipped, new_checkpoint) where new_checkpoint has the same structure.
    """
    imported = 0
    skipped = 0
    new_checkpoint: dict = {}

    for jsonl_file in sorted(jsonl_dir.glob("*.jsonl")):
        fname = jsonl_file.name
        current_size = jsonl_file.stat().st_size

        if checkpoint is not None:
            prev = checkpoint.get(fname, {})
            prev_size = prev.get("size", 0)

            if current_size == prev_size:
                # No new data — carry forward checkpoint entry unchanged
                new_checkpoint[fname] = prev
                continue

            if current_size < prev_size:
                # File shrank (unexpected) — read from start to be safe
                logger.warning(
                    f"  {fname}: size shrank ({prev_size} → {current_size}), reading from start"
                )
                prev_size = 0
        else:
            prev_size = 0

        file_imported = 0
        with open(jsonl_file, encoding="utf-8") as f:
            if prev_size > 0:
                f.seek(prev_size)
                logger.debug(f"  {fname}: seeking to byte {prev_size}")

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
        elif checkpoint is not None and prev_size > 0:
            logger.debug(f"  {fname}: no new decisions (new bytes were dupes)")

        prev_imported = (checkpoint or {}).get(fname, {}).get("imported", 0)
        new_checkpoint[fname] = {
            "size": current_size,
            "imported": prev_imported + file_imported,
        }

    return imported, skipped, new_checkpoint


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


def build_database(
    output_dir: Path,
    db_path: Path | None = None,
    incremental: bool = False,
    no_optimize: bool = False,
    full_rebuild: bool = False,
) -> Path:
    """
    Build/update the FTS5 database from all available sources.

    Args:
        output_dir: Directory containing decisions/ and data/ subdirs.
        db_path: Path for the SQLite DB (default: output_dir/decisions.db).
        incremental: Only read new bytes from JSONL files using checkpoint.
        no_optimize: Skip the FTS5 optimize step.
        full_rebuild: Delete existing DB and checkpoint, rebuild from scratch.

    Returns the path to the database.
    """
    db_path = db_path or output_dir / "decisions.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path = output_dir / ".fts5_checkpoint.json"

    # Full rebuild: delete DB and checkpoint
    if full_rebuild:
        if db_path.exists():
            logger.info(f"Full rebuild: deleting {db_path}")
            db_path.unlink()
        if checkpoint_path.exists():
            logger.info(f"Full rebuild: deleting {checkpoint_path}")
            checkpoint_path.unlink()

    # Load checkpoint for incremental mode
    checkpoint = None
    if incremental and checkpoint_path.exists():
        try:
            checkpoint = json.loads(checkpoint_path.read_text()).get("files", {})
            logger.info(f"Loaded checkpoint: {len(checkpoint)} files tracked")
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load checkpoint, reading all files: {e}")
            checkpoint = None

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA_SQL)

    # Count existing
    existing = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    # Seed checkpoint: if incremental mode, no checkpoint file, but DB already has data,
    # create a checkpoint from current JSONL file sizes to avoid re-reading everything.
    jsonl_dir = output_dir / "decisions"
    if incremental and checkpoint is None and existing > 0 and jsonl_dir.exists():
        logger.info(
            f"Seeding checkpoint from current file sizes (DB has {existing} rows)"
        )
        checkpoint = {}
        for jf in sorted(jsonl_dir.glob("*.jsonl")):
            checkpoint[jf.name] = {"size": jf.stat().st_size, "imported": 0}
        # Save immediately so it persists even if interrupted
        checkpoint_path.write_text(json.dumps({
            "files": checkpoint,
            "last_full_build": None,
            "last_incremental": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        logger.info(f"Seeded checkpoint: {len(checkpoint)} files tracked")

    # Import from JSONL (run_scraper.py output)
    jsonl_imported, jsonl_skipped, new_checkpoint = 0, 0, {}
    if jsonl_dir.exists():
        logger.info(f"Importing from JSONL: {jsonl_dir}")
        jsonl_imported, jsonl_skipped, new_checkpoint = import_jsonl(
            conn, jsonl_dir, checkpoint if incremental else None,
        )

    # Import from Parquet (pipeline.py output)
    parquet_dir = output_dir / "data" / "daily"
    pq_imported, pq_skipped = 0, 0
    if parquet_dir.exists():
        logger.info(f"Importing from Parquet: {parquet_dir}")
        pq_imported, pq_skipped = import_parquet(conn, parquet_dir)

    total_imported = jsonl_imported + pq_imported
    total_skipped = jsonl_skipped + pq_skipped

    if not no_optimize and total_imported > 0:
        logger.info("Running FTS5 optimize...")
        conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('optimize')")
        conn.commit()
    elif no_optimize and total_imported > 0:
        logger.info("Skipping FTS5 optimize (--no-optimize)")
        conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    # Court breakdown
    courts = conn.execute(
        "SELECT court, COUNT(*) as n FROM decisions GROUP BY court ORDER BY n DESC"
    ).fetchall()

    conn.close()

    # Save checkpoint
    if incremental or full_rebuild:
        now = datetime.now(timezone.utc).isoformat()
        # Load existing checkpoint metadata to preserve last_full_build
        prev_meta = {}
        if checkpoint_path.exists() and not full_rebuild:
            try:
                prev_meta = json.loads(checkpoint_path.read_text())
            except (json.JSONDecodeError, OSError):
                pass

        meta = {
            "files": new_checkpoint,
            "last_full_build": now if full_rebuild else prev_meta.get("last_full_build"),
            "last_incremental": now if incremental else prev_meta.get("last_incremental"),
        }
        checkpoint_path.write_text(json.dumps(meta, indent=2))
        logger.info(f"Saved checkpoint: {len(new_checkpoint)} files tracked")

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
    parser.add_argument(
        "--incremental", action="store_true",
        help="Only read new bytes from JSONL files (skip already-processed content)"
    )
    parser.add_argument(
        "--no-optimize", action="store_true",
        help="Skip FTS5 optimize step (useful with --incremental)"
    )
    parser.add_argument(
        "--full-rebuild", action="store_true",
        help="Delete existing DB and checkpoint, rebuild from scratch"
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
        build_database(
            output_dir, db_path,
            incremental=args.incremental,
            no_optimize=args.no_optimize,
            full_rebuild=args.full_rebuild,
        )


if __name__ == "__main__":
    main()
