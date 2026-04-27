#!/usr/bin/env python3
"""
Quick Publish — insert new JSONL decisions into FTS5 DB without full rebuild.

Designed to run after the BGer poller triggers a scrape, so new decisions
are searchable within minutes instead of waiting for the nightly full rebuild.

Architecture:
  1. Copy decisions.db to decisions.db.quick (if not exists or stale)
  2. Read JSONL files, find entries not yet in the DB
  3. INSERT OR IGNORE new rows (FTS5 triggers handle index sync)
  4. Atomic os.replace() swap
  5. MCP workers pick up new file on next connection

This is safe to run concurrently with MCP workers because:
  - Workers use immutable=1 on the current file
  - We write to a temp copy, then atomic swap
  - Workers reconnect and see the new file

Usage:
  python3 scripts/quick_publish.py                    # insert all new
  python3 scripts/quick_publish.py --courts bger      # only specific courts
  python3 scripts/quick_publish.py --dry-run          # count only
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[1]
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))

from build_fts5 import insert_decision

logger = logging.getLogger("quick_publish")

OUTPUT_DIR = REPO_DIR / "output"
DB_PATH = OUTPUT_DIR / "decisions.db"
JSONL_DIR = OUTPUT_DIR / "decisions"
TMP_PATH = Path(str(DB_PATH) + ".quick")


def _resolve_real_path(p: Path) -> Path:
    """Resolve symlinks to get the actual file path (for atomic replace on same fs)."""
    return p.resolve()


def _get_existing_ids(conn: sqlite3.Connection) -> set[str]:
    """Get all decision_ids currently in the DB."""
    return {row[0] for row in conn.execute("SELECT decision_id FROM decisions")}


def _read_new_jsonl(courts: list[str] | None, existing_ids: set[str]) -> list[dict]:
    """Read JSONL files and return rows not yet in the DB."""
    new_rows = []
    jsonl_files = sorted(JSONL_DIR.glob("*.jsonl"))

    for jf in jsonl_files:
        court = jf.stem
        if courts and court not in courts:
            continue
        try:
            with open(jf) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    row = json.loads(line)
                    did = row.get("decision_id", "")
                    if did and did not in existing_ids:
                        new_rows.append(row)
        except Exception as e:
            logger.warning("Failed to read %s: %s", jf, e)

    return new_rows


def quick_publish(courts: list[str] | None = None, dry_run: bool = False) -> int:
    """Insert new JSONL decisions into FTS5 DB. Returns count of inserted rows.

    The temp .quick copy is always cleaned up on exit (success, error, or
    early return). Without this, a crash mid-run leaves a 60+ GB stale
    file on disk — that filled the data volume to 100% on 2026-04-26 and
    blocked the publish pipeline.
    """
    real_db = _resolve_real_path(DB_PATH)
    real_tmp = real_db.parent / (real_db.name + ".quick")

    if not real_db.exists():
        logger.error("FTS5 DB not found at %s", real_db)
        return 0

    t0 = time.time()
    swap_done = False
    conn = None

    try:
        # Step 1: Copy current DB to work on
        logger.info("Copying %s to %s (%s)", real_db, real_tmp,
                    "{:.0f} MB".format(real_db.stat().st_size / 1e6))
        if not dry_run:
            shutil.copy2(str(real_db), str(real_tmp))

        # Step 2: Find new rows
        conn = sqlite3.connect(str(real_tmp) if not dry_run else f"file:{real_db}?mode=ro",
                               uri=dry_run)
        existing_ids = _get_existing_ids(conn)
        logger.info("Existing decisions: %d", len(existing_ids))

        new_rows = _read_new_jsonl(courts, existing_ids)
        logger.info("New decisions to insert: %d", len(new_rows))

        if not new_rows:
            logger.info("Nothing to insert")
            return 0

        if dry_run:
            for r in new_rows[:10]:
                logger.info("  Would insert: %s (%s)", r.get("decision_id"), r.get("court"))
            return len(new_rows)

        # Step 3: Ensure WAL mode is off (for immutable=1 compat after swap)
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA busy_timeout=5000")

        # Step 4: Insert new rows
        inserted = 0
        for row in new_rows:
            if insert_decision(conn, row):
                inserted += 1
        conn.commit()

        total_after = conn.execute("SELECT count(*) FROM decisions").fetchone()[0]
        conn.close()
        conn = None

        elapsed = time.time() - t0
        logger.info("Inserted %d/%d new decisions (%d total, %.1fs)",
                    inserted, len(new_rows), total_after, elapsed)

        # Step 5: Atomic swap (consumes real_tmp — no cleanup needed below)
        os.replace(str(real_tmp), str(real_db))
        swap_done = True
        logger.info("Atomic swap complete — new decisions are live")

        return inserted
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if not swap_done and real_tmp.exists():
            try:
                real_tmp.unlink()
                logger.info("Cleaned up temp file %s", real_tmp)
            except OSError as e:
                logger.warning("Failed to clean up %s: %s", real_tmp, e)


def main():
    parser = argparse.ArgumentParser(description="Quick publish — insert new decisions into FTS5")
    parser.add_argument("--courts", type=str, help="Comma-separated court codes (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Count only, don't modify DB")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    courts = args.courts.split(",") if args.courts else None
    inserted = quick_publish(courts=courts, dry_run=args.dry_run)
    if inserted:
        logger.info("Done — %d new decisions published", inserted)
    else:
        logger.info("Done — no new decisions")


if __name__ == "__main__":
    main()
