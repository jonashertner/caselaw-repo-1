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
import re
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from db_schema import INSERT_COLUMNS, INSERT_OR_IGNORE_SQL, SCHEMA_SQL
from models import make_canonical_key

logger = logging.getLogger("build_fts5")

# ── Text cleaning ────────────────────────────────────────────

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACE_RE = re.compile(r"[ \t]+")
_HTML_ENTITIES = {
    "&nbsp;": " ",
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&#39;": "'",
    "&apos;": "'",
}


def _fix_mojibake(text: str) -> str:
    """Fix double-encoded UTF-8 (UTF-8 bytes decoded as Latin-1).

    Common pattern: 'ä' (U+00E4) stored as 'Ã¤' (C3 A4 decoded as Latin-1).
    """
    try:
        # If the text contains typical mojibake sequences, try to fix
        fixed = text.encode("latin-1").decode("utf-8")
        # Sanity check: the fixed version should be shorter or equal
        if len(fixed) <= len(text):
            return fixed
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    return text


def _clean_text(text: str | None) -> str | None:
    """Strip HTML tags, fix HTML entities, fix mojibake, normalize whitespace."""
    if not text:
        return text

    # Strip HTML tags
    text = _HTML_TAG_RE.sub(" ", text)

    # Replace HTML entities
    for entity, replacement in _HTML_ENTITIES.items():
        if entity in text:
            text = text.replace(entity, replacement)

    # Fix mojibake (only if likely — check for common mojibake markers)
    if "\xc3" in text:
        text = _fix_mojibake(text)

    # Normalize whitespace (collapse runs of spaces/tabs, preserve newlines)
    text = _MULTI_SPACE_RE.sub(" ", text)

    return text.strip()


# ── BGer regeste extraction ──────────────────────────────────

_REGESTE_START_RE = re.compile(
    r"(?:^|\n)\s*Regeste\b[:\s]*\n",
    re.IGNORECASE,
)
_REGESTE_END_MARKERS = [
    re.compile(r"^\s*Sachverhalt\b", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*Faits\b", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*Fatti\b", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*Urteilskopf\b", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*A\.\s", re.MULTILINE),
]


def _extract_regeste_from_text(full_text: str) -> str | None:
    """Try to extract regeste from BGE/BGer full_text.

    BGE decisions typically contain:
      Regeste
      <regeste text in DE>
      Regeste      (or Regesto)
      <regeste in FR/IT>
      Sachverhalt / Faits / Fatti / A.
    """
    m = _REGESTE_START_RE.search(full_text)
    if not m:
        return None

    start = m.end()
    # Find the end: next major section header
    end = len(full_text)
    for pat in _REGESTE_END_MARKERS:
        em = pat.search(full_text, start)
        if em and em.start() < end:
            end = em.start()

    regeste = full_text[start:end].strip()
    # Skip if too short or too long (probably a false match)
    if len(regeste) < 20 or len(regeste) > 5000:
        return None
    return regeste


# ── Dedup + post-processing ──────────────────────────────────

def _dedup_decisions(conn: sqlite3.Connection) -> int:
    """Remove duplicate decisions sharing the same canonical_key.

    The canonical_key aggressively normalizes court + docket + date so that
    formatting variants (dots vs underscores, case, etc.) collapse together.
    Falls back to exact (court, docket_number, decision_date) if canonical_key
    is not yet populated.

    Keeps the version with the longest full_text (preferring non-empty regeste).
    Returns number of rows deleted.
    """
    # Check if canonical_key column exists and is populated
    has_canonical = False
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE canonical_key IS NOT NULL AND canonical_key != ''"
        ).fetchone()
        has_canonical = row[0] > 0
    except sqlite3.OperationalError:
        pass

    if has_canonical:
        # Exclude keys with empty docket part (format: court|DOCKET|date)
        dup_sql = """
            SELECT canonical_key, COUNT(*) as cnt
            FROM decisions
            WHERE canonical_key IS NOT NULL AND canonical_key != ''
              AND canonical_key NOT LIKE '%||%'
            GROUP BY canonical_key
            HAVING cnt > 1
        """
        groups = conn.execute(dup_sql).fetchall()
        if not groups:
            return 0

        deleted = 0
        for (canonical_key, cnt) in groups:
            rows = conn.execute(
                """
                SELECT decision_id, LENGTH(COALESCE(full_text, '')), LENGTH(COALESCE(regeste, ''))
                FROM decisions
                WHERE canonical_key = ?
                ORDER BY
                    CASE WHEN LENGTH(COALESCE(regeste, '')) > 0 THEN 0 ELSE 1 END,
                    LENGTH(COALESCE(full_text, '')) DESC
                """,
                (canonical_key,),
            ).fetchall()
            for row in rows[1:]:
                conn.execute("DELETE FROM decisions WHERE decision_id = ?", (row[0],))
                deleted += 1
    else:
        # Fallback: exact match on (court, docket_number, decision_date)
        dup_sql = """
            SELECT court, docket_number, decision_date, COUNT(*) as cnt
            FROM decisions
            WHERE docket_number IS NOT NULL AND LENGTH(TRIM(docket_number)) > 0
            GROUP BY court, docket_number, decision_date
            HAVING cnt > 1
        """
        groups = conn.execute(dup_sql).fetchall()
        if not groups:
            return 0

        deleted = 0
        for court, docket, date, cnt in groups:
            rows = conn.execute(
                """
                SELECT decision_id, LENGTH(COALESCE(full_text, '')), LENGTH(COALESCE(regeste, ''))
                FROM decisions
                WHERE court = ? AND docket_number = ? AND decision_date IS ?
                ORDER BY
                    CASE WHEN LENGTH(COALESCE(regeste, '')) > 0 THEN 0 ELSE 1 END,
                    LENGTH(COALESCE(full_text, '')) DESC
                """,
                (court, docket, date),
            ).fetchall()
            for row in rows[1:]:
                conn.execute("DELETE FROM decisions WHERE decision_id = ?", (row[0],))
                deleted += 1

    # ── Pass 2: date-agnostic dedup ──
    # Same court+docket but different dates (common with entscheidsuche vs
    # direct scrape where publication vs decision date differs).
    # Group by the court|docket portion of canonical_key, ignoring the date.
    all_rows = conn.execute(
        "SELECT decision_id, canonical_key, LENGTH(COALESCE(full_text, '')), "
        "LENGTH(COALESCE(regeste, '')) FROM decisions "
        "WHERE canonical_key IS NOT NULL AND canonical_key <> ''"
    ).fetchall()
    groups2 = defaultdict(list)
    for did, ckey, tlen, rlen in all_rows:
        parts = ckey.split("|")
        if len(parts) == 3 and parts[1]:
            groups2[f"{parts[0]}|{parts[1]}"].append((did, tlen, rlen))

    deleted2 = 0
    for entries in groups2.values():
        if len(entries) < 2:
            continue
        # Keep version with non-empty regeste first, then longest full_text
        entries.sort(key=lambda x: (0 if x[2] > 0 else 1, -x[1]))
        for did, _, _ in entries[1:]:
            conn.execute("DELETE FROM decisions WHERE decision_id = ?", (did,))
            deleted2 += 1
    if deleted2:
        logger.info(f"  Pass 2 (date-agnostic): removed {deleted2} duplicates")
    deleted += deleted2

    conn.commit()
    return deleted


def _fill_missing_regeste(conn: sqlite3.Connection) -> int:
    """Extract regeste from full_text for BGer/BGE decisions with empty regeste."""
    cursor = conn.execute(
        """
        SELECT decision_id, full_text FROM decisions
        WHERE court IN ('bger', 'bge')
          AND (regeste IS NULL OR LENGTH(TRIM(regeste)) = 0)
          AND LENGTH(COALESCE(full_text, '')) > 200
        """
    )
    updated = 0
    batch: list[tuple[str, str]] = []
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        for decision_id, full_text in rows:
            regeste = _extract_regeste_from_text(full_text or "")
            if regeste:
                batch.append((regeste, decision_id))
        if batch:
            conn.executemany(
                "UPDATE decisions SET regeste = ? WHERE decision_id = ?",
                batch,
            )
            updated += len(batch)
            batch.clear()

    if updated:
        conn.commit()
    return updated


def _log_quality_summary(conn: sqlite3.Connection) -> None:
    """Log a summary of remaining data quality issues."""
    total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    short = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE LENGTH(COALESCE(full_text, '')) < 500"
    ).fetchone()[0]
    no_regeste = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE regeste IS NULL OR LENGTH(TRIM(regeste)) = 0"
    ).fetchone()[0]
    no_date = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date IS NULL OR LENGTH(TRIM(decision_date)) = 0"
    ).fetchone()[0]
    logger.info(f"  Quality: {short} short text (<500), {no_regeste} no regeste, {no_date} no date (of {total})")


def insert_decision(conn: sqlite3.Connection, row: dict) -> bool:
    """Insert a single decision. Returns True if inserted, False if skipped (duplicate)."""
    try:
        # Clean text fields
        for field in ("full_text", "regeste", "title"):
            if field in row and row[field]:
                row[field] = _clean_text(row[field])

        # Handle cited_decisions — could be list or JSON string
        cited = row.get("cited_decisions", [])
        if isinstance(cited, list):
            cited = json.dumps(cited)
        row["cited_decisions"] = cited

        # json_data: full row as JSON blob (after cleaning)
        row["json_data"] = json.dumps(row, default=str)

        # Canonical key for dedup (aggressive normalization of court+docket+date)
        row["canonical_key"] = make_canonical_key(
            row.get("court", ""), row.get("docket_number", ""), row.get("decision_date"),
        )

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


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Add missing columns to an existing decisions table.

    Safe to call on every startup — ALTER TABLE ADD COLUMN is a no-op
    if the column already exists (caught by the try/except).
    """
    migrations = [
        ("canonical_key", "TEXT"),
    ]
    for col_name, col_type in migrations:
        try:
            conn.execute(f"ALTER TABLE decisions ADD COLUMN {col_name} {col_type}")
            logger.info(f"Schema migration: added column '{col_name}'")
        except sqlite3.OperationalError:
            pass  # column already exists


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

    # Migrate: add columns that may be missing in older databases
    _migrate_schema(conn)

    # Count existing
    existing = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    jsonl_dir = output_dir / "decisions"
    if incremental and checkpoint is None:
        logger.info(
            "No checkpoint file found — first incremental run will read all JSONL files. "
            "Subsequent runs will be fast. To skip this, run --full-rebuild first."
        )

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

    # ── Post-import data quality passes ──
    if total_imported > 0:
        logger.info("Deduplicating decisions...")
        deduped = _dedup_decisions(conn)
        if deduped:
            logger.info(f"  Removed {deduped} duplicate decisions")

        logger.info("Filling missing regeste for BGer/BGE decisions...")
        filled = _fill_missing_regeste(conn)
        if filled:
            logger.info(f"  Extracted regeste for {filled} decisions")

        _log_quality_summary(conn)

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
                build_database(
                    output_dir, db_path,
                    incremental=args.incremental,
                    no_optimize=args.no_optimize,
                    full_rebuild=args.full_rebuild,
                )
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
