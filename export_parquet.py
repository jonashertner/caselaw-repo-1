#!/usr/bin/env python3
"""
export_parquet.py — Export decisions to per-court Parquet files
================================================================

Reads from the deduplicated FTS5 SQLite database (preferred) or falls
back to JSONL files.  One Parquet file per court in output/dataset/.

Usage:
    python3 export_parquet.py                          # auto-detect DB
    python3 export_parquet.py --db output/decisions.db  # explicit DB
    python3 export_parquet.py --jsonl                   # force JSONL
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("export_parquet")

# Explicit PyArrow schema matching the Decision model
DECISION_SCHEMA = pa.schema([
    # Identity
    pa.field("decision_id", pa.string(), nullable=False),
    pa.field("court", pa.string(), nullable=False),
    pa.field("canton", pa.string(), nullable=False),
    pa.field("chamber", pa.string(), nullable=True),
    # Case identification
    pa.field("docket_number", pa.string(), nullable=False),
    pa.field("docket_number_2", pa.string(), nullable=True),
    pa.field("decision_date", pa.string(), nullable=True),
    pa.field("publication_date", pa.string(), nullable=True),
    # Content
    pa.field("language", pa.string(), nullable=False),
    pa.field("title", pa.string(), nullable=True),
    pa.field("legal_area", pa.string(), nullable=True),
    pa.field("regeste", pa.string(), nullable=True),
    pa.field("abstract_de", pa.string(), nullable=True),
    pa.field("abstract_fr", pa.string(), nullable=True),
    pa.field("abstract_it", pa.string(), nullable=True),
    pa.field("full_text", pa.string(), nullable=False),
    # Metadata
    pa.field("outcome", pa.string(), nullable=True),
    pa.field("decision_type", pa.string(), nullable=True),
    pa.field("judges", pa.string(), nullable=True),
    pa.field("clerks", pa.string(), nullable=True),
    pa.field("collection", pa.string(), nullable=True),
    pa.field("appeal_info", pa.string(), nullable=True),
    # References
    pa.field("source_url", pa.string(), nullable=False),
    pa.field("pdf_url", pa.string(), nullable=True),
    pa.field("bge_reference", pa.string(), nullable=True),
    pa.field("cited_decisions", pa.string(), nullable=True),  # JSON array as string
    # Provenance
    pa.field("scraped_at", pa.string(), nullable=True),
    pa.field("external_id", pa.string(), nullable=True),
    pa.field("source", pa.string(), nullable=True),           # "entscheidsuche", "direct_scrape"
    pa.field("source_id", pa.string(), nullable=True),        # Source-specific ID (e.g. Signatur)
    pa.field("source_spider", pa.string(), nullable=True),    # Spider/scraper name at source
    pa.field("content_hash", pa.string(), nullable=True),     # MD5 of full_text for dedup
    # Computed fields
    pa.field("has_full_text", pa.bool_(), nullable=False),
    pa.field("text_length", pa.int32(), nullable=False),
])


def normalize_row(row: dict) -> dict:
    """Normalize a decision dict for Parquet export."""
    # Convert date/datetime objects to ISO strings
    for key in ("decision_date", "publication_date", "scraped_at"):
        val = row.get(key)
        if isinstance(val, (date, datetime)):
            row[key] = val.isoformat()
        elif val == "None" or val is None:
            row[key] = None

    # Ensure non-nullable fields have defaults
    if not row.get("decision_id"):
        row["decision_id"] = "unknown"
    if not row.get("court"):
        row["court"] = "unknown"
    if not row.get("canton"):
        row["canton"] = "XX"
    if not row.get("docket_number"):
        row["docket_number"] = "unknown"
    if row.get("decision_date") == "1970-01-01":
        row["decision_date"] = None  # Don't invent dates
    if not row.get("language"):
        row["language"] = "de"
    if not row.get("full_text"):
        row["full_text"] = ""
    if not row.get("source_url"):
        row["source_url"] = ""

    # Ensure cited_decisions is a JSON string
    cited = row.get("cited_decisions", [])
    if isinstance(cited, list):
        row["cited_decisions"] = json.dumps(cited)

    # Map entscheidsuche-specific provenance fields to generic names
    if row.get("entscheidsuche_signatur") and not row.get("source_id"):
        row["source_id"] = row["entscheidsuche_signatur"]
    if row.get("entscheidsuche_spider") and not row.get("source_spider"):
        row["source_spider"] = row["entscheidsuche_spider"]

    # Computed fields
    full_text = row.get("full_text") or ""
    row["has_full_text"] = bool(full_text.strip())
    row["text_length"] = len(full_text)

    # Ensure all schema fields exist
    for field in DECISION_SCHEMA:
        if field.name not in row:
            row[field.name] = None

    return row


def load_decisions(input_dir: Path) -> dict[str, dict]:
    """Load all JSONL files, deduplicating by decision_id (keeps first-seen)."""
    decisions: dict[str, dict] = {}
    jsonl_files = sorted(input_dir.glob("*.jsonl"))

    if not jsonl_files:
        logger.warning(f"No JSONL files found in {input_dir}")
        return decisions

    for jsonl_file in jsonl_files:
        count = 0
        with open(jsonl_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    did = row.get("decision_id")
                    if did and did not in decisions:
                        decisions[did] = row
                        count += 1
                except json.JSONDecodeError:
                    continue
        logger.debug(f"  {jsonl_file.name}: {count} decisions")

    logger.info(f"Loaded {len(decisions)} unique decisions from {len(jsonl_files)} files")
    return decisions


BATCH_SIZE = 5000  # rows per batch to stay under memory limits


def export_parquet(input_dir: Path, output_dir: Path) -> dict[str, int]:
    """Export decisions to per-court Parquet files. Returns {court: count}.

    Two-pass approach to stay memory-efficient:
    1. First pass: collect all unique decision_ids per court (just IDs, not data)
    2. Second pass: stream data, write per-court Parquet using ParquetWriter

    This avoids loading full texts into memory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_files = sorted(input_dir.glob("*.jsonl"))

    if not jsonl_files:
        logger.warning(f"No JSONL files found in {input_dir}")
        return {}

    schema_fields = {f.name for f in DECISION_SCHEMA}
    results = {}

    # Global dedup: keep first-seen immutable record for each decision_id.
    global_seen: set[str] = set()

    # Use per-court ParquetWriter objects for streaming writes
    writers: dict[str, pq.ParquetWriter] = {}

    try:
        for jsonl_file in jsonl_files:
            file_count = 0
            batch_by_court: dict[str, list[dict]] = {}

            with open(jsonl_file, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                        did = row.get("decision_id")
                        if not did or did in global_seen:
                            continue
                        global_seen.add(did)

                        # Skip rows missing required fields (match FTS5 constraints)
                        if not all(row.get(k) for k in ("court", "canton", "docket_number", "language")):
                            missing = [k for k in ("court", "canton", "docket_number", "language") if not row.get(k)]
                            logger.warning(f"Skipping {did}: missing {', '.join(missing)}")
                            continue

                        court = row.get("court", "unknown")
                        batch_by_court.setdefault(court, []).append(row)
                        file_count += 1

                        # Flush per-court batches when they get large
                        if len(batch_by_court.get(court, [])) >= BATCH_SIZE:
                            rows = batch_by_court.pop(court)
                            _write_rows(rows, court, output_dir, writers, schema_fields)
                            results[court] = results.get(court, 0) + len(rows)
                    except json.JSONDecodeError:
                        continue

            # Flush remaining rows for this file
            for court, rows in batch_by_court.items():
                _write_rows(rows, court, output_dir, writers, schema_fields)
                results[court] = results.get(court, 0) + len(rows)

            if file_count:
                logger.info(f"  Processed {jsonl_file.name}: {file_count} decisions")

    finally:
        # Close all writers and atomically rename .tmp → .parquet
        for court, writer in writers.items():
            writer.close()
            tmp_path = output_dir / f"{court}.parquet.tmp"
            final_path = output_dir / f"{court}.parquet"
            if tmp_path.exists():
                os.replace(str(tmp_path), str(final_path))
            logger.info(f"  {court}: {results.get(court, 0)} total")

    logger.info(f"Exported {sum(results.values())} decisions across {len(results)} courts")
    return results


def _write_rows(
    rows: list[dict],
    court: str,
    output_dir: Path,
    writers: dict[str, pq.ParquetWriter],
    schema_fields: set,
):
    """Write rows to a per-court ParquetWriter (streaming, no read-back)."""
    normalized = [normalize_row(row) for row in rows]
    clean_rows = [{k: r.get(k) for k in schema_fields} for r in normalized]
    table = pa.Table.from_pylist(clean_rows, schema=DECISION_SCHEMA)

    if court not in writers:
        filepath = output_dir / f"{court}.parquet.tmp"
        writers[court] = pq.ParquetWriter(str(filepath), DECISION_SCHEMA, compression="zstd")

    writers[court].write_table(table)


def export_from_db(db_path: Path, output_dir: Path) -> dict[str, int]:
    """Export decisions from the deduplicated FTS5 SQLite DB to Parquet.

    Reads from the database built by build_fts5.py, ensuring Parquet files
    match the search index exactly (same dedup, same row count).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(f"file:{db_path}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row

    schema_fields = {f.name for f in DECISION_SCHEMA}
    results: dict[str, int] = {}
    writers: dict[str, pq.ParquetWriter] = {}

    try:
        total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        courts = [r[0] for r in conn.execute(
            "SELECT DISTINCT court FROM decisions ORDER BY court"
        ).fetchall()]
        logger.info(f"Exporting {total} decisions from {len(courts)} courts")

        for court in courts:
            cursor = conn.execute(
                "SELECT * FROM decisions WHERE court = ?", (court,)
            )
            col_names = [desc[0] for desc in cursor.description]

            batch: list[dict] = []
            for row_tuple in cursor:
                d = dict(zip(col_names, row_tuple))
                batch.append(d)

                if len(batch) >= BATCH_SIZE:
                    _write_rows(batch, court, output_dir, writers, schema_fields)
                    results[court] = results.get(court, 0) + len(batch)
                    batch = []

            if batch:
                _write_rows(batch, court, output_dir, writers, schema_fields)
                results[court] = results.get(court, 0) + len(batch)

            logger.info(f"  {court}: {results.get(court, 0)} decisions")

    finally:
        conn.close()
        for court, writer in writers.items():
            writer.close()
            tmp_path = output_dir / f"{court}.parquet.tmp"
            final_path = output_dir / f"{court}.parquet"
            if tmp_path.exists():
                os.replace(str(tmp_path), str(final_path))

    # Remove stale parquet files for courts no longer in DB
    existing_pq = {p.stem for p in output_dir.glob("*.parquet")}
    stale = existing_pq - set(results.keys())
    for court_name in stale:
        stale_path = output_dir / f"{court_name}.parquet"
        stale_path.unlink()
        logger.info(f"  Removed stale {court_name}.parquet")

    logger.info(f"Exported {sum(results.values())} decisions across {len(results)} courts")
    return results


def main():
    parser = argparse.ArgumentParser(description="Export decisions to Parquet")
    parser.add_argument(
        "--db", type=str, default="output/decisions.db",
        help="FTS5 SQLite database (preferred source, default: output/decisions.db)",
    )
    parser.add_argument(
        "--jsonl", action="store_true",
        help="Force reading from JSONL files instead of the database",
    )
    parser.add_argument(
        "--input", type=str, default="output/decisions",
        help="Input directory for JSONL files (fallback, default: output/decisions)",
    )
    parser.add_argument(
        "--output", type=str, default="output/dataset",
        help="Output directory for Parquet files (default: output/dataset)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    db_path = Path(args.db)
    if not args.jsonl and db_path.exists():
        logger.info(f"Reading from FTS5 database: {db_path}")
        results = export_from_db(db_path, Path(args.output))
    else:
        if not args.jsonl:
            logger.warning(f"No database at {db_path}, falling back to JSONL")
        results = export_parquet(Path(args.input), Path(args.output))
    if results:
        total = sum(results.values())
        print(f"\nExported {total} decisions to {len(results)} Parquet files")
    else:
        print("No decisions exported", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
