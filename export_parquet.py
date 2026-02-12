#!/usr/bin/env python3
"""
export_parquet.py — Export JSONL decisions to Parquet files
============================================================

Reads all output/decisions/*.jsonl files, deduplicates by decision_id
(keeps latest), and writes one Parquet file per court.

Output: output/dataset/{court}.parquet

Usage:
    python3 export_parquet.py
    python3 export_parquet.py --input output/decisions --output output/dataset
"""
from __future__ import annotations

import argparse
import json
import logging
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
    pa.field("decision_date", pa.string(), nullable=False),
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

    # Ensure cited_decisions is a JSON string
    cited = row.get("cited_decisions", [])
    if isinstance(cited, list):
        row["cited_decisions"] = json.dumps(cited)

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
    """Load all JSONL files, deduplicating by decision_id (keeps latest)."""
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
                    if did:
                        decisions[did] = row  # later entries overwrite earlier
                        count += 1
                except json.JSONDecodeError:
                    continue
        logger.debug(f"  {jsonl_file.name}: {count} decisions")

    logger.info(f"Loaded {len(decisions)} unique decisions from {len(jsonl_files)} files")
    return decisions


def export_parquet(input_dir: Path, output_dir: Path) -> dict[str, int]:
    """Export decisions to per-court Parquet files. Returns {court: count}."""
    output_dir.mkdir(parents=True, exist_ok=True)
    decisions = load_decisions(input_dir)

    if not decisions:
        logger.warning("No decisions to export")
        return {}

    # Group by court
    by_court: dict[str, list[dict]] = {}
    for row in decisions.values():
        court = row.get("court", "unknown")
        by_court.setdefault(court, []).append(row)

    results = {}
    for court, rows in sorted(by_court.items()):
        normalized = [normalize_row(row) for row in rows]

        # Build table with explicit schema — only include schema fields
        schema_fields = {f.name for f in DECISION_SCHEMA}
        clean_rows = [{k: r.get(k) for k in schema_fields} for r in normalized]

        table = pa.Table.from_pylist(clean_rows, schema=DECISION_SCHEMA)

        filepath = output_dir / f"{court}.parquet"
        pq.write_table(table, filepath, compression="zstd")
        results[court] = len(rows)
        logger.info(f"  {court}: {len(rows)} decisions → {filepath.name}")

    logger.info(f"Exported {sum(results.values())} decisions across {len(results)} courts")
    return results


def main():
    parser = argparse.ArgumentParser(description="Export JSONL decisions to Parquet")
    parser.add_argument(
        "--input", type=str, default="output/decisions",
        help="Input directory containing JSONL files (default: output/decisions)",
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

    results = export_parquet(Path(args.input), Path(args.output))
    if results:
        total = sum(results.values())
        print(f"\nExported {total} decisions to {len(results)} Parquet files")
    else:
        print("No decisions exported", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
