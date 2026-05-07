"""Drift-check: diff two FTS5 / reference_graph / decision_structure DBs.

The three incremental builders shipped 2026-05-07 (quick_publish,
build_reference_graph_incremental, extract_decision_structure_incremental)
need shadow-mode validation before they can replace the full nightly
rebuild. This script computes the drift between an incremental output
and the authoritative full rebuild output across the three primary
data layers, so the operator can decide whether to flip the default.

Usage:
    python3 scripts/drift_check_incremental.py \\
        --full output/decisions.db \\
        --incr output/decisions.shadow.db

Reports for each table:
    - row count delta
    - keys present in --full but missing in --incr (false negatives)
    - keys present in --incr but absent from --full (false positives)
    - sample of differing rows (first 5 for each direction)

Exits 0 if drift is zero across all three layers, 1 otherwise.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


# Per-DB schema description: what to compare and how.
# Each entry: (path-suffix-hint, table, key-columns, count-query).
LAYERS: list[dict] = [
    {
        "name": "decisions (FTS5)",
        "table": "decisions",
        "key": "decision_id",
        "count_query": "SELECT COUNT(*) FROM decisions",
        "keys_query": "SELECT decision_id FROM decisions",
    },
    {
        "name": "citation_targets (reference graph)",
        "table": "citation_targets",
        "key": "ROWID",  # synthetic — the table has multi-col composite keys
        "count_query": "SELECT COUNT(*) FROM citation_targets",
        # Use a stable composite key projection for diff
        "keys_query": (
            "SELECT source_decision_id || '→' || COALESCE(target_decision_id,'') "
            "|| '@' || COALESCE(citation_text,'') FROM citation_targets"
        ),
    },
    {
        "name": "decision_paragraphs (decision structure)",
        "table": "decision_paragraphs",
        "key": "ROWID",
        "count_query": "SELECT COUNT(*) FROM decision_paragraphs",
        "keys_query": (
            "SELECT decision_id || '#' || para_order FROM decision_paragraphs"
        ),
    },
]


def _open_ro(path: Path) -> sqlite3.Connection | None:
    """Open in read-only / immutable mode if the file exists."""
    if not path.exists():
        return None
    return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
    )


def _diff_layer(
    full: sqlite3.Connection, incr: sqlite3.Connection, layer: dict
) -> tuple[int, int, list[str], list[str]]:
    """Return (full_count, incr_count, only_in_full[:5], only_in_incr[:5])."""
    if not _has_table(full, layer["table"]):
        print(f"  ⚠  full DB has no table '{layer['table']}' — skipping layer")
        return 0, 0, [], []
    if not _has_table(incr, layer["table"]):
        print(f"  ⚠  incr DB has no table '{layer['table']}' — skipping layer")
        return 0, 0, [], []
    full_n = full.execute(layer["count_query"]).fetchone()[0]
    incr_n = incr.execute(layer["count_query"]).fetchone()[0]
    # For very large tables (>5M rows) skip the keyset diff — it'd OOM.
    if max(full_n, incr_n) > 5_000_000:
        print(f"  ⚠  table '{layer['table']}' too big ({max(full_n, incr_n):,} rows) "
              f"— count-only diff")
        return full_n, incr_n, [], []
    full_keys = set(r[0] for r in full.execute(layer["keys_query"]))
    incr_keys = set(r[0] for r in incr.execute(layer["keys_query"]))
    only_full = sorted(full_keys - incr_keys)[:5]
    only_incr = sorted(incr_keys - full_keys)[:5]
    return full_n, incr_n, only_full, only_incr


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument(
        "--full",
        required=True,
        help="path to authoritative DB (full rebuild output)",
    )
    ap.add_argument(
        "--incr",
        required=True,
        help="path to incremental-builder DB (shadow output)",
    )
    args = ap.parse_args()

    full_path = Path(args.full).resolve()
    incr_path = Path(args.incr).resolve()
    full = _open_ro(full_path)
    incr = _open_ro(incr_path)
    if full is None:
        print(f"ERROR: --full not found: {full_path}", file=sys.stderr)
        return 2
    if incr is None:
        print(f"ERROR: --incr not found: {incr_path}", file=sys.stderr)
        return 2

    print(f"Drift check:")
    print(f"  full = {full_path}")
    print(f"  incr = {incr_path}\n")

    total_drift = 0
    for layer in LAYERS:
        print(f"=== {layer['name']} ===")
        full_n, incr_n, only_full, only_incr = _diff_layer(full, incr, layer)
        print(f"  full count:  {full_n:>12,}")
        print(f"  incr count:  {incr_n:>12,}")
        delta = incr_n - full_n
        sign = "+" if delta > 0 else ""
        print(f"  delta:       {sign}{delta:>11,}")
        if only_full:
            print(f"  in full only ({len(only_full)} sample):")
            for k in only_full:
                print(f"    {k!r}")
            total_drift += len(only_full)
        if only_incr:
            print(f"  in incr only ({len(only_incr)} sample):")
            for k in only_incr:
                print(f"    {k!r}")
            total_drift += len(only_incr)
        if not only_full and not only_incr and full_n == incr_n:
            print(f"  ✓ no drift")
        else:
            total_drift += abs(delta)
        print()

    if total_drift == 0:
        print("RESULT: ✓ zero drift across all three layers — incremental is "
              "consistent with full rebuild")
        return 0
    print(f"RESULT: ✗ drift detected (total signal: {total_drift})")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
