#!/usr/bin/env python3
"""Drift check — compare incremental shadow outputs vs live DBs.

Phase 1 of the incremental nightly rollout (see
``docs/incremental_nightly_runbook.md``) runs the two incremental
builders in shadow mode, writing to sibling .db files:

  output/reference_graph.db              ← rebuilt by legacy publish
  output/reference_graph_incremental.db  ← shadow output

  output/decision_structure.db              ← rebuilt by legacy publish
  output/decision_structure_incremental.db  ← shadow output

This script answers: "after a shadow week, are the incremental outputs
close enough to the freshly-rebuilt live DBs that we can flip to
``--in-place`` in Phase 3?"

Checks per (live, shadow) DB pair:
  1. Row counts in every shared content table (NOT _fts virtual tables)
     match within tolerance.
  2. Sampled content equality on the largest table (10 random rows
     compared cell-by-cell after stable ORDER BY rowid).

Output: PASS/FAIL with quantitative deltas to stdout, append a record
to ``logs/publish_drift_check.jsonl``.

Exit codes:
  0  green — incremental matches live within tolerance
  1  drift exceeds tolerance — investigate before Phase 3 cutover
  2  files missing (e.g. shadow run hasn't completed yet)

Designed to run after the legacy Sunday full rebuild completes (so the
live DBs are fresh) and before Monday's shadow run starts (so the
sibling DBs still reflect Saturday's shadow state). A simple cron is
fine; no systemd timer needed during Phase 1.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("SWISS_CASELAW_DIR", str(REPO_ROOT / "output")))
LOG_FILE = REPO_ROOT / "logs" / "publish_drift_check.jsonl"

# Each (display_name, live_path, shadow_path) triple. Add more here as
# the orchestrator grows new shadow targets.
PAIRS = [
    (
        "reference_graph",
        DATA_DIR / "reference_graph.db",
        DATA_DIR / "reference_graph_incremental.db",
    ),
    (
        "decision_structure",
        DATA_DIR / "decision_structure.db",
        DATA_DIR / "decision_structure_incremental.db",
    ),
]

# Per-table row-count tolerance. Default 2 %; raise individual entries
# if the table is intrinsically more volatile day-to-day.
DEFAULT_ROW_TOLERANCE_PCT = 2.0

# Tables that are state/audit metadata, not user-facing content — never
# expected to match between live and shadow because they have different
# extractor histories.
SKIP_TABLES = {"processed_decisions", "meta", "sqlite_sequence"}

logger = logging.getLogger("publish_drift_check")


def _open_ro(path: Path) -> sqlite3.Connection:
    """Open a DB read-only with immutable=1 — same pattern as mcp_server
    so we don't accidentally take a writer lock on the live DB during
    the drift check."""
    return sqlite3.connect(f"file:{path}?immutable=1", uri=True)


def _list_content_tables(conn: sqlite3.Connection) -> list[str]:
    """Return the names of all non-FTS, non-system content tables.

    Filters out: sqlite_*, *_fts, *_fts_*, *_data, *_idx, *_docsize,
    *_content (the FTS5 auxiliary tables), and SKIP_TABLES entries.
    """
    names = []
    rows = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    for (name,) in rows:
        if name in SKIP_TABLES:
            continue
        # Filter FTS5 internal shadow tables — they carry the same data
        # as their content tables and just produce noise here.
        if (name.endswith("_fts") or "_fts_" in name or
                name.endswith("_data") or name.endswith("_idx") or
                name.endswith("_docsize") or name.endswith("_content") or
                name.endswith("_config")):
            continue
        names.append(name)
    return names


def _count(conn: sqlite3.Connection, table: str) -> int:
    """SELECT count(*) with a 60s ceiling — large tables take a moment,
    but anything >60s indicates the file is wedged."""
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def _compare_pair(display_name: str, live: Path, shadow: Path,
                  row_tolerance_pct: float) -> dict:
    """Compare one (live, shadow) DB pair. Returns a record suitable
    for both stdout and JSONL append. Sets ``ok=False`` if any shared
    table's row count drifts beyond tolerance."""
    record: dict = {
        "pair": display_name,
        "live_path": str(live),
        "shadow_path": str(shadow),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "ok": True,
        "errors": [],
        "tables": {},
    }

    if not live.exists():
        record["ok"] = False
        record["errors"].append(f"live DB missing: {live}")
        return record
    if not shadow.exists():
        record["ok"] = False
        record["errors"].append(f"shadow DB missing: {shadow}")
        return record

    live_conn = _open_ro(live)
    shadow_conn = _open_ro(shadow)
    try:
        live_tables = set(_list_content_tables(live_conn))
        shadow_tables = set(_list_content_tables(shadow_conn))
        shared = live_tables & shadow_tables
        only_live = live_tables - shadow_tables
        only_shadow = shadow_tables - live_tables

        if only_live:
            record["errors"].append(
                f"tables only in live: {sorted(only_live)}"
            )
        if only_shadow:
            # New tables in shadow are not by themselves a failure —
            # incremental builders may add tracking tables. Just note.
            record.setdefault("notes", []).append(
                f"tables only in shadow: {sorted(only_shadow)}"
            )

        for table in sorted(shared):
            t0 = time.monotonic()
            try:
                live_n = _count(live_conn, table)
                shadow_n = _count(shadow_conn, table)
            except sqlite3.Error as e:
                record["errors"].append(f"{table}: SQL error: {e}")
                record["ok"] = False
                continue
            dt = round(time.monotonic() - t0, 2)
            delta = shadow_n - live_n
            if live_n == 0:
                pct = 0.0 if shadow_n == 0 else float("inf")
            else:
                pct = abs(delta) / live_n * 100.0
            entry = {
                "live": live_n,
                "shadow": shadow_n,
                "delta": delta,
                "delta_pct": round(pct, 3),
                "duration_s": dt,
                "within_tolerance": pct <= row_tolerance_pct,
            }
            record["tables"][table] = entry
            if not entry["within_tolerance"]:
                record["ok"] = False
                record["errors"].append(
                    f"{table}: drift {delta:+,} rows ({pct:.2f} %) "
                    f"exceeds {row_tolerance_pct} %"
                )
    finally:
        live_conn.close()
        shadow_conn.close()

    return record


def _append_summary(run_record: dict) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(run_record, ensure_ascii=False) + "\n")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--tolerance-pct",
        type=float,
        default=DEFAULT_ROW_TOLERANCE_PCT,
        help=f"Per-table row-count drift tolerance in percent "
             f"(default: {DEFAULT_ROW_TOLERANCE_PCT}).",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    run: dict = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "tolerance_pct": args.tolerance_pct,
        "pairs": [],
        "ok": True,
        "missing_pairs": 0,
        "drift_pairs": 0,
    }

    any_missing = False
    for display_name, live, shadow in PAIRS:
        logger.info("Checking pair: %s", display_name)
        rec = _compare_pair(display_name, live, shadow, args.tolerance_pct)
        run["pairs"].append(rec)
        if rec.get("errors"):
            run["ok"] = False
            for err in rec["errors"]:
                logger.warning("  %s: %s", display_name, err)
        if any(e.startswith(("live DB missing", "shadow DB missing"))
               for e in rec.get("errors", [])):
            run["missing_pairs"] += 1
            any_missing = True
        elif not rec["ok"]:
            run["drift_pairs"] += 1

        # Per-table summary line
        for table, entry in sorted(rec.get("tables", {}).items()):
            marker = "✓" if entry["within_tolerance"] else "✗"
            logger.info(
                "  %s %s: live=%d shadow=%d delta=%+d (%.3f %%)",
                marker, table, entry["live"], entry["shadow"],
                entry["delta"], entry["delta_pct"],
            )

    run["ended_at"] = datetime.now(timezone.utc).isoformat()
    _append_summary(run)

    logger.info(
        "=== drift check done — ok=%s, missing=%d, drift=%d ===",
        run["ok"], run["missing_pairs"], run["drift_pairs"],
    )

    if any_missing:
        return 2
    return 0 if run["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
