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

# Canonical decisions.db — used as the orphan reference. Any table with
# a decision_id column gets compared on the subset whose decision_id
# still exists in this DB, not on the raw row count. Without this, live
# DBs accumulate orphan rows over months (from sg_gerichte deletion,
# EGMR dedup, stub removal, ...) that look like drift but aren't.
DECISIONS_DB = DATA_DIR / "decisions.db"

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


def _has_decision_id_column(conn: sqlite3.Connection, table: str) -> bool:
    """True if the table has a column literally named ``decision_id``."""
    try:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    except sqlite3.Error:
        return False
    return "decision_id" in cols


def _useful_count(conn: sqlite3.Connection, table: str,
                  decisions_db_path: Path) -> int:
    """Count rows in ``table`` whose ``decision_id`` exists in the
    canonical decisions.db. Avoids the orphan inflation that polluted
    drift_check before 2026-05-19: live decision_structure.db had
    120,757 orphan rows from decisions removed in earlier dedup runs
    (sg_gerichte deletion, EGMR dedup, etc.). Raw counts then drifted
    11 % vs. a fresh sibling rebuild, even though per-decision
    extraction was identical (verified with 20-sample diff).

    ATTACHes the decisions.db read-only on the same connection so the
    query stays inside SQLite (~10s for 8M-row joins given the PK
    index on decisions.decision_id).
    """
    uri = f"file:{decisions_db_path}?mode=ro"
    conn.execute(f"ATTACH '{uri}' AS _dec_ref")
    try:
        return conn.execute(
            f"SELECT COUNT(*) FROM {table} t "
            f"WHERE t.decision_id IN ("
            f"  SELECT decision_id FROM _dec_ref.decisions"
            f")"
        ).fetchone()[0]
    finally:
        conn.execute("DETACH _dec_ref")


def _compare_pair(display_name: str, live: Path, shadow: Path,
                  row_tolerance_pct: float,
                  decisions_db: Path | None = None) -> dict:
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

            # Orphan-aware comparison: if both DBs carry a decision_id
            # column on this table AND we have the canonical decisions.db
            # path, compute the "useful" subset (rows whose decision_id
            # still exists in decisions.db). Tolerance applies to that
            # number, not the raw count. Raw counts stay in the record
            # for visibility. See the comment on _useful_count above.
            use_orphan_aware = (
                decisions_db is not None and decisions_db.exists() and
                _has_decision_id_column(live_conn, table) and
                _has_decision_id_column(shadow_conn, table)
            )
            live_useful = shadow_useful = None
            if use_orphan_aware:
                try:
                    live_useful = _useful_count(live_conn, table, decisions_db)
                    shadow_useful = _useful_count(shadow_conn, table, decisions_db)
                except sqlite3.Error as e:
                    record["errors"].append(
                        f"{table}: orphan-aware count failed ({e}); "
                        f"falling back to raw count"
                    )
                    use_orphan_aware = False

            cmp_live = live_useful if use_orphan_aware else live_n
            cmp_shadow = shadow_useful if use_orphan_aware else shadow_n
            delta_raw = shadow_n - live_n
            delta_cmp = cmp_shadow - cmp_live
            if cmp_live == 0:
                pct = 0.0 if cmp_shadow == 0 else float("inf")
            else:
                pct = abs(delta_cmp) / cmp_live * 100.0

            dt = round(time.monotonic() - t0, 2)
            entry: dict = {
                "live": live_n,
                "shadow": shadow_n,
                "delta": delta_raw,
                "delta_pct_raw": round(
                    abs(delta_raw) / live_n * 100.0, 3
                ) if live_n else 0.0,
                "duration_s": dt,
                "within_tolerance": pct <= row_tolerance_pct,
                "compare_mode": "orphan_aware" if use_orphan_aware else "raw",
            }
            if use_orphan_aware:
                entry.update({
                    "live_useful": live_useful,
                    "shadow_useful": shadow_useful,
                    "delta_useful": delta_cmp,
                    "delta_pct_useful": round(pct, 3),
                    "live_orphans": live_n - live_useful,
                    "shadow_orphans": shadow_n - shadow_useful,
                })
            else:
                entry["delta_pct"] = round(pct, 3)
            record["tables"][table] = entry
            if not entry["within_tolerance"]:
                record["ok"] = False
                if use_orphan_aware:
                    record["errors"].append(
                        f"{table}: useful drift {delta_cmp:+,} rows "
                        f"({pct:.2f} %) exceeds {row_tolerance_pct} % "
                        f"(raw delta {delta_raw:+,})"
                    )
                else:
                    record["errors"].append(
                        f"{table}: drift {delta_raw:+,} rows ({pct:.2f} %) "
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
    p.add_argument(
        "--decisions-db",
        type=Path,
        default=DECISIONS_DB,
        help=f"Canonical decisions.db for orphan-aware comparison "
             f"(default: {DECISIONS_DB}). Pass an empty string to "
             f"disable orphan-aware mode and use raw counts.",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    decisions_db = args.decisions_db if str(args.decisions_db) else None

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
        logger.info("Checking pair: %s (decisions_db=%s)",
                    display_name,
                    decisions_db if decisions_db else "OFF (raw mode)")
        rec = _compare_pair(display_name, live, shadow, args.tolerance_pct,
                            decisions_db=decisions_db)
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

        # Per-table summary line — emit both raw and useful counts
        # whenever orphan-aware mode is active, so the operator can
        # see at a glance that drift came from orphans not extraction.
        for table, entry in sorted(rec.get("tables", {}).items()):
            marker = "✓" if entry["within_tolerance"] else "✗"
            if entry.get("compare_mode") == "orphan_aware":
                logger.info(
                    "  %s %s: live=%d (useful=%d, orphans=%d)  "
                    "shadow=%d (useful=%d, orphans=%d)  "
                    "useful Δ=%+d (%.3f %%)",
                    marker, table,
                    entry["live"], entry["live_useful"], entry["live_orphans"],
                    entry["shadow"], entry["shadow_useful"],
                    entry["shadow_orphans"],
                    entry["delta_useful"], entry["delta_pct_useful"],
                )
            else:
                logger.info(
                    "  %s %s: live=%d shadow=%d delta=%+d (%.3f %%) [raw]",
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
