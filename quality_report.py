#!/usr/bin/env python3
"""
quality_report.py — Data quality report for Swiss court decisions
==================================================================

Reads the FTS5 database and emits a quality_report.json with anomaly
counts and per-court metrics. Optionally gates publish if thresholds
are exceeded.

Usage:
    python3 quality_report.py
    python3 quality_report.py --db output/decisions.db --output output/quality_report.json
    python3 quality_report.py --gate --max-invalid-date-pct 0.05
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("quality_report")


def _install_heartbeat(conn: sqlite3.Connection, interval_s: float = 60.0) -> None:
    """Emit a log line at most once per interval_s while any query runs.

    The full_text scans below are O(corpus) cold random reads on the network
    data volume and can run for many minutes as a single SQL statement. The
    publish stall-watchdog kills a step after 5400s with no output, so a single
    long query with no interior progress can be false-killed (root cause of the
    2026-07-06/08 Step 2b watchdog kills). A SQLite progress handler fires every
    N virtual-machine opcodes; returning 0 never aborts the query. This
    guarantees the subprocess emits output during long scans, so the watchdog
    sees liveness. Cheap: the clock is only read every N opcodes.
    """
    state = {"last": time.monotonic()}

    def _beat() -> int:
        now = time.monotonic()
        if now - state["last"] >= interval_s:
            logger.info("quality_report: ...still scanning")
            state["last"] = now
        return 0

    conn.set_progress_handler(_beat, 2_000_000)


def generate_quality_report(db_path: Path) -> dict:
    """Analyze the FTS5 database and return quality metrics."""
    # Read-only immutable open (invariant #1): no write lock, no rollback
    # journal, and no contention with the parallel aux builders that stream the
    # same decisions.db. The old read-write open forced lock/journal overhead
    # that, combined with the full-blob scans below, ballooned this step past
    # the 5400s publish watchdog when co-scheduled with 2c/2g (2026-07-06/08).
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    _install_heartbeat(conn)

    report: dict = {}
    report["db_path"] = str(db_path)
    report["generated_at"] = datetime.now(timezone.utc).isoformat()

    # Heartbeats between blocks: each is O(corpus) I/O, so a slow-but-progressing
    # run must emit output or the publish stall-watchdog (kills on 5400s of
    # silence) treats it as a hang. logger.info reaches the captured subprocess
    # stream that the watchdog reads.
    logger.info("quality_report: counting decisions...")
    total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    report["total_decisions"] = total

    if total == 0:
        conn.close()
        report["error"] = "Database is empty"
        return report

    # --- Date quality ---
    date_null = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date IS NULL OR decision_date = '' OR decision_date = 'None'"
    ).fetchone()[0]

    date_future = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date > date('now', '+7 days')"
    ).fetchone()[0]

    date_ancient = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date IS NOT NULL AND decision_date < '1800-01-01' AND decision_date != '' AND decision_date != 'None'"
    ).fetchone()[0]

    date_invalid_format = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date IS NOT NULL AND decision_date != '' AND decision_date != 'None' AND length(decision_date) < 10"
    ).fetchone()[0]

    # Single query to avoid double-counting overlapping categories
    date_total_bad = conn.execute("""
        SELECT COUNT(*) FROM decisions WHERE
            decision_date IS NULL OR decision_date = '' OR decision_date = 'None'
            OR decision_date > date('now', '+7 days')
            OR (decision_date < '1800-01-01' AND decision_date != '' AND decision_date != 'None')
            OR (length(decision_date) < 10 AND decision_date != '' AND decision_date != 'None')
    """).fetchone()[0]
    report["dates"] = {
        "null_or_empty": date_null,
        "future": date_future,
        "pre_1800": date_ancient,
        "invalid_format": date_invalid_format,
        "total_anomalies": date_total_bad,
        "anomaly_pct": round(date_total_bad / total * 100, 3),
    }

    # --- Full text quality ---
    # The global empty/placeholder counts are derived from the per-court
    # aggregation below (one scan) rather than their own scans. The dominant
    # cost of this step is touching the full_text blob of every row, and on the
    # network-backed data volume each row is a cold random page read; three
    # separate full scans (empty, placeholder, per-court) pushed the step past
    # the 5400s publish watchdog (2026-07-06/08). Collapsing them into the single
    # GROUP BY below cuts that ~3x. text_empty == SUM(per-court text_missing) and
    # text_placeholder == SUM(per-court placeholder) exactly, since every row
    # belongs to exactly one (court, canton) group. report["full_text"] is filled
    # in right after that scan.

    # --- Language distribution (check for unexpected values) ---
    lang_dist = conn.execute(
        "SELECT language, COUNT(*) as count FROM decisions GROUP BY language ORDER BY count DESC"
    ).fetchall()
    valid_langs = {"de", "fr", "it", "rm"}
    unexpected_langs = {r["language"]: r["count"] for r in lang_dist if r["language"] not in valid_langs}
    report["languages"] = {
        "distribution": {r["language"]: r["count"] for r in lang_dist},
        "unexpected": unexpected_langs,
    }

    # --- Per-court quality (also yields the global full-text counts) ---
    # This is the one scan that touches the full_text blob. length(substr(...,1,50))
    # < 50 is provably identical to length(full_text) < 50 (>=50 chars: both
    # false; <50: both count the same k chars) but reads only the first ~50 chars
    # instead of the whole blob. The placeholder markers are always written by the
    # scrapers as the ENTIRE full_text value (e.g. "[Text extraction failed for
    # {docket}]" / "(metadata only ...)"), so they are standalone strings <60 chars
    # at position 1; substr(1,200) covers them with margin without a full
    # leading-wildcard scan. Same class of fix as the 2026-07-08 RSS feed timeout.
    logger.info("quality_report: per-court + full-text aggregation (single scan)...")
    court_quality = conn.execute("""
        SELECT
            court,
            canton,
            COUNT(*) as total,
            SUM(CASE WHEN decision_date IS NULL OR decision_date = '' OR decision_date = 'None' THEN 1 ELSE 0 END) as date_missing,
            SUM(CASE WHEN full_text IS NULL OR full_text = '' OR length(substr(full_text, 1, 50)) < 50 THEN 1 ELSE 0 END) as text_missing,
            SUM(CASE WHEN substr(full_text, 1, 200) LIKE '%[Text extraction failed%' OR substr(full_text, 1, 200) LIKE '%(metadata only)%' THEN 1 ELSE 0 END) as text_placeholder,
            SUM(CASE WHEN decision_date > date('now', '+7 days') THEN 1 ELSE 0 END) as date_future,
            MIN(CASE WHEN decision_date IS NOT NULL AND decision_date != 'None' AND decision_date > '1800-01-01' THEN decision_date END) as earliest,
            MAX(CASE WHEN decision_date IS NOT NULL AND decision_date != 'None' AND decision_date < '2100-01-01' THEN decision_date END) as latest
        FROM decisions
        GROUP BY court, canton
        ORDER BY total DESC
    """).fetchall()

    courts = []
    for r in court_quality:
        ct = {
            "court": r["court"],
            "canton": r["canton"],
            "total": r["total"],
            "date_missing": r["date_missing"],
            "text_missing": r["text_missing"],
            "date_future": r["date_future"],
            "earliest": r["earliest"],
            "latest": r["latest"],
        }
        ct["quality_score"] = round(
            (1 - (ct["date_missing"] + ct["text_missing"]) / max(ct["total"], 1)) * 100, 1
        )
        courts.append(ct)

    report["by_court"] = courts

    # Global full-text counts derived from the single per-court scan above.
    # Identical to the former standalone queries: every row is in exactly one
    # (court, canton) group, so the global count is the sum of the per-group
    # counts. total_missing intentionally adds the two (a short placeholder row
    # is counted in both), preserving the original metric's behavior.
    text_empty = sum(r["text_missing"] for r in court_quality)
    text_placeholder = sum(r["text_placeholder"] for r in court_quality)
    report["full_text"] = {
        "empty_or_short": text_empty,
        "placeholder": text_placeholder,
        "total_missing": text_empty + text_placeholder,
        "missing_pct": round((text_empty + text_placeholder) / total * 100, 3),
    }

    # --- Duplicate detection (same docket+court, different decision_id) ---
    logger.info("quality_report: duplicate detection...")
    dupe_count = conn.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT 1
            FROM decisions
            GROUP BY court, docket_number
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    dupes = conn.execute("""
        SELECT court, docket_number, COUNT(*) as cnt
        FROM decisions
        GROUP BY court, docket_number
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    report["duplicates"] = {
        "count": dupe_count,
        "top": [{"court": r["court"], "docket": r["docket_number"], "count": r["cnt"]} for r in dupes[:10]],
    }

    # --- Overall quality score ---
    date_quality = 1 - (report["dates"]["anomaly_pct"] / 100)
    text_quality = 1 - (report["full_text"]["missing_pct"] / 100)
    report["overall_quality_score"] = round((date_quality * 0.5 + text_quality * 0.5) * 100, 1)

    conn.close()
    return report


def check_gates(report: dict, max_invalid_date_pct: float, max_missing_text_pct: float) -> list[str]:
    """Check quality gates. Returns list of failure reasons (empty = pass)."""
    failures = []
    if report.get("dates", {}).get("anomaly_pct", 100) > max_invalid_date_pct:
        failures.append(
            f"Date anomaly rate {report['dates']['anomaly_pct']:.3f}% exceeds threshold {max_invalid_date_pct}%"
        )
    if report.get("full_text", {}).get("missing_pct", 100) > max_missing_text_pct:
        failures.append(
            f"Missing text rate {report['full_text']['missing_pct']:.3f}% exceeds threshold {max_missing_text_pct}%"
        )
    return failures


def main():
    parser = argparse.ArgumentParser(description="Generate data quality report")
    parser.add_argument(
        "--db", type=str, default="output/decisions.db",
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--output", type=str, default="output/quality_report.json",
        help="Output path for quality report",
    )
    parser.add_argument(
        "--gate", action="store_true",
        help="Exit with error if quality thresholds are exceeded",
    )
    parser.add_argument(
        "--max-invalid-date-pct", type=float, default=5.0,
        help="Max allowed date anomaly percentage (default: 5.0)",
    )
    parser.add_argument(
        "--max-missing-text-pct", type=float, default=10.0,
        help="Max allowed missing text percentage (default: 10.0)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)

    report = generate_quality_report(db_path)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # Print summary
    print(f"Quality Report: {report['total_decisions']} decisions")
    print(f"  Date anomalies:  {report['dates']['anomaly_pct']:.2f}% ({report['dates']['total_anomalies']})")
    print(f"  Missing text:    {report['full_text']['missing_pct']:.2f}% ({report['full_text']['total_missing']})")
    print(f"  Duplicates:      {report['duplicates']['count']}")
    print(f"  Overall quality: {report['overall_quality_score']}%")
    print(f"  Report: {output_path}")

    if args.gate:
        failures = check_gates(report, args.max_invalid_date_pct, args.max_missing_text_pct)
        if failures:
            print("\nQuality gate FAILED:")
            for f in failures:
                print(f"  - {f}")
            sys.exit(1)
        else:
            print("\nQuality gate PASSED")


if __name__ == "__main__":
    main()
