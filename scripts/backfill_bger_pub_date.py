#!/usr/bin/env python3
"""One-off backfill: populate BGer.publication_date NULLs.

Audit 2026-05-18 found that 99.3 % of bger rows (174,370 of 175,594)
have publication_date IS NULL. Two distinct cohorts:

  ─ Recent (Mar 2026 onwards, ~1,777 rows): scraped daily via
    bger-poller within ≤24h of BGer making the decision public.
    Setting pub_date = date(scraped_at) is ≤1-day accurate.

  ─ February 2026 Bootstrap (172,593 rows): one-time full-corpus
    rescrape after the Eurospider scraper rewrite. These decisions
    span ~20 years of BGer history. date(scraped_at) would falsely
    stamp them all as Feb 2026. We fall back to decision_date as a
    lower-bound proxy (a decision can't be published before it's
    rendered); rows without decision_date stay NULL rather than get
    a fabricated value.

The forward-direction fix (scrapers/bger.py — commit cbd9a70) ensures
new rows land with publication_date set from the discovery path
(Neuheiten check_date / RSS pubDate). This script only fills the
historical gap; subsequent runs are no-ops because the WHERE clauses
no longer match.

Safety:
  - Reads counts before and after for each cohort.
  - Two UPDATEs in a single transaction; rollback on any error.
  - Predicates ensure idempotency (publication_date IS NULL).
  - No rows are touched if their inferred value would be NULL
    (the second predicate adds AND decision_date IS NOT NULL).
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

DEFAULT_DB = Path(
    os.environ.get("OCL_DECISIONS_DB", "/opt/caselaw/repo/output/decisions.db")
)
CUTOFF = "2026-03-01T00:00:00"  # boundary: Mar 1 2026 = first daily-poller month

logger = logging.getLogger("backfill_bger_pub_date")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--dry-run", action="store_true",
                   help="Print counts but do not run UPDATE.")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if not args.db.exists():
        logger.error("decisions.db not found at %s", args.db)
        return 1

    conn = sqlite3.connect(str(args.db), timeout=30.0)
    conn.execute("PRAGMA busy_timeout=15000")

    def n(sql: str, *params) -> int:
        return conn.execute(sql, params).fetchone()[0]

    # ── Pre-counts ─────────────────────────────────────────────────────
    total = n("SELECT COUNT(*) FROM decisions WHERE court='bger'")
    null_pub = n(
        "SELECT COUNT(*) FROM decisions "
        "WHERE court='bger' AND publication_date IS NULL"
    )
    cohort_recent = n(
        "SELECT COUNT(*) FROM decisions WHERE court='bger' "
        "AND publication_date IS NULL "
        "AND scraped_at >= ?",
        CUTOFF,
    )
    cohort_legacy_with_dd = n(
        "SELECT COUNT(*) FROM decisions WHERE court='bger' "
        "AND publication_date IS NULL "
        "AND (scraped_at IS NULL OR scraped_at < ?) "
        "AND decision_date IS NOT NULL",
        CUTOFF,
    )
    cohort_legacy_no_dd = n(
        "SELECT COUNT(*) FROM decisions WHERE court='bger' "
        "AND publication_date IS NULL "
        "AND (scraped_at IS NULL OR scraped_at < ?) "
        "AND decision_date IS NULL",
        CUTOFF,
    )

    print(f"=== BGer pub_date backfill — before ===")
    print(f"  total bger:                   {total:>8,}")
    print(f"  publication_date IS NULL:     {null_pub:>8,}  ({100*null_pub/total:.1f} %)")
    print(f"  cohort A (recent ≥ {CUTOFF[:10]}): {cohort_recent:>8,}  ← UPDATE to date(scraped_at)")
    print(f"  cohort B (legacy w/ dec_date):    {cohort_legacy_with_dd:>8,}  ← UPDATE to decision_date")
    print(f"  cohort C (legacy w/o dec_date):   {cohort_legacy_no_dd:>8,}  ← LEFT NULL (no proxy)")
    will_update = cohort_recent + cohort_legacy_with_dd
    print(f"  total to update:              {will_update:>8,}")

    if args.dry_run:
        print("\n--dry-run set; no UPDATE executed.")
        return 0

    if will_update == 0:
        print("\nNothing to do; all NULL bger rows already covered or already filled.")
        return 0

    # ── UPDATEs in one transaction ────────────────────────────────────
    print()
    t0 = time.monotonic()
    try:
        conn.execute("BEGIN")
        # Cohort A — recent daily-scraped rows. date(scraped_at) is ≤24h off.
        cur_a = conn.execute(
            "UPDATE decisions "
            "   SET publication_date = date(scraped_at) "
            " WHERE court='bger' "
            "   AND publication_date IS NULL "
            "   AND scraped_at >= ?",
            (CUTOFF,),
        )
        n_a = cur_a.rowcount
        # Cohort B — legacy Feb-2026 bootstrap with a known decision_date.
        # We use decision_date as a lower-bound proxy (publication ≥ ruling).
        cur_b = conn.execute(
            "UPDATE decisions "
            "   SET publication_date = decision_date "
            " WHERE court='bger' "
            "   AND publication_date IS NULL "
            "   AND (scraped_at IS NULL OR scraped_at < ?) "
            "   AND decision_date IS NOT NULL",
            (CUTOFF,),
        )
        n_b = cur_b.rowcount
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.exception("UPDATE failed — rolled back")
        return 2

    dt = time.monotonic() - t0

    print(f"UPDATE A (recent → date(scraped_at)):    {n_a:>8,} rows")
    print(f"UPDATE B (legacy → decision_date):       {n_b:>8,} rows")
    print(f"both committed in {dt:.2f}s")

    # ── Post-counts ───────────────────────────────────────────────────
    null_after = n(
        "SELECT COUNT(*) FROM decisions "
        "WHERE court='bger' AND publication_date IS NULL"
    )
    print(f"\n=== after ===")
    print(f"  publication_date IS NULL: {null_after:>8,}  (was {null_pub:,})")
    print(f"  delta: -{null_pub - null_after:,}")

    print(f"\n  sample (5 latest by pub_date):")
    for r in conn.execute(
        "SELECT publication_date, decision_date, "
        "       substr(scraped_at,1,16) AS scraped, docket_number "
        "FROM decisions WHERE court='bger' "
        "ORDER BY publication_date DESC NULLS LAST LIMIT 5"
    ):
        print(f"    pub={r[0]}  dec={r[1]}  scraped={r[2]}  {r[3]}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
