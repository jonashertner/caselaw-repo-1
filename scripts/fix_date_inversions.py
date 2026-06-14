#!/usr/bin/env python3
"""Correct publication_date < decision_date inversions.

Policy (2026-06-13): decision_date is MANDATORY and TRUSTED (the header
ruling date); publication_date is OPTIONAL. A court cannot publish before
it rules, so every row with publication_date < decision_date is a
mislabel or date-parse error.

Correction rule — conservative by design:
  NULL the suspect publication_date; never alter decision_date.

Why not swap the two dates? Swapping always *resolves* the inversion
arithmetically, but it would corrupt the trusted decision_date whenever
the cause is a wrong publication_date (e.g. the ~1-year year-parse bug:
dec=2025-03-14, pub=2024-03-14 — swapping would back-date the ruling).
Since publication_date is optional, dropping a known-wrong value is
strictly safe; a correct-but-mislabelled publication_date that we null
can be re-derived later by the direct-scraper program. We never trade a
trusted field for an untrusted one.

USAGE:
  python3 scripts/fix_date_inversions.py                 # dry-run report
  python3 scripts/fix_date_inversions.py --apply --db /path/to/copy.db

SAFETY: --apply writes to the given DB. NEVER point it at the live
immutable decisions.db (invariant #1) — run it on a build copy, or port
this rule into build_fts5's date-normalisation pass (the proper
production home: it then self-heals every rebuild AND guards new
inversions at ingest). This script is the one-off + the spec for that
pass.
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import Counter
from datetime import date

INVERSION_WHERE = (
    "publication_date IS NOT NULL AND publication_date != '' "
    "AND decision_date IS NOT NULL AND decision_date != '' "
    "AND publication_date < decision_date"
)


def _gap_band(dd: str, pd: str) -> str:
    try:
        d1 = date.fromisoformat(dd[:10])
        d2 = date.fromisoformat(pd[:10])
    except ValueError:
        return "unparseable"
    days = (d2 - d1).days
    if -3 <= days <= 0:
        return "0-3 days (doc-vs-ruling date)"
    if -31 <= days < -3:
        return "days-to-1mo"
    if -400 <= days <= -330:
        return "~1 year (year-parse bug)"
    return "larger (months-years)"


def report(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        f"SELECT court, decision_date, publication_date FROM decisions "
        f"WHERE {INVERSION_WHERE}"
    ).fetchall()
    by_court: Counter = Counter()
    by_band: Counter = Counter()
    for court, dd, pd in rows:
        by_court[court] += 1
        by_band[_gap_band(dd, pd)] += 1
    print(f"publication_date < decision_date inversions: {len(rows):,}")
    print("\nby gap band (all would have publication_date NULLed):")
    for band, n in by_band.most_common():
        print(f"  {band:<34} {n:,}")
    print("\nby court (top 15):")
    for court, n in by_court.most_common(15):
        print(f"  {court:<30} {n:,}")
    print("\nsamples:")
    for court, dd, pd in rows[:8]:
        print(f"  {court:<26} decision={dd}  publication={pd}  -> publication NULLed")
    return len(rows)


def apply_fix(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        f"UPDATE decisions SET publication_date = NULL WHERE {INVERSION_WHERE}"
    )
    conn.commit()
    return cur.rowcount


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="output/decisions.db",
                   help="Corpus DB (default output/decisions.db)")
    p.add_argument("--apply", action="store_true",
                   help="Apply the fix (NULL suspect publication_date). "
                        "Without this, dry-run report only.")
    args = p.parse_args()

    if not args.apply:
        conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
        try:
            n = report(conn)
        finally:
            conn.close()
        print(f"\n[dry-run] would NULL publication_date on {n:,} rows. "
              f"Re-run with --apply --db <writable copy> to execute.")
        return 0

    conn = sqlite3.connect(args.db)
    try:
        n = apply_fix(conn)
    finally:
        conn.close()
    print(f"[applied] NULLed publication_date on {n:,} rows in {args.db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
