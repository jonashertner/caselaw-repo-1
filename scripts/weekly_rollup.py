#!/usr/bin/env python3
"""
weekly_rollup.py — aggregate daily_* analytics into weekly_* tables.

Most client classes don't reach the k=10 daily k-anonymity floor, so
their per-day cell collapses to NULL. Weekly aggregation raises the
denominator: a client doing 3 distinct cohorts per day for 7 days is
under k=10 daily but typically clears k=10 weekly (~21 distinct
cohort-days, ~5-15 distinct cohorts).

We aggregate from the **already-written** daily tables — no need to
re-read tier-2 logs. For ``weekly_reach`` we cannot perfectly merge
HLL sketches (the sketch state isn't persisted), so we report the
**max-of-week** estimate, which is a conservative lower bound on
unique cohorts in the week.

Privacy contract preserved:

* No new raw data is read or stored.
* Weekly aggregates carry the same DP-noise (epsilon=1.0) applied at
  the weekly granularity.
* Weekly counts gated by k=10 at the weekly level; counts that fail
  the gate are NULL in ``n_public`` (and have the unsuppressed
  DP-noised value in ``n_dp``).
* The script never accesses tier-1 logs; daily_reach already
  reflects whatever cohort sources fed the daily rollup.

Tables written:

* ``weekly_tool_calls`` — (iso_week, client_class, endpoint_class).
* ``weekly_reach``      — (iso_week, client_class) max-of-week HLL.
* ``weekly_status``     — (iso_week, status_bucket).

Usage::

    python3 scripts/weekly_rollup.py                       # last 12 weeks
    python3 scripts/weekly_rollup.py --weeks 26
    python3 scripts/weekly_rollup.py --db /opt/.../analytics.db
"""
from __future__ import annotations

import argparse
import math
import os
import random
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

K_ANON = 10
DP_EPSILON = 1.0
DP_SENSITIVITY = 1


def laplace_noise(scale: float, rng: random.Random) -> float:
    u = rng.random() - 0.5
    return -scale * math.copysign(math.log(1 - 2 * abs(u)), u)


def dp_count(n: int, rng: random.Random,
             epsilon: float = DP_EPSILON,
             sensitivity: int = DP_SENSITIVITY) -> int:
    noised = n + laplace_noise(sensitivity / epsilon, rng)
    return max(0, int(round(noised)))


SCHEMA = """
CREATE TABLE IF NOT EXISTS weekly_tool_calls (
    iso_week         TEXT    NOT NULL,   -- e.g. "2026-W20"
    client_class     TEXT    NOT NULL,
    endpoint_class   TEXT    NOT NULL,
    n_exact          INTEGER NOT NULL,
    n_public         INTEGER,            -- K_ANON-gated, DP-noised
    n_dp             INTEGER,            -- always populated, DP-noised
    days_in_window   INTEGER NOT NULL,
    PRIMARY KEY (iso_week, client_class, endpoint_class)
);

CREATE TABLE IF NOT EXISTS weekly_reach (
    iso_week                 TEXT    NOT NULL,
    client_class             TEXT    NOT NULL,
    n_cohorts_max_daily      INTEGER,
    n_cohorts_sum_daily      INTEGER,
    n_cohorts_public         INTEGER,
    n_cohorts_dp             INTEGER,
    days_in_window           INTEGER NOT NULL,
    PRIMARY KEY (iso_week, client_class)
);

CREATE TABLE IF NOT EXISTS weekly_status (
    iso_week      TEXT    NOT NULL,
    status_bucket TEXT    NOT NULL,
    n_exact       INTEGER NOT NULL,
    n_public      INTEGER,
    n_dp          INTEGER,
    PRIMARY KEY (iso_week, status_bucket)
);
"""


def iso_week_of(d: date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y:04d}-W{w:02d}"


def aggregate(conn: sqlite3.Connection, weeks: int) -> dict:
    """Build weekly aggregates from existing daily tables for the last
    `weeks` ISO weeks. Returns counts of inserted rows for reporting.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=weeks * 7)).date()
    rng = random.Random(f"ocl-weekly-rollup-{datetime.now(timezone.utc).date()}")
    inserted = {"weekly_tool_calls": 0, "weekly_reach": 0, "weekly_status": 0}

    conn.executescript(SCHEMA)

    # ── weekly_tool_calls ─────────────────────────────────────────────
    rows = conn.execute(
        """
        SELECT day, client_class, endpoint_class, n_exact
        FROM daily_tool_calls
        WHERE day >= ?
        """,
        (cutoff.isoformat(),),
    ).fetchall()
    # Group by (iso_week, client, endpoint)
    bucket: dict[tuple[str, str, str], dict] = {}
    for day_str, client, endpoint, n_exact in rows:
        try:
            d = date.fromisoformat(day_str)
        except ValueError:
            continue
        wk = iso_week_of(d)
        key = (wk, client, endpoint)
        b = bucket.setdefault(key, {"n_exact": 0, "days": 0})
        b["n_exact"] += int(n_exact or 0)
        b["days"] += 1

    conn.execute(
        "DELETE FROM weekly_tool_calls WHERE iso_week >= ?",
        (iso_week_of(cutoff),),
    )
    for (wk, client, endpoint), b in bucket.items():
        n = b["n_exact"]
        n_public = dp_count(n, rng) if n >= K_ANON else None
        n_dp = dp_count(n, rng)
        conn.execute(
            """INSERT INTO weekly_tool_calls
               (iso_week, client_class, endpoint_class,
                n_exact, n_public, n_dp, days_in_window)
               VALUES (?,?,?,?,?,?,?)""",
            (wk, client, endpoint, n, n_public, n_dp, b["days"]),
        )
        inserted["weekly_tool_calls"] += 1

    # ── weekly_reach ─────────────────────────────────────────────────
    # We cannot merge HLL sketches (not persisted), so we report two
    # signals: max(daily HLL) — a conservative lower bound — and
    # sum(daily HLL) — an upper bound that double-counts returning
    # cohorts but useful as a ceiling. The "public" column uses the max.
    rows = conn.execute(
        """
        SELECT day, client_class, n_cohorts_hll_estimate
        FROM daily_reach
        WHERE day >= ?
        """,
        (cutoff.isoformat(),),
    ).fetchall()
    bucket2: dict[tuple[str, str], dict] = {}
    for day_str, client, est in rows:
        try:
            d = date.fromisoformat(day_str)
        except ValueError:
            continue
        wk = iso_week_of(d)
        key = (wk, client)
        b = bucket2.setdefault(key, {"max": 0, "sum": 0, "days": 0})
        e = int(est or 0)
        if e > b["max"]:
            b["max"] = e
        b["sum"] += e
        b["days"] += 1

    conn.execute(
        "DELETE FROM weekly_reach WHERE iso_week >= ?",
        (iso_week_of(cutoff),),
    )
    for (wk, client), b in bucket2.items():
        mx = b["max"]
        n_public = dp_count(mx, rng) if mx >= K_ANON else None
        n_dp = dp_count(mx, rng)
        conn.execute(
            """INSERT INTO weekly_reach
               (iso_week, client_class, n_cohorts_max_daily,
                n_cohorts_sum_daily, n_cohorts_public, n_cohorts_dp,
                days_in_window)
               VALUES (?,?,?,?,?,?,?)""",
            (wk, client, mx, b["sum"], n_public, n_dp, b["days"]),
        )
        inserted["weekly_reach"] += 1

    # ── weekly_status ────────────────────────────────────────────────
    rows = conn.execute(
        """
        SELECT day, status_bucket, n_exact
        FROM daily_status
        WHERE day >= ?
        """,
        (cutoff.isoformat(),),
    ).fetchall()
    bucket3: dict[tuple[str, str], int] = {}
    for day_str, bk, n_exact in rows:
        try:
            d = date.fromisoformat(day_str)
        except ValueError:
            continue
        wk = iso_week_of(d)
        key = (wk, bk)
        bucket3[key] = bucket3.get(key, 0) + int(n_exact or 0)

    conn.execute(
        "DELETE FROM weekly_status WHERE iso_week >= ?",
        (iso_week_of(cutoff),),
    )
    for (wk, bk), n in bucket3.items():
        n_public = dp_count(n, rng) if n >= K_ANON else None
        n_dp = dp_count(n, rng)
        conn.execute(
            """INSERT INTO weekly_status
               (iso_week, status_bucket, n_exact, n_public, n_dp)
               VALUES (?,?,?,?,?)""",
            (wk, bk, n, n_public, n_dp),
        )
        inserted["weekly_status"] += 1

    conn.commit()
    return inserted


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--db", type=Path,
        default=Path(os.environ.get("SWISS_CASELAW_DIR", "output"))
        / "analytics.db",
    )
    p.add_argument("--weeks", type=int, default=12)
    args = p.parse_args()

    if not args.db.exists():
        print(f"ERROR: {args.db} does not exist", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(args.db))
    try:
        counts = aggregate(conn, args.weeks)
    finally:
        conn.close()
    print(f"weekly rollup complete: {counts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
