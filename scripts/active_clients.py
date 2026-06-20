"""Privacy-preserving active-client counter.

Computes TRUE windowed (weekly / monthly) distinct-client counts per
client_class by merging the per-day HLL sketches persisted in
`daily_reach.hll_sketch`. The daily scalar estimate cannot be combined across
days (you can't add distinct counts of overlapping sets); the sketch can —
register-wise max is set union — so a window's distinct count is the estimate
of the merged sketch.

This fixes the existing `weekly_reach` columns, which only had `sum_daily`
(double-counts a client active on N days) and `max_daily` (one peak day) —
neither a real windowed distinct count.

k-anonymity (suppress public value below K) and Laplace DP noise mirror
rollup_analytics.py, so the public columns keep the same formal guarantees.

Limitation: only days with a persisted sketch contribute. Tier-1 logs (the
sketch source) are 72h-retained, so sketches exist only from when persistence
was deployed forward — windowed counts become fully accurate once a complete
window of daily sketches has accumulated (≤7 days for weekly, ≤1 month for
monthly). `days_in_window` reports the coverage so partial windows are visible.

Usage::

    python3 scripts/active_clients.py                 # week + month, recent
    python3 scripts/active_clients.py --window week
"""
from __future__ import annotations

import argparse
import math
import os
import random
import sqlite3
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from analytics_hll import HLL  # noqa: E402

K_ANON_DEFAULT = 10
DP_EPSILON_DEFAULT = 1.0

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS active_clients (
    window_type        TEXT NOT NULL,   -- 'week' | 'month'
    window             TEXT NOT NULL,   -- '2026-W20' | '2026-06'
    client_class       TEXT NOT NULL,
    n_distinct_hll     INTEGER,         -- merged-sketch distinct estimate
    n_distinct_public  INTEGER,         -- DP-noised, NULL if < k_anon (k-anon gate)
    n_distinct_dp      INTEGER,         -- DP-noised, always populated
    days_in_window     INTEGER,         -- daily sketches that contributed
    PRIMARY KEY (window_type, window, client_class)
);
"""


# ── Laplace DP (mirrors rollup_analytics.py) ───────────────────────────
def laplace_noise(scale: float, rng: random.Random) -> float:
    u = rng.random() - 0.5
    return -scale * math.copysign(1.0, u) * math.log(1.0 - 2.0 * abs(u))


def dp_count(n: int, rng: random.Random,
             epsilon: float = DP_EPSILON_DEFAULT, sensitivity: float = 1.0) -> int:
    return max(0, int(round(n + laplace_noise(sensitivity / epsilon, rng))))


def window_key(day_iso: str, window_type: str) -> str:
    d = date.fromisoformat(day_iso[:10])
    if window_type == "month":
        return f"{d.year:04d}-{d.month:02d}"
    if window_type == "week":
        iso_year, iso_week, _ = d.isocalendar()
        return f"{iso_year:04d}-W{iso_week:02d}"
    raise ValueError(f"unknown window_type: {window_type}")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)


def compute_active_clients(
    conn: sqlite3.Connection,
    *,
    window_type: str,
    rng: random.Random,
    k_anon: int = K_ANON_DEFAULT,
    epsilon: float = DP_EPSILON_DEFAULT,
    only_windows: set[str] | None = None,
) -> int:
    """Merge daily sketches into per-(window, client) distinct counts and upsert
    into `active_clients`. Returns the number of rows written."""
    ensure_schema(conn)
    rows = conn.execute(
        "SELECT day, client_class, hll_sketch FROM daily_reach "
        "WHERE hll_sketch IS NOT NULL AND hll_sketch <> ''"
    ).fetchall()

    blobs: dict[tuple[str, str], list[str]] = defaultdict(list)
    days: dict[tuple[str, str], set[str]] = defaultdict(set)
    for day_iso, client, sketch in rows:
        wk = window_key(day_iso, window_type)
        if only_windows is not None and wk not in only_windows:
            continue
        blobs[(wk, client)].append(sketch)
        days[(wk, client)].add(day_iso)

    written = 0
    for (wk, client), sketches in blobs.items():
        n = HLL.union(sketches).estimate()
        public = dp_count(n, rng, epsilon) if n >= k_anon else None
        dp = dp_count(n, rng, epsilon)
        conn.execute(
            "INSERT INTO active_clients "
            "(window_type, window, client_class, n_distinct_hll, "
            " n_distinct_public, n_distinct_dp, days_in_window) "
            "VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(window_type, window, client_class) DO UPDATE SET "
            " n_distinct_hll=excluded.n_distinct_hll, "
            " n_distinct_public=excluded.n_distinct_public, "
            " n_distinct_dp=excluded.n_distinct_dp, "
            " days_in_window=excluded.days_in_window",
            (window_type, wk, client, n, public, dp, len(days[(wk, client)])),
        )
        written += 1
    conn.commit()
    return written


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path,
                   default=Path(os.environ.get("SWISS_CASELAW_DIR", "output")) / "analytics.db")
    p.add_argument("--window", choices=("week", "month", "both"), default="both")
    p.add_argument("--k-anon", type=int, default=K_ANON_DEFAULT)
    p.add_argument("--epsilon", type=float, default=DP_EPSILON_DEFAULT)
    args = p.parse_args()

    if not args.db.exists():
        print(f"ERROR: {args.db} does not exist", file=sys.stderr)
        return 2

    rng = random.Random()
    conn = sqlite3.connect(str(args.db))
    try:
        wts = ("week", "month") if args.window == "both" else (args.window,)
        for wt in wts:
            n = compute_active_clients(conn, window_type=wt, rng=rng,
                                       k_anon=args.k_anon, epsilon=args.epsilon)
            print(f"  active_clients[{wt}]: wrote {n} (window,client) rows", file=sys.stderr)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
