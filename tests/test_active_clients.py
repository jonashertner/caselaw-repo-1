"""Tests for the active-client counter.

Proves the property the old weekly_reach columns lacked: a client active on
every day of a window with overlapping cohorts is counted ONCE (distinct), not
N times (sum_daily) — and the k-anon + DP gates still apply to public columns.
"""
from __future__ import annotations

import random
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO / "scripts"))

from analytics_hll import HLL  # noqa: E402
from active_clients import compute_active_clients, window_key  # noqa: E402


def _daily_reach_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE daily_reach (day TEXT, client_class TEXT, "
        "n_cohorts_hll_estimate INTEGER, hll_sketch TEXT)"
    )
    return conn


def _sketch(cohorts) -> str:
    h = HLL(p=12)
    for c in cohorts:
        h.add(c)
    return h.serialize()


def _within(est, truth, tol=0.10):
    return abs(est - truth) <= tol * truth


def test_window_key_week_and_month():
    assert window_key("2026-06-15", "week") == "2026-W25"
    assert window_key("2026-06-15", "month") == "2026-06"


def test_weekly_distinct_dedups_repeat_days():
    """claude_desktop: SAME 40 cohorts every day for 7 days (week 2026-W25).
    Distinct = 40, NOT 280 (sum_daily)."""
    conn = _daily_reach_db()
    same = [f"dt-{i}" for i in range(40)]
    for dom in range(15, 22):                      # 2026-06-15 .. 06-21 = W25
        conn.execute(
            "INSERT INTO daily_reach VALUES (?,?,?,?)",
            (f"2026-06-{dom}", "claude_desktop", 40, _sketch(same)),
        )
    compute_active_clients(conn, window_type="week", rng=random.Random(1))
    row = conn.execute(
        "SELECT n_distinct_hll, days_in_window FROM active_clients "
        "WHERE window='2026-W25' AND client_class='claude_desktop'"
    ).fetchone()
    assert row is not None
    n_distinct, days = row
    assert _within(n_distinct, 40), n_distinct      # union, ~40
    assert n_distinct < 100                          # NOT ~280
    assert days == 7


def test_weekly_distinct_unions_growing_cohorts():
    """chatgpt: 100 fresh cohorts/day for 7 days, no overlap → distinct ~700."""
    conn = _daily_reach_db()
    for d, dom in enumerate(range(15, 22)):
        cohorts = [f"cg-{d}-{i}" for i in range(100)]
        conn.execute("INSERT INTO daily_reach VALUES (?,?,?,?)",
                     (f"2026-06-{dom}", "chatgpt", 100, _sketch(cohorts)))
    compute_active_clients(conn, window_type="week", rng=random.Random(2))
    n = conn.execute(
        "SELECT n_distinct_hll FROM active_clients "
        "WHERE window='2026-W25' AND client_class='chatgpt'"
    ).fetchone()[0]
    assert _within(n, 700), n


def test_k_anon_gate_and_dp_populated():
    """A small client (<k_anon distinct) gets NULL public but populated dp."""
    conn = _daily_reach_db()
    conn.execute("INSERT INTO daily_reach VALUES (?,?,?,?)",
                 ("2026-06-15", "rare_client", 3, _sketch(["a", "b", "c"])))
    compute_active_clients(conn, window_type="week", rng=random.Random(3), k_anon=10)
    pub, dp, n = conn.execute(
        "SELECT n_distinct_public, n_distinct_dp, n_distinct_hll FROM active_clients "
        "WHERE client_class='rare_client'"
    ).fetchone()
    assert n == 3
    assert pub is None            # suppressed below k-anon
    assert dp is not None         # DP-noised value always present


def test_monthly_window_merges_across_weeks():
    conn = _daily_reach_db()
    # two different weeks in the same month, same client, disjoint cohorts
    for dom, tag in ((3, "early"), (25, "late")):
        cohorts = [f"{tag}-{i}" for i in range(150)]
        conn.execute("INSERT INTO daily_reach VALUES (?,?,?,?)",
                     (f"2026-06-{dom:02d}", "cursor", 150, _sketch(cohorts)))
    compute_active_clients(conn, window_type="month", rng=random.Random(4))
    n, days = conn.execute(
        "SELECT n_distinct_hll, days_in_window FROM active_clients "
        "WHERE window='2026-06' AND client_class='cursor'"
    ).fetchone()
    assert _within(n, 300), n     # 150 + 150 disjoint
    assert days == 2
