"""Tests for the mergeable HLL behind the active-client counter.

The load-bearing property: merging the daily sketches of a client that appears
on many days with overlapping cohorts yields the DISTINCT count (set union),
not the sum — which is exactly what makes a true windowed active-client metric
possible (and what the old sum_daily column got wrong).
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO / "scripts"))

from analytics_hll import HLL  # noqa: E402


def _within(est, truth, tol=0.05):
    return abs(est - truth) <= tol * truth


def test_estimate_is_accurate_for_known_cardinality():
    h = HLL(p=12)
    for i in range(5000):
        h.add(f"cohort-{i}")
    assert _within(h.estimate(), 5000), h.estimate()


def test_serialize_roundtrip_preserves_estimate():
    h = HLL(p=12)
    for i in range(3000):
        h.add(f"c{i}")
    blob = h.serialize()
    assert blob.startswith("12:")
    h2 = HLL.deserialize(blob)
    assert h2.registers == h.registers
    assert h2.estimate() == h.estimate()


def test_merge_is_set_union_not_sum():
    # Two days, 1000 cohorts each, 500 shared → true distinct = 1500.
    a = HLL(p=12)
    b = HLL(p=12)
    for i in range(1000):
        a.add(f"u{i}")            # u0..u999
    for i in range(500, 1500):
        b.add(f"u{i}")            # u500..u1499  (overlap u500..u999)
    a.merge(b)
    assert _within(a.estimate(), 1500), a.estimate()       # union, ~1500
    # A naive sum of the two daily estimates would be ~2000 — the bug we fix.
    assert a.estimate() < 1800


def test_union_of_serialized_sketches_dedups_repeat_days():
    # Same client, SAME 200 cohorts every day for 7 days. Distinct = 200,
    # but sum_daily would report 1400.
    daily = []
    for _day in range(7):
        h = HLL(p=12)
        for i in range(200):
            h.add(f"stable-{i}")
        daily.append(h.serialize())
    merged = HLL.union(daily)
    assert _within(merged.estimate(), 200, tol=0.08), merged.estimate()


def test_union_handles_empty_and_none():
    assert HLL.union([]).estimate() == 0
    h = HLL(p=12)
    for i in range(100):
        h.add(f"x{i}")
    assert _within(HLL.union([None, "", h.serialize()]).estimate(), 100, tol=0.1)


def test_merge_rejects_mismatched_precision():
    import pytest
    with pytest.raises(ValueError):
        HLL(p=12).merge(HLL(p=10))
