"""Pre-swap row-count gate (build_fts5).

The full-rebuild atomic swap had no guard: an empty / partial / corrupt
build (the 2026-05 ENOSPC + WAL-corruption incidents produced near-empty
.tmp builds) would os.replace() the healthy production DB, and the
zero-row check only ran AFTER the swap — workers already serving the bad
inode. _check_swap_row_gate refuses a swap that drops the corpus below
95% of the live row count.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import build_fts5  # noqa: E402


def test_blocks_catastrophic_shrink():
    with pytest.raises(RuntimeError, match="refusing to swap"):
        build_fts5._check_swap_row_gate(900_000, 990_000)  # 90.9% < 95%


def test_blocks_empty_build():
    with pytest.raises(RuntimeError):
        build_fts5._check_swap_row_gate(0, 990_000)


def test_allows_growth():
    build_fts5._check_swap_row_gate(990_500, 990_000)  # no raise


def test_allows_small_shrink_within_tolerance():
    # 96% retained — legitimate (e.g. a modest dedup pass)
    build_fts5._check_swap_row_gate(int(990_000 * 0.96), 990_000)


def test_boundary_at_95_percent():
    # exactly 95% passes; just under fails
    build_fts5._check_swap_row_gate(int(990_000 * 0.95) + 1, 990_000)
    with pytest.raises(RuntimeError):
        build_fts5._check_swap_row_gate(int(990_000 * 0.95) - 100, 990_000)


def test_noop_when_no_live_db():
    # first build ever (no live DB) — never blocks
    build_fts5._check_swap_row_gate(5, 0)
    build_fts5._check_swap_row_gate(0, 0)


def test_override_env(monkeypatch):
    monkeypatch.setenv("OCL_SKIP_SWAP_GATE", "1")
    build_fts5._check_swap_row_gate(1, 990_000)  # overridden — no raise


# ── Per-court pre-swap gate ──────────────────────────────────
# The global gate above only sees the corpus total; a per-court collapse (the
# SG alphabetical dedup-collision: ~90% loss of a chamber, ~-181 rows net) is
# <5% of 990k and sails through it. _check_swap_per_court_gate is the floor for
# that class. Calibration 2026-06-16: max legitimate per-court drop on a court
# ≥500 rows across the last 10 snapshots was -0.0%, so 0.80 won't false-trip.


def test_per_court_collapse_blocks():
    # SG-collision class: a large chamber loses ~91%
    live = {"bger": 190_000, "sg_kantonsgericht": 7_559, "ti_gerichte": 59_000}
    new = {"bger": 190_050, "sg_kantonsgericht": 680, "ti_gerichte": 59_100}
    with pytest.raises(RuntimeError, match="per-court gate"):
        build_fts5._check_swap_per_court_gate(new, live)


def test_per_court_small_court_drop_allowed():
    # sub-500-row micro-courts can drop freely (below min_live_rows)
    live = {"bger": 190_000, "zh_mietgericht": 1, "tg_anwaltskommission": 5}
    new = {"bger": 190_000, "zh_mietgericht": 0, "tg_anwaltskommission": 0}
    build_fts5._check_swap_per_court_gate(new, live)  # no raise


def test_per_court_growth_allowed():
    build_fts5._check_swap_per_court_gate(
        {"bger": 191_000, "ti_gerichte": 60_000},
        {"bger": 190_000, "ti_gerichte": 59_000})


def test_per_court_within_tolerance_passes():
    # 85% retained on a large court — legitimate churn, clears the 0.80 floor
    build_fts5._check_swap_per_court_gate({"bger": int(190_000 * 0.85)}, {"bger": 190_000})


def test_per_court_boundary_80pct():
    build_fts5._check_swap_per_court_gate({"bger": 80_001}, {"bger": 100_000})  # >80% passes
    with pytest.raises(RuntimeError):
        build_fts5._check_swap_per_court_gate({"bger": 79_000}, {"bger": 100_000})  # <80% fails


def test_per_court_new_court_ignored():
    # a court present only in the new build is not a regression
    build_fts5._check_swap_per_court_gate(
        {"bger": 190_000, "new_court": 1_200}, {"bger": 190_000})


def test_per_court_whole_court_vanish_blocks():
    # an entire large court dropping to zero is the catastrophic case
    with pytest.raises(RuntimeError, match="vd_gerichte"):
        build_fts5._check_swap_per_court_gate(
            {"bger": 190_000}, {"bger": 190_000, "vd_gerichte": 53_000})


def test_per_court_noop_when_no_live_db():
    build_fts5._check_swap_per_court_gate({"bger": 5}, {})  # first build ever — no raise


def test_per_court_override_env(monkeypatch):
    monkeypatch.setenv("OCL_SKIP_SWAP_GATE", "1")
    build_fts5._check_swap_per_court_gate({"bger": 0}, {"bger": 190_000})  # overridden
