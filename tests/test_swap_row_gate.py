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
