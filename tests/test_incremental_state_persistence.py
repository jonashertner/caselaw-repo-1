"""Regression tests for the 2026-06-11 incremental-shadow findings.

The incremental builders peeked for processed-state ONLY at the live DB
(reference_graph.db / decision_structure.db) — which the legacy nightly
full publish rebuilds from scratch WITHOUT state tables. Result: every
shadow night logged bootstrap_reason="no_state" and re-bootstrapped the
full ~990k corpus (~5.6-6.2h total), racing systemd's 6h start-timeout
(runs on 06-02/05/09 were SIGTERM'd during generate_stats and never
wrote their summary line).

Fix under test: _select_diff_base prefers the PREVIOUS incremental
output (the sibling, which carries the state tables), falling back to
the live DB (the in-place path), and only then bootstrapping.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import search_stack.build_reference_graph_incremental as graph_inc  # noqa: E402
import search_stack.extract_decision_structure_incremental as struct_inc  # noqa: E402

MODULES = [graph_inc, struct_inc]


def _make_state_db(path: Path, version: str | None) -> None:
    """Create a minimal DB with (optionally) a meta.extractor_version."""
    conn = sqlite3.connect(str(path))
    if version is not None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)"
        )
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('extractor_version', ?)",
            (version,),
        )
    else:
        # A DB with no meta table at all (what the legacy full builders
        # produce).
        conn.execute("CREATE TABLE IF NOT EXISTS dummy (x INTEGER)")
    conn.commit()
    conn.close()


@pytest.mark.parametrize("mod", MODULES, ids=["graph", "structure"])
def test_peek_missing_file_returns_none(mod, tmp_path):
    assert mod._peek_extractor_version(tmp_path / "nope.db") is None


@pytest.mark.parametrize("mod", MODULES, ids=["graph", "structure"])
def test_peek_no_meta_table_returns_none(mod, tmp_path):
    db = tmp_path / "legacy.db"
    _make_state_db(db, version=None)
    assert mod._peek_extractor_version(db) is None


@pytest.mark.parametrize("mod", MODULES, ids=["graph", "structure"])
def test_peek_reads_version(mod, tmp_path):
    db = tmp_path / "state.db"
    _make_state_db(db, version="42")
    assert mod._peek_extractor_version(db) == "42"


@pytest.mark.parametrize("mod", MODULES, ids=["graph", "structure"])
def test_sibling_preferred_over_stateless_live(mod, tmp_path):
    """THE core fix: live DB exists but is stateless (nightly full
    rebuild); the sibling from the previous incremental run has valid
    state → incremental, based on the sibling."""
    live = tmp_path / "live.db"
    sibling = tmp_path / "live_incremental.db"
    _make_state_db(live, version=None)
    _make_state_db(sibling, version=mod.EFFECTIVE_EXTRACTOR_VERSION)

    base, reason = mod._select_diff_base(live, sibling, force_full=False)
    assert base == sibling
    assert reason is None


@pytest.mark.parametrize("mod", MODULES, ids=["graph", "structure"])
def test_falls_back_to_live_with_state(mod, tmp_path):
    """No sibling yet, but live DB carries state (post-cutover world or
    a manually seeded base) → use live."""
    live = tmp_path / "live.db"
    sibling = tmp_path / "live_incremental.db"  # does not exist
    _make_state_db(live, version=mod.EFFECTIVE_EXTRACTOR_VERSION)

    base, reason = mod._select_diff_base(live, sibling, force_full=False)
    assert base == live
    assert reason is None


@pytest.mark.parametrize("mod", MODULES, ids=["graph", "structure"])
def test_no_state_anywhere_bootstraps(mod, tmp_path):
    live = tmp_path / "live.db"
    sibling = tmp_path / "live_incremental.db"
    _make_state_db(live, version=None)

    base, reason = mod._select_diff_base(live, sibling, force_full=False)
    assert base is None
    assert reason == "no_state"


@pytest.mark.parametrize("mod", MODULES, ids=["graph", "structure"])
def test_version_mismatch_bootstraps_with_reason(mod, tmp_path):
    live = tmp_path / "live.db"
    sibling = tmp_path / "live_incremental.db"
    _make_state_db(sibling, version="0")  # stale extractor
    _make_state_db(live, version=None)

    base, reason = mod._select_diff_base(live, sibling, force_full=False)
    assert base is None
    assert reason is not None and reason.startswith("version_mismatch:")


@pytest.mark.parametrize("mod", MODULES, ids=["graph", "structure"])
def test_force_full_overrides_valid_state(mod, tmp_path):
    live = tmp_path / "live.db"
    sibling = tmp_path / "live_incremental.db"
    _make_state_db(sibling, version=mod.EFFECTIVE_EXTRACTOR_VERSION)

    base, reason = mod._select_diff_base(live, sibling, force_full=True)
    assert base is None
    assert reason == "force_full"


@pytest.mark.parametrize("mod", MODULES, ids=["graph", "structure"])
def test_in_place_mode_collapses_to_live_only(mod, tmp_path):
    """When output == live (in-place), the selection degenerates to the
    original single-candidate behavior."""
    live = tmp_path / "live.db"
    _make_state_db(live, version=mod.EFFECTIVE_EXTRACTOR_VERSION)

    base, reason = mod._select_diff_base(live, live, force_full=False)
    assert base == live
    assert reason is None
