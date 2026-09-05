"""KNOWN_GAP_OFFSETS: structural portal-vs-ours differences that a full
rescan has proven not to be a backlog. Each entry must carry its evidence so
the claim can be re-tested; the persistent-gap alert must stay quiet while
the measured gap does not grow past it."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.check_scraper_freshness import (  # noqa: E402
    GAP_PERSIST_DAYS, KNOWN_GAP_OFFSETS, check_persistent_gaps,
)


def test_every_known_offset_is_evidenced():
    for court, entry in KNOWN_GAP_OFFSETS.items():
        assert isinstance(entry["gap"], int) and entry["gap"] > 0, court
        assert entry["verified"] >= "2026-08-01", court
        ev = entry["evidence"]
        assert "RESCAN_ALL" in ev, court
        assert "+0" in ev or "0 new" in ev, court      # the rescan returned nothing


def test_fribourg_offset_matches_the_2026_09_05_rescan():
    fr = KNOWN_GAP_OFFSETS["fr_gerichte"]
    assert fr["gap"] == 11
    assert fr["verified"] == "2026-09-05"
    assert "14685" in fr["evidence"] and "14674" in fr["evidence"]


def _health(gap: int) -> dict:
    return {"scrapers": {"fr_gerichte": {
        "success": True, "gap": gap, "our_count": 14685 - gap, "portal_count": 14685}}}


def test_persistent_gap_stays_quiet_at_the_known_offset(tmp_path):
    state = tmp_path / "gap_state.json"
    alerts = []
    for i in range(GAP_PERSIST_DAYS + 2):
        alerts += check_persistent_gaps(_health(11), f"2026-09-{5 + i:02d}", state_path=state)
    assert alerts == []


def test_persistent_gap_alerts_once_it_grows_past_the_offset(tmp_path):
    state = tmp_path / "gap_state.json"
    alerts = []
    for i in range(GAP_PERSIST_DAYS + 1):
        alerts += check_persistent_gaps(_health(12), f"2026-09-{5 + i:02d}", state_path=state)
    assert any("fr_gerichte" in a and "12" in a for a in alerts)
