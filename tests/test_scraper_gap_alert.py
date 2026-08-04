"""Persistent coverage-gap alerting (scripts/check_scraper_freshness.py).

Motivation (2026-08-03): sz_gerichte reported "[OK] … gap 51" every night
for three weeks. The health file carried the number the whole time and
nothing read it, because a successful run was treated as sufficient
evidence of coverage. It is not: the newest-first scan stops after 200
consecutive known decisions, so older shortfalls are unreachable and stay
invisible behind a green run.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.check_scraper_freshness import check_persistent_gaps  # noqa: E402


def _health(gap, court="sz_gerichte", ours=3364, portal=3415):
    return {"scrapers": {court: {"success": True, "gap": gap,
                                 "our_count": ours, "portal_count": portal}}}


def test_single_day_gap_is_not_alarmed(tmp_path):
    p = tmp_path / "gap.json"
    assert check_persistent_gaps(_health(51), "2026-08-01", p) == []


def test_gap_alerts_only_after_persisting(tmp_path):
    # a court without a verified offset (sz_gerichte is allowlisted, see
    # test_confirmed_structural_offset_does_not_alarm)
    p = tmp_path / "gap.json"
    h = _health(51, court="ag_zivilgericht")
    assert check_persistent_gaps(h, "2026-08-01", p) == []
    assert check_persistent_gaps(h, "2026-08-02", p) == []
    out = check_persistent_gaps(h, "2026-08-03", p)
    assert len(out) == 1
    assert "GAP ag_zivilgericht" in out[0]
    assert "51 Entscheide" in out[0]
    assert "OCL_SCRAPER_RESCAN_ALL=1" in out[0]   # names the remedy


def test_same_day_rechecks_do_not_accelerate_the_alert(tmp_path):
    p = tmp_path / "gap.json"
    for _ in range(5):
        assert check_persistent_gaps(
            _health(51, court="ag_zivilgericht"), "2026-08-01", p) == []


def test_small_gap_is_ignored(tmp_path):
    p = tmp_path / "gap.json"
    for d in ("2026-08-01", "2026-08-02", "2026-08-03"):
        assert check_persistent_gaps(_health(2), "2026-08-02", p) == []
        assert check_persistent_gaps(_health(2), d, p) == []


def test_closed_gap_clears_the_state(tmp_path):
    p = tmp_path / "gap.json"
    check_persistent_gaps(_health(51, court="ag_zivilgericht"), "2026-08-01", p)
    check_persistent_gaps(_health(51, court="ag_zivilgericht"), "2026-08-02", p)
    # catch-up run closes it
    assert check_persistent_gaps(_health(0, court="ag_zivilgericht"),
                                 "2026-08-03", p) == []
    assert json.loads(p.read_text()) == {}
    # and the counter starts from zero if it ever reappears
    assert check_persistent_gaps(_health(51, court="ag_zivilgericht"),
                                 "2026-08-04", p) == []


def test_missing_or_broken_state_file_is_tolerated(tmp_path):
    p = tmp_path / "sub" / "gap.json"
    assert check_persistent_gaps(_health(51, court="ag_zivilgericht"),
                                 "2026-08-01", p) == []
    p.write_text("{ not json")
    assert check_persistent_gaps(_health(51, court="ag_zivilgericht"),
                                 "2026-08-02", p) == []


def test_health_without_gap_field_is_ignored(tmp_path):
    p = tmp_path / "gap.json"
    h = {"scrapers": {"x": {"success": True, "gap": None},
                      "y": {"success": True}}}
    assert check_persistent_gaps(h, "2026-08-01", p) == []


def test_confirmed_structural_offset_does_not_alarm(tmp_path):
    """sz_gerichte's 51 survived a full rescan that walked every portal
    page and returned nothing new — the portal simply lists more rows than
    it yields distinct decisions. Alerting on it nightly would be crying
    wolf, so a verified offset is allowed through."""
    p = tmp_path / "gap.json"
    for d in ("2026-08-05", "2026-08-06", "2026-08-07", "2026-08-08"):
        assert check_persistent_gaps(_health(51), d, p) == []


def test_growth_beyond_the_allowance_still_alarms(tmp_path):
    """The allowance is a ceiling, not a blanket: if the gap grows past
    the confirmed offset, that is new missing content and must alert."""
    p = tmp_path / "gap.json"
    for d in ("2026-08-05", "2026-08-06"):
        assert check_persistent_gaps(_health(80), d, p) == []
    out = check_persistent_gaps(_health(80), "2026-08-07", p)
    assert len(out) == 1 and "GAP sz_gerichte: 80" in out[0]


def test_allowance_is_court_specific(tmp_path):
    p = tmp_path / "gap.json"
    for d in ("2026-08-05", "2026-08-06"):
        check_persistent_gaps(_health(51, court="zh_obergericht"), d, p)
    out = check_persistent_gaps(_health(51, court="zh_obergericht"),
                                "2026-08-07", p)
    assert len(out) == 1 and "zh_obergericht" in out[0]
