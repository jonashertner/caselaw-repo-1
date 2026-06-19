"""Date-windowed Tribuna discovery (offline).

be_verwaltungsgericht's portal under-fills deep offset pages (offset pagination
over an unstable date sort), reaching only ~9,009 of 11,420. Discovery is
partitioned by year via field[16]="YYYY" (verified live: per-year totals sum to
the full corpus). Windows that still under-fill split finer (year→month→day) up
to DATE_WINDOW_MAX_DEPTH.

These tests drive the windowing control flow with a stubbed ``_collect_window``
(no network) — splitting decisions, portal_count accounting, since_date
pruning, and dedup.
"""
from __future__ import annotations

import sys

sys.path.insert(0, ".")

from datetime import date

from models import make_decision_id


def _scraper():
    from scrapers.cantonal.be_verwaltungsgericht import BEVerwaltungsgerichtScraper
    s = BEVerwaltungsgerichtScraper()
    s.state._seen = set()
    s.state._gaps = {}
    return s


def test_be_vg_windowing_is_configured():
    from scrapers.cantonal.be_verwaltungsgericht import BEVerwaltungsgerichtScraper as S
    assert S.DATE_WINDOW_FIELD == 16
    assert S.DATE_WINDOW_START_YEAR <= 2002


def test_override_injects_into_payload():
    s = _scraper()
    plain = s._build_search_body("cred", 0, None, "VG")
    filt = s._build_search_body("cred", 0, None, "VG", search_field_overrides={16: "2015"})
    assert "2015" not in plain
    assert "2015" in filt
    assert plain != filt


def test_year_only_walks_without_splitting(monkeypatch):
    """MAX_DEPTH=0 (the shipped default): under-filled years are walked, not split."""
    s = _scraper()
    s.DATE_WINDOW_START_YEAR = 2014
    s.DATE_WINDOW_MAX_DEPTH = 0
    calls = []

    def fake_collect(cred, court, value):
        calls.append(value)
        if value == "2015":  # under-filled: server says 1016, pagination yields 800
            return 1016, [{"docket_number": f"2015-{i}", "decision_date": "2015-06-01"} for i in range(800)]
        if value == "2014":
            return 3, [{"docket_number": f"2014-{i}", "decision_date": "2014-02-02"} for i in range(3)]
        return 0, []

    monkeypatch.setattr(s, "_collect_window", fake_collect)
    stubs = list(s._search_court("cred", "VG", since_date=None))
    assert all("-" not in c for c in calls)          # no month windows queried
    assert len(stubs) == 803                          # 800 (2015) + 3 (2014)
    assert s.portal_count == 1016 + 3                 # each year's server total, once


def test_underfilled_year_splits_into_months_when_depth_allows(monkeypatch):
    s = _scraper()
    s.DATE_WINDOW_START_YEAR = 2015
    s.DATE_WINDOW_MAX_DEPTH = 1
    calls = []

    def fake_collect(cred, court, value):
        calls.append(value)
        if value == "2015":  # under-filled and large -> should split
            return 1016, [{"docket_number": f"y{i}", "decision_date": "2015-06-01"} for i in range(800)]
        if value.startswith("2015-"):  # each month complete, 10 stubs
            return 10, [{"docket_number": f"{value}-{i}", "decision_date": f"{value}-15"} for i in range(10)]
        return 0, []

    monkeypatch.setattr(s, "_collect_window", fake_collect)
    stubs = list(s._search_court("cred", "VG", since_date=None))
    assert "2015" in calls and "2015-06" in calls     # split happened
    assert len(stubs) == 120                           # 12 months * 10 (year stubs discarded)
    assert s.portal_count == 1016                       # year counted once, months not double-counted


def test_small_underfilled_window_not_split(monkeypatch):
    """A window at/under SPLIT_OVER is walked even if under-filled (avoids needless splitting)."""
    s = _scraper()
    s.DATE_WINDOW_START_YEAR = 2015
    s.DATE_WINDOW_MAX_DEPTH = 1
    s.DATE_WINDOW_SPLIT_OVER = 150
    calls = []

    def fake_collect(cred, court, value):
        calls.append(value)
        if value == "2015":
            return 100, [{"docket_number": f"y{i}", "decision_date": "2015-06-01"} for i in range(95)]
        return 0, []

    monkeypatch.setattr(s, "_collect_window", fake_collect)
    stubs = list(s._search_court("cred", "VG", since_date=None))
    assert not any("-" in c for c in calls)            # total 100 <= 150 -> no month split
    assert len(stubs) == 95


def test_since_date_limits_start_year(monkeypatch):
    s = _scraper()
    s.DATE_WINDOW_START_YEAR = 2000
    calls = []
    monkeypatch.setattr(s, "_collect_window", lambda c, ct, v: (calls.append(v) or (0, [])))
    list(s._search_court("cred", "VG", since_date=date(2024, 1, 1)))
    assert min(int(c) for c in calls) == 2024          # never walks below since_date.year


def test_yield_new_dedups_and_skips_known():
    s = _scraper()
    s.state._seen = {make_decision_id("be_verwaltungsgericht", "KNOWN")}
    stubs = [
        {"docket_number": "A", "decision_date": "2020-01-01"},
        {"docket_number": "A", "decision_date": "2020-01-01"},   # duplicate
        {"docket_number": "KNOWN", "decision_date": "2020-01-01"},  # already held
        {"docket_number": "B", "decision_date": "2020-01-01"},
    ]
    out = list(s._yield_new(stubs, None))
    assert [x["docket_number"] for x in out] == ["A", "B"]
    assert all("decision_id" in x for x in out)


def test_yield_new_applies_since_date():
    s = _scraper()
    stubs = [
        {"docket_number": "OLD", "decision_date": "2010-01-01"},
        {"docket_number": "NEW", "decision_date": "2025-01-01"},
    ]
    out = [x["docket_number"] for x in s._yield_new(stubs, date(2024, 1, 1))]
    assert out == ["NEW"]
