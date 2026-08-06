"""Corpus-growth stall detection (scripts/check_scraper_freshness.py).

Motivation (2026-08-06): the silent-skip heuristic infers an outage from a
fast zero-new run. That inference only holds for scrapers whose runtime
scales with the corpus. zh_sozialversicherungsgericht discovers its whole
listing in ONE request (~19s) and then fetches each new decision under the
rate limit (~60s), so a zero-new day is necessarily a ~30s day. It produced
six false alarms on that basis. Exempting it would have left the court with
no outage detection at all, hence this architecture-independent check: a
live court adds decisions, whatever its scan strategy.

Thresholds are calibrated against 113 nightly runs per court, not guessed —
see STALL_TIGHT_DAYS / STALL_DEFAULT_DAYS.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.check_scraper_freshness import (  # noqa: E402
    SILENT_SKIP_EXEMPT_SOURCES,
    STALL_DEFAULT_DAYS,
    STALL_TIGHT_DAYS,
    check_stalled_corpus,
)

ZH = "zh_sozialversicherungsgericht"
ZH_LIMIT = STALL_TIGHT_DAYS[ZH]


def _health(count, court=ZH, success=True):
    return {"scrapers": {court: {"success": success, "our_count": count,
                                 "new_count": 0, "duration_s": 29}}}


def _days(n, start=1):
    out, d = [], 1
    while len(out) < n:
        out.append("2026-{:02d}-{:02d}".format(8 + (d - 1) // 28, (d - 1) % 28 + 1))
        d += 1
    return out[start - 1:] if start > 1 else out


def _prove_alive(p, court=ZH, base=34218):
    """A court must be seen growing before a flat stretch means anything."""
    check_stalled_corpus(_health(base - 1, court), "2026-07-30", p)
    check_stalled_corpus(_health(base, court), "2026-07-31", p)


def test_flat_corpus_alerts_only_after_the_full_window(tmp_path):
    p = tmp_path / "stall.json"
    _prove_alive(p)                   # last growth 2026-07-31 = flat day 1
    days = _days(ZH_LIMIT - 1)
    for d in days[:-1]:
        assert check_stalled_corpus(_health(34218), d, p) == []
    out = check_stalled_corpus(_health(34218), days[-1], p)
    assert len(out) == 1
    assert "STALL zh_sozialversicherungsgericht" in out[0]
    assert "34218" in out[0]


def test_never_growing_source_never_alerts(tmp_path):
    """mkg, weko and the anwaltsaufsicht series sat flat for 112 straight
    runs with zero growth. Claiming they 'stopped growing' would be false."""
    p = tmp_path / "stall.json"
    for d in _days(STALL_DEFAULT_DAYS + 5):
        assert check_stalled_corpus(_health(2500, court="weko"), d, p) == []
    assert json.loads(p.read_text())["weko"]["grew"] is False


def test_growth_restarts_the_clock(tmp_path):
    p = tmp_path / "stall.json"
    _prove_alive(p)
    days = _days(ZH_LIMIT + 2)
    for d in days[:-2]:
        check_stalled_corpus(_health(34218), d, p)
    assert check_stalled_corpus(_health(34219), days[-2], p) == []
    state = json.loads(p.read_text())[ZH]
    assert state["days"] == [days[-2]] and state["grew"] is True
    assert check_stalled_corpus(_health(34219), days[-1], p) == []


def test_default_window_clears_the_longest_real_quiet_spell():
    """be_zivilstraf stayed flat for 64 consecutive runs and then published
    a batch of 279. The default must sit above that, or a live court gets
    called dead."""
    assert STALL_DEFAULT_DAYS > 64


def test_tight_window_is_far_above_the_courts_own_rhythm():
    """ZH SVG's longest observed zero-growth streak was 3 runs in 113."""
    assert ZH_LIMIT >= 12


def test_same_day_rechecks_do_not_accelerate_the_alert(tmp_path):
    p = tmp_path / "stall.json"
    _prove_alive(p)
    for _ in range(ZH_LIMIT + 5):
        assert check_stalled_corpus(_health(34218), "2026-08-01", p) == []


def test_small_corpora_are_not_judged_this_way(tmp_path):
    """An 881-row chamber series legitimately publishes nothing for months."""
    p = tmp_path / "stall.json"
    for d in _days(STALL_DEFAULT_DAYS + 5):
        assert check_stalled_corpus(
            _health(881, court="zh_steuerrekursgericht"), d, p) == []


def test_dead_and_tolerated_sources_are_skipped(tmp_path):
    p = tmp_path / "stall.json"
    for court in ("be_steuerrekurs", "ecthr"):
        for d in _days(STALL_DEFAULT_DAYS + 5):
            assert check_stalled_corpus(_health(50000, court=court), d, p) == []


def test_failed_runs_do_not_accumulate_stall_days(tmp_path):
    """A failing scraper is already alerted as FAIL; do not double-report."""
    p = tmp_path / "stall.json"
    _prove_alive(p)
    for d in _days(ZH_LIMIT + 5):
        assert check_stalled_corpus(_health(34218, success=False), d, p) == []


def test_missing_or_broken_state_file_is_tolerated(tmp_path):
    p = tmp_path / "sub" / "stall.json"
    assert check_stalled_corpus(_health(34218), "2026-08-01", p) == []
    p.write_text("{ not json")
    assert check_stalled_corpus(_health(34218), "2026-08-02", p) == []


def test_health_without_counts_is_ignored(tmp_path):
    p = tmp_path / "stall.json"
    h = {"scrapers": {"x": {"success": True, "our_count": None},
                      "y": {"success": True}}}
    for d in _days(STALL_DEFAULT_DAYS + 5):
        assert check_stalled_corpus(h, d, p) == []


def test_zh_sozialversicherungsgericht_is_exempt_from_the_duration_heuristic():
    """Its runtime is 20s + 60s per new decision — a zero-new day is a ~30s
    day by construction, so duration carries no signal about portal health.
    Verified 2026-08-06: the flagged 2026-08-04 run had discovery return
    34,243 rows against 34,245 known IDs."""
    assert ZH in SILENT_SKIP_EXEMPT_SOURCES
