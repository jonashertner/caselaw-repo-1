"""Gap alerts must not cry wolf, and must not prescribe a proven-useless fix.

2026-08-25: the nightly check paged with seven alerts. A full rescan of all
six named courts recovered EXACTLY ZERO decisions in 2h10m, which split them
into two kinds that the alert text did not distinguish:

  * gr_gerichte (gap 50) and vs_gerichte (gap 399) — the scraper enumerated
    every portal row and found all of them already stored. The "gap" is the
    portal's ROW count minus our DISTINCT-DECISION count, i.e. duplicate
    listings. Nothing is missing; these must stop alerting.
  * be_verwaltungsgericht (gap 2,159) — genuinely short, but a rescan cannot
    close it: recovery collapses for pre-2014 windows (2013: 82/674) while
    2017-2024 run at 97-99%. The alert kept recommending the rescan anyway.

Five of seven alerts were noise, which buried the one real signal (a SOCKS
tunnel outage). These tests pin both halves of the fix.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.check_scraper_freshness import (  # noqa: E402
    GAP_PERSIST_DAYS,
    KNOWN_GAP_OFFSETS,
    KNOWN_GAP_REMEDIES,
    check_persistent_gaps,
)


def _health(court: str, gap: int, portal: int, ours: int) -> dict:
    return {"scrapers": {court: {"gap": gap, "portal_count": portal,
                                 "our_count": ours}}}


def _persisted(tmp_path, court, days):
    """Pre-seed the state file so the gap already counts as persistent."""
    p = tmp_path / "gap_state.json"
    import json
    p.write_text(json.dumps({court: {"gap": 1, "days": days}}))
    return p


def test_structural_gaps_are_suppressed(tmp_path):
    """gr/vs were proven structural by full enumeration — no alert, ever."""
    for court, gap, portal, ours in (("gr_gerichte", 50, 14856, 14806),
                                     ("vs_gerichte", 399, 4995, 4596)):
        state = _persisted(tmp_path, court,
                           ["2026-08-20", "2026-08-21", "2026-08-22"])
        alerts = check_persistent_gaps(_health(court, gap, portal, ours),
                                       "2026-08-23", state_path=state)
        assert alerts == [], f"{court} should be silent, got {alerts}"


def test_a_gap_LARGER_than_the_known_offset_still_alerts(tmp_path):
    """Suppression is bounded by the verified number, not blanket per court.
    If gr ever loses 500 decisions we must still hear about it."""
    state = _persisted(tmp_path, "gr_gerichte",
                       ["2026-08-20", "2026-08-21", "2026-08-22"])
    alerts = check_persistent_gaps(_health("gr_gerichte", 500, 15306, 14806),
                                   "2026-08-23", state_path=state)
    assert len(alerts) == 1 and "gr_gerichte" in alerts[0]


def test_bern_gets_the_real_remedy_not_the_useless_rescan(tmp_path):
    """The rescan advice was measured not to work for Bern; the alert must
    say so and point at the actual defect instead."""
    state = _persisted(tmp_path, "be_verwaltungsgericht",
                       ["2026-08-20", "2026-08-21", "2026-08-22"])
    alerts = check_persistent_gaps(
        _health("be_verwaltungsgericht", 2159, 11594, 9435),
        "2026-08-23", state_path=state)
    assert len(alerts) == 1
    msg = alerts[0]
    assert "OCL_SCRAPER_RESCAN_ALL" not in msg, \
        "must not prescribe the rescan that was proven not to work"
    assert "#68" in msg and "2013" in msg


def test_unknown_court_still_gets_the_generic_rescan_advice(tmp_path):
    """The default path is unchanged for courts we have not investigated."""
    state = _persisted(tmp_path, "xx_gerichte",
                       ["2026-08-20", "2026-08-21", "2026-08-22"])
    alerts = check_persistent_gaps(_health("xx_gerichte", 40, 1000, 960),
                                   "2026-08-23", state_path=state)
    assert len(alerts) == 1
    assert "OCL_SCRAPER_RESCAN_ALL=1 python3 run_scraper.py xx_gerichte" in alerts[0]


def test_tunnel_courts_are_NOT_suppressed(tmp_path):
    """2026-08-25: their rescans returned +0 through a retry-exhausted tunnel,
    so a remedy entry was forbidden (a +0 that may mean 'could not fetch' is
    not evidence). SUPERSEDED 2026-09-02: clean retry-free RESCAN_ALL runs
    (0 errors, full pagination) plus 3-agent forensics named the causes —
    per-fiche identity collisions (NE, real) and upstream zero-byte PDFs (JU).
    The correct state is now: never in KNOWN_GAP_OFFSETS (the gap count must
    stay visible), but ALWAYS in KNOWN_GAP_REMEDIES (the digest must stop
    prescribing rescans measured not to work)."""
    for court in ("ju_gerichte", "ne_gerichte", "ne_jurisprudence_adm"):
        assert court not in KNOWN_GAP_OFFSETS
        assert court in KNOWN_GAP_REMEDIES
        assert "RESCAN_ALL 2026-09-02" in KNOWN_GAP_REMEDIES[court]

def test_every_suppression_carries_re_testable_evidence():
    """A suppression is a claim about production. It must record when it was
    verified and what was run, so it can be re-tested rather than trusted."""
    for court, entry in KNOWN_GAP_OFFSETS.items():
        assert isinstance(entry.get("gap"), int), court
        assert entry.get("verified"), court
        ev = entry.get("evidence", "")
        assert len(ev) > 80, f"{court}: evidence too thin to re-test"
        assert "RESCAN_ALL" in ev or "rescan" in ev.lower(), court


def test_persistence_threshold_unchanged():
    """A single day's gap must never alarm — portals double-count and
    withdraw decisions routinely."""
    assert GAP_PERSIST_DAYS >= 3
