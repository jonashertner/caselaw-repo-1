"""Defect I (2026-09-03 review): scrapers/fedlex.py broke multi-work SR
numbers on ELI year alone. SR 956.1 (FINMAG) has the in-force act
eli/cc/2008/736 and its repealed predecessor eli/cc/2008/68; both are
"2008", the sort was stable, and the repealed PDF-only work won on row
order, so FINMAG was absent from production. 36 SRs tie on year, 7 were
decided by row order.

select_work() now ranks by in-force status first. All offline: the SPARQL
call is stubbed at fedlex.sparql_query.
"""
from __future__ import annotations

import logging

from scrapers import fedlex

IN_FORCE = "https://fedlex.data.admin.ch/vocabulary/enforcement-status/0"
REPEALED = "https://fedlex.data.admin.ch/vocabulary/enforcement-status/3"


def _row(work: str, status: str | None = None, latest: str = "2025-01-01", eif: str | None = None) -> dict:
    row = {"work": work, "srNumber": "956.1", "latestDate": latest}
    if status:
        row["inForceStatus"] = status
    if eif:
        row["dateEntryInForce"] = eif
    return row


FINMAG_LIVE = _row("https://fedlex.data.admin.ch/eli/cc/2008/736", IN_FORCE, "2025-04-01", "2009-01-01")
FINMAG_DEAD = _row("https://fedlex.data.admin.ch/eli/cc/2008/68", REPEALED, "2024-03-03", "2008-02-01")


def test_status_rank():
    assert fedlex._status_rank(FINMAG_LIVE) == 0
    assert fedlex._status_rank(_row("w")) == 1
    assert fedlex._status_rank(FINMAG_DEAD) == 2
    assert fedlex._status_rank(_row("w", REPEALED + "/")) == 2


def test_tied_year_in_force_second_in_input_wins():
    # The FINMAG case: repealed first in row order, both 2008.
    assert fedlex.select_work([FINMAG_DEAD, FINMAG_LIVE]) is FINMAG_LIVE
    assert fedlex.select_work([FINMAG_LIVE, FINMAG_DEAD]) is FINMAG_LIVE


def test_in_force_beats_newer_repealed_work():
    dead_newer = _row("https://fedlex.data.admin.ch/eli/cc/2020/1", REPEALED, "2025-01-01")
    live_older = _row("https://fedlex.data.admin.ch/eli/cc/1999/404", IN_FORCE, "2024-03-03")
    assert fedlex.select_work([dead_newer, live_older]) is live_older


def test_unknown_status_beats_repealed_and_loses_to_in_force():
    unknown = _row("https://fedlex.data.admin.ch/eli/cc/1990/5", None, "2020-01-01")
    dead = _row("https://fedlex.data.admin.ch/eli/cc/2015/9", REPEALED, "2025-01-01")
    live = _row("https://fedlex.data.admin.ch/eli/cc/1980/2", IN_FORCE, "2010-01-01")
    assert fedlex.select_work([dead, unknown]) is unknown
    assert fedlex.select_work([unknown, dead, live]) is live


def test_year_decides_among_in_force_works():
    # BV 1999 replaces BV 1874 (eli/cc/1/...): both tagged in force here
    old_bv = _row("https://fedlex.data.admin.ch/eli/cc/1/1_1_1", IN_FORCE, "2021-11-28")
    new_bv = _row("https://fedlex.data.admin.ch/eli/cc/1999/404", IN_FORCE, "2024-03-03")
    assert fedlex.select_work([old_bv, new_bv]) is new_bv
    assert fedlex.select_work([new_bv, old_bv]) is new_bv


def test_entry_into_force_breaks_year_ties_before_consolidation_date():
    """Same status and year: the act that entered into force last wins even
    when the older one has a more recent consolidation. SR 818.101.24 is the
    live case: three repealed Covid ordinances, Verordnung 3 (eif 2020-06-22,
    has XML) must beat Verordnung 2 (eif 2020-03-13, consolidated 2026, no XML)."""
    v2 = _row("https://fedlex.data.admin.ch/eli/cc/2020/141", REPEALED, "2026-06-24", "2020-03-13")
    v2["dateNoLongerInForce"] = "2020-06-22"
    v3 = _row("https://fedlex.data.admin.ch/eli/cc/2020/438", REPEALED, "2020-06-22", "2020-06-22")
    assert fedlex.select_work([v2, v3]) is v3
    assert fedlex.select_work([v3, v2]) is v3
    a = _row("https://fedlex.data.admin.ch/eli/cc/2008/1", IN_FORCE, "2026-01-01", "2008-01-01")
    b = _row("https://fedlex.data.admin.ch/eli/cc/2008/2", IN_FORCE, "2024-01-01", "2009-01-01")
    assert fedlex.select_work([a, b]) is b


def test_latest_date_breaks_remaining_ties_then_input_order():
    a = _row("https://fedlex.data.admin.ch/eli/cc/2008/1", IN_FORCE, "2023-01-01")
    b = _row("https://fedlex.data.admin.ch/eli/cc/2008/2", IN_FORCE, "2024-06-01")
    assert fedlex.select_work([a, b]) is b
    assert fedlex.select_work([b, a]) is b
    c = dict(a, work="https://fedlex.data.admin.ch/eli/cc/2008/3")
    assert fedlex.select_work([a, c]) is a  # full tie: first candidate


def test_single_candidate_returned():
    assert fedlex.select_work([FINMAG_LIVE]) is FINMAG_LIVE


def test_discover_query_selects_status_and_entry_into_force(monkeypatch):
    captured: list[str] = []

    def fake_sparql(query, timeout=120):
        captured.append(query)
        return []

    monkeypatch.setattr(fedlex, "sparql_query", fake_sparql)
    assert fedlex.discover_laws() == []
    q = captured[0]
    assert "OPTIONAL { ?work jolux:inForceStatus ?inForceStatus }" in q
    assert "OPTIONAL { ?work jolux:dateEntryInForce ?dateEntryInForce }" in q
    select = q.split("WHERE")[0]
    assert "?inForceStatus" in select and "?dateEntryInForce" in select
    group_by = q.split("GROUP BY")[1]
    assert "?inForceStatus" in group_by and "?dateEntryInForce" in group_by


def test_docstring_no_longer_claims_latest_date_wins():
    doc = fedlex.discover_laws.__doc__
    assert "select_work" in doc and "in-force" in doc
    assert "latest consolidation\n    date wins" not in doc


# ── the "tiebreak wrong" signal ──────────────────────────────────────────────

def test_error_logged_when_discarded_candidate_has_xml(caplog):
    loser = dict(FINMAG_DEAD, consolidation_uri="https://fedlex.data.admin.ch/eli/cc/2008/68/20240303")
    xml_urls = {loser["consolidation_uri"]: {"de": "https://x/de.xml", "fr": "https://x/fr.xml"}}
    with caplog.at_level(logging.ERROR, logger="fedlex"):
        flagged = fedlex._report_multi_sr_losers("956.1", FINMAG_LIVE, [loser], xml_urls)
    assert flagged == [loser]
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    msg = errors[0].getMessage()
    assert "SR 956.1" in msg
    assert "eli/cc/2008/736" in msg and "status=in-force" in msg
    assert "eli/cc/2008/68" in msg and "status=repealed" in msg
    assert "de, fr" in msg


def test_no_error_when_losers_have_no_xml_either(caplog):
    loser = dict(FINMAG_DEAD, consolidation_uri="https://fedlex.data.admin.ch/eli/cc/2008/68/20240303")
    with caplog.at_level(logging.ERROR, logger="fedlex"):
        assert fedlex._report_multi_sr_losers("956.1", FINMAG_LIVE, [loser], {}) == []
        assert fedlex._report_multi_sr_losers("956.1", FINMAG_LIVE, [dict(FINMAG_DEAD)], {"x": {"de": "u"}}) == []
    assert not [r for r in caplog.records if r.levelno == logging.ERROR]


def test_passed_end_date_beats_a_stale_in_force_flag():
    """Release gate 2026-09-03: Fedlex leaves inForceStatus at /0 on superseded
    ordinances while the successor carries no flag; nine live SRs (641.811.912,
    747.224.211/221, 0.748.127.19x.xx, ...) would have flipped back to the
    predecessor. The passed dateNoLongerInForce must decide."""
    old = _row("https://fedlex.data.admin.ch/eli/cc/2006/94", IN_FORCE, "2024-10-21", "2006-01-01")
    old["dateNoLongerInForce"] = "2024-10-21"
    new = _row("https://fedlex.data.admin.ch/eli/cc/2025/81", None, "2024-10-21", "2024-10-21")
    assert fedlex._status_rank(old, today="2026-09-03") == 2
    assert fedlex._status_rank(new, today="2026-09-03") == 1
    assert fedlex.select_work([old, new], today="2026-09-03") is new
    assert fedlex.select_work([new, old], today="2026-09-03") is new
    # Up to the day before the changeover the old ordinance is the one to serve.
    assert fedlex._status_rank(old, today="2024-10-20") == 0
    assert fedlex.select_work([old, new], today="2024-10-20") is old
    # A typed literal with a time part still compares as a date.
    old["dateNoLongerInForce"] = "2024-10-21T00:00:00"
    assert fedlex._status_rank(old, today="2024-10-21") == 2


def test_discover_query_selects_no_longer_in_force(monkeypatch):
    captured = {}

    def fake(query, timeout=600):
        captured["q"] = query
        return []

    monkeypatch.setattr(fedlex, "sparql_query", fake)
    fedlex.discover_laws()
    q = captured["q"]
    assert "?dateNoLongerInForce" in q.split("WHERE")[0]
    assert "OPTIONAL { ?work jolux:dateNoLongerInForce ?dateNoLongerInForce }" in q
    assert "GROUP BY ?work ?srNumber ?inForceStatus ?dateEntryInForce ?dateNoLongerInForce" in q
