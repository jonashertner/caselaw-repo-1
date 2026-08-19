"""A cantonal abbreviation that misses must still answer.

Measured 2026-08-19: `laws` is the highest-volume tool and answers 68.7%,
with id_not_found dominating the misses. The cause is structural —
cantonal_laws.db has no abbreviation column at all, so all 15,608
cantonal laws are unreachable by /api/laws/{abbreviation}, the primary
lookup path. The resolver matches only the TITLE, which works for laws
that spell their short form out ("Gerichtsorganisationsgesetz (GOG)")
and fails for the ones that do not: Zurich's tax act is titled plainly
"Steuergesetz", so StG misses although the law is right there.

Until the abbreviations are sourced properly, a miss at least hands back
real candidates from the same canton instead of a dead end.

What is NOT done here is worth recording. An earlier attempt resolved
the abbreviation from the law's own text, on the theory that an act
declares its short form as "(StG)". Swiss drafting writes the same
bracketed form on first CITATION, so against production data 6 of 12
lookups "resolved" and nearly all were the wrong act — StG in ZH gave
the Finanzausgleichsverordnung. Offering candidates the caller chooses
between is honest; asserting one of them is not, and in a legal corpus a
confident wrong statute is worse than no answer.

The canton is load-bearing throughout. StG is the tax act in ZH, BE and
AG and the federal stamp-duty act as well, so nothing may be identified
by abbreviation alone.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


# ── candidates when the abbreviation misses ──────────────────────────

def test_candidates_come_from_the_requested_canton(monkeypatch):
    """Scoped to one canton, because StG is the tax act in ZH, BE and AG
    and the federal stamp-duty act as well."""
    seen = {}
    def fake(**kw):
        seen.update(kw)
        return {"results": [{"sr_number": "631.1", "title": "Steuergesetz"}]}
    monkeypatch.setattr(m, "search_laws", fake)
    out = m._cantonal_candidates("ZH", "StG", "de")
    assert seen["canton"] == "ZH" and seen["jurisdiction"] == "cantonal"
    assert out[0]["canton"] == "ZH"


def test_every_candidate_is_canton_qualified(monkeypatch):
    monkeypatch.setattr(m, "search_laws", lambda **kw: {"results": [
        {"sr_number": "631.1", "title": "Steuergesetz"},
        {"systematic_number": "700.1", "title": "Planungs- und Baugesetz"},
    ]})
    for c in m._cantonal_candidates("ZH", "StG", "de"):
        assert c["key"] == "ZH/" + c["sr_number"], "number must carry its canton"


def test_rows_without_a_number_are_dropped(monkeypatch):
    """A candidate the caller cannot re-request is not a candidate."""
    monkeypatch.setattr(m, "search_laws", lambda **kw: {"results": [
        {"title": "no number here"}, {"sr_number": "631.1", "title": "Steuergesetz"}]})
    out = m._cantonal_candidates("ZH", "StG", "de")
    assert len(out) == 1 and out[0]["sr_number"] == "631.1"


def test_bounded(monkeypatch):
    monkeypatch.setattr(m, "search_laws", lambda **kw: {"results": [
        {"sr_number": str(i), "title": "x"} for i in range(20)]})
    assert len(m._cantonal_candidates("ZH", "StG", "de", limit=3)) == 3


def test_never_raises(monkeypatch):
    """Telemetry-grade robustness: a failing search must degrade to no
    candidates, never break the lookup that called it."""
    def boom(**kw):
        raise RuntimeError("search is down")
    monkeypatch.setattr(m, "search_laws", boom)
    assert m._cantonal_candidates("ZH", "StG", "de") == []
    assert m._cantonal_candidates("ZH", "", "de") == []
