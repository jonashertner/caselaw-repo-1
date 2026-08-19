"""A cantonal abbreviation that misses must still answer.

Measured 2026-08-19: `laws` is the highest-volume tool and answers 68.7%,
with id_not_found dominating the misses. The cause is structural —
cantonal_laws.db has no abbreviation column at all, so all 15,608
cantonal laws are unreachable by /api/laws/{abbreviation}, the primary
lookup path. The resolver matches only the TITLE, which works for laws
that spell their short form out ("Gerichtsorganisationsgesetz (GOG)")
and fails for the ones that do not: Zurich's tax act is titled plainly
"Steuergesetz", so StG misses although the law is right there.

Where a name is still missing, a miss at least hands back
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


# ── the canton-prefixed name ─────────────────────────────────────────

def test_a_prefixed_name_splits_into_canton_and_abbreviation():
    assert m.split_qualified_law_name("ZH/StG") == ("ZH", "StG")
    assert m.split_qualified_law_name("be/BauG") == ("BE", "BauG")


def test_an_unprefixed_name_is_federal():
    """Bare StG is the federal stamp-duty act; the Confederation's
    collection is the unprefixed default."""
    assert m.split_qualified_law_name("StG") == (None, "StG")
    assert m.split_qualified_law_name("FINMAG") == (None, "FINMAG")


def test_an_abbreviation_containing_a_space_is_not_split():
    """'EG SchKG' is one name. Only a two-letter head before a slash is
    a canton."""
    assert m.split_qualified_law_name("EG SchKG") == (None, "EG SchKG")


def test_the_prefix_decides_the_jurisdiction(monkeypatch):
    """ZH/StG must reach Zurich even when the caller left canton at its
    CH default — otherwise the name and the parameter can disagree and
    the user silently gets a federal act."""
    seen = {}
    monkeypatch.setattr(m, "_get_law_cantonal",
                        lambda sr, ab, art, lang, ct: seen.update(
                            {"canton": ct, "abbr": ab}) or {"ok": True})
    m.get_law(abbreviation="ZH/StG")
    assert seen == {"canton": "ZH", "abbr": "StG"}


def test_published_names_outrank_derived_ones():
    """A name the canton published must win over one we inferred, so a
    guess can never shadow the canton's own answer."""
    import sqlite3
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.execute("""CREATE TABLE law_names (canton TEXT, language TEXT,
                 sr_number TEXT, name TEXT, name_folded TEXT,
                 name_type TEXT, qualified TEXT, source TEXT)""")
    c.execute("INSERT INTO law_names VALUES "
              "('ZH','de','999.9','StG','stg','abbreviation','ZH/StG','title')")
    c.execute("INSERT INTO law_names VALUES "
              "('ZH','de','631.1','StG','stg','abbreviation','ZH/StG','lexwork_api')")
    assert m._cantonal_sr_from_name(c, "ZH", "StG", "de") == "631.1"


def test_short_title_also_resolves():
    """Portals pack 'Organisationsgesetz, OG' into one field; a reader
    may type either half."""
    import sqlite3
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.execute("""CREATE TABLE law_names (canton TEXT, language TEXT,
                 sr_number TEXT, name TEXT, name_folded TEXT,
                 name_type TEXT, qualified TEXT, source TEXT)""")
    c.execute("INSERT INTO law_names VALUES "
              "('ZG','de','151.1','OG','og','abbreviation','ZG/OG','lexwork_api')")
    c.execute("INSERT INTO law_names VALUES "
              "('ZG','de','151.1','Organisationsgesetz','organisationsgesetz',"
              "'short_title','ZG/Organisationsgesetz','lexwork_api')")
    assert m._cantonal_sr_from_name(c, "ZG", "OG", "de") == "151.1"
    assert m._cantonal_sr_from_name(c, "ZG", "Organisationsgesetz", "de") == "151.1"


def test_missing_table_does_not_break_the_lookup():
    """An older mirror has no law_names table; it must keep serving."""
    import sqlite3
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    assert m._cantonal_sr_from_name(c, "ZH", "StG", "de") is None


def test_a_law_without_an_abbreviation_is_still_reachable():
    """The 60% that have no short form at all. A full title carries the
    canton prefix just as well, so ZH/Steuergesetz resolves even when no
    abbreviation was ever published."""
    import sqlite3
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.execute("""CREATE TABLE law_names (canton TEXT, language TEXT,
                 sr_number TEXT, name TEXT, name_folded TEXT,
                 name_type TEXT, qualified TEXT, source TEXT)""")
    c.execute("INSERT INTO law_names VALUES ('ZH','de','631.1','Steuergesetz','steuergesetz',"
              "'title','ZH/Steuergesetz','corpus_title')")
    assert m._cantonal_sr_from_name(c, "ZH", "Steuergesetz", "de") == "631.1"
    assert m._cantonal_sr_from_name(c, "ZH", "steuergesetz", "de") == "631.1"


def test_a_short_name_wins_over_a_title_for_the_same_string():
    """If one string is both an abbreviation of law A and the title of
    law B, the abbreviation is what the caller meant."""
    import sqlite3
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.execute("""CREATE TABLE law_names (canton TEXT, language TEXT,
                 sr_number TEXT, name TEXT, name_folded TEXT,
                 name_type TEXT, qualified TEXT, source TEXT)""")
    c.execute("INSERT INTO law_names VALUES ('ZH','de','111.1','GG','gg','title','ZH/GG','corpus_title')")
    c.execute("INSERT INTO law_names VALUES ('ZH','de','222.2','GG','gg','abbreviation','ZH/GG','lexwork_api')")
    assert m._cantonal_sr_from_name(c, "ZH", "GG", "de") == "222.2"
