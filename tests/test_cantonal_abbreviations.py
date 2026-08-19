"""Cantonal abbreviations: the naming scheme and the guards on it.

A cantonal abbreviation is only unique inside its canton — StG is the tax
act in ZH, BE and AG, BauG the building act in BE and AG — so the canton
is part of the name, not a parameter that can be dropped. Federal law
takes no prefix: bare StG is the federal stamp-duty act.

The guards exist because two extraction approaches were tried and
measured against production before this one:

  * from the law's body text, on the theory that an act declares its
    short form as "(StG)" — but Swiss drafting writes that same form on
    first CITATION, so 6 of 12 lookups resolved to the wrong act;
  * from any parenthesis in the title, which offered "Findelkind" as the
    abbreviation of a citizenship act.

So a portal's own field is trusted, anything derived must survive the
acronym check, and nothing is ever invented.

Offline: no network, no portal.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.cantonal_laws.abbreviations import (  # noqa: E402
    harvest_titles, is_plausible_acronym, looks_like_abbreviation,
    parse_qualified, qualify, split_field,
)


# ── the naming scheme ────────────────────────────────────────────────

def test_cantonal_laws_carry_their_canton():
    assert qualify("ZH", "StG") == "ZH/StG"
    assert qualify("BE", "StG") == "BE/StG"
    assert qualify("ge", "LPA") == "GE/LPA"


def test_federal_law_takes_no_prefix():
    """The Confederation's collection is the unprefixed default, which is
    how practitioners write it — and what keeps bare StG (stamp duty)
    distinct from ZH/StG (Zurich tax)."""
    assert qualify("CH", "StG") == "StG"
    assert qualify("", "StG") == "StG"


def test_the_name_round_trips():
    for ct, ab in (("ZH", "StG"), ("BE", "BauG"), ("GE", "LPA")):
        assert parse_qualified(qualify(ct, ab)) == (ct, ab)


def test_an_unprefixed_name_means_federal():
    assert parse_qualified("StG") == (None, "StG")
    assert parse_qualified("FINMAG") == (None, "FINMAG")


def test_parsing_is_forgiving_about_case_and_spacing():
    assert parse_qualified("zh/StG") == ("ZH", "StG")
    assert parse_qualified("ZH / StG") == ("ZH", "StG")


def test_a_slash_inside_the_abbreviation_is_not_a_prefix():
    """'EV RPG' and similar carry spaces; only a two-letter head before
    the first slash is a canton."""
    assert parse_qualified("EG SchKG") == (None, "EG SchKG")


# ── what the portals actually put in that field ──────────────────────

def test_short_title_and_abbreviation_are_split():
    """Real values from the Zug portal: the field holds both."""
    assert split_field("Organisationsgesetz, OG") == ("OG", "Organisationsgesetz")
    assert split_field("PH-Gesetz, PHG") == ("PHG", "PH-Gesetz")
    assert split_field("Wahl- und Abstimmungsverordnung, WAV") == (
        "WAV", "Wahl- und Abstimmungsverordnung")


def test_a_bare_abbreviation_stays_one():
    assert split_field("KV") == ("KV", None)
    assert split_field("EG SchKG") == ("EG SchKG", None)


def test_a_long_name_is_not_mistaken_for_an_abbreviation():
    """Portals also drop full names in this field; those are short
    titles, and offering them as abbreviations would pollute lookup."""
    abbr, short = split_field("Entschädigungsverordnung")
    assert abbr is None and short == "Entschädigungsverordnung"
    assert not looks_like_abbreviation("Entschädigungsverordnung")


# ── the acronym guard on derived entries ─────────────────────────────

def test_real_acronyms_pass():
    assert is_plausible_acronym("StG", "Steuergesetz")
    assert is_plausible_acronym("PBG", "Planungs- und Baugesetz")
    assert is_plausible_acronym("GG", "Gemeindegesetz")
    assert is_plausible_acronym("LPA", "Loi sur la procédure administrative")
    assert is_plausible_acronym(
        "LATC", "Loi sur l'aménagement du territoire et les constructions")


def test_the_findelkind_case_is_rejected():
    """The parenthetical that made naive title extraction unusable."""
    assert not is_plausible_acronym(
        "Findelkind", "Gesetz über das Kantons- und das Gemeindebürgerrecht")
    assert not is_plausible_acronym("MIKA", "Vollziehungsverordnung zum Ausländerrecht")


def test_title_harvest_keeps_only_what_survives_the_guard():
    rows = [
        {"canton": "GE", "language": "fr", "sr_number": "E 5 10",
         "title": "Loi sur la procédure administrative (LPA)"},
        {"canton": "AG", "language": "de", "sr_number": "1",
         "title": "Gesetz über das Gemeindebürgerrecht (Findelkind)"},
    ]
    got = harvest_titles(rows)
    assert [r["sr_number"] for r in got] == ["E 5 10"]
    assert got[0]["qualified"] == "GE/LPA"
    assert got[0]["source"] == "title", "provenance must be recorded"


def test_every_record_records_where_it_came_from():
    """A wrong entry has to be traceable to its source, and a derived
    entry must never outrank one the canton published itself."""
    rows = [{"canton": "GE", "language": "fr", "sr_number": "E 5 10",
             "title": "Loi sur la procédure administrative (LPA)"}]
    for r in harvest_titles(rows):
        assert r["source"] in ("title", "lexwork_api")
        assert r["canton"] and r["sr_number"]
