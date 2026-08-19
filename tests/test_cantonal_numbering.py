"""Systematic numbers: the key a practitioner cites, parsed off the title.

Cantonal registers publish "101 Constitution...", "101.000 Costituzione
..." or Geneva's "A 1 01 Acte d'union...". Two scrapers failed to split
the number off: sil.py matched a numeric-only pattern, which Geneva's
alphanumeric RSG numbers never satisfy, so 864 GE laws kept the source
filename (rsg_a1_01); ti.py fell back to the row's index position, so
623 TI laws were numbered 1..623. Neither is citable and neither
resolves through get_law.

The regression risk in fixing it is NE, which shares sil.py and already
parsed correctly — hence the numeric branch is tried first and is pinned
here unchanged.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.cantonal_laws.numbering import (  # noqa: E402
    slug_matches_number, split_number_and_title)
from search_stack.build_cantonal_laws_db import _synthetic_id  # noqa: E402


def test_numeric_numbers_parse_as_they_always_did():
    """NE and TI shapes — the branch that must not move."""
    assert split_number_and_title("101 Constitution de la République") == \
        ("101", "Constitution de la République")
    assert split_number_and_title("101.000  Costituzione della Repubblica") == \
        ("101.000", "Costituzione della Repubblica")
    assert split_number_and_title("831.2a Loi sur les prestations") == \
        ("831.2a", "Loi sur les prestations")


def test_geneva_alphanumeric_numbers_parse():
    assert split_number_and_title("A 1 01 Acte d'union de la République") == \
        ("A 1 01", "Acte d'union de la République")
    assert split_number_and_title("B 5 15.24 Règlement fixant les débours") == \
        ("B 5 15.24", "Règlement fixant les débours")
    assert split_number_and_title("L 2 40 Loi instituant 2 fonds") == \
        ("L 2 40", "Loi instituant 2 fonds")


def test_an_unparseable_title_keeps_the_callers_value():
    """A wrong number resolves a lookup to the wrong act — worse than a
    placeholder. Nothing is guessed."""
    assert split_number_and_title("Loi sans numéro", fallback="rsg_x") == \
        ("rsg_x", "Loi sans numéro")
    assert split_number_and_title("", fallback="7") == ("7", "")


def test_a_bare_number_with_no_title_is_not_split():
    """Splitting here would leave the law with an empty title."""
    sr, title = split_number_and_title("101", fallback="fb")
    assert (sr, title) == ("fb", "101")


def test_geneva_slug_cross_checks_against_the_parsed_number():
    assert slug_matches_number("rsg_a1_01", "A 1 01")
    assert slug_matches_number("rsg_b5_15_24", "B 5 15.24")
    assert not slug_matches_number("rsg_a1_01", "A 1 02")


def test_the_slug_spells_a_decimal_point_p():
    """Two thirds of Geneva's laws are sub-numbered, and the slug writes
    "A 1 11.0" as rsg_a1_11p0. Missing that reported 546 correct parses
    as mismatches."""
    assert slug_matches_number("rsg_a1_11p0", "A 1 11.0")
    assert slug_matches_number("rsg_a2_04p03", "A 2 04.03")
    assert not slug_matches_number("rsg_a1_11p0", "A 1 11.9")


def test_synthetic_ids_are_stable_across_processes():
    """The whole point: `hash()` re-rolled every build, so nothing could
    reference a direct-sourced law's id across runs."""
    assert _synthetic_id("ZH", "101") == _synthetic_id("ZH", "101")
    assert _synthetic_id("ZH", "101") == 4715663746657657, \
        "the id scheme changed — every direct law gets a new id"


def test_synthetic_ids_cannot_be_mistaken_for_lexfind_ids():
    """Real LexFind ids are five digits; synthetic ones sit above 2**52,
    and below 2**53 so JSON clients keep full precision."""
    got = _synthetic_id("TI", "101.000")
    assert got > (1 << 52)
    assert got < (1 << 53)
