"""GitHub #57: `chamber` was wrong for court=bger in two ways, from one map.

`ABTEILUNG_MAP["CH_BGer_007"]` named the *Beschwerdekammer des Bundes-
strafgerichts* — a Federal Criminal Court body, not a Federal Supreme Court
division — and carried the 7B/7F docket prefixes. That produced:

  A. ~3,300 of 3,535 7B/7F decisions labelled "I. Strafrechtliche Abteilung",
     because the map had no II. entry and plain substring matching let
     "I. Strafrechtliche Abteilung" match inside "II. strafrechtliche
     Abteilung" (and "Cour de droit pénal" inside "IIe Cour de droit pénal",
     and "Corte di diritto penale" inside "II Corte di diritto penale").
  B. 847 decisions across every docket prefix carrying the foreign court,
     because at 54 characters it was the longest name in the map and so was
     tested first by the whole-document text scan — any judgment merely
     mentioning the Beschwerdekammer inherited it.

Expected values below were confirmed against the stored full_text of the
decisions the issue cites.
"""
from __future__ import annotations

import pytest

from scrapers.bger import (
    ABTEILUNG_MAP,
    PREFIX_TO_ABTEILUNG,
    chamber_from_text,
)

FOREIGN_COURT_MARKERS = (
    "bundesstrafgericht",
    "tribunal pénal fédéral",
    "tribunale penale federale",
)


# ── the map itself ─────────────────────────────────────────────────

@pytest.mark.parametrize("prefix", ["7B", "7F"])
def test_criminal_procedure_dockets_map_to_the_second_division(prefix):
    """The 2023 reorganisation moved criminal-procedure appeals to 7B/7F
    under the II. strafrechtliche Abteilung."""
    _sig, info = PREFIX_TO_ABTEILUNG[prefix]
    assert info["de"] == "II. Strafrechtliche Abteilung"
    assert info["fr"] == "IIe Cour de droit pénal"
    assert info["it"] == "II Corte di diritto penale"


def test_sixth_series_still_maps_to_the_first_division():
    """Control: 6B stayed with the I. criminal division after 2023."""
    _sig, info = PREFIX_TO_ABTEILUNG["6B"]
    assert info["de"] == "I. Strafrechtliche Abteilung"


def test_no_foreign_court_is_assignable_as_a_bger_chamber():
    """Pattern B's root cause: a court that is not a BGer division must not
    be in the map at all, in any language."""
    for sig, info in ABTEILUNG_MAP.items():
        for lang in ("de", "fr", "it"):
            name = info[lang].lower()
            for marker in FOREIGN_COURT_MARKERS:
                assert marker not in name, (
                    f"{sig}[{lang}] = {info[lang]!r} names a different court")


# ── Pattern A: the substring collision ─────────────────────────────

@pytest.mark.parametrize("text,expected", [
    # The closing formula of a real 7B decision, per language.
    (("Im Namen der II. strafrechtlichen Abteilung\n"
      "II. strafrechtliche Abteilung"), "II. Strafrechtliche Abteilung"),
    ("Au nom de la IIe Cour de droit pénal du Tribunal fédéral suisse",
     "II. Strafrechtliche Abteilung"),
    ("In nome della II Corte di diritto penale del Tribunale federale",
     "II. Strafrechtliche Abteilung"),
])
def test_second_criminal_division_is_not_read_as_the_first(text, expected):
    assert chamber_from_text(text) == expected


@pytest.mark.parametrize("text,expected", [
    (("Im Namen der I. strafrechtlichen Abteilung\n"
      "I. strafrechtliche Abteilung"), "I. Strafrechtliche Abteilung"),
    ("Au nom de la Cour de droit pénal du Tribunal fédéral suisse",
     "I. Strafrechtliche Abteilung"),
    ("In nome della Corte di diritto penale del Tribunale federale",
     "I. Strafrechtliche Abteilung"),
])
def test_first_criminal_division_still_resolves(text, expected):
    """The guard must not cost us the pre-2023 corpus, where the criminal
    division is named without a numeral in FR/IT."""
    assert chamber_from_text(text) == expected


def test_roman_numeral_guard_is_not_only_sort_order():
    """Belt and braces: even if the I. entry were tested first, the
    lookbehind stops it matching inside 'II.'."""
    from scrapers.bger import _ABTEILUNG_NAME_PATTERNS

    first_division_de = next(
        rx for rx, de in _ABTEILUNG_NAME_PATTERNS
        if de == "I. Strafrechtliche Abteilung"
        and "Strafrechtliche" in rx.pattern)
    assert not first_division_de.search("II. strafrechtliche Abteilung")
    assert first_division_de.search("I. strafrechtliche Abteilung")


# ── Pattern B: a foreign court mentioned in the body ───────────────

def test_mentioning_the_federal_criminal_court_does_not_steal_the_chamber():
    """bger_1B_467_2020 shape: the Beschwerdekammer appears as the lower
    instance, the I. public-law division decided."""
    text = (
        "Bundesgericht\nI. öffentlich-rechtliche Abteilung\n"
        "Beschwerde gegen den Beschluss der Beschwerdekammer des "
        "Bundesstrafgerichts vom 3. September 2020.\n"
    )
    assert chamber_from_text(text) == "I. Öffentlich-rechtliche Abteilung"


def test_passing_reference_to_the_federal_criminal_court_is_ignored():
    """bger_5A_320/2019 shape: no procedural link to the Federal Criminal
    Court at all, it is named only in a remark about a misaddressed filing."""
    text = (
        "Au nom de la IIe Cour de droit civil du Tribunal fédéral suisse\n"
        "le recourant avait adressé son écriture au Tribunal pénal fédéral\n"
    )
    assert chamber_from_text(text) == "II. Zivilrechtliche Abteilung"


def test_no_division_named_returns_none():
    assert chamber_from_text("Ein Entscheid ohne Abteilungsangabe.") is None
