"""M-3 (2026-06-28 audit): cantonal citation strings used the raw collection code
as the court abbreviation ('FR_GERICHTE 101 2026 140 vom 17. August 2026') and
surfaced a placeholder/future date. Fix: a readable court label per language, and
no unreliable date inside a citable string.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def test_cantonal_court_label_derivation():
    assert m._cantonal_court_label("zh_obergericht", "ZH", "de") == "Obergericht ZH"
    assert m._cantonal_court_label("be_verwaltungsgericht", "BE", "de") == "Verwaltungsgericht BE"
    # platform/generic code -> language-appropriate generic, never the raw code
    assert m._cantonal_court_label("fr_gerichte", "FR", "de") == "Gericht FR"
    assert m._cantonal_court_label("fr_gerichte", "FR", "fr") == "Tribunal FR"
    assert m._cantonal_court_label("ti_gerichte", "TI", "it") == "Tribunale TI"


def test_citation_date_reliability():
    assert m._citation_date_reliable("2025-09-27") is True
    assert m._citation_date_reliable("2026-01-01") is False   # placeholder
    assert m._citation_date_reliable("2026-08-17") is False   # future
    assert m._citation_date_reliable(None) is False


def test_cantonal_citation_string_no_raw_code_no_bad_date():
    dec = {
        "court": "fr_gerichte", "canton": "FR",
        "docket_number": "101 2026 140", "decision_date": "2026-08-17",  # future
        "decision_id": "fr_gerichte_101 2026 140",
    }
    cs = m._build_citation_strings(dec)
    assert "FR_GERICHTE" not in cs["citation_string_de"]
    assert cs["citation_string_de"].startswith("Gericht FR")
    assert cs["citation_string_fr"].startswith("Tribunal FR")
    assert "2026" not in cs["citation_string_de"].split("140")[-1]  # future date suppressed
    assert "vom" not in cs["citation_string_de"]


def test_real_dated_cantonal_keeps_date():
    dec = {
        "court": "zh_obergericht", "canton": "ZH",
        "docket_number": "LB230012", "decision_date": "2024-03-15",
        "decision_id": "zh_obergericht_LB230012",
    }
    cs = m._build_citation_strings(dec)
    assert cs["citation_string_de"].startswith("Obergericht ZH LB230012")
    assert "vom" in cs["citation_string_de"]  # reliable date kept
