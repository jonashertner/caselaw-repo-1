"""Strasbourg citation strings carried no party name: every ECtHR decision cited
as 'Gericht CE 30696/09 vom 21. Januar 2011' (generic cantonal fallback, because
the three ecthr_* courts were absent from _COURT_CITATION_CODES), or as
'EGMR 30696/09 vom ...' for hudoc_ch/bge_egmr. Under R1 that string is what a
caller must copy verbatim, and neither form identifies a case to a reader.

Fix: EGMR/CourEDH/CorteEDU + party clause from the stored HUDOC docname +
labelled application number(s). Case is preserved rather than title-cased —
str.title() turns 'TÜRKİYE' into 'Türki̇ye'.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _cite(**kw):
    dec = {"decision_id": "x", "canton": "CE", "decision_date": "2015-03-02"}
    dec.update(kw)
    return m._build_citation_strings(dec)


# ---------------------------------------------------------------- case name

def test_collection_prefix_stripped():
    assert m._ecthr_case_name("AFFAIRE STOYKOV c. BULGARIE") == "STOYKOV c. BULGARIE"
    assert m._ecthr_case_name("CASE OF GÜL v. SWITZERLAND") == "GÜL v. SWITZERLAND"


def test_third_party_apparatus_stripped_but_not_dashed_party_names():
    # 75 rows carry a translation/summary tail after ' - ['
    assert m._ecthr_case_name(
        "CASE OF RUIZ RIVERA v. SWITZERLAND - [German Translation] summary by "
        "the Austrian Institute for Human Rights (ÖIM)"
    ) == "RUIZ RIVERA v. SWITZERLAND"
    # ...but four real case names contain a bare ' - ' and must survive intact
    assert m._ecthr_case_name("AFFAIRE FILIPPOS MAVROPOULOS - PAN. ZISIS O.E. c. GRECE") \
        == "FILIPPOS MAVROPOULOS - PAN. ZISIS O.E. c. GRECE"


def test_bracket_note_stripped_parenthesis_kept():
    assert m._ecthr_case_name("AFFAIRE İPEK c. TURQUIE [Extraits]") == "İPEK c. TURQUIE"
    # '(No. 2)' distinguishes two judgments between the same parties — keep it
    assert "(No. 2)" in m._ecthr_case_name(
        "CASE OF VEREIN GEGEN TIERFABRIKEN SCHWEIZ (VgT) v. SWITZERLAND (No. 2)")


def test_source_case_preserved_not_titlecased():
    # str.title() would emit 'Türki̇ye' (U+0130 -> 'i' + combining dot)
    assert m._ecthr_case_name("AFFAIRE İ.Ç. c. TÜRKİYE") == "İ.Ç. c. TÜRKİYE"


# ------------------------------------------------------- application numbers

def test_application_number_forms():
    assert m._ecthr_app_numbers("ecthr_chamber", "30696/09", "de") == "Nr. 30696/09"
    assert m._ecthr_app_numbers("ecthr_chamber", "43868/18_25883/21", "de") \
        == "Nr. 43868/18 und 25883/21"
    assert m._ecthr_app_numbers("ecthr_chamber", "43868/18_25883/21", "fr") \
        == "n° 43868/18 et 25883/21"
    assert m._ecthr_app_numbers("ecthr_committee", "31429/23_33504/23_33514/23", "de") \
        == "Nr. 31429/23 u.a."
    assert m._ecthr_app_numbers("ecthr_committee", "31429/23_33504/23_33514/23", "it") \
        == "n. 31429/23 e al."


def test_bge_egmr_docket_reconstructed_to_application_number():
    # internal key 'YYYYMMDD_<appno>_<yy>', never printed raw
    assert m._ecthr_app_numbers("bge_egmr", "20201020_78630_12", "de") == "Nr. 78630/12"


def test_unrecognised_docket_is_not_labelled_as_application_number():
    # a few hudoc_ch rows carry a raw HUDOC itemid
    assert m._ecthr_app_numbers("hudoc_ch", "001-25894", "de") == ""
    cs = _cite(court="hudoc_ch", canton="CH", docket_number="001-25894",
               decision_date="1994-08-31", title="HAUSER-RIVA contre la SUISSE")
    assert "Nr. 001-25894" not in cs["citation_string_de"]
    assert "001-25894" in cs["citation_string_de"]


# ------------------------------------------------------------------- dates

def test_bge_egmr_prefers_docket_encoded_date_over_wrong_stored_date():
    # 68 of 487 bge_egmr rows have a decision_date contradicting their docket;
    # the docket is the correct one (Beeler: judgment of 20 October 2020).
    assert m._ecthr_citation_date("bge_egmr", "20201020_78630_12", "1994-02-22") == "2020-10-20"
    cs = _cite(court="bge_egmr", canton="CH", docket_number="20201020_78630_12",
               decision_date="1994-02-22", title="Beeler gegen Schweiz")
    assert cs["citation_string_de"] == "EGMR Beeler gegen Schweiz, Nr. 78630/12 vom 20. Oktober 2020"
    assert "1994" not in cs["citation_string_de"]


def test_placeholder_date_suppressed():
    cs = _cite(court="hudoc_ch", canton="CH", docket_number="16279/90",
               decision_date="1990-01-01", title="A.M. v. SWITZERLAND")
    assert cs["citation_string_de"] == "EGMR A.M. v. SWITZERLAND, Nr. 16279/90"


# ------------------------------------------------------------ full strings

def test_full_citation_strings_all_languages():
    cs = _cite(court="ecthr_chamber", docket_number="43868/18_25883/21",
               decision_date="2024-02-20", title="AFFAIRE WA BAILE c. SUISSE")
    assert cs["citation_string_de"] == "EGMR WA BAILE c. SUISSE, Nr. 43868/18 und 25883/21 vom 20. Februar 2024"
    assert cs["citation_string_fr"] == "CourEDH WA BAILE c. SUISSE, n° 43868/18 et 25883/21 du 20 février 2024"
    assert cs["citation_string_it"] == "CorteEDU WA BAILE c. SUISSE, n. 43868/18 e 25883/21 del 20 febbraio 2024"


def test_pinpoint_uses_section_sign():
    cs = m._build_citation_strings(
        {"court": "ecthr_grand_chamber", "canton": "CE", "decision_id": "x",
         "docket_number": "30696/09", "decision_date": "2011-01-21",
         "title": "AFFAIRE M.S.S. c. BELGIQUE ET GRECE"}, pinpoint="250")
    assert cs["citation_string_de"].endswith(", § 250")
    assert cs["citation_string_fr"].endswith(", § 250")


def test_no_generic_cantonal_fallback_for_ecthr_courts():
    for court in ("ecthr_chamber", "ecthr_committee", "ecthr_grand_chamber"):
        cs = _cite(court=court, docket_number="30696/09", title="AFFAIRE M.S.S. c. BELGIQUE ET GRECE")
        assert "Gericht CE" not in cs["citation_string_de"]
        assert cs["citation_string_de"].startswith("EGMR M.S.S. c. BELGIQUE ET GRECE")


def test_missing_title_degrades_without_dangling_comma():
    cs = _cite(court="ecthr_chamber", docket_number="30696/09",
               decision_date="2011-01-21", title=None)
    assert cs["citation_string_de"] == "EGMR Nr. 30696/09 vom 21. Januar 2011"


def test_non_ecthr_courts_unchanged():
    assert m._build_citation_strings(
        {"court": "bger", "canton": "CH", "decision_id": "x",
         "docket_number": "6B_1234/2020", "decision_date": "2021-05-04"}
    )["citation_string_de"] == "BGer 6B_1234/2020 vom 4. Mai 2021"
    assert m._build_citation_strings(
        {"court": "zh_obergericht", "canton": "ZH", "decision_id": "x",
         "docket_number": "LB123", "decision_date": "2019-02-02"}
    )["citation_string_de"] == "Obergericht ZH LB123 vom 2. Februar 2019"
