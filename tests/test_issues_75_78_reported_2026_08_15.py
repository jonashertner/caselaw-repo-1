"""Regression tests for the 2026-08-15 external bug reports (#75-#78).

Each test encodes a measurement the reporter published against the live
server (swiss-caselaw 1.27.0), reproduced here against the code paths.

  #76  citation_string carried a space where the federal docket grammar
       requires an underscore ('BGer 6B 267/2012'), for 49,845 of 192,582
       bger rows. R1 tells consumers to copy citation_string verbatim, so
       the anti-hallucination channel emitted a notation that does not
       exist in Swiss practice.
  #78  search_practice: _sanitize_fts5 quotes a bare OR into '"OR"', which
       FTS5 requires as an AND term — 'X OR X' returned 73 where
       'X AND X' returned 220. search_laws got the fix in #60.
  #75  the lower-bound marker was omitted whenever the strict-FTS count
       already exceeded the un-enlarged candidate pool (limit <= 15), so
       'Found 73 decisions' rendered as exact while the same query at a
       higher offset served 2515+.
  #77  dedup ran on the already-sliced page, so limit=50 delivered 49 rows
       and limit=200 delivered 192 — silently breaking the documented
       'showing < limit => complete' contract.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


# ── #76: federal docket normalisation ────────────────────────────────────

def test_space_docket_restored_for_bger():
    # every docket the reporter sampled (seed=42), in their reported form
    for raw, want in [
        ("4A 177/2021", "4A_177/2021"), ("6B 267/2012", "6B_267/2012"),
        ("9C 850/2009", "9C_850/2009"), ("2C 650/2011", "2C_650/2011"),
        ("9C 240/2007", "9C_240/2007"), ("4D 32/2010", "4D_32/2010"),
        ("8C 287/2023", "8C_287/2023"), ("2C 571/2023", "2C_571/2023"),
        ("9C 400/2012", "9C_400/2012"), ("4D 88/2009", "4D_88/2009"),
    ]:
        assert m._normalize_federal_docket(raw, "bger") == want


def test_correct_dockets_are_untouched():
    assert m._normalize_federal_docket("7B_53/2023", "bger") == "7B_53/2023"
    assert m._normalize_federal_docket("9C_530/2025", "bger") == "9C_530/2025"


def test_cantonal_dockets_keep_their_spaces():
    # cantonal series legitimately contain spaces; normalising them would
    # invent a docket that does not exist
    for raw, court in [("410 2024 329", "bl_gerichte"),
                       ("PKG 2024 1", "gr_gerichte"),
                       ("SK2 2024 31", "gr_gerichte"),
                       ("200 2024 491", "be_verwaltungsgericht")]:
        assert m._normalize_federal_docket(raw, court) == raw


def test_bge_reporter_form_untouched():
    assert m._normalize_federal_docket("142 II 590", "bge") == "142 II 590"


def test_other_federal_courts_covered():
    assert m._normalize_federal_docket("1C 96/2014", "bvger") == "1C_96/2014"
    assert m._normalize_federal_docket("BB 2023 1", "bstger") == "BB 2023 1"


def test_evg_era_two_digit_years_covered():
    # 16,611 further federal rows the report's own pattern missed: a
    # single-letter chamber code with a two-digit year. decision_id is the
    # ground truth (bger_C_1_07 -> 'C_1/07').
    for raw, want in [("C 1/07", "C_1/07"), ("U 49/98", "U_49/98"),
                      ("I 350/99", "I_350/99"), ("H 12/04", "H_12/04"),
                      ("K 101/06", "K_101/06"), ("B 4/04", "B_4/04")]:
        assert m._normalize_federal_docket(raw, "bger") == want


def test_no_slash_no_rewrite():
    # the slash is the safety property: federal collection dockets and every
    # cantonal series lack it, so they can never be rewritten
    for raw in ("BB 2023 1", "SK 2015 12", "410 2024 329", "PKG 2024 1"):
        assert m._normalize_federal_docket(raw, "bstger") == raw


def test_citation_string_uses_the_normalised_docket():
    cites = m._build_citation_strings({
        "court": "bger", "docket_number": "6B 267/2012",
        "decision_id": "bger_6B_267_2012", "decision_date": "2012-11-05",
    })
    for lang in ("de", "fr", "it"):
        s = cites[f"citation_string_{lang}"]
        assert "6B_267/2012" in s, s
        assert "6B 267/2012" not in s, s


def test_empty_and_none_dockets_are_safe():
    assert m._normalize_federal_docket("", "bger") == ""
    assert m._normalize_federal_docket("x", None) == "x"


# ── #78: search_practice operator handling ───────────────────────────────

def test_or_is_recognised_as_explicit_syntax():
    # the exact queries from the report
    for q in ("Vollzugshilfe OR Vollzugshilfe",
              "Nachtarbeit OR Bewilligung",
              "Kreisschreiben OR Vollzugshilfe",
              "Nachtarbeit OR Mehrwertsteuer"):
        assert m._has_explicit_fts_syntax(q) is True, q


def test_sanitizer_still_breaks_or_which_is_why_raw_is_used():
    # documents the defect the fix routes around: OR becomes a required term
    assert '"OR"' in m._sanitize_fts5("Vollzugshilfe OR Vollzugshilfe")


def test_plain_query_is_not_explicit():
    assert m._has_explicit_fts_syntax("Nachtarbeit") is False
    assert m._has_explicit_fts_syntax("Vollzugshilfe Gewaesserschutz") is False


def test_statute_or_is_not_operator_syntax():
    # 'OR' as Obligationenrecht must not be read as an operator (the guard
    # that predates this fix and must survive it)
    assert m._has_explicit_fts_syntax("Art. 41 OR") is False
