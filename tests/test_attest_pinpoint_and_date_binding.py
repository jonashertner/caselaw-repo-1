"""GitHub #81 and #82 (reproduction A).

#81 — Swiss decisions are routinely cited with a letter on the Erwägung
("E. 4b", "consid. 6a"). The pinpoint capture group was [\\d.]+, so the letter
fell outside the match. That produced both reported symptoms: the citation span
ended mid-citation, so rebuilding the annotated text put the stray letter after
the status marker ("E. 4 ✓b"), and the pinpoint passed to the grounding judge
was the parent E. 4, which had it read a different sub-paragraph and report a
correctly-cited claim as unsupported.

#82A — the date audit looked a flat 60 characters past each citation, straight
through any neighbouring citation. In "(BGE 119 II 380 E. 4b; BGer 4A_231/2014
vom 23. September 2014)" the date belongs to the docket that follows, but it
landed inside the BGE citation's window and was reported as the BGE's date: a
date error raised against a citation that carries no date at all.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _one(text: str) -> dict:
    cits = m._parse_citations_in_text(text)
    assert cits, text
    return cits[0]


# ── #81: sub-letter pinpoints ───────────────────────────────────────────────

def test_sub_letter_pinpoint_captured():
    c = _one("void under Art. 19/20 OR (BGE 119 II 380 E. 4b).")
    assert c["pinpoint"] == "4b"


def test_sub_letter_pinpoint_second_reported_case():
    assert _one("see BGE 120 II 155 E. 6a")["pinpoint"] == "6a"


def test_consid_form_with_letter():
    assert _one("BGE 131 III 12, consid. 4c;")["pinpoint"] == "4c"


def test_span_covers_the_whole_citation():
    # The stray-letter artefact ("E. 4 ✓b") was a span that stopped short. The
    # matched span must end after the letter, not before it.
    text = "void under Art. 19/20 OR (BGE 119 II 380 E. 4b)."
    c = _one(text)
    assert text[c["span"][0]:c["span"][1]].endswith("E. 4b")


def test_numeric_pinpoints_unchanged():
    assert _one("BGE 131 III 12 E. 2.3.1 and more")["pinpoint"] == "2.3.1"
    assert _one("BGE 131 III 12 E. 2 only")["pinpoint"] == "2"
    assert _one("BGE 131 III 12 E. 2.1.")["pinpoint"] == "2.1"


def test_multi_letter_suffix_declines_rather_than_truncating():
    # "E. 4bis" must not become a confident "4b". Losing a pinpoint costs
    # precision; inventing one costs correctness.
    c = _one("BGE 131 III 12 E. 4bis is odd")
    assert c["pinpoint"] is None
    assert c["full_match"] == "BGE 131 III 12"


# ── #82A: a date binds only to the citation it follows ──────────────────────

DRAFT = ("A bribery promise is void under Art. 19/20 OR "
         "(BGE 119 II 380 E. 4b; BGer 4A_231/2014 vom 23. September 2014, E. 5.1).")
DATES = {"bge_BGE_119_II_380": "1993-09-02", "bger_4A_231_2014": "2014-09-23"}


def _dated(draft: str, dates: dict) -> list[dict]:
    cits = m._parse_citations_in_text(draft)
    for c in cits:
        c["_decision_date"] = dates.get(c["decision_id_guess"])
    return cits


def test_date_does_not_bind_across_a_citation_boundary():
    assert m._audit_dates(DRAFT, _dated(DRAFT, DATES)) == []


def test_date_still_binds_to_its_own_citation():
    draft = "See BGer 4A_231/2014 vom 23. September 2014, E. 5.1."
    cits = _dated(draft, {"bger_4A_231_2014": "2011-01-05"})  # corpus disagrees
    issues = m._audit_dates(draft, cits)
    assert len(issues) == 1
    assert issues[0]["claimed_date"] == "2014-09-23"
    assert issues[0]["actual_date"] == "2011-01-05"


def test_correct_date_raises_nothing():
    draft = "See BGer 4A_231/2014 vom 23. September 2014, E. 5.1."
    assert m._audit_dates(draft, _dated(draft, {"bger_4A_231_2014": "2014-09-23"})) == []


def test_chain_of_citations_with_one_trailing_date():
    draft = ("(BGE 119 II 380 E. 4b; BGe 120 II 155 E. 6a; "
             "BGer 4A_231/2014 vom 23. September 2014).")
    draft = draft.replace("BGe 120", "BGE 120")
    dates = dict(DATES, bge_BGE_120_II_155="1994-05-10")
    assert m._audit_dates(draft, _dated(draft, dates)) == []
