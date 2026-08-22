"""GitHub #85: find_relevant_erwaegung returned no_match while the correct
Erwägung sat at rank 1, and told the caller the cause was "BM25 gap < 1.2".

Measured against the reported claim, the gap is not the cause. Four independent
gates reach the suppression branch and only one of them is the gap; the reported
cases fail on token coverage and stay suppressed even when the gap is wide. The
old hint named the gap unconditionally, so anyone debugging it was pointed at
the wrong knob.

These tests pin two things: the reported reason is the gate that actually fired,
and the verdicts themselves are unchanged. Loosening a threshold is a separate
decision that needs the search benchmark behind it, not a unit test.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

# Verbatim from the issue: a claim written as a full proposition, and the
# Erwägung that states the same rule far more tersely.
CLAIM_A = ("Ob ein Vertrag gegen die guten Sitten verstoesst, bestimmt sich nur "
           "nach dem Inhalt und den daraus folgenden Wirkungen, nicht nach den "
           "Vorgaengen, die zum Vertragsschluss gefuehrt haben")
TEXT_A = ("Ob ein Vertrag gegen die guten Sitten verstoesst, ist nur anhand "
          "seines Inhaltes abzuwaegen.")


def _score(scores, claim=CLAIM_A, text=TEXT_A, **kw):
    sink: dict = {}
    label = m._score_pinpoint_confidence(scores, claim, text, reason_sink=sink, **kw)
    return label, sink


def test_the_gap_is_not_what_suppresses_the_reported_case():
    # Wide gap (1.6) and a very strong absolute score: still suppressed, and
    # the reason is coverage. This is the finding the issue's title got wrong.
    label, sink = _score([-55.14, -34.5])
    assert label is None
    assert sink["reason"] == "low_token_coverage"
    assert sink["matched_tokens"] == 5 and sink["claim_tokens"] == 14


def test_narrow_gap_reports_the_combined_gate():
    label, sink = _score([-55.14, -50.1])
    assert label is None
    assert sink["reason"] == "weak_gap_and_partial_coverage"
    assert sink["gap_ratio"] is not None


def test_stopword_only_claim_reports_its_own_reason():
    # Umlauts matter: _LEGAL_STOPWORDS holds "erwägung", so the ASCII
    # transliteration "Erwaegung" is NOT filtered and takes a different path.
    label, sink = _score([-1.0], claim="Verfahren Beschwerde Bundesgericht Erwägung")
    assert label is None
    assert sink["reason"] == "claim_has_no_semantic_tokens"


def test_ascii_transliterated_german_bypasses_the_stopword_filter():
    # Documents current behaviour rather than endorsing it: a caller sending
    # "Erwaegung" instead of "Erwägung" gets past the lexical-bait guard,
    # because the stopword list is umlaut-only. Worth knowing before anyone
    # relies on that guard.
    label, sink = _score([-1.0], claim="Verfahren Beschwerde Bundesgericht Erwaegung")
    assert label is None
    assert sink["reason"] != "claim_has_no_semantic_tokens"


def test_no_candidates_reports_its_own_reason():
    label, sink = _score([])
    assert label is None
    assert sink["reason"] == "no_candidate_paragraphs"


def test_hint_names_coverage_not_the_gap():
    _label, sink = _score([-55.14, -34.5])
    hint = m._low_confidence_hint(sink, [{"e_number": "5", "score": 55.14}])
    assert "coverage" in hint.lower()
    assert "BM25 gap < 1.2" not in hint
    # Actionable: route to the verbatim tool rather than only forbidding a cite.
    assert "get_erwaegung" in hint
    assert "E. 5" in hint


def test_hint_still_refuses_to_endorse_the_match():
    # The anti-guessing guarantee is the point of the tool; explaining the
    # suppression must not turn into licensing a citation.
    _label, sink = _score([-55.14, -34.5])
    hint = m._low_confidence_hint(sink, [{"e_number": "5", "score": 55.14}])
    assert "Do not cite it" in hint


# ── verdicts must be unchanged ────────────────────────────────────────────────

def test_confident_matches_still_confident():
    # High coverage + wide gap: unchanged "high".
    claim = "Schadenersatz Genugtuung Haftpflicht"
    text = "Schadenersatz und Genugtuung nach Haftpflicht sind geschuldet."
    assert m._score_pinpoint_confidence([-9.0, -1.0], claim, text) == "high"


def test_reason_sink_is_optional_and_default_behaviour_identical():
    # Every existing caller passes no sink; the label must not depend on it.
    args = ([-55.14, -50.1], CLAIM_A, TEXT_A)
    assert m._score_pinpoint_confidence(*args) is None
    assert m._score_pinpoint_confidence(*args, reason_sink={}) is None


def test_high_coverage_rescue_still_fires():
    # Narrow gap but near-full coverage and a strong score -> "medium".
    claim = "Verjaehrungsfrist absolute zehn Jahre anzuwenden"
    text = ("Die absolute Verjaehrungsfrist von zehn Jahren ist anzuwenden, "
            "auch hier.")
    assert m._score_pinpoint_confidence([-20.0, -19.0], claim, text) == "medium"
