"""Regression tests for the prior-only / retrieval-augmented switch.

Exercises ``benchmarks.swiss_legal_rag_bench.evaluate.evaluate_question``
in both conditions with a mocked retriever + generator so no
ANTHROPIC_API_KEY is needed. Verifies:

  1. Prior-only skips retrieval (retriever.callcount == 0).
  2. Prior-only generator receives the ``_PRIOR_ONLY_SENTINEL``
     instead of passages.
  3. Retrieval-augmented condition calls the retriever once per
     question and threads snippets in the [P1] / [STATUTE] format.
  4. Error-class taxonomy: prior-only collapses to {correct,
     hallucination}; retrieval-augmented exercises all four bins.
  5. ``--prior-only`` CLI flag is wired and recorded in the summary.

Paper §8 cites these two conditions side-by-side; the harness must
support both flips without ad-hoc patches.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from benchmarks.swiss_legal_rag_bench.evaluate import (  # noqa: E402
    Question,
    _PRIOR_ONLY_SENTINEL,
    evaluate_question,
)


@pytest.fixture
def sample_question() -> Question:
    return Question(
        id="q-test-001",
        language="de",
        legal_area="Test",
        difficulty="basic",
        question=(
            "Welche vier Voraussetzungen müssen kumulativ erfüllt "
            "sein für eine Haftung nach Art. 41 OR?"
        ),
        reference_answer="Schaden, Widerrechtlichkeit, Kausalität, Verschulden.",
        evidence={
            "statutes": [{"law_code": "OR", "sr_number": "220",
                          "article": "41", "language": "de"}],
            "decisions": ["bge_BGE_132_III_122"],
        },
        claim_type="elements_of_norm",
    )


def _patch_judges(monkeypatch, *, correct, grounded):
    import benchmarks.swiss_legal_rag_bench.evaluate as ev
    monkeypatch.setattr(ev, "judge_correctness",
                        lambda answer, ref: (correct, "stub"))
    monkeypatch.setattr(ev, "judge_groundedness",
                        lambda answer, ctx: (grounded, "stub"))


# ── prior-only ──────────────────────────────────────────────────


def test_prior_only_skips_retrieval(sample_question, monkeypatch):
    retriever_calls = []

    def _no_call_retriever(*args, **kw):
        retriever_calls.append(args)
        return []

    captured_ctx = []

    def _capture_generator(query, retrieved_text, **kw):
        captured_ctx.append(retrieved_text)
        return "Schaden, Widerrechtlichkeit, Kausalität, Verschulden — Art. 41 OR."

    _patch_judges(monkeypatch, correct=True, grounded=False)

    result = evaluate_question(
        sample_question,
        mcp_url="http://stub",
        top_k=5,
        prior_only=True,
        retriever=_no_call_retriever,
        generator=_capture_generator,
    )

    assert retriever_calls == [], (
        f"prior-only must skip retrieval; got {len(retriever_calls)} call(s)"
    )
    assert captured_ctx == [_PRIOR_ONLY_SENTINEL]
    assert result.retrieved_decision_ids == []
    assert result.retrieved_statute_ids == []
    assert result.latency_ms.get("retrieve") == 0
    assert result.error_class == "correct"
    assert result.correctness is True


def test_prior_only_wrong_answer_is_hallucination(sample_question, monkeypatch):
    _patch_judges(monkeypatch, correct=False, grounded=False)
    result = evaluate_question(
        sample_question,
        mcp_url="http://stub",
        top_k=5,
        prior_only=True,
        retriever=lambda *a, **kw: [],
        generator=lambda q, ctx, **kw: "Es gibt keine solchen Voraussetzungen.",
    )
    assert result.error_class == "hallucination"
    assert result.correctness is False
    assert result.retrieval_accuracy is False


# ── retrieval-augmented ──────────────────────────────────────────


def test_retrieval_augmented_calls_retriever_once(sample_question, monkeypatch):
    n_calls = {"retrieve": 0, "statute": 0}

    def _retriever(query, language, top_k):
        n_calls["retrieve"] += 1
        return [{
            "decision_id": "bge_BGE_132_III_122",
            "court": "BGer",
            "regeste": "Schaden + Widerrechtlichkeit + Kausalität + Verschulden.",
            "snippet": "",
            "url": "",
        }]

    def _statute_text(law_code, article, language, mcp_url):
        n_calls["statute"] += 1
        return ("Wer einem andern widerrechtlich Schaden zufügt, "
                "ist ihm zu Ersatz verpflichtet.")

    captured_ctx = []

    def _generator(query, retrieved_text, **kw):
        captured_ctx.append(retrieved_text)
        return "Schaden, Widerrechtlichkeit, Kausalität, Verschulden [P1]."

    _patch_judges(monkeypatch, correct=True, grounded=True)

    import benchmarks.swiss_legal_rag_bench.evaluate as ev
    monkeypatch.setattr(ev, "_mcp_get_statute_text", _statute_text)

    result = evaluate_question(
        sample_question,
        mcp_url="http://stub",
        top_k=5,
        prior_only=False,
        retriever=_retriever,
        generator=_generator,
    )

    assert n_calls["retrieve"] == 1
    assert n_calls["statute"] == 1
    ctx = captured_ctx[0]
    assert "[P1 bge_BGE_132_III_122 — BGer]" in ctx
    assert "[STATUTE: Art. 41 OR]" in ctx
    assert result.retrieval_accuracy is True
    assert result.retrieved_decision_ids == ["bge_BGE_132_III_122"]
    assert result.error_class == "correct"


def test_retrieval_augmented_taxonomy_buckets(sample_question, monkeypatch):
    """Verify the four error bins are reachable in retrieval-augmented mode."""
    import benchmarks.swiss_legal_rag_bench.evaluate as ev
    monkeypatch.setattr(ev, "_mcp_get_statute_text", lambda *a, **kw: "")

    cases = [
        (False, False, True,  "hallucination"),
        (False, True,  False, "retrieval"),
        (False, True,  True,  "reasoning"),
        (True,  True,  True,  "correct"),
    ]
    for correct, grounded, retrieved_match, expected in cases:
        _patch_judges(monkeypatch, correct=correct, grounded=grounded)
        retriever_hits = (
            [{"decision_id": "bge_BGE_132_III_122", "court": "BGer",
              "regeste": "match", "snippet": "", "url": ""}]
            if retrieved_match else
            [{"decision_id": "bge_other_999", "court": "BGer",
              "regeste": "miss", "snippet": "", "url": ""}]
        )
        result = evaluate_question(
            sample_question,
            mcp_url="http://stub",
            top_k=5,
            prior_only=False,
            retriever=lambda *a, **kw: retriever_hits,
            generator=lambda q, ctx, **kw: "stub answer",
        )
        assert result.error_class == expected, (
            f"correct={correct} grounded={grounded} match={retrieved_match} "
            f"-> expected {expected!r}, got {result.error_class!r}"
        )


# ── CLI ─────────────────────────────────────────────────────────


def test_cli_records_prior_only_in_summary(tmp_path, sample_question, monkeypatch):
    import benchmarks.swiss_legal_rag_bench.evaluate as ev
    qf = tmp_path / "questions.jsonl"
    qf.write_text(json.dumps({
        "id": sample_question.id,
        "language": sample_question.language,
        "legal_area": sample_question.legal_area,
        "difficulty": sample_question.difficulty,
        "question": sample_question.question,
        "reference_answer": sample_question.reference_answer,
        "evidence": sample_question.evidence,
        "claim_type": sample_question.claim_type,
    }) + "\n")
    out = tmp_path / "out.json"

    _patch_judges(monkeypatch, correct=True, grounded=False)
    monkeypatch.setattr(ev, "_mcp_retrieve",
                        lambda *a, **kw: pytest.fail("retrieval should not run"))
    monkeypatch.setattr(ev, "_mcp_get_statute_text", lambda *a, **kw: "")
    monkeypatch.setattr(ev, "_claude_generate",
                        lambda q, ctx, **kw: "stub answer")

    monkeypatch.setattr(
        sys, "argv",
        ["evaluate", "--questions", str(qf), "--prior-only",
         "--output", str(out), "--limit", "1"],
    )
    ev.main()

    summary = json.loads(out.read_text())
    assert summary["condition"] == "prior-only"
    assert summary["evaluator"]["prior_only"] is True
    assert summary["evaluator"]["top_k"] == 0
