# tests/test_enrich_curriculum.py
"""Tests for the Anthropic-API-based curriculum enrichment script."""
import json
import pytest
from unittest.mock import patch, MagicMock
from study.enrich_curriculum import (
    build_enrichment_prompt,
    parse_enrichment_response,
    needs_enrichment,
)
from study.curriculum_engine import CurriculumCase


def _make_case(**kwargs) -> CurriculumCase:
    defaults = dict(
        decision_id="bge_135 III 1",
        bge_ref="BGE 135 III 1",
        significance_de="Test significance.",
        statutes=["Art. 1 OR"],
        difficulty=3,
        key_erwagungen=["2", "3"],
    )
    defaults.update(kwargs)
    return CurriculumCase(**defaults)


def test_needs_enrichment_true():
    case = _make_case(socratic_questions=[], hypotheticals=[])
    assert needs_enrichment(case) is True


def test_needs_enrichment_false_when_full():
    case = _make_case(
        socratic_questions=[{"level": 1, "question": "Q?", "model_answer": "A."}],
        hypotheticals=[{"scenario": "S", "likely_outcome_shift": "O"}],
    )
    assert needs_enrichment(case) is False


def test_needs_enrichment_false_when_no_decision_id():
    case = _make_case(decision_id="", socratic_questions=[], hypotheticals=[])
    assert needs_enrichment(case) is False


def test_build_prompt_contains_required_fields():
    case = _make_case()
    decision_text = "Sachverhalt: ... Erwägungen: 2. ... 3. ..."
    prompt = build_enrichment_prompt(case, decision_text=decision_text)
    assert "BGE 135 III 1" in prompt
    assert "Art. 1 OR" in prompt
    assert "socratic_questions" in prompt
    assert "hypotheticals" in prompt
    assert "model_answer" in prompt


def test_parse_enrichment_response_valid():
    response = json.dumps({
        "socratic_questions": [
            {"level": i, "level_label": f"L{i}", "question": f"Q{i}?",
             "hint": "hint", "model_answer": f"A{i}."}
            for i in range(1, 6)
        ],
        "hypotheticals": [
            {"type": "add_complication", "scenario": "S1",
             "discussion_points": ["D1"], "likely_outcome_shift": "O1"},
            {"type": "swap_parties", "scenario": "S2",
             "discussion_points": ["D2"], "likely_outcome_shift": "O2"},
        ],
        "reading_guide_de": "Lesen Sie E. 2.",
        "reading_guide_fr": "Lisez le considérant 2.",
        "reading_guide_it": "",
        "key_erwagungen": ["2", "3"],
        "significance_fr": "Arrêt de principe.",
        "significance_it": "",
    })
    result = parse_enrichment_response(response)
    assert len(result["socratic_questions"]) == 5
    assert len(result["hypotheticals"]) == 2
    assert result["reading_guide_de"] == "Lesen Sie E. 2."


def test_parse_enrichment_response_invalid_json():
    with pytest.raises(ValueError, match="Invalid JSON"):
        parse_enrichment_response("not json")


def test_parse_enrichment_response_wrong_question_count():
    response = json.dumps({
        "socratic_questions": [{"level": 1, "question": "Q?", "model_answer": "A."}],
        "hypotheticals": [],
    })
    with pytest.raises(ValueError, match="5 socratic questions"):
        parse_enrichment_response(response)
