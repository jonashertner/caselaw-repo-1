# tests/test_curriculum_100.py
"""Validation tests for the BGE-100 canonical curriculum."""
import pytest
from study.curriculum_engine import load_curriculum

EXPECTED_AREAS = {
    "vertragsrecht": 8,
    "haftpflicht": 8,
    "sachenrecht": 7,
    "familienrecht": 7,
    "arbeitsrecht": 7,
    "mietrecht": 6,
    "strafrecht_at": 8,
    "grundrechte": 7,
    "strafrecht_bt": 7,
    "erbrecht": 7,
    "gesellschaftsrecht": 7,
    "zivilprozessrecht": 7,
    "strafprozessrecht": 7,
    "oeffentliches_prozessrecht": 7,
}


def test_total_case_count():
    areas = load_curriculum()
    total = sum(len(m.cases) for a in areas for m in a.modules)
    assert total == 100, f"Expected 100 cases, got {total}"


def test_area_count():
    areas = load_curriculum()
    assert len(areas) == 14, f"Expected 14 areas, got {len(areas)}"


def test_required_area_ids():
    area_ids = {a.area_id for a in load_curriculum()}
    assert area_ids == set(EXPECTED_AREAS.keys()), (
        f"Missing: {set(EXPECTED_AREAS) - area_ids}, "
        f"Extra: {area_ids - set(EXPECTED_AREAS)}"
    )


def test_area_case_counts():
    areas = load_curriculum()
    for a in areas:
        count = sum(len(m.cases) for m in a.modules)
        expected = EXPECTED_AREAS.get(a.area_id, -1)
        assert count == expected, (
            f"{a.area_id}: expected {expected} cases, got {count}"
        )


def test_no_duplicate_bge_refs():
    areas = load_curriculum()
    refs = [c.bge_ref for a in areas for m in a.modules for c in m.cases]
    dupes = [r for r in refs if refs.count(r) > 1]
    assert not dupes, f"Duplicate BGE refs: {set(dupes)}"


def test_required_fields_per_case():
    areas = load_curriculum()
    for a in areas:
        for m in a.modules:
            for c in m.cases:
                assert c.bge_ref, f"Missing bge_ref in {a.area_id}/{m.id}"
                assert c.significance_de, (
                    f"Missing significance_de for {c.bge_ref}"
                )
                assert c.difficulty in range(1, 6), (
                    f"Invalid difficulty {c.difficulty} for {c.bge_ref}"
                )
                assert c.statutes, f"Missing statutes for {c.bge_ref}"


def test_difficulty_distribution():
    """No area should have all cases at the same difficulty."""
    areas = load_curriculum()
    for a in areas:
        diffs = [c.difficulty for m in a.modules for c in m.cases]
        assert len(set(diffs)) > 1, (
            f"{a.area_id}: all cases have the same difficulty {diffs[0]}"
        )


def test_enrichment_completeness():
    """Every case must have socratic questions, hypotheticals, and reading guide."""
    areas = load_curriculum()
    for a in areas:
        for m in a.modules:
            for c in m.cases:
                loc = f"{a.area_id}/{c.bge_ref}"
                assert c.socratic_questions, f"Missing socratic_questions: {loc}"
                assert c.hypotheticals, f"Missing hypotheticals: {loc}"
                assert c.reading_guide_de, f"Missing reading_guide_de: {loc}"
                assert c.key_erwagungen, f"Missing key_erwagungen: {loc}"


def test_socratic_question_structure():
    """Each case must have exactly 5 questions at levels 1-5, each with hint and model_answer."""
    areas = load_curriculum()
    for a in areas:
        for m in a.modules:
            for c in m.cases:
                loc = f"{a.area_id}/{c.bge_ref}"
                qs = c.socratic_questions
                assert len(qs) == 5, f"Expected 5 questions, got {len(qs)}: {loc}"
                levels = sorted(q.get("level") for q in qs)
                assert levels == [1, 2, 3, 4, 5], (
                    f"Expected levels [1,2,3,4,5], got {levels}: {loc}"
                )
                for q in qs:
                    lvl = q.get("level")
                    assert "hint" in q, (
                        f"Q{lvl} missing 'hint' (has discussion_points?): {loc}"
                    )
                    assert "model_answer" in q, (
                        f"Q{lvl} missing 'model_answer': {loc}"
                    )
                    assert "discussion_points" not in q, (
                        f"Q{lvl} has 'discussion_points' — misplaced hypothetical: {loc}"
                    )


def test_hypothetical_structure():
    """Each case must have exactly 2 hypotheticals with required fields."""
    areas = load_curriculum()
    for a in areas:
        for m in a.modules:
            for c in m.cases:
                loc = f"{a.area_id}/{c.bge_ref}"
                hyps = c.hypotheticals
                assert len(hyps) == 2, (
                    f"Expected 2 hypotheticals, got {len(hyps)}: {loc}"
                )
                for i, h in enumerate(hyps):
                    for key in ("type", "scenario", "discussion_points", "likely_outcome_shift"):
                        assert key in h, (
                            f"Hypothetical {i+1} missing '{key}': {loc}"
                        )
