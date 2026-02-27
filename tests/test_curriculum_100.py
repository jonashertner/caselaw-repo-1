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
