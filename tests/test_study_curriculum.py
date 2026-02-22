from __future__ import annotations

import json
from pathlib import Path

from study.curriculum_engine import (
    CurriculumArea,
    CurriculumCase,
    find_case,
    list_areas,
    load_curriculum,
)

CURRICULUM_DIR = Path(__file__).resolve().parent.parent / "study" / "curriculum"


def test_load_all_curriculum():
    areas = load_curriculum()
    assert len(areas) >= 1  # at least one curriculum file exists


def test_load_curriculum_by_area():
    areas = load_curriculum(area="vertragsrecht")
    assert len(areas) == 1
    assert areas[0].area_id == "vertragsrecht"


def test_load_curriculum_unknown_area():
    areas = load_curriculum(area="nonexistent")
    assert areas == []


def test_list_areas_returns_summaries():
    summaries = list_areas(language="de")
    assert len(summaries) >= 1
    first = summaries[0]
    assert "area_id" in first
    assert "name" in first
    assert "case_count" in first


def test_find_case_by_topic():
    result = find_case("Vertragsschluss")
    if result is not None:  # depends on curriculum data
        assert isinstance(result, CurriculumCase)
        assert result.decision_id


def test_find_case_by_statute():
    result = find_case("Art. 41 OR")
    # May return None if curriculum doesn't cover this exact query
    if result is not None:
        assert isinstance(result, CurriculumCase)


def test_curriculum_json_schema():
    """All curriculum JSON files must match the expected schema."""
    for json_path in sorted(CURRICULUM_DIR.glob("*.json")):
        data = json.loads(json_path.read_text(encoding="utf-8"))
        assert "area_id" in data, f"{json_path.name}: missing area_id"
        assert "modules" in data, f"{json_path.name}: missing modules"
        for mod in data["modules"]:
            assert "id" in mod, f"{json_path.name}: module missing id"
            assert "cases" in mod, f"{json_path.name}: module {mod['id']} missing cases"
            for case in mod["cases"]:
                assert "decision_id" in case, f"{json_path.name}: case missing decision_id"
                assert "difficulty" in case, f"{json_path.name}: case missing difficulty"
                assert 1 <= case["difficulty"] <= 5, f"{json_path.name}: difficulty out of range"


def test_curriculum_prerequisites_are_dag():
    """Prerequisite references must not form cycles."""
    areas = load_curriculum()
    all_ids = set()
    for area in areas:
        for mod in area.modules:
            for case in mod.cases:
                all_ids.add(case.decision_id)

    # Check all prerequisites reference existing cases
    for area in areas:
        for mod in area.modules:
            for case in mod.cases:
                for prereq in case.prerequisites:
                    assert prereq in all_ids, (
                        f"Prerequisite {prereq} of {case.decision_id} not found"
                    )

    # Simple cycle detection via topological sort
    from collections import defaultdict, deque
    graph: dict[str, list[str]] = defaultdict(list)
    in_degree: dict[str, int] = {cid: 0 for cid in all_ids}
    for area in areas:
        for mod in area.modules:
            for case in mod.cases:
                for prereq in case.prerequisites:
                    graph[prereq].append(case.decision_id)
                    in_degree[case.decision_id] = in_degree.get(case.decision_id, 0) + 1

    queue = deque(cid for cid, deg in in_degree.items() if deg == 0)
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    assert visited == len(all_ids), "Prerequisite graph contains a cycle"
