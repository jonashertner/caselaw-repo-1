"""Curriculum loading and case selection for the Socratic tutor."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

CURRICULUM_DIR = Path(__file__).resolve().parent / "curriculum"


@dataclass
class CurriculumCase:
    decision_id: str
    bge_ref: str = ""
    title_de: str = ""
    title_fr: str = ""
    title_it: str = ""
    concepts_de: list[str] = field(default_factory=list)
    concepts_fr: list[str] = field(default_factory=list)
    concepts_it: list[str] = field(default_factory=list)
    statutes: list[str] = field(default_factory=list)
    difficulty: int = 1
    prerequisites: list[str] = field(default_factory=list)
    significance_de: str = ""
    significance_fr: str = ""
    significance_it: str = ""
    # Set at load time from parent context
    area_id: str = ""
    module_id: str = ""


@dataclass
class CurriculumModule:
    id: str
    name_de: str = ""
    name_fr: str = ""
    name_it: str = ""
    statutes: list[str] = field(default_factory=list)
    cases: list[CurriculumCase] = field(default_factory=list)


@dataclass
class CurriculumArea:
    area_id: str
    area_de: str = ""
    area_fr: str = ""
    area_it: str = ""
    description_de: str = ""
    modules: list[CurriculumModule] = field(default_factory=list)


def load_curriculum(*, area: str | None = None) -> list[CurriculumArea]:
    """Load curriculum from JSON files. Optionally filter by area_id."""
    areas: list[CurriculumArea] = []
    for json_path in sorted(CURRICULUM_DIR.glob("*.json")):
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        area_id = data.get("area_id", json_path.stem)
        if area is not None and area_id != area:
            continue

        modules = []
        for mod_data in data.get("modules", []):
            cases = []
            for c in mod_data.get("cases", []):
                cases.append(CurriculumCase(
                    decision_id=c["decision_id"],
                    bge_ref=c.get("bge_ref", ""),
                    title_de=c.get("title_de", ""),
                    title_fr=c.get("title_fr", ""),
                    title_it=c.get("title_it", ""),
                    concepts_de=c.get("concepts_de", []),
                    concepts_fr=c.get("concepts_fr", []),
                    concepts_it=c.get("concepts_it", []),
                    statutes=c.get("statutes", []),
                    difficulty=c.get("difficulty", 1),
                    prerequisites=c.get("prerequisites", []),
                    significance_de=c.get("significance_de", ""),
                    significance_fr=c.get("significance_fr", ""),
                    significance_it=c.get("significance_it", ""),
                    area_id=area_id,
                    module_id=mod_data.get("id", ""),
                ))
            modules.append(CurriculumModule(
                id=mod_data.get("id", ""),
                name_de=mod_data.get("name_de", ""),
                name_fr=mod_data.get("name_fr", ""),
                name_it=mod_data.get("name_it", ""),
                statutes=mod_data.get("statutes", []),
                cases=cases,
            ))

        areas.append(CurriculumArea(
            area_id=area_id,
            area_de=data.get("area_de", ""),
            area_fr=data.get("area_fr", ""),
            area_it=data.get("area_it", ""),
            description_de=data.get("description_de", ""),
            modules=modules,
        ))

    return areas


def list_areas(*, language: str = "de") -> list[dict]:
    """Return summary of all curriculum areas."""
    result = []
    for a in load_curriculum():
        case_count = sum(len(m.cases) for m in a.modules)
        name_key = f"area_{language}" if language in ("de", "fr", "it") else "area_de"
        result.append({
            "area_id": a.area_id,
            "name": getattr(a, name_key, a.area_de) or a.area_de,
            "module_count": len(a.modules),
            "case_count": case_count,
        })
    return result


def find_case(
    topic: str,
    *,
    difficulty: int | None = None,
    language: str | None = None,
) -> CurriculumCase | None:
    """Find the best matching curriculum case for a topic string.

    Searches module names, case concepts, case statutes, and case titles.
    Returns the best match or None.
    """
    topic_lower = topic.lower()
    areas = load_curriculum()

    best: CurriculumCase | None = None
    best_score = 0

    for area in areas:
        for mod in area.modules:
            mod_match = topic_lower in mod.name_de.lower() or topic_lower in mod.name_fr.lower()
            for case in mod.cases:
                if difficulty is not None and case.difficulty > difficulty:
                    continue

                score = 0
                # Check title match
                for title in (case.title_de, case.title_fr, case.title_it):
                    if topic_lower in title.lower():
                        score += 3

                # Check concept match
                for concepts in (case.concepts_de, case.concepts_fr, case.concepts_it):
                    for concept in concepts:
                        if topic_lower in concept.lower():
                            score += 2

                # Check statute match
                for statute in case.statutes:
                    if topic_lower in statute.lower():
                        score += 2

                # Module context match
                if mod_match:
                    score += 1

                # Area id match
                if topic_lower in area.area_id:
                    score += 1

                if score > best_score:
                    best_score = score
                    best = case

    return best
