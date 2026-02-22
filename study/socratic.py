"""Assemble study packages and brief comparison data for MCP tools."""
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from study.parser import ParsedDecision, parse_decision
from study.curriculum_engine import (
    CurriculumCase,
    find_case,
    load_curriculum,
    list_areas,
)


def build_study_package(
    *,
    decision: dict,
    mode: str = "guided",
    curriculum_case: CurriculumCase | None = None,
    citation_counts: tuple[int, int] = (0, 0),
    related_cases: list[dict] | None = None,
) -> dict[str, Any]:
    """Build a structured study package from a fetched decision.

    Args:
        decision: Row dict from get_decision_by_id (has full_text, regeste, etc.)
        mode: "guided", "brief", or "quick"
        curriculum_case: Matching curriculum entry, if any
        citation_counts: (incoming, outgoing) from citation graph
        related_cases: Prerequisite/successor cases from curriculum

    Returns:
        Structured dict for the MCP tool response.
    """
    parsed = parse_decision(
        decision.get("full_text", ""),
        language=decision.get("language", "de"),
        regeste=decision.get("regeste", ""),
    )

    base = {
        "decision_id": decision.get("decision_id", ""),
        "docket_number": decision.get("docket_number", ""),
        "decision_date": decision.get("decision_date", ""),
        "court": decision.get("court", ""),
        "chamber": decision.get("chamber", ""),
        "language": decision.get("language", ""),
        "cited_by_count": citation_counts[0],
        "cites_count": citation_counts[1],
        "parse_quality": parsed.parse_quality,
    }

    if curriculum_case:
        base["curriculum"] = {
            "area_id": curriculum_case.area_id,
            "module_id": curriculum_case.module_id,
            "bge_ref": curriculum_case.bge_ref,
            "title_de": curriculum_case.title_de,
            "concepts_de": curriculum_case.concepts_de,
            "statutes": curriculum_case.statutes,
            "difficulty": curriculum_case.difficulty,
            "significance_de": curriculum_case.significance_de,
        }

    if mode == "quick":
        # Minimal: regeste + top-level Erwägung numbers + statutes + citation count
        all_statutes = set()
        top_erwagungen = []
        for e in parsed.erwagungen:
            all_statutes.update(e.statute_refs)
            if e.depth == 1:
                top_erwagungen.append(e.number)
        base["regeste"] = parsed.regeste
        base["top_erwagungen"] = top_erwagungen
        base["all_statutes"] = sorted(all_statutes)
        return base

    if mode == "brief":
        # Parsed sections + statute refs — for briefing exercises
        base["regeste"] = parsed.regeste
        base["sachverhalt"] = parsed.sachverhalt
        base["erwagungen"] = [
            {
                "number": e.number,
                "depth": e.depth,
                "statute_refs": e.statute_refs,
                "text": e.text,
            }
            for e in parsed.erwagungen
        ]
        base["dispositiv"] = parsed.dispositiv
        return base

    # mode == "guided" (default): full package
    base["regeste"] = parsed.regeste
    base["sachverhalt"] = parsed.sachverhalt
    base["erwagungen"] = [
        {
            "number": e.number,
            "depth": e.depth,
            "statute_refs": e.statute_refs,
            "text": e.text,
        }
        for e in parsed.erwagungen
    ]
    base["dispositiv"] = parsed.dispositiv

    if related_cases:
        base["related_cases"] = related_cases

    return base


def build_brief_comparison(
    *,
    decision: dict,
    student_brief: str,
) -> dict[str, Any]:
    """Build a structured comparison between a student's brief and the decision.

    Returns the parsed decision ground truth alongside the student text,
    structured for the calling LLM to generate pedagogical feedback.
    """
    parsed = parse_decision(
        decision.get("full_text", ""),
        language=decision.get("language", "de"),
        regeste=decision.get("regeste", ""),
    )

    # Extract ground truth elements
    all_statutes = set()
    erwagung_summaries = []
    for e in parsed.erwagungen:
        all_statutes.update(e.statute_refs)
        erwagung_summaries.append({
            "number": e.number,
            "depth": e.depth,
            "statute_refs": e.statute_refs,
            # First 500 chars as summary — full text too long for comparison
            "summary": e.text[:500] + ("..." if len(e.text) > 500 else ""),
        })

    return {
        "decision_id": decision.get("decision_id", ""),
        "docket_number": decision.get("docket_number", ""),
        "language": decision.get("language", ""),
        "parse_quality": parsed.parse_quality,
        "ground_truth": {
            "regeste": parsed.regeste,
            "sachverhalt_excerpt": parsed.sachverhalt[:1000] + (
                "..." if len(parsed.sachverhalt) > 1000 else ""
            ),
            "erwagung_summaries": erwagung_summaries,
            "dispositiv": parsed.dispositiv,
            "statutes": sorted(all_statutes),
        },
        "student_brief": student_brief,
    }
