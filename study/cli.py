"""Thin CLI for testing study tools locally.

Usage:
    python -m study.cli study "Art. 41 OR" --difficulty 2 --lang de
    python -m study.cli curriculum vertragsrecht
    python -m study.cli check bge_144_III_93 --brief "The court held..."
"""
from __future__ import annotations

import argparse
import json
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))


def main():
    parser = argparse.ArgumentParser(description="Socratic Case Law Tutor CLI")
    sub = parser.add_subparsers(dest="command")

    # study
    p_study = sub.add_parser("study", help="Study a leading case")
    p_study.add_argument("topic", nargs="?", help="Legal topic or concept")
    p_study.add_argument("--id", help="Specific decision_id")
    p_study.add_argument("--difficulty", type=int, help="Max difficulty (1-5)")
    p_study.add_argument("--lang", default="de", help="Language (de/fr/it)")
    p_study.add_argument("--mode", default="guided", choices=["guided", "brief", "quick"])

    # curriculum
    p_curr = sub.add_parser("curriculum", help="List curriculum")
    p_curr.add_argument("area", nargs="?", help="Filter by Rechtsgebiet")
    p_curr.add_argument("--lang", default="de")

    # check
    p_check = sub.add_parser("check", help="Check a case brief")
    p_check.add_argument("decision_id", help="BGE decision_id")
    p_check.add_argument("--brief", required=True, help="Student's brief text")
    p_check.add_argument("--lang", default="de")

    args = parser.parse_args()

    if args.command == "study":
        from mcp_server import get_decision_by_id, _count_citations, _find_leading_cases
        from study.socratic import build_study_package
        from study.curriculum_engine import find_case

        decision_id = args.id
        curriculum_case = None
        if not decision_id and args.topic:
            curriculum_case = find_case(args.topic, difficulty=args.difficulty, language=args.lang)
            if curriculum_case:
                decision_id = curriculum_case.decision_id
            else:
                lc = _find_leading_cases(query=args.topic, court="bge", limit=1)
                cases = lc.get("cases", [])
                if cases:
                    decision_id = cases[0]["decision_id"]

        if not decision_id:
            print("No matching case found.", file=sys.stderr)
            sys.exit(1)

        decision = get_decision_by_id(decision_id)
        if not decision:
            print(f"Decision not found: {decision_id}", file=sys.stderr)
            sys.exit(1)

        result = build_study_package(
            decision=decision,
            mode=args.mode,
            curriculum_case=curriculum_case,
            citation_counts=_count_citations(decision_id),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "curriculum":
        from study.curriculum_engine import list_areas, load_curriculum
        if args.area:
            areas = load_curriculum(area=args.area)
            if not areas:
                print(f"Unknown area: {args.area}", file=sys.stderr)
                sys.exit(1)
            a = areas[0]
            print(json.dumps({
                "area_id": a.area_id,
                "name": a.area_de,
                "modules": [
                    {"id": m.id, "name": m.name_de, "cases": len(m.cases)}
                    for m in a.modules
                ],
            }, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(list_areas(language=args.lang), ensure_ascii=False, indent=2))

    elif args.command == "check":
        from mcp_server import get_decision_by_id
        from study.socratic import build_brief_comparison
        decision = get_decision_by_id(args.decision_id)
        if not decision:
            print(f"Decision not found: {args.decision_id}", file=sys.stderr)
            sys.exit(1)
        result = build_brief_comparison(decision=decision, student_brief=args.brief)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
