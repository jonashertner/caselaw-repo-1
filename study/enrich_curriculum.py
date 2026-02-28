# study/enrich_curriculum.py
"""Batch-generate Socratic Q&A and hypotheticals for curriculum cases.

Usage:
    python -m study.enrich_curriculum [--area AREA_ID] [--dry-run]

Requires: ANTHROPIC_API_KEY env var.
For each case missing socratic_questions or hypotheticals:
  1. Fetches full decision text from DB
  2. Calls Claude to generate metadata
  3. Writes results back to curriculum JSON
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from study.curriculum_engine import CurriculumCase, load_curriculum, CURRICULUM_DIR

_LANG_CONFIG = {
    "de": {
        "bloom_labels": "1=Verständnis, 2=Regelidentifikation, 3=Anwendung, 4=Analyse, 5=Bewertung",
        "question_lang": "German",
        "answer_lang": "German",
        "scenario_lang": "German",
        "significance_field": "significance_de",
    },
    "fr": {
        "bloom_labels": "1=Compréhension, 2=Identification de la règle, 3=Application, 4=Analyse, 5=Évaluation",
        "question_lang": "French",
        "answer_lang": "French",
        "scenario_lang": "French",
        "significance_field": "significance_fr",
    },
    "it": {
        "bloom_labels": "1=Comprensione, 2=Identificazione della regola, 3=Applicazione, 4=Analisi, 5=Valutazione",
        "question_lang": "Italian",
        "answer_lang": "Italian",
        "scenario_lang": "Italian",
        "significance_field": "significance_it",
    },
}

ENRICHMENT_PROMPT_TEMPLATE = """\
You are enriching a Swiss law school curriculum entry for the landmark decision {bge_ref}.

CASE METADATA:
- BGE reference: {bge_ref}
- Legal area: {area_id} / {module_id}
- Key statutes: {statutes}
- Significance: {significance}
- Key Erwägungen: {key_erwagungen}
- Difficulty: {difficulty}/5
- Decision language: {decision_lang}

DECISION TEXT (excerpt):
{decision_text}

Generate the following JSON object (no markdown, pure JSON).
All questions, hints, model answers, scenarios and outcome descriptions MUST be written in {question_lang}.

{{
  "socratic_questions": [
    // Exactly 5 entries, one per Bloom level ({bloom_labels}). Each entry:
    {{
      "level": <1-5>,
      "level_label": "<label in {question_lang}>",
      "question": "<case-specific question in {question_lang}>",
      "hint": "<where to look in the decision, in {question_lang}>",
      "model_answer": "<2-4 sentence answer in {question_lang}>"
    }}
  ],
  "hypotheticals": [
    // Exactly 2 entries, types: "add_complication" and "swap_parties"
    {{
      "type": "<type>",
      "scenario": "<what-if scenario in {question_lang}>",
      "discussion_points": ["<point 1 in {question_lang}>", "<point 2 in {question_lang}>"],
      "likely_outcome_shift": "<how outcome changes, 2-3 sentences in {question_lang}>"
    }}
  ],
  "reading_guide_de": "<2-3 sentence reading guide in German pointing to key Erwägungen>",
  "reading_guide_fr": "<same in French>",
  "reading_guide_it": "<same in Italian, can be empty string>",
  "key_erwagungen": [<list of Erwägung numbers as strings, e.g. "2", "3.1">],
  "significance_fr": "<1-sentence significance in French>",
  "significance_it": "<1-sentence significance in Italian, can be empty>"
}}
"""


def needs_enrichment(case: CurriculumCase, *, force: bool = False) -> bool:
    """Return True if the case needs enrichment."""
    if not case.decision_id:
        return False
    if force:
        return True
    return not case.socratic_questions or not case.hypotheticals


def build_enrichment_prompt(
    case: CurriculumCase, *, decision_text: str, language: str = "de"
) -> str:
    """Build the enrichment prompt for a case in the given language."""
    lang = language if language in _LANG_CONFIG else "de"
    cfg = _LANG_CONFIG[lang]
    text_excerpt = decision_text[:6000] if decision_text else "(text unavailable)"
    significance = (
        getattr(case, cfg["significance_field"], "") or case.significance_de or "–"
    )
    return ENRICHMENT_PROMPT_TEMPLATE.format(
        bge_ref=case.bge_ref,
        area_id=case.area_id,
        module_id=case.module_id,
        statutes=", ".join(case.statutes) or "–",
        significance=significance,
        key_erwagungen=", ".join(case.key_erwagungen) or "–",
        difficulty=case.difficulty,
        decision_lang=language,
        question_lang=cfg["question_lang"],
        answer_lang=cfg["answer_lang"],
        scenario_lang=cfg["scenario_lang"],
        bloom_labels=cfg["bloom_labels"],
        decision_text=text_excerpt,
    )


def parse_enrichment_response(response_text: str) -> dict[str, Any]:
    """Parse and validate the Claude response. Raises ValueError on invalid shape."""
    # Strip markdown code fences if present (e.g. ```json ... ```)
    text = response_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON from enrichment response: {e}") from e

    questions = data.get("socratic_questions", [])
    if len(questions) != 5:
        raise ValueError(
            f"Expected 5 socratic questions, got {len(questions)}"
        )
    for q in questions:
        if not q.get("model_answer"):
            raise ValueError(f"Socratic question missing model_answer: {q}")

    return data


def _fetch_decision_text(decision_id: str, db_path: str) -> str:
    """Fetch full_text from FTS5 DB for a decision_id."""
    import sqlite3
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cur = con.cursor()
        try:
            cur.execute(
                "SELECT full_text, regeste FROM decisions WHERE decision_id = ?",
                (decision_id,),
            )
            row = cur.fetchone()
        finally:
            con.close()
        if row:
            return (row[1] or "") + "\n\n" + (row[0] or "")
    except Exception as exc:
        print(f"    WARN: could not fetch text for {decision_id}: {exc}")
    return ""


def _call_claude(prompt: str, api_key: str) -> str:
    """Call Claude API and return the text response."""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text


def enrich_all(
    *,
    area: str | None = None,
    dry_run: bool = False,
    force: bool = False,
    target_lang: str | None = None,
    rate_limit_sleep: float = 0.35,
    db_path: str | None = None,
) -> dict[str, int]:
    """Enrich all curriculum cases missing metadata.

    Args:
        area: Restrict to one area_id.
        dry_run: Print what would be done without calling the API.
        force: Re-enrich cases that already have socratic_questions/hypotheticals.
        target_lang: Only enrich cases whose actual_language matches this value
                     (e.g. "fr" or "it"). Combined with force, re-generates
                     questions in the correct language for FR/IT cases.
        rate_limit_sleep: Seconds to sleep between API calls.
        db_path: Path to FTS5 decisions DB. Defaults to output/decisions.db.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key and not dry_run:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    dbp = db_path or str(
        Path(__file__).resolve().parent.parent / "output" / "decisions.db"
    )

    stats = {"enriched": 0, "skipped": 0, "errors": 0}
    areas = load_curriculum(area=area)

    for curr_area in areas:
        json_path = CURRICULUM_DIR / f"{curr_area.area_id}.json"
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except OSError:
            continue

        changed = False
        for mod_data, mod in zip(data["modules"], curr_area.modules):
            for case_data, case in zip(mod_data["cases"], mod.cases):
                # Skip if language filter is set and doesn't match
                if target_lang and case.actual_language != target_lang:
                    stats["skipped"] += 1
                    continue

                if not needs_enrichment(case, force=force):
                    stats["skipped"] += 1
                    continue

                prompt_lang = case.actual_language if case.actual_language in ("fr", "it") else "de"
                print(f"  Enriching {case.bge_ref} ({curr_area.area_id}/{mod.id}) [{prompt_lang}]...")
                decision_text = _fetch_decision_text(case.decision_id, dbp)
                prompt = build_enrichment_prompt(
                    case, decision_text=decision_text, language=prompt_lang
                )

                if dry_run:
                    print(f"    [DRY RUN] Would call API for {case.bge_ref}")
                    stats["skipped"] += 1
                    continue

                try:
                    response = _call_claude(prompt, api_key)
                    enrichment = parse_enrichment_response(response)
                    # Write back to case_data dict
                    case_data["socratic_questions"] = enrichment["socratic_questions"]
                    case_data["hypotheticals"] = enrichment["hypotheticals"]
                    if enrichment.get("reading_guide_de"):
                        case_data["reading_guide_de"] = enrichment["reading_guide_de"]
                    if enrichment.get("reading_guide_fr"):
                        case_data["reading_guide_fr"] = enrichment["reading_guide_fr"]
                    if enrichment.get("reading_guide_it"):
                        case_data["reading_guide_it"] = enrichment["reading_guide_it"]
                    if enrichment.get("key_erwagungen"):
                        case_data["key_erwagungen"] = enrichment["key_erwagungen"]
                    if enrichment.get("significance_fr"):
                        case_data["significance_fr"] = enrichment["significance_fr"]
                    if enrichment.get("significance_it"):
                        case_data["significance_it"] = enrichment["significance_it"]
                    changed = True
                    stats["enriched"] += 1
                    print(f"    OK — {len(enrichment['socratic_questions'])} Qs, "
                          f"{len(enrichment['hypotheticals'])} hyps")
                    time.sleep(rate_limit_sleep)
                except Exception as e:
                    print(f"    ERROR: {e}")
                    stats["errors"] += 1

        if changed and not dry_run:
            json_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            print(f"  Saved {json_path.name}")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich curriculum metadata via Claude API")
    parser.add_argument("--area", default=None, help="Restrict to one area_id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true",
                        help="Re-enrich cases that already have questions/hypotheticals")
    parser.add_argument("--lang", default=None, choices=["de", "fr", "it"],
                        help="Only enrich cases whose actual_language matches this value")
    parser.add_argument("--db", default=None)
    args = parser.parse_args()
    stats = enrich_all(
        area=args.area,
        dry_run=args.dry_run,
        force=args.force,
        target_lang=args.lang,
        db_path=args.db,
    )
    print(f"\nSummary: {stats}")


if __name__ == "__main__":
    main()
