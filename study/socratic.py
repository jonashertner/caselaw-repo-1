"""Assemble study packages and brief comparison data for MCP tools."""
from __future__ import annotations

from typing import Any

from study.parser import ParsedDecision, parse_decision
from study.curriculum_engine import (
    CurriculumCase,
)

# ── Bloom-level labels (multilingual) ────────────────────────

_BLOOM_LEVELS = [
    {
        "level": 1,
        "level_id": "verstandnis",
        "label_de": "Verständnis",
        "label_fr": "Compréhension",
        "label_it": "Comprensione",
        "prompt_de": "Was hat das Gericht entschieden?",
        "prompt_fr": "Qu'a décidé le tribunal?",
        "prompt_it": "Cosa ha deciso il tribunale?",
    },
    {
        "level": 2,
        "level_id": "regelidentifikation",
        "label_de": "Regelidentifikation",
        "label_fr": "Identification de la règle",
        "label_it": "Identificazione della regola",
        "prompt_de": "Welche Norm wurde angewandt und wie?",
        "prompt_fr": "Quelle norme a été appliquée et comment?",
        "prompt_it": "Quale norma è stata applicata e come?",
    },
    {
        "level": 3,
        "level_id": "anwendung",
        "label_de": "Anwendung",
        "label_fr": "Application",
        "label_it": "Applicazione",
        "prompt_de": "Warum greifen die Voraussetzungen hier?",
        "prompt_fr": "Pourquoi les conditions sont-elles remplies ici?",
        "prompt_it": "Perché i presupposti sono soddisfatti qui?",
    },
    {
        "level": 4,
        "level_id": "analyse",
        "label_de": "Analyse",
        "label_fr": "Analyse",
        "label_it": "Analisi",
        "prompt_de": "Was wäre anders, wenn sich ein Sachverhaltselement ändert?",
        "prompt_fr": "Que changerait-il si un élément factuel était différent?",
        "prompt_it": "Cosa cambierebbe se un elemento fattuale fosse diverso?",
    },
    {
        "level": 5,
        "level_id": "bewertung",
        "label_de": "Bewertung",
        "label_fr": "Évaluation",
        "label_it": "Valutazione",
        "prompt_de": "Überzeugt die Begründung? Warum (nicht)?",
        "prompt_fr": "Le raisonnement est-il convaincant? Pourquoi (pas)?",
        "prompt_it": "Il ragionamento è convincente? Perché (no)?",
    },
]

# ── Brief template sections ──────────────────────────────────

_BRIEF_TEMPLATE_SECTIONS = [
    {
        "name_de": "Leitsatz",
        "name_fr": "Ratio decidendi",
        "name_it": "Massima",
        "weight": 15,
        "instructions_de": "Formulieren Sie die ratio decidendi in 1-2 Sätzen: Was hat das Bundesgericht als Rechtssatz aufgestellt?",
        "instructions_fr": "Formulez la ratio decidendi en 1-2 phrases: quelle règle le Tribunal fédéral a-t-il posée?",
        "max_words": 50,
    },
    {
        "name_de": "Rechtsregel",
        "name_fr": "Règle de droit",
        "name_it": "Regola giuridica",
        "weight": 20,
        "instructions_de": "Nennen Sie die angewandte Norm (Gesetz + Artikel) und die Auslegung des Bundesgerichts.",
        "instructions_fr": "Indiquez la norme appliquée (loi + article) et l'interprétation du Tribunal fédéral.",
        "max_words": 80,
    },
    {
        "name_de": "Sachverhalt",
        "name_fr": "Faits",
        "name_it": "Fatti",
        "weight": 15,
        "instructions_de": "Nur die rechtserheblichen Tatsachen. Keine Verfahrensgeschichte, keine irrelevanten Details.",
        "instructions_fr": "Uniquement les faits juridiquement pertinents. Pas d'historique procédural.",
        "max_words": 100,
    },
    {
        "name_de": "Kernerwägungen",
        "name_fr": "Considérants clés",
        "name_it": "Considerandi chiave",
        "weight": 25,
        "instructions_de": "Argumentationskette: Prämissen → Schlussfolgerung. Welche Schritte führen zum Ergebnis?",
        "instructions_fr": "Chaîne d'argumentation: prémisses → conclusion. Quelles étapes mènent au résultat?",
        "max_words": 150,
    },
    {
        "name_de": "Dispositiv",
        "name_fr": "Dispositif",
        "name_it": "Dispositivo",
        "weight": 10,
        "instructions_de": "Ergebnis + Kostenfolge. Gutheissung/Abweisung/Rückweisung?",
        "instructions_fr": "Résultat + frais. Admission/rejet/renvoi?",
        "max_words": 30,
    },
    {
        "name_de": "Bedeutung",
        "name_fr": "Portée",
        "name_it": "Portata",
        "weight": 15,
        "instructions_de": "Präjudizielle Wirkung: Wie hat dieser Entscheid die Rechtsprechung verändert?",
        "instructions_fr": "Portée préjudicielle: comment cet arrêt a-t-il modifié la jurisprudence?",
        "max_words": 60,
    },
]

# ── Study phases ─────────────────────────────────────────────

_STUDY_PHASES = {
    "de": [
        {
            "name": "Orientierung",
            "goal": "Überblick gewinnen: Rechtsgebiet, Parteien, Ergebnis",
            "instructions": "Lesen Sie die Regeste und das Dispositiv. Identifizieren Sie: (1) Welches Rechtsgebiet? (2) Wer sind die Parteien? (3) Wie hat das Gericht entschieden? Notieren Sie die Antworten in Stichpunkten.",
            "duration_min": 2,
            "focus_sections": ["regeste", "dispositiv"],
        },
        {
            "name": "Geführtes Lesen",
            "goal": "Regel + Anwendung + Begründung herausarbeiten",
            "instructions": "Lesen Sie die markierten Kernerwägungen (is_key=true). Für jede Erwägung: (1) Welche Norm wird zitiert? (2) Wie wird sie ausgelegt? (3) Wie wird sie auf den Sachverhalt angewandt? Markieren Sie die Schlüsselstellen.",
            "duration_min": 10,
            "focus_sections": ["erwagungen"],
        },
        {
            "name": "Synthese",
            "goal": "Eigene Zusammenfassung und kritische Reflexion",
            "instructions": "Schreiben Sie ein Case Brief (verwenden Sie die Vorlage). Vergleichen Sie Ihr Brief mit der Regeste. Beantworten Sie die Socratic Questions. Diskutieren Sie die Hypotheticals.",
            "duration_min": 15,
            "focus_sections": ["brief_template", "socratic_questions", "hypotheticals"],
        },
    ],
    "fr": [
        {
            "name": "Orientation",
            "goal": "Vue d'ensemble: domaine juridique, parties, résultat",
            "instructions": "Lisez le regeste et le dispositif. Identifiez: (1) Quel domaine juridique? (2) Qui sont les parties? (3) Comment le tribunal a-t-il statué? Notez les réponses en points.",
            "duration_min": 2,
            "focus_sections": ["regeste", "dispositiv"],
        },
        {
            "name": "Lecture guidée",
            "goal": "Dégager la règle, l'application et le raisonnement",
            "instructions": "Lisez les considérants clés (is_key=true). Pour chaque considérant: (1) Quelle norme est citée? (2) Comment est-elle interprétée? (3) Comment est-elle appliquée aux faits? Soulignez les passages essentiels.",
            "duration_min": 10,
            "focus_sections": ["erwagungen"],
        },
        {
            "name": "Synthèse",
            "goal": "Résumé personnel et réflexion critique",
            "instructions": "Rédigez un case brief (utilisez le modèle). Comparez avec le regeste. Répondez aux questions socratiques. Discutez les hypothétiques.",
            "duration_min": 15,
            "focus_sections": ["brief_template", "socratic_questions", "hypotheticals"],
        },
    ],
    "it": [
        {
            "name": "Orientamento",
            "goal": "Panoramica: ambito giuridico, parti, esito",
            "instructions": "Leggete il regesto e il dispositivo. Identificate: (1) Quale ambito giuridico? (2) Chi sono le parti? (3) Come ha deciso il tribunale? Annotate le risposte per punti.",
            "duration_min": 2,
            "focus_sections": ["regeste", "dispositiv"],
        },
        {
            "name": "Lettura guidata",
            "goal": "Estrarre regola, applicazione e motivazione",
            "instructions": "Leggete i considerandi chiave (is_key=true). Per ogni considerando: (1) Quale norma è citata? (2) Come viene interpretata? (3) Come viene applicata ai fatti? Sottolineate i passaggi essenziali.",
            "duration_min": 10,
            "focus_sections": ["erwagungen"],
        },
        {
            "name": "Sintesi",
            "goal": "Riassunto personale e riflessione critica",
            "instructions": "Scrivete un case brief (usate il modello). Confrontate con il regesto. Rispondete alle domande socratiche. Discutete le ipotesi.",
            "duration_min": 15,
            "focus_sections": ["brief_template", "socratic_questions", "hypotheticals"],
        },
    ],
}


def build_study_package(
    *,
    decision: dict,
    mode: str = "guided",
    curriculum_case: CurriculumCase | None = None,
    citation_counts: tuple[int, int] = (0, 0),
    related_cases: list[dict] | None = None,
    requested_language: str = "de",
) -> dict[str, Any]:
    """Build a structured study package from a fetched decision.

    Args:
        decision: Row dict from get_decision_by_id (has full_text, regeste, etc.)
        mode: "guided", "brief", or "quick"
        curriculum_case: Matching curriculum entry, if any
        citation_counts: (incoming, outgoing) from citation graph
        related_cases: Prerequisite/successor cases from curriculum
        requested_language: Student's preferred language

    Returns:
        Structured dict for the MCP tool response.
    """
    decision_language = decision.get("language", "de")
    parsed = parse_decision(
        decision.get("full_text", ""),
        language=decision_language,
        regeste=decision.get("regeste", ""),
    )
    lang_key = requested_language if requested_language in ("de", "fr", "it") else "de"

    base: dict[str, Any] = {
        "decision_id": decision.get("decision_id", ""),
        "docket_number": decision.get("docket_number", ""),
        "decision_date": decision.get("decision_date", ""),
        "court": decision.get("court", ""),
        "chamber": decision.get("chamber", ""),
        "language": decision_language,
        "cited_by_count": citation_counts[0],
        "cites_count": citation_counts[1],
        "parse_quality": parsed.parse_quality,
        "is_excerpt": parsed.is_excerpt,
    }

    # Language mismatch warning
    if decision_language != requested_language:
        lang_names = {"de": "Deutsch", "fr": "Französisch", "it": "Italienisch"}
        base["language_note"] = (
            f"Dieses Urteil ist in {lang_names.get(decision_language, decision_language)} verfasst. "
            f"Die Regeste ist dreisprachig (DE/FR/IT), der Volltext jedoch nur "
            f"in {lang_names.get(decision_language, decision_language)}."
        )

    if curriculum_case:
        curriculum_data: dict[str, Any] = {
            "area_id": curriculum_case.area_id,
            "module_id": curriculum_case.module_id,
            "bge_ref": curriculum_case.bge_ref,
            "title": getattr(curriculum_case, f"title_{lang_key}", curriculum_case.title_de) or curriculum_case.title_de,
            "concepts": getattr(curriculum_case, f"concepts_{lang_key}", curriculum_case.concepts_de) or curriculum_case.concepts_de,
            "statutes": curriculum_case.statutes,
            "difficulty": curriculum_case.difficulty,
            "significance": getattr(curriculum_case, f"significance_{lang_key}", curriculum_case.significance_de) or curriculum_case.significance_de,
        }
        if curriculum_case.key_erwagungen:
            curriculum_data["key_erwagungen"] = curriculum_case.key_erwagungen
        reading_guide = getattr(curriculum_case, f"reading_guide_{lang_key}", "") or curriculum_case.reading_guide_de
        if reading_guide:
            curriculum_data["reading_guide"] = reading_guide
        base["curriculum"] = curriculum_data

    if mode == "quick":
        # Minimal: regeste + ratio + review_cards
        all_statutes = set()
        top_erwagungen = []
        for e in parsed.erwagungen:
            all_statutes.update(e.statute_refs)
            if e.depth == 1:
                top_erwagungen.append(e.number)
        base["regeste"] = parsed.regeste
        base["top_erwagungen"] = top_erwagungen
        base["all_statutes"] = sorted(all_statutes)
        base["review_cards"] = _build_review_cards(parsed, curriculum_case, lang_key)
        return base

    if mode == "brief":
        # Parsed sections + brief template + review cards
        base["regeste"] = parsed.regeste
        base["sachverhalt"] = parsed.sachverhalt
        base["erwagungen"] = _format_erwagungen(parsed.erwagungen, curriculum_case)
        base["dispositiv"] = parsed.dispositiv
        base["brief_template"] = _build_brief_template(lang_key)
        base["review_cards"] = _build_review_cards(parsed, curriculum_case, lang_key)
        return base

    # mode == "guided" (default): full package with all 4 new sections
    base["regeste"] = parsed.regeste
    base["sachverhalt"] = parsed.sachverhalt
    base["erwagungen"] = _format_erwagungen(parsed.erwagungen, curriculum_case)
    base["dispositiv"] = parsed.dispositiv

    if related_cases:
        base["related_cases"] = related_cases

    # New section 1: Socratic questions
    base["socratic_questions"] = _build_socratic_questions(
        parsed, curriculum_case, lang_key,
    )

    # New section 2: Study phases
    base["study_phases"] = _STUDY_PHASES.get(lang_key, _STUDY_PHASES["de"])

    # New section 3: Hypothetical variations
    base["hypotheticals"] = _build_hypotheticals(curriculum_case, lang_key)

    # New section 4: Review cards
    base["review_cards"] = _build_review_cards(parsed, curriculum_case, lang_key)

    # Brief template included in guided mode too
    base["brief_template"] = _build_brief_template(lang_key)

    return base


# ── Internal helpers ─────────────────────────────────────────

def _format_erwagungen(
    erwagungen: list,
    curriculum_case: CurriculumCase | None,
) -> list[dict]:
    """Format Erwägungen with optional key_erwagungen highlighting."""
    key_set = set(curriculum_case.key_erwagungen) if curriculum_case and curriculum_case.key_erwagungen else set()
    result = []
    for e in erwagungen:
        entry: dict[str, Any] = {
            "number": e.number,
            "depth": e.depth,
            "statute_refs": e.statute_refs,
            "text": e.text,
        }
        if key_set and e.number in key_set:
            entry["is_key"] = True
        result.append(entry)
    return result


def _build_socratic_questions(
    parsed: ParsedDecision,
    curriculum_case: CurriculumCase | None,
    lang_key: str,
) -> list[dict]:
    """Build Socratic questions — from curriculum JSON or fallback from parsed data."""
    # If curriculum has pre-authored questions, use them
    if curriculum_case and curriculum_case.socratic_questions:
        return curriculum_case.socratic_questions

    # Fallback: generate from parsed decision structure
    questions = []
    # Collect statutes and key Erwägung text for generating hints
    all_statutes = set()
    key_erwagungen_text = []
    key_set = set(curriculum_case.key_erwagungen) if curriculum_case and curriculum_case.key_erwagungen else set()

    for e in parsed.erwagungen:
        all_statutes.update(e.statute_refs)
        if key_set and e.number in key_set:
            key_erwagungen_text.append(f"E. {e.number}")

    statutes_str = ", ".join(sorted(all_statutes)[:3]) if all_statutes else "die einschlägige Norm"
    key_erw_str = ", ".join(key_erwagungen_text[:3]) if key_erwagungen_text else "die Kernerwägungen"

    for bloom in _BLOOM_LEVELS:
        prompt_key = f"prompt_{lang_key}"
        label_key = f"label_{lang_key}"
        q: dict[str, Any] = {
            "level": bloom["level"],
            "level_label": bloom.get(label_key, bloom["label_de"]),
            "question": bloom.get(prompt_key, bloom["prompt_de"]),
        }
        # Level-specific hints
        if bloom["level"] == 1:
            q["hint"] = "Lesen Sie die Regeste und das Dispositiv." if lang_key == "de" else "Lisez le regeste et le dispositif." if lang_key == "fr" else "Leggete il regesto e il dispositivo."
        elif bloom["level"] == 2:
            q["hint"] = f"Suchen Sie nach {statutes_str} in {key_erw_str}." if lang_key == "de" else f"Cherchez {statutes_str} dans les considérants clés."
        elif bloom["level"] == 3:
            q["hint"] = f"Prüfen Sie die Subsumtion in {key_erw_str}." if lang_key == "de" else "Examinez la subsomption dans les considérants clés."
        elif bloom["level"] == 4:
            q["hint"] = "Verändern Sie ein Sachverhaltselement und überlegen Sie die Folgen." if lang_key == "de" else "Modifiez un élément factuel et réfléchissez aux conséquences."
        elif bloom["level"] == 5:
            q["hint"] = "Argumentieren Sie Pro und Contra der bundesgerichtlichen Begründung." if lang_key == "de" else "Argumentez pour et contre le raisonnement du Tribunal fédéral."

        questions.append(q)

    return questions


def _build_hypotheticals(
    curriculum_case: CurriculumCase | None,
    lang_key: str,
) -> list[dict]:
    """Build hypothetical variations — from curriculum JSON or empty."""
    if curriculum_case and curriculum_case.hypotheticals:
        return curriculum_case.hypotheticals
    return []


def _build_review_cards(
    parsed: ParsedDecision,
    curriculum_case: CurriculumCase | None,
    lang_key: str,
) -> list[dict]:
    """Build review cards for spaced repetition from parsed decision.

    Cards test conceptual understanding — legal rules, distinctions,
    application logic — rather than trivia about decision structure.
    Sources: curriculum socratic questions (with model answers),
    hypotheticals, and significance metadata.
    """
    cards: list[dict] = []
    bge = curriculum_case.bge_ref if curriculum_case else ""

    # ── From Socratic questions: turn Q+model_answer into cards ───
    if curriculum_case and curriculum_case.socratic_questions:
        for sq in curriculum_case.socratic_questions:
            answer = sq.get("model_answer", "")
            if not answer:
                continue
            level = sq.get("level", 0)
            # Skip level 4 (analysis / "what if") — those are better as
            # hypotheticals. Keep levels 1-3 (comprehension, rule ID,
            # application) and 5 (evaluation).
            if level == 4:
                continue
            label = sq.get("level_label", "")
            cards.append({
                "front": sq["question"],
                "back": answer,
                "tags": [label.lower()] if label else [],
            })

    # ── From hypotheticals: "what changes if X?" cards ────────────
    if curriculum_case and curriculum_case.hypotheticals:
        for hyp in curriculum_case.hypotheticals:
            scenario = hyp.get("scenario", "")
            outcome = hyp.get("likely_outcome_shift", "")
            if scenario and outcome:
                cards.append({
                    "front": scenario,
                    "back": outcome,
                    "tags": ["variation", hyp.get("type", "")],
                })

    # ── Significance: why does this case matter? ──────────────────
    if curriculum_case:
        sig = (
            getattr(curriculum_case, f"significance_{lang_key}", "")
            or curriculum_case.significance_de
        )
        if sig:
            front = {
                "de": f"Warum ist {bge} ein Leitentscheid? Was hat er verändert?",
                "fr": f"Pourquoi {bge} est-il un arrêt de principe? Qu'a-t-il changé?",
                "it": f"Perché {bge} è una sentenza di principio? Cosa ha cambiato?",
            }
            cards.append({
                "front": front.get(lang_key, front["de"]),
                "back": sig,
                "tags": ["bedeutung"],
            })

    # ── Fallback for non-curriculum cases: ratio from regeste ─────
    if not cards and parsed.regeste:
        regeste_short = parsed.regeste[:400]
        front = {
            "de": "Was ist die Kernaussage dieses Entscheids und welche Regel stellt er auf?",
            "fr": "Quelle est la règle posée par cet arrêt?",
            "it": "Qual è la regola stabilita da questa decisione?",
        }
        cards.append({
            "front": front.get(lang_key, front["de"]),
            "back": regeste_short,
            "tags": ["ratio"],
        })

    return cards


def _build_brief_template(lang_key: str) -> list[dict]:
    """Build the case brief template with section instructions."""
    template = []
    for section in _BRIEF_TEMPLATE_SECTIONS:
        name_key = f"name_{lang_key}"
        instr_key = f"instructions_{lang_key}"
        entry: dict[str, Any] = {
            "name": section.get(name_key, section["name_de"]),
            "weight_percent": section["weight"],
            "instructions": section.get(instr_key, section["instructions_de"]),
            "max_words": section["max_words"],
        }
        template.append(entry)
    return template


# ── Brief comparison with rubric ─────────────────────────────

def build_brief_comparison(
    *,
    decision: dict,
    student_brief: str,
    language: str = "de",
    curriculum_case: CurriculumCase | None = None,
) -> dict[str, Any]:
    """Build a structured comparison between a student's brief and the decision.

    Returns the parsed decision ground truth alongside the student text,
    structured for the calling LLM to generate pedagogical feedback.
    Includes brief_template, rubric sections, and common_mistakes.
    """
    lang_key = language if language in ("de", "fr", "it") else "de"
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

    result: dict[str, Any] = {
        "decision_id": decision.get("decision_id", ""),
        "docket_number": decision.get("docket_number", ""),
        "language": decision.get("language", ""),
        "feedback_language": lang_key,
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

    # Add rubric sections for structured feedback
    result["rubric"] = _build_rubric(lang_key)

    # Add brief template for reference
    result["brief_template"] = _build_brief_template(lang_key)

    # Add common mistakes per section
    result["common_mistakes"] = _build_common_mistakes(lang_key)

    # Add curriculum context if available
    if curriculum_case:
        sig = getattr(curriculum_case, f"significance_{lang_key}", "") or curriculum_case.significance_de
        result["curriculum_context"] = {
            "title": getattr(curriculum_case, f"title_{lang_key}", curriculum_case.title_de) or curriculum_case.title_de,
            "significance": sig,
            "key_erwagungen": curriculum_case.key_erwagungen,
            "statutes": curriculum_case.statutes,
        }

    return result


def _build_rubric(lang_key: str) -> list[dict]:
    """Build a scoring rubric for each brief section."""
    rubric = []
    for section in _BRIEF_TEMPLATE_SECTIONS:
        name_key = f"name_{lang_key}"
        rubric.append({
            "section": section.get(name_key, section["name_de"]),
            "weight_percent": section["weight"],
            "scoring_criteria": _rubric_criteria(section["name_de"], lang_key),
        })
    return rubric


def _rubric_criteria(section_name: str, lang_key: str) -> dict:
    """Return scoring criteria for a brief section."""
    criteria_map = {
        "Leitsatz": {
            "de": {
                "excellent": "Ratio decidendi korrekt und präzise in 1-2 Sätzen formuliert",
                "good": "Ratio im Wesentlichen erfasst, evtl. etwas zu breit oder zu eng",
                "needs_work": "Ratio fehlt, verwechselt mit Sachverhalt oder Dispositiv",
            },
            "fr": {
                "excellent": "Ratio decidendi correcte et précise en 1-2 phrases",
                "good": "Ratio globalement saisie, peut-être un peu trop large ou étroite",
                "needs_work": "Ratio manquante, confondue avec les faits ou le dispositif",
            },
        },
        "Rechtsregel": {
            "de": {
                "excellent": "Korrekte Norm mit präziser Auslegung des Bundesgerichts",
                "good": "Richtige Norm, aber Auslegung unvollständig oder ungenau",
                "needs_work": "Falsche oder fehlende Norm, keine Auslegung",
            },
            "fr": {
                "excellent": "Norme correcte avec interprétation précise du Tribunal fédéral",
                "good": "Bonne norme, mais interprétation incomplète",
                "needs_work": "Norme incorrecte ou manquante",
            },
        },
        "Sachverhalt": {
            "de": {
                "excellent": "Nur rechtserhebliche Tatsachen, klar strukturiert",
                "good": "Wesentliches erfasst, aber mit irrelevanten Details oder Lücken",
                "needs_work": "Verfahrensgeschichte statt Sachverhalt, oder Kernfakten fehlen",
            },
            "fr": {
                "excellent": "Uniquement les faits juridiquement pertinents, bien structurés",
                "good": "Essentiel saisi, mais détails non pertinents ou lacunes",
                "needs_work": "Historique procédural au lieu des faits, ou faits essentiels manquants",
            },
        },
        "Kernerwägungen": {
            "de": {
                "excellent": "Vollständige Argumentationskette mit Prämissen und Schlussfolgerung",
                "good": "Hauptargument erfasst, aber Zwischenschritte fehlen",
                "needs_work": "Nur Ergebnis wiedergegeben, keine Argumentation",
            },
            "fr": {
                "excellent": "Chaîne d'argumentation complète avec prémisses et conclusion",
                "good": "Argument principal saisi, mais étapes intermédiaires manquantes",
                "needs_work": "Seul le résultat est rapporté, pas d'argumentation",
            },
        },
        "Dispositiv": {
            "de": {
                "excellent": "Ergebnis + Kostenfolge korrekt und vollständig",
                "good": "Ergebnis richtig, Kostenfolge fehlt",
                "needs_work": "Ergebnis falsch oder fehlend",
            },
            "fr": {
                "excellent": "Résultat + frais corrects et complets",
                "good": "Résultat correct, frais manquants",
                "needs_work": "Résultat incorrect ou manquant",
            },
        },
        "Bedeutung": {
            "de": {
                "excellent": "Präjudizielle Wirkung klar beschrieben, Einordnung in Rechtsprechung",
                "good": "Bedeutung erkannt, aber ohne Einordnung",
                "needs_work": "Keine Aussage zur Bedeutung des Entscheids",
            },
            "fr": {
                "excellent": "Portée préjudicielle clairement décrite, mise en contexte",
                "good": "Portée reconnue, mais sans mise en contexte",
                "needs_work": "Aucune mention de la portée de l'arrêt",
            },
        },
    }
    lang = lang_key if lang_key in ("de", "fr") else "de"
    return criteria_map.get(section_name, criteria_map["Leitsatz"]).get(lang, criteria_map["Leitsatz"]["de"])


def _build_common_mistakes(lang_key: str) -> list[dict]:
    """Return common case brief mistakes by section."""
    if lang_key == "fr":
        return [
            {"section": "Ratio decidendi", "mistake": "Confondre le résultat (dispositif) avec la règle de droit (ratio)"},
            {"section": "Ratio decidendi", "mistake": "Formuler la ratio trop largement — elle doit être spécifique à l'arrêt"},
            {"section": "Règle de droit", "mistake": "Citer l'article sans l'interprétation du tribunal"},
            {"section": "Faits", "mistake": "Inclure l'historique procédural au lieu des faits pertinents"},
            {"section": "Considérants clés", "mistake": "Résumer chaque considérant au lieu de suivre la chaîne d'argumentation"},
            {"section": "Portée", "mistake": "Omettre la portée préjudicielle — quel principe nouveau est posé?"},
        ]
    if lang_key == "it":
        return [
            {"section": "Massima", "mistake": "Confondere il risultato (dispositivo) con la regola di diritto (ratio)"},
            {"section": "Regola giuridica", "mistake": "Citare l'articolo senza l'interpretazione del tribunale"},
            {"section": "Fatti", "mistake": "Includere la cronologia procedurale invece dei fatti rilevanti"},
            {"section": "Considerandi chiave", "mistake": "Riassumere ogni considerando invece di seguire la catena argomentativa"},
            {"section": "Portata", "mistake": "Omettere la portata del precedente"},
        ]
    # Default: de
    return [
        {"section": "Leitsatz", "mistake": "Verwechslung von Ergebnis (Dispositiv) und Rechtsregel (Ratio)"},
        {"section": "Leitsatz", "mistake": "Ratio zu breit formuliert — sie muss fallspezifisch sein"},
        {"section": "Rechtsregel", "mistake": "Artikel zitiert ohne die Auslegung des Bundesgerichts"},
        {"section": "Sachverhalt", "mistake": "Verfahrensgeschichte statt rechtserhebliche Tatsachen"},
        {"section": "Kernerwägungen", "mistake": "Jede Erwägung einzeln zusammengefasst statt Argumentationskette nachgezeichnet"},
        {"section": "Bedeutung", "mistake": "Präjudizielle Wirkung nicht erkannt — welcher neue Grundsatz wird aufgestellt?"},
    ]
