from __future__ import annotations

import json
from unittest.mock import patch

from study.socratic import build_study_package, build_brief_comparison


FAKE_DECISION = {
    "decision_id": "bge_144_III_93",
    "docket_number": "144 III 93",
    "decision_date": "2018-01-22",
    "court": "bge",
    "chamber": "I. zivilrechtliche Abteilung",
    "language": "fr",
    "regeste": "Prêt ou donation. Art. 312 CO, Art. 239 CO.",
    "full_text": """Sachverhalt

A. Les parties ont vécu ensemble.

Erwägungen

5. Il est établi que le demandeur a versé le montant.

5.1. Le prêt est un contrat (Art. 312 CO).

5.2. La donation est la disposition (Art. 239 CO).

Demnach erkennt das Bundesgericht:

1. Le recours est rejeté.
""",
}


def test_build_study_package_guided():
    result = build_study_package(
        decision=FAKE_DECISION,
        mode="guided",
        citation_counts=(347, 14),
    )
    assert result["decision_id"] == "bge_144_III_93"
    assert result["cited_by_count"] == 347
    assert "erwagungen" in result
    assert len(result["erwagungen"]) >= 3
    assert "sachverhalt" in result
    assert "dispositiv" in result


def test_build_study_package_quick():
    result = build_study_package(
        decision=FAKE_DECISION,
        mode="quick",
    )
    assert "regeste" in result
    assert "top_erwagungen" in result
    assert "sachverhalt" not in result  # quick mode omits full sections


def test_build_study_package_brief():
    result = build_study_package(
        decision=FAKE_DECISION,
        mode="brief",
    )
    assert "erwagungen" in result
    assert "sachverhalt" in result


def test_build_brief_comparison():
    result = build_brief_comparison(
        decision=FAKE_DECISION,
        student_brief="The court held that a loan requires restitution.",
    )
    assert "ground_truth" in result
    assert "student_brief" in result
    assert result["student_brief"] == "The court held that a loan requires restitution."
    assert "regeste" in result["ground_truth"]
    assert "erwagung_summaries" in result["ground_truth"]
    assert len(result["ground_truth"]["statutes"]) >= 1
