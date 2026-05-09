"""Regression: _split_paragraphs must not over-fragment PDF-extracted
BGE Erwägungen that have citation references on their own lines.

2026-05-09: bge_BGE_133_III_121 was rendering with one <p> per legal
citation because the HUDOC paragraph-per-line fallback was triggering
on PDF-extracted BGE prose where citations got hard-line-wrapped.
The hardening: only apply per-line fallback when the line-length
distribution looks like real HUDOC (legal-prose lines), not when the
majority of lines are short citation-only fragments.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from seo_pages import _split_paragraphs


def test_citation_broken_text_collapses_to_one_paragraph():
    text = (
        "Il n'est pas conteste, ni contestable du reste, que les parties "
        "etaient liees par un mandat (\n"
        "art. 394 ss CO\n;\nATF 119 II 456\n"
        "consid. 2 et les arrets cites). L'\n"
        "art. 398 al. 2 CO\n"
        "rend le mandataire responsable envers le mandant de la bonne et "
        "fidele execution du mandat. L'alinea 1 de cette disposition "
        "renvoie aux regles regissant la responsabilite du travailleur "
        "dans les rapports de travail, soit a l'\n"
        "art. 321e CO\n"
        ". Cette disposition prevoit que le travailleur est responsable du\n"
        "BGE 133 III 121 S. 124\n"
        "dommage qu'il cause a l'employeur intentionnellement ou par "
        "negligence (al. 1) et elle determine la mesure de la diligence "
        "requise (al. 2).\n"
    )
    text += "\n".join(
        ["art. 8 CC", ";", "art. 9 CC", ";", "art. 10 CC", ";", "art. 11 CC"]
        * 5
    )
    assert text.count("\n") >= 30
    assert "\n\n" not in text
    paras = _split_paragraphs(text)
    assert len(paras) <= 3, f"over-fragmented: {len(paras)} paragraphs"
    full = " ".join(paras)
    for cite in ("art. 394 ss CO", "ATF 119 II 456", "art. 321e CO"):
        assert cite in full, f"citation missing: {cite}"
    assert len(paras[0]) > 100, (
        f"first paragraph too short ({len(paras[0])} chars): {paras[0]!r}"
    )


def test_real_hudoc_pattern_still_paragraph_per_line():
    paras = [
        "This is a typical HUDOC paragraph with at least sixty characters "
        "of legal prose."
    ] * 35
    text = "\n".join(paras)
    assert text.count("\n") >= 30
    assert "\n\n" not in text
    result = _split_paragraphs(text)
    assert len(result) == 35, (
        f"HUDOC pattern broke: expected 35, got {len(result)}"
    )


def test_short_text_unaffected():
    text = "Auftrag; Haftung des Arztes.\nAllgemeine Voraussetzungen (E. 3)."
    result = _split_paragraphs(text)
    assert 1 <= len(result) <= 2


def test_dispositiv_list_items_preserved():
    text = (
        "Die Beschwerde wird abgewiesen, soweit darauf einzutreten ist.\n"
        "1. Die Gerichtskosten werden der Beschwerdefuehrerin auferlegt.\n"
        "2. Es wird keine Parteientschaedigung zugesprochen.\n"
        "3. Dieses Urteil wird den Parteien schriftlich mitgeteilt."
    )
    result = _split_paragraphs(text)
    assert any("1. Die Gerichtskosten" in p for p in result)
    assert any("2. Es wird keine" in p for p in result)
