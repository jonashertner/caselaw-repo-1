"""Trilingual glossary — deterministic DE/FR/IT cross-language expansion
for Italian search recall (user report 2026-07-04)."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from search_stack.legal_glossary import trilingual_equivalents  # noqa: E402


def test_german_query_adds_italian_and_french():
    out = trilingual_equivalents("Verjährung der Forderung")
    assert "prescrizione" in out
    assert "prescription" in out
    assert "verjährung" not in [o.lower() for o in out]  # not the input form


def test_french_query_adds_italian():
    out = trilingual_equivalents("résiliation du bail")
    assert "disdetta" in out       # résiliation
    assert "locazione" in out      # bail
    assert "kündigung" in out


def test_italian_query_adds_german_french():
    out = trilingual_equivalents("ricorso in materia penale")
    assert "beschwerde" in out
    assert "recours" in out


def test_multiword_phrase_matched():
    out = trilingual_equivalents("droit d'être entendu")
    assert "diritto di essere sentito" in out
    assert "rechtliches gehör" in out


def test_synonym_list_input():
    out = trilingual_equivalents(["Kündigung", "Arbeitsvertrag"])
    assert "disdetta" in out
    assert "contratto di lavoro" in out


def test_no_false_positive_on_substring():
    # 'vol' (theft) must not fire on 'volume' or 'volontaire'
    out = trilingual_equivalents("le volume du dossier")
    assert "furto" not in out
    assert "diebstahl" not in out


def test_unknown_terms_return_empty():
    assert trilingual_equivalents("quantum blockchain tokenomics") == []
    assert trilingual_equivalents("") == []
    assert trilingual_equivalents([]) == []
