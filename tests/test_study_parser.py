from __future__ import annotations

from study.parser import Erwagung, ParsedDecision, parse_decision


# ── Minimal synthetic texts for unit tests ────────────────────

DE_DECISION = """
Sachverhalt

A. Die Beschwerdeführerin ist Eigentümerin eines Grundstücks.

B. Das Obergericht wies die Klage ab.

Erwägungen

1. Die Beschwerde ist zulässig (Art. 72 BGG).

1.1. Gemäss Art. 8 BV sind alle Menschen vor dem Gesetz gleich.

1.2. Der Grundsatz von Treu und Glauben (Art. 2 ZGB) ist zu beachten.

2. In der Sache selbst ist die Beschwerde unbegründet.

2.1. Die Vorinstanz hat Art. 41 OR korrekt angewendet.

Demnach erkennt das Bundesgericht:

1. Die Beschwerde wird abgewiesen.
2. Die Gerichtskosten werden der Beschwerdeführerin auferlegt.
"""

FR_DECISION = """
Faits

A. Le recourant est propriétaire d'un immeuble.

B. Le Tribunal cantonal a rejeté la demande.

Considérants

1. Le recours est recevable (art. 72 LTF).

1.1. Selon l'art. 8 Cst., tous les êtres humains sont égaux devant la loi.

2. Sur le fond, le recours est mal fondé.

Par ces motifs, le Tribunal fédéral prononce:

1. Le recours est rejeté.
"""

IT_DECISION = """
Fatti

A. Il ricorrente è proprietario di un immobile.

B. Il Tribunale cantonale ha respinto la domanda.

Considerandi

1. Il ricorso è ammissibile (art. 72 LTF).

2. Nel merito, il ricorso è infondato.

Per questi motivi, il Tribunale federale pronuncia:

1. Il ricorso è respinto.
"""


def test_parse_german_decision():
    result = parse_decision(DE_DECISION, language="de", regeste="Testregeste")
    assert isinstance(result, ParsedDecision)
    assert "Eigentümerin" in result.sachverhalt
    assert "abgewiesen" in result.dispositiv
    assert len(result.erwagungen) >= 4  # 1, 1.1, 1.2, 2, 2.1
    assert result.regeste == "Testregeste"
    assert result.language == "de"
    assert result.parse_quality >= 0.9


def test_parse_french_decision():
    result = parse_decision(FR_DECISION, language="fr", regeste="")
    assert "propriétaire" in result.sachverhalt
    assert "rejeté" in result.dispositiv
    assert len(result.erwagungen) >= 2
    assert result.parse_quality >= 0.9


def test_parse_italian_decision():
    result = parse_decision(IT_DECISION, language="it", regeste="")
    assert "proprietario" in result.sachverhalt
    assert "respinto" in result.dispositiv
    assert len(result.erwagungen) >= 2
    assert result.parse_quality >= 0.9


def test_erwagung_numbering_and_depth():
    result = parse_decision(DE_DECISION, language="de", regeste="")
    numbers = [e.number for e in result.erwagungen]
    assert "1" in numbers or "1." in numbers
    assert "1.1" in numbers or "1.1." in numbers

    top = [e for e in result.erwagungen if e.depth == 1]
    sub = [e for e in result.erwagungen if e.depth == 2]
    assert len(top) >= 2
    assert len(sub) >= 2


def test_statute_refs_per_erwagung():
    result = parse_decision(DE_DECISION, language="de", regeste="")
    # E. 1.1 mentions Art. 8 BV
    e11 = [e for e in result.erwagungen if e.number in ("1.1", "1.1.")]
    assert len(e11) == 1
    refs = e11[0].statute_refs
    assert any("BV" in r for r in refs)


def test_empty_text_returns_low_quality():
    result = parse_decision("", language="de", regeste="")
    assert result.parse_quality <= 0.1
    assert result.erwagungen == []


def test_partial_parse_returns_medium_quality():
    """Text with Erwägungen but no clear Sachverhalt header."""
    text = """
1. Die Beschwerde ist zulässig.
2. Die Beschwerde ist unbegründet.
Demnach erkennt das Bundesgericht:
1. Abgewiesen.
"""
    result = parse_decision(text, language="de", regeste="")
    assert 0.3 <= result.parse_quality <= 0.7
    assert len(result.erwagungen) >= 2
