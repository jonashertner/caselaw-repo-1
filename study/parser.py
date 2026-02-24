"""Parse BGE decision full_text into structural components."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from search_stack.reference_extraction import extract_statute_references


@dataclass
class Erwagung:
    number: str           # "1", "1.1", "3.2.1"
    text: str
    statute_refs: list[str] = field(default_factory=list)
    depth: int = 1        # 1=top, 2=sub, 3=sub-sub


@dataclass
class ParsedDecision:
    sachverhalt: str = ""
    erwagungen: list[Erwagung] = field(default_factory=list)
    dispositiv: str = ""
    regeste: str = ""
    language: str = "de"
    parse_quality: float = 0.0
    is_excerpt: bool = False  # "Auszug" — partial decision, missing sections expected


# ── Section header patterns ──────────────────────────────────

_SACHVERHALT_PATTERNS = [
    # DE
    re.compile(r"^\s*Sachverhalt\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*Aus\s+den?\s+Sachverhalt", re.MULTILINE | re.IGNORECASE),
    # FR
    re.compile(r"^\s*Faits\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*En\s+fait\b", re.MULTILINE | re.IGNORECASE),
    # IT
    re.compile(r"^\s*Fatti\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*In\s+fatto\b", re.MULTILINE | re.IGNORECASE),
]

_ERWAGUNGEN_PATTERNS = [
    # DE
    re.compile(r"^\s*Erwägungen?\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*Aus\s+den\s+Erwägungen\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*Auszug\s+aus\s+den\s+Erwägungen\b", re.MULTILINE | re.IGNORECASE),
    # FR
    re.compile(r"^\s*Considérants?\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*En\s+droit\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*Extrait\s+des\s+considérants\b", re.MULTILINE | re.IGNORECASE),
    # IT
    re.compile(r"^\s*Considerand[io]\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*In\s+diritto\b", re.MULTILINE | re.IGNORECASE),
]

_DISPOSITIV_PATTERNS = [
    # DE
    re.compile(r"^\s*Demnach\s+erkennt\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*(?:Das\s+)?Bundesgericht\s+erkennt\b", re.MULTILINE | re.IGNORECASE),
    # FR
    re.compile(r"^\s*Par\s+ces\s+motifs\b", re.MULTILINE | re.IGNORECASE),
    # IT
    re.compile(r"^\s*Per\s+questi\s+motivi\b", re.MULTILINE | re.IGNORECASE),
]

# Partial decision markers — missing Sachverhalt/Dispositiv is expected
_EXCERPT_PATTERNS = [
    # DE
    re.compile(r"Auszug\s+aus\s+de[mnr]\s+(?:Urteil|Erwägungen|Entscheid)", re.IGNORECASE),
    # FR
    re.compile(r"Extrait\s+d[eu]\s+(?:l'arrêt|jugement|considérants?)", re.IGNORECASE),
    # IT
    re.compile(r"Estratto\s+d(?:ella|i)\s+(?:sentenza|considerand[io])", re.IGNORECASE),
]

# Numbered Erwägung: "1.", "1.1.", "1.1.1.", "5.2."
_ERWAGUNG_NUM = re.compile(r"^\s*(\d+(?:\.\d+)*)\.\s", re.MULTILINE)

# Letter-labeled sub-sections: "a)", "b)", "aa)", "bb)" at start of line
_ERWAGUNG_LETTER = re.compile(r"^\s*([a-z]{1,2})\)\s", re.MULTILINE)


def _find_section_start(text: str, patterns: list[re.Pattern]) -> int | None:
    """Return the character offset of the first matching section header, or None."""
    best: int | None = None
    for pat in patterns:
        m = pat.search(text)
        if m and (best is None or m.start() < best):
            best = m.start()
    return best


def _extract_erwagungen(text: str) -> list[Erwagung]:
    """Split reasoning text into numbered Erwägungen.

    Handles both numbered sub-sections (1.1., 1.2.) and letter-labeled
    sub-sections (a), b), aa)) common in BGE decisions.
    """
    # Normalize sub-numbers on their own line: "5.1\n" → "5.1.\n"
    # Real BGE texts often have sub-headings without trailing periods.
    text = re.sub(r"^(\s*\d+\.\d+(?:\.\d+)*)\s*$", r"\1.", text, flags=re.MULTILINE)
    matches = list(_ERWAGUNG_NUM.finditer(text))
    if not matches:
        return []

    erwagungen: list[Erwagung] = []
    for i, m in enumerate(matches):
        number = m.group(1)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        depth = number.count(".") + 1

        # Check for letter-labeled sub-sections within this Erwägung
        letter_matches = list(_ERWAGUNG_LETTER.finditer(body))
        if letter_matches:
            # Text before first letter sub-section is the parent intro
            intro = body[:letter_matches[0].start()].strip()
            if intro:
                refs = extract_statute_references(intro)
                erwagungen.append(Erwagung(
                    number=number,
                    text=intro,
                    statute_refs=[r.normalized for r in refs],
                    depth=depth,
                ))

            # Each letter sub-section
            for j, lm in enumerate(letter_matches):
                letter = lm.group(1)
                sub_start = lm.end()
                sub_end = (
                    letter_matches[j + 1].start()
                    if j + 1 < len(letter_matches)
                    else len(body)
                )
                sub_body = body[sub_start:sub_end].strip()
                sub_number = f"{number}.{letter}"
                refs = extract_statute_references(sub_body)
                erwagungen.append(Erwagung(
                    number=sub_number,
                    text=sub_body,
                    statute_refs=[r.normalized for r in refs],
                    depth=depth + 1,
                ))
        else:
            # No letter sub-sections — add as single Erwägung
            refs = extract_statute_references(body)
            ref_strs = [r.normalized for r in refs]
            erwagungen.append(Erwagung(
                number=number,
                text=body,
                statute_refs=ref_strs,
                depth=depth,
            ))

    return erwagungen


def parse_decision(
    full_text: str,
    *,
    language: str = "de",
    regeste: str = "",
) -> ParsedDecision:
    """Parse a BGE decision's full_text into structural components.

    Returns a ParsedDecision with best-effort parsing and a parse_quality score.
    """
    if not full_text or not full_text.strip():
        return ParsedDecision(regeste=regeste, language=language)

    text = full_text

    # ── Detect excerpt (partial decision) ─────────────────────
    is_excerpt = any(p.search(text) for p in _EXCERPT_PATTERNS)

    # ── Locate section boundaries ────────────────────────────
    sach_start = _find_section_start(text, _SACHVERHALT_PATTERNS)
    erw_start = _find_section_start(text, _ERWAGUNGEN_PATTERNS)
    disp_start = _find_section_start(text, _DISPOSITIV_PATTERNS)

    # If no explicit Erwägungen header, try to find first numbered paragraph
    if erw_start is None:
        first_num = _ERWAGUNG_NUM.search(text)
        if first_num:
            erw_start = first_num.start()

    # ── Extract sections ─────────────────────────────────────

    # Sachverhalt: from header to Erwägungen (or Dispositiv, or end)
    sachverhalt = ""
    if sach_start is not None:
        sach_end = erw_start or disp_start or len(text)
        sachverhalt = text[sach_start:sach_end].strip()

    # Erwägungen: from header to Dispositiv (or end)
    erwagungen_text = ""
    erwagungen: list[Erwagung] = []
    if erw_start is not None:
        erw_end = disp_start or len(text)
        erwagungen_text = text[erw_start:erw_end].strip()
        erwagungen = _extract_erwagungen(erwagungen_text)

    # Dispositiv: from header to end
    dispositiv = ""
    if disp_start is not None:
        dispositiv = text[disp_start:].strip()

    # ── Quality scoring ──────────────────────────────────────
    # For excerpts: only score what's expected (Erwägungen).
    # For full decisions: score all three sections.
    if is_excerpt:
        quality = 0.0
        if erwagungen:
            quality = 0.8  # Erwägungen found with numbered sections
        elif erw_start is not None:
            quality = 0.6  # Erwägungen text found but no numbered sections
        elif sachverhalt or dispositiv:
            quality = 0.3  # No Erwägungen but other sections present
        if sachverhalt:
            quality = min(quality + 0.1, 1.0)
        if dispositiv:
            quality = min(quality + 0.1, 1.0)
    else:
        quality = 0.0
        if sachverhalt:
            quality += 0.3
        if erw_start is not None:
            quality += 0.3
            if erwagungen:
                quality += 0.1
        if dispositiv:
            quality += 0.3

    return ParsedDecision(
        sachverhalt=sachverhalt,
        erwagungen=erwagungen,
        dispositiv=dispositiv,
        regeste=regeste,
        language=language,
        parse_quality=min(quality, 1.0),
        is_excerpt=is_excerpt,
    )
