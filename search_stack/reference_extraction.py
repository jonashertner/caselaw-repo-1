"""
Legal reference extraction for Swiss case law text.

Extracts:
- Statute references (e.g., "Art. 8 EMRK", "Art. 34 Abs. 2 BV")
- Case citations (BGE and docket-like references)
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass

_ARTICLE_MARKER = r"(?:Art\.?|Artikel)"
_PARAGRAPH_MARKER = r"(?:Abs\.?|Absatz|al\.?|alin(?:ea)?\.?|cpv\.?|co\.?|para\.?)"
_ORDINAL_SUFFIX = r"(?:bis|ter|quater|quinquies|sexies)"
_INVALID_LAW_CODES = {
    "AL",
    "ABS",
    "ABSATZ",
    "ALIN",
    "ALINEA",
    "CPV",
    "PARA",
    "BIS",
    "TER",
    "QUATER",
    "QUINQUIES",
    "SEXIES",
}

STATUTE_PATTERN = re.compile(
    rf"""
    \b{_ARTICLE_MARKER}\s*
    (?P<article>\d+[a-z]?(?:\s*{_ORDINAL_SUFFIX})?)\s*
    (?:{_PARAGRAPH_MARKER}\s*(?P<paragraph>\d+[a-z]?))?\s*
    (?:{_ORDINAL_SUFFIX}\s*)?
    (?P<law>[A-Z][A-Z0-9]{{1,11}}(?:/[A-Z0-9]{{2,6}})?)
    \b
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

BGE_PATTERN = re.compile(
    r"\bBGE\s+(?P<vol>\d{2,3})\s+(?P<div>[IVX]{1,4})\s+(?P<page>\d{1,4})\b",
    flags=re.IGNORECASE,
)

DOCKET_PATTERNS = [
    # 1A.122/2005, 2C_37/2016, D-7414/2015
    re.compile(r"\b[A-Z0-9]{1,4}[._-]\d{1,6}[/_]\d{4}\b"),
    # VB.2018.00411, RR.2012.25
    re.compile(r"\b[A-Z]{1,6}\.\d{4}\.\d{1,6}\b"),
    # 151 I 62 (internal BGE style references without explicit "BGE")
    re.compile(r"\b\d{2,3}\s+[IVX]{1,4}\s+\d{1,4}\b"),
]


@dataclass(frozen=True)
class StatuteReference:
    raw: str
    law_code: str
    article: str
    paragraph: str | None
    normalized: str


@dataclass(frozen=True)
class CaseCitation:
    raw: str
    citation_type: str  # bge | docket
    normalized: str


def extract_references(text: str) -> dict[str, list[dict]]:
    statutes = [asdict(s) for s in extract_statute_references(text)]
    citations = [asdict(c) for c in extract_case_citations(text)]
    return {
        "statutes": statutes,
        "citations": citations,
    }


def extract_statute_references(text: str) -> list[StatuteReference]:
    if not text:
        return []

    refs: list[StatuteReference] = []
    seen: set[str] = set()

    for match in STATUTE_PATTERN.finditer(text):
        raw = match.group(0).strip()
        article = re.sub(r"\s+", "", (match.group("article") or "").lower())
        paragraph = match.group("paragraph")
        law_code = match.group("law").upper()
        if law_code in _INVALID_LAW_CODES:
            continue
        normalized = _normalize_statute(article=article, paragraph=paragraph, law_code=law_code)

        if normalized in seen:
            continue
        seen.add(normalized)
        refs.append(
            StatuteReference(
                raw=raw,
                law_code=law_code,
                article=article,
                paragraph=paragraph.lower() if paragraph else None,
                normalized=normalized,
            )
        )
    return refs


def extract_case_citations(text: str) -> list[CaseCitation]:
    if not text:
        return []

    refs: list[CaseCitation] = []
    seen: set[str] = set()

    # BGE citations, e.g., "BGE 147 I 268"
    for match in BGE_PATTERN.finditer(text):
        raw = match.group(0).strip()
        normalized = f"BGE {match.group('vol')} {match.group('div').upper()} {match.group('page')}"
        if normalized in seen:
            continue
        seen.add(normalized)
        refs.append(CaseCitation(raw=raw, citation_type="bge", normalized=normalized))

    for pattern in DOCKET_PATTERNS:
        for match in pattern.finditer(text):
            raw = match.group(0).strip()
            if pattern is DOCKET_PATTERNS[-1]:
                # Avoid double-counting BGE refs as docket-style refs.
                prefix = text[max(0, match.start() - 8):match.start()]
                if re.search(r"\bBGE\s*$", prefix, flags=re.IGNORECASE):
                    continue
            normalized = _normalize_docket(raw)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            refs.append(CaseCitation(raw=raw, citation_type="docket", normalized=normalized))

    return refs


def _normalize_statute(*, article: str, paragraph: str | None, law_code: str) -> str:
    if paragraph:
        return f"ART.{article}.ABS.{paragraph.lower()}.{law_code.upper()}"
    return f"ART.{article}.{law_code.upper()}"


def _normalize_docket(text: str) -> str:
    # Preserve BGE-like spacing while normalizing punctuation/case.
    compact = re.sub(r"\s+", " ", text.strip().upper())
    if re.match(r"^\d{2,3}\s+[IVX]{1,4}\s+\d{1,4}$", compact):
        return compact

    normalized = text.strip().upper()
    normalized = normalized.replace("-", "_").replace(".", "_").replace("/", "_")
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")
