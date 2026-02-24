"""
Unified schema for Swiss court decisions.
All scrapers must produce Decision objects conforming to this schema.

Unified Decision schema for all Swiss court scrapers.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class Decision(BaseModel):
    """A single court decision in the unified schema."""

    # === Identity ===
    decision_id: str = Field(
        ...,
        description="Unique deterministic ID: {court}_{docket_normalized}. "
        "e.g., bger_6B_1234_2025, bvger_A-1234_2025",
    )
    court: str = Field(
        ...,
        description="Standardized court code. "
        "Federal: bger, bge, bvger, bstger, bpatger. "
        "Cantonal: {canton_lower}_{court_type}, e.g., zh_obergericht, ge_cour_justice",
    )
    canton: str = Field(
        ...,
        description="CH for federal courts, two-letter canton code otherwise (ZH, BE, GE, ...)",
    )
    chamber: Optional[str] = Field(
        None,
        description="Chamber/Abteilung if available (e.g., 'I. zivilrechtliche Abteilung')",
    )

    # === Case identification ===
    docket_number: str = Field(
        ...,
        description="Original docket/Geschäftsnummer as published (e.g., '6B_1234/2025')",
    )
    docket_number_2: Optional[str] = Field(
        None,
        description="Secondary docket number if available",
    )
    decision_date: Optional[date] = Field(None, description="Date of the decision")
    publication_date: Optional[date] = Field(
        None, description="Date published online, if known"
    )

    # === Content ===
    language: str = Field(
        ..., description="Language code: de, fr, it, rm", pattern=r"^(de|fr|it|rm)$"
    )
    title: Optional[str] = Field(None, description="Subject/Gegenstand/Objet")
    legal_area: Optional[str] = Field(
        None, description="Rechtsgebiet/Domaine juridique"
    )
    regeste: Optional[str] = Field(
        None, description="Headnote/Regeste/Considérant principal"
    )
    # Trilingual abstracts (primarily for BGE Leitentscheide)
    abstract_de: Optional[str] = Field(None, description="German abstract/Regeste")
    abstract_fr: Optional[str] = Field(None, description="French abstract/Résumé")
    abstract_it: Optional[str] = Field(None, description="Italian abstract/Riassunto")
    full_text: str = Field(..., description="Complete decision text (plain text)")

    # === Metadata ===
    outcome: Optional[str] = Field(
        None,
        description="Gutheissung, Abweisung, Nichteintreten, teilweise Gutheissung, etc.",
    )
    decision_type: Optional[str] = Field(
        None,
        description="Entscheidart: Urteil, Beschluss, Verfügung, etc.",
    )
    judges: Optional[str] = Field(
        None, description="Participating judges, comma-separated"
    )
    clerks: Optional[str] = Field(
        None, description="Gerichtsschreiber, comma-separated"
    )
    collection: Optional[str] = Field(
        None,
        description="If published in official collection (e.g., 'BGE 140 III 264')",
    )
    appeal_info: Optional[str] = Field(
        None,
        description="Weiterzug info (appeal status / subsequent proceedings)",
    )

    # === References ===
    source_url: str = Field(..., description="Permanent URL to the original decision")
    pdf_url: Optional[str] = Field(None, description="Direct URL to PDF if available")
    bge_reference: Optional[str] = Field(
        None, description="BGE reference if published (e.g., 'BGE 140 III 264')"
    )
    cited_decisions: list[str] = Field(
        default_factory=list,
        description="List of cited decision references extracted from text",
    )

    # === Provenance ===
    scraped_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="When this was scraped"
    )
    # Optional external identifier for cross-referencing with other databases
    external_id: Optional[str] = Field(
        None,
        description="External cross-reference ID (e.g. for third-party legal databases)",
    )

    @field_validator("decision_id")
    @classmethod
    def validate_decision_id(cls, v: str) -> str:
        if not v or "_" not in v:
            raise ValueError(f"decision_id must be in format court_docket: {v}")
        return v

    @field_validator("language")
    @classmethod
    def validate_language(cls, v: str) -> str:
        return v.lower()

    @field_validator("canton")
    @classmethod
    def validate_canton(cls, v: str) -> str:
        return v.upper()


# ============================================================
# Helper functions
# ============================================================


def normalize_docket(docket: str) -> str:
    """
    Normalize a docket number for use in decision_id.

    '6B_1234/2025' -> '6B_1234_2025'
    'A-1234/2025'  -> 'A-1234_2025'
    """
    return re.sub(r"[/\\]", "_", docket.strip())


def make_decision_id(court: str, docket: str) -> str:
    """Create a deterministic decision_id from court code and docket number."""
    return f"{court}_{normalize_docket(docket)}"


def make_canonical_key(court: str, docket: str, decision_date: str | None = None) -> str:
    """Create a canonical key for deduplication.

    Aggressively normalizes court + docket + date so that formatting
    variants (dots vs underscores vs slashes, case differences) all
    collapse to the same key.

    Examples:
        ('bl_gerichte', 'BL.2020.1', '2020-05-15') → 'bl_gerichte|BL20201|20200515'
        ('bl_gerichte', 'BL_2020_1', '2020-05-15') → 'bl_gerichte|BL20201|20200515'
    """
    docket_norm = re.sub(r"[^A-Z0-9]", "", (docket or "").upper())
    date_compact = (decision_date or "").replace("-", "")[:8]
    return f"{court}|{docket_norm}|{date_compact}"


# ============================================================
# Citation extraction
# ============================================================

# Matches BGE references like "BGE 140 III 264" or "ATF 140 III 264"
BGE_PATTERN = re.compile(
    r"\b(BGE|ATF|DTF)\s+(\d{1,3})\s+(I{1,3}[AV]?|V)\s+(\d+)\b"
)

# Matches BGer docket numbers: 6B_1234/2025, 2C_123/2024
BGER_DOCKET_PATTERN = re.compile(
    r"\b(\d[A-Z]_\d+/\d{4}|\d[A-Z]\.\d+/\d{4})\b"
)

# Matches BVGer docket numbers: A-1234/2020, B-5678/2021
BVGER_DOCKET_PATTERN = re.compile(
    r"\b([A-F]-\d+/\d{4})\b"
)

# Matches BStGer docket numbers: SK.2020.1, BB.2021.123
BSTGER_DOCKET_PATTERN = re.compile(
    r"\b([A-Z]{2}\.\d{4}\.\d+)\b"
)


def extract_citations(text: str) -> list[str]:
    """Extract cited BGE references and docket numbers from decision text."""
    citations = set()

    for m in BGE_PATTERN.finditer(text):
        citations.add(f"BGE {m.group(2)} {m.group(3)} {m.group(4)}")

    for m in BGER_DOCKET_PATTERN.finditer(text):
        citations.add(m.group(1))

    for m in BVGER_DOCKET_PATTERN.finditer(text):
        citations.add(m.group(1))

    for m in BSTGER_DOCKET_PATTERN.finditer(text):
        citations.add(m.group(1))

    return sorted(citations)


# ============================================================
# Language detection
# ============================================================

_LANG_WORDS = {
    "de": re.compile(
        r"\b(?:der|die|das|ein|eine|einer|er|sie|ihn|hat|hatte|hätte|ist|war|sind)\b",
        re.IGNORECASE,
    ),
    "fr": re.compile(
        r"\b(?:le|lui|elle|je|on|vous|nous|leur|qui|quand|parce|que|faire|sont|vont)\b",
        re.IGNORECASE,
    ),
    "it": re.compile(
        r"\b(?:della|del|di|casi|una|al|questa|più|primo|grado|che|diritto|leggi|corte)\b",
        re.IGNORECASE,
    ),
}


def detect_language(text: str) -> str:
    """
    Detect language of a decision text using word frequency analysis.

    Method: count common words per language and pick the highest
    per language, highest count wins.
    """
    scores = {
        lang: len(pattern.findall(text[:5000]))
        for lang, pattern in _LANG_WORDS.items()
    }
    return max(scores, key=scores.get)  # type: ignore


# ============================================================
# Date normalization
# ============================================================

_DATE_PATTERNS = [
    # DD.MM.YYYY
    (re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})"), lambda m: date(int(m.group(3)), int(m.group(2)), int(m.group(1)))),
    # YYYY-MM-DD (ISO)
    (re.compile(r"(\d{4})-(\d{2})-(\d{2})"), lambda m: date(int(m.group(1)), int(m.group(2)), int(m.group(3)))),
    # DD. Monat YYYY (German months)
    (re.compile(r"(\d{1,2})\.?\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+(\d{4})", re.IGNORECASE), None),
    # DD mois YYYY (French months)
    (re.compile(r"(\d{1,2})\.?\s*(?:er)?\s*(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})", re.IGNORECASE), None),
    # Just a year
    (re.compile(r"^(\d{4})$"), lambda m: date(int(m.group(1)), 1, 1)),
]

_MONTH_NAMES = {
    "januar": 1, "février": 2, "februar": 2, "märz": 3, "mars": 3,
    "april": 4, "avril": 4, "mai": 5, "juni": 6, "juin": 6,
    "juli": 7, "juillet": 7, "august": 8, "août": 8,
    "september": 9, "septembre": 9, "oktober": 10, "octobre": 10,
    "november": 11, "novembre": 11, "dezember": 12, "décembre": 12,
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5,
    "giugno": 6, "luglio": 7, "agosto": 8, "settembre": 9,
    "ottobre": 10, "dicembre": 12,
}


def parse_date(text: str) -> date | None:
    """
    Parse a date string in various Swiss formats.

    Supports: DD.MM.YYYY, YYYY-MM-DD, DD. Monat YYYY (de/fr/it), bare YYYY.
    Normalizes dates from various Swiss court formats (7 regex patterns).
    """
    if not text:
        return None
    text = text.strip()

    # Try DD.MM.YYYY and ISO first (most common)
    for pattern, converter in _DATE_PATTERNS[:2]:
        m = pattern.search(text)
        if m and converter:
            try:
                return converter(m)
            except (ValueError, IndexError):
                continue

    # Try month name patterns (de/fr/it)
    for pattern, _ in _DATE_PATTERNS[2:4]:
        m = pattern.search(text)
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            year = int(m.group(3))
            month = _MONTH_NAMES.get(month_name)
            if month:
                try:
                    return date(year, month, day)
                except ValueError:
                    continue

    # Try bare year (guard against year 0 or obviously invalid years)
    m = _DATE_PATTERNS[4][0].match(text)
    if m:
        year = int(m.group(1))
        if 1800 <= year <= 2100:
            return date(year, 1, 1)

    return None


# ============================================================
# Court codes registry
# ============================================================

FEDERAL_COURTS = {
    "bger": "Bundesgericht / Tribunal fédéral",
    "bge": "BGE Leitentscheide / Arrêts de principe",
    "bvger": "Bundesverwaltungsgericht / Tribunal administratif fédéral",
    "bstger": "Bundesstrafgericht / Tribunal pénal fédéral",
    "bpatger": "Bundespatentgericht / Tribunal fédéral des brevets",
}

CANTONAL_CODES = {
    "AG": "Aargau", "AI": "Appenzell Innerrhoden", "AR": "Appenzell Ausserrhoden",
    "BE": "Bern", "BL": "Basel-Landschaft", "BS": "Basel-Stadt",
    "FR": "Fribourg", "GE": "Genève", "GL": "Glarus", "GR": "Graubünden",
    "JU": "Jura", "LU": "Luzern", "NE": "Neuchâtel", "NW": "Nidwalden",
    "OW": "Obwalden", "SG": "St. Gallen", "SH": "Schaffhausen",
    "SO": "Solothurn", "SZ": "Schwyz", "TG": "Thurgau", "TI": "Ticino",
    "UR": "Uri", "VD": "Vaud", "VS": "Valais", "ZG": "Zug", "ZH": "Zürich",
}
