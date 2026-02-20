"""
Aargau Courts Scraper (AG Gerichte)
====================================
Scrapes court decisions from the DecWork platform at decwork.ag.ch.

Architecture:
- Single POST to /api/main/v1/de/decrees_chronology returns ALL decisions (9,884+)
- Each decision has a PDF at /api/main/v1/de/decrees_pdf/{decree_id}
- Detail endpoint at /api/main/v1/de/decrees/{decree_id} gives metadata
- Text must be extracted from PDFs (pdfplumber)

Data source: https://gesetzessammlungen.ag.ch (frontend)
API backend: https://decwork.ag.ch (DecWork by Sitrox AG)

Coverage:
- 1999–present
- Pre-2022: ~100-170/year (Leitentscheide only, guiding_decree=true)
- 2022+: ~1,800/year (all published decisions)

Chronology entry fields:
- id: chronology entry ID (NOT the decree ID — can reference a different decree!)
- decree_id: the actual decree ID → use this for PDF and detail endpoints
- number: docket number (e.g., "VBE.2021.498", "RRB 2022-000950")
- decree_date: ISO date string
- institution_name: slash-separated court/chamber path
- guiding_decree: bool (Leitentscheid flag)
- guidance_summary: Regeste text (only for guiding decrees)

Detail endpoint (/decrees/{decree_id}) additionally provides:
- publication_date: when decision was published online
- linked_tols: referenced legal provisions with LexFind permalinks
  (only for guiding decrees; not fetched in bulk to avoid 9,884 extra requests)

Courts covered (31 institutions):
- Obergericht: Zivilgericht (5 Kammern + KESB + SchKK), Strafgericht (4 Kammern),
  Verwaltungsgericht (3 Kammern), Versicherungsgericht (4 Kammern),
  Handelsgericht (2 Kammern)
- Spezialverwaltungsgericht (Steuern, Kausalabgaben)
- Justizgericht, Justizleitung
- Regierungsrat
- Anwaltskommission, Aufsichtskommission
- 4 Departemente (BVU, BKS, GS, VI)

Platform: LexWork/DecWork (Sitrox AG) — used by 16 cantons
"""

from __future__ import annotations

import io
import logging
from typing import Iterator

from base_scraper import BaseScraper
from models import Decision, detect_language, extract_citations, make_decision_id, parse_date

logger = logging.getLogger(__name__)

# ============================================================
# Constants
# ============================================================

HOST = "https://decwork.ag.ch"
CHRONOLOGY_URL = f"{HOST}/api/main/v1/de/decrees_chronology"
DETAIL_URL = f"{HOST}/api/main/v1/de/decrees"  # /{decree_id}
PDF_URL = f"{HOST}/api/main/v1/de/decrees_pdf"  # /{decree_id}

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://gesetzessammlungen.ag.ch",
}

# Source URL template (links to the frontend, not the API)
SOURCE_URL_TEMPLATE = "https://gesetzessammlungen.ag.ch/app/de/decrees/{decree_id}"


# ============================================================
# Institution name → court/chamber mapping
# ============================================================

# The API returns institution_name like:
#   "Obergericht / Verwaltungsgericht / 3. Kammer"
#   "Obergericht / Zivilgericht / 4. Kammer"
#   "Spezialverwaltungsgericht / Abteilung Steuern"
#   "Regierungsrat"
#   "Departement Bau, Verkehr und Umwelt / Rechtsabteilung"
#
# Parsing strategy: iterate ALL parts and use the LAST (most specific)
# match. "Obergericht" is just the umbrella term — the real court is
# "Verwaltungsgericht", "Zivilgericht", etc. in the second position.
# Everything after the matched part becomes the chamber.

_COURT_MAP = {
    # --- Obergericht divisions (these override the generic "obergericht") ---
    "verwaltungsgericht": "ag_verwaltungsgericht",
    "versicherungsgericht": "ag_versicherungsgericht",
    "zivilgericht": "ag_zivilgericht",
    "strafgericht": "ag_strafgericht",
    "handelsgericht": "ag_handelsgericht",
    # --- Standalone courts ---
    "spezialverwaltungsgericht": "ag_spezialverwaltungsgericht",
    "justizgericht": "ag_justizgericht",
    # --- Other bodies ---
    "regierungsrat": "ag_regierungsrat",
    "justizleitung": "ag_justizleitung",
    "anwaltskommission": "ag_anwaltskommission",
    "aufsichtskommission": "ag_aufsichtskommission",
    # --- Departments (quasi-judicial administrative decisions) ---
    "departement bau": "ag_departement_bvu",
    "departement bildung": "ag_departement_bks",
    "departement gesundheit": "ag_departement_gs",
    "departement volkswirtschaft": "ag_departement_vi",
    # --- Generic fallback (only if nothing else matches) ---
    "obergericht": "ag_obergericht",
}


def parse_institution(institution_name: str) -> tuple[str, str | None]:
    """
    Parse institution_name into (court_code, chamber).

    Strategy: scan ALL slash-separated parts; keep updating court_code
    with the LAST (most specific) match. This ensures "Obergericht /
    Verwaltungsgericht / 3. Kammer" correctly maps to ag_verwaltungsgericht
    with chamber "3. Kammer", not ag_obergericht.

    Returns:
        (court_code, chamber) where court_code is like 'ag_verwaltungsgericht'
        and chamber is the remaining parts joined with ' / '.
    """
    if not institution_name:
        return "ag_gerichte", None

    parts = [p.strip() for p in institution_name.split("/")]
    court_code = "ag_gerichte"  # fallback
    matched_index = -1

    for i, part in enumerate(parts):
        part_lower = part.lower().strip()
        # Check keywords LONGEST FIRST to avoid substring collisions
        # (e.g., "verwaltungsgericht" is a substring of "spezialverwaltungsgericht")
        for keyword, code in sorted(_COURT_MAP.items(), key=lambda x: len(x[0]), reverse=True):
            if keyword in part_lower:
                court_code = code
                matched_index = i
                # Don't break outer — keep scanning for more specific matches
                break  # break inner only (longest match already won)

    # Everything after the last matched part is chamber info
    if matched_index >= 0 and matched_index < len(parts) - 1:
        chamber_parts = parts[matched_index + 1 :]
        chamber = " / ".join(p.strip() for p in chamber_parts).strip() or None
    else:
        chamber = None

    return court_code, chamber


# ============================================================
# PDF text extraction
# ============================================================


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes. Tries pdfplumber → pymupdf → pdfminer."""
    # 1. pdfplumber (good for modern PDFs with text layers)
    try:
        import pdfplumber

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            result = "\n\n".join(pages)
            if len(result) >= 50:
                return result
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")

    # 2. pymupdf / fitz (handles older PDFs that pdfplumber misses)
    try:
        import fitz

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = [page.get_text() for page in doc]
        doc.close()
        result = "\n\n".join(p for p in pages if p.strip())
        if len(result) >= 50:
            return result
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"pymupdf failed: {e}")

    # 3. pdfminer (last resort)
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract

        result = pdfminer_extract(io.BytesIO(pdf_bytes))
        if result and len(result) >= 50:
            return result
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"pdfminer failed: {e}")

    logger.error(
        "PDF text extraction failed with all backends. "
        "Install pymupdf: pip install pymupdf"
    )
    return ""


# ============================================================
# Scraper
# ============================================================


class AGGerichteScraper(BaseScraper):
    """
    Scraper for Aargau cantonal court decisions via DecWork API.

    Strategy:
    1. POST to chronology endpoint → get all decree stubs (single call)
    2. For each new decree: download PDF, extract text, build Decision
    3. Rate-limit PDF downloads (1.5s delay — 9,884 PDFs is ~4 hours)

    The chronology endpoint returns everything in one call, so discover_new()
    yields all unknown decree stubs. State tracking prevents re-downloading.
    """

    REQUEST_DELAY = 1.5  # Be respectful with 9,884+ PDFs
    TIMEOUT = 60  # PDFs can be large
    MAX_ERRORS = 50  # Higher tolerance for a bulk scraper

    @property
    def court_code(self) -> str:
        return "ag_gerichte"

    def _fetch_chronology(self) -> dict:
        """Fetch the complete chronology index from DecWork."""
        logger.info("Fetching AG chronology index...")
        response = self.post(
            CHRONOLOGY_URL,
            headers=HEADERS,
            json={},
        )
        data = response.json()
        count = data.get("count", "?")
        logger.info(f"AG chronology: {count} total decisions")
        return data

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """
        Discover all AG decisions from chronology endpoint.

        Yields stubs for decisions not yet in state.
        Newest first (reverse chronological) for incremental scraping.
        """
        data = self._fetch_chronology()
        chronology = data.get("chronology", {})

        # Parse since_date
        if since_date and isinstance(since_date, str):
            since_date = parse_date(since_date)

        total = 0
        yielded = 0

        # Process years in reverse (newest first)
        for year in sorted(chronology.keys(), reverse=True):
            year_int = int(year)

            # Skip years entirely before since_date
            if since_date and year_int < since_date.year:
                continue

            months = chronology[year]
            # Process months in reverse
            for month in sorted(months.keys(), reverse=True):
                entries = months[month]
                total += len(entries)

                for entry in entries:
                    decree_id = str(entry["decree_id"])
                    docket = entry.get("number", "")
                    decree_date_str = entry.get("decree_date", "")

                    # Apply since_date filter
                    if since_date and decree_date_str:
                        entry_date = parse_date(decree_date_str)
                        if entry_date and entry_date < since_date:
                            continue

                    # Build a deterministic decision_id for state tracking
                    # Use the specific court code from institution_name
                    institution = entry.get("institution_name", "")
                    specific_court, _ = parse_institution(institution)
                    decision_id = make_decision_id(specific_court, docket) if docket else f"ag_{decree_id}"

                    if self.state.is_known(decision_id):
                        continue

                    yielded += 1
                    yield {
                        "decree_id": decree_id,
                        "docket_number": docket,
                        "decree_date": decree_date_str,
                        "institution_name": institution,
                        "guiding_decree": entry.get("guiding_decree", False),
                        "guidance_summary": entry.get("guidance_summary", ""),
                        "decision_id": decision_id,
                    }

        logger.info(
            f"AG discovery: {total} total entries, {yielded} new to scrape"
        )

    def fetch_decision(self, stub: dict) -> Decision | None:
        """
        Fetch PDF and metadata for a single AG decision.

        Steps:
        1. Download PDF from /decrees_pdf/{decree_id}
        2. Extract text with pdfplumber
        3. Optionally fetch detail endpoint for extra metadata
        4. Build Decision object
        """
        decree_id = stub["decree_id"]
        docket = stub["docket_number"]

        # === 1. Download PDF ===
        pdf_endpoint = f"{PDF_URL}/{decree_id}"
        try:
            response = self.get(
                pdf_endpoint,
                headers={"Origin": "https://gesetzessammlungen.ag.ch"},
                timeout=self.TIMEOUT,
            )
        except Exception as e:
            logger.warning(f"AG PDF download failed for {docket} (id={decree_id}): {e}")
            return None

        content_type = response.headers.get("Content-Type", "")
        if "pdf" not in content_type and len(response.content) < 100:
            logger.warning(
                f"AG unexpected response for {docket}: "
                f"Content-Type={content_type}, size={len(response.content)}"
            )
            return None

        # === 2. Extract text from PDF ===
        full_text = extract_text_from_pdf(response.content)
        if not full_text or len(full_text) < 50:
            logger.warning(
                f"AG PDF text extraction too short for {docket}: "
                f"{len(full_text or '')} chars"
            )
            # Still proceed — some decisions may be very short or image-only
            if not full_text:
                full_text = f"[PDF text extraction failed for {docket}]"

        # === 3. Parse metadata ===
        institution = stub.get("institution_name", "")
        specific_court, chamber = parse_institution(institution)
        decree_date = parse_date(stub["decree_date"])
        if not decree_date:
            logger.warning(f"AG unparseable date for {docket}: {stub['decree_date']}")
            return None

        language = detect_language(full_text) if len(full_text) > 100 else "de"

        # Build regeste from guidance_summary (for Leitentscheide)
        regeste = stub.get("guidance_summary") or None

        # Title from institution + docket
        title = f"{institution} — {docket}" if institution else docket

        decision_id = stub["decision_id"]

        return Decision(
            decision_id=decision_id,
            court=specific_court,
            canton="AG",
            chamber=chamber,
            docket_number=docket,
            decision_date=decree_date,
            language=language,
            title=title,
            regeste=regeste,
            full_text=full_text,
            source_url=SOURCE_URL_TEMPLATE.format(decree_id=decree_id),
            pdf_url=pdf_endpoint,
            decision_type="Leitentscheid" if stub.get("guiding_decree") else None,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
            external_id=f"decwork_ag_{decree_id}",
        )