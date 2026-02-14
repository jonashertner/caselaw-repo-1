"""
Valais Courts Scraper (VS Gerichte)
====================================
Scrapes court decisions from the JustSearch REST API.

Architecture:
- GET /api/search/?offset=N&limit=10&sort=-date_decision → paginated JSON
- GET /api/documents/{id}/ → full document detail with page-by-page text
- No authentication required
- Bilingual: French (68%) and German (32%)
- ~4,568 decisions (2015-present)

Platform: JustSearch (Nuxt.js + REST API by Arcanite)
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Iterator

from base_scraper import BaseScraper
from models import (
    Decision,
    detect_language,
    extract_citations,
    make_decision_id,
    parse_date,
)

logger = logging.getLogger(__name__)

API_BASE = "https://api-justsearche.vs.ch/api"
SEARCH_URL = f"{API_BASE}/search/"
DOC_URL = f"{API_BASE}/documents"
PAGE_SIZE = 50


class VSGerichteScraper(BaseScraper):
    """
    Scraper for Valais court decisions via JustSearch REST API.

    Strategy: paginate through all decisions sorted by decision date descending,
    fetch full document detail for text extraction.
    """

    REQUEST_DELAY = 1.5
    TIMEOUT = 30
    MAX_ERRORS = 50

    @property
    def court_code(self):
        return "vs_gerichte"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        total_yielded = 0
        offset = 0

        while True:
            try:
                params = {
                    "offset": offset,
                    "limit": PAGE_SIZE,
                    "sort": "-date_decision",
                }
                r = self.get(SEARCH_URL, params=params)
                data = r.json()
            except Exception as e:
                logger.error(f"VS: search failed at offset {offset}: {e}")
                break

            results = data.get("results", [])
            total = data.get("count", 0)

            if not results:
                break

            if offset == 0:
                logger.info(f"VS: {total} total decisions")

            for item in results:
                stub = self._parse_result(item)
                if not stub:
                    continue

                if self.state.is_known(stub["decision_id"]):
                    continue

                if since_date and stub.get("decision_date"):
                    if stub["decision_date"] < since_date:
                        # Results sorted by date desc, so we can stop
                        logger.info(f"VS: reached since_date {since_date}, stopping")
                        total_yielded += 0  # don't count this one
                        return

                total_yielded += 1
                yield stub

            offset += len(results)
            if offset >= total:
                break

            if offset % 500 == 0:
                logger.info(f"VS: discovered {total_yielded} new at offset {offset}/{total}")

        logger.info(f"VS: discovery complete: {total_yielded} new stubs")

    def _parse_result(self, item: dict) -> dict | None:
        """Parse a search result item into a stub dict."""
        doc_id = item.get("id")
        if not doc_id:
            return None

        case_number_obj = item.get("case_number", {})
        case_number = case_number_obj.get("text", "") if case_number_obj else ""
        if not case_number:
            case_number = case_number_obj.get("id", "") if case_number_obj else ""

        decision_date_str = item.get("date_decision")
        decision_date = None
        if decision_date_str:
            try:
                decision_date = date.fromisoformat(decision_date_str)
            except (ValueError, TypeError):
                pass

        pub_date_str = item.get("date_publication")
        pub_date = None
        if pub_date_str:
            try:
                pub_date = date.fromisoformat(pub_date_str)
            except (ValueError, TypeError):
                pass

        language_obj = item.get("language", {})
        language = language_obj.get("id", "fr") if language_obj else "fr"
        # Map API language codes
        if language == "fr":
            lang = "fr"
        elif language == "de":
            lang = "de"
        else:
            lang = "fr"

        tribunal_obj = item.get("tribunal", {})
        tribunal = tribunal_obj.get("text", "") if tribunal_obj else ""
        tribunal_abbr = tribunal_obj.get("abbreviation", "") if tribunal_obj else ""

        instance_obj = item.get("case_instance", {})
        instance = instance_obj.get("text", "") if instance_obj else ""

        nature_obj = item.get("legal_nature", {})
        legal_nature = nature_obj.get("text", "") if nature_obj else ""

        file_name = item.get("file_name", "")
        docket = case_number or doc_id[:20]

        decision_id = make_decision_id("vs_gerichte", docket)

        return {
            "decision_id": decision_id,
            "doc_id": doc_id,
            "docket_number": docket,
            "decision_date": decision_date,
            "publication_date": pub_date,
            "language": lang,
            "tribunal": tribunal,
            "tribunal_abbr": tribunal_abbr,
            "instance": instance,
            "legal_nature": legal_nature,
            "file_name": file_name,
            "url": f"{DOC_URL}/{doc_id}/",
        }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download PDF and extract text."""
        doc_id = stub.get("doc_id")
        if not doc_id:
            return None

        # Download PDF via /api/documents/{id}/file/
        pdf_url = f"{DOC_URL}/{doc_id}/file/"
        full_text = ""
        try:
            r = self.get(pdf_url, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                full_text = self._extract_pdf_text(r.content)
        except Exception as e:
            logger.warning(f"VS: PDF download failed for {stub['docket_number']}: {e}")

        if not full_text or len(full_text) < 50:
            logger.warning(f"VS: short text for {stub['docket_number']}: {len(full_text)} chars")
            if not full_text:
                full_text = f"[Text extraction failed for {stub['docket_number']}]"

        decision_date = stub.get("decision_date")
        if not decision_date:
            decision_date = date.today()

        language = stub.get("language", "fr")
        if len(full_text) > 100:
            language = detect_language(full_text)

        # Build chamber from tribunal + instance
        chamber_parts = []
        if stub.get("tribunal"):
            chamber_parts.append(stub["tribunal"])
        if stub.get("instance"):
            chamber_parts.append(stub["instance"])
        chamber = " / ".join(chamber_parts) if chamber_parts else None

        return Decision(
            decision_id=stub["decision_id"],
            court="vs_gerichte",
            canton="VS",
            chamber=chamber,
            docket_number=stub["docket_number"],
            decision_date=decision_date,
            publication_date=stub.get("publication_date"),
            language=language,
            title=stub.get("legal_nature") or None,
            legal_area=stub.get("legal_nature") or None,
            full_text=full_text,
            source_url=stub.get("url"),
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )

    @staticmethod
    def _extract_pdf_text(pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes."""
        try:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            pages = []
            for page in doc:
                pages.append(page.get_text())
            doc.close()
            return "\n\n".join(pages)
        except ImportError:
            pass

        try:
            import pdfplumber
            import io
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                return "\n\n".join(pages)
        except ImportError:
            pass

        return ""
