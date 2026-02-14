"""
Uri Courts Scraper (UR Gerichte)
=================================
Scrapes court decisions from the i-web.ch CMS at www.ur.ch.

Architecture:
- GET /rechtsprechung → HTML page with <table data-entities="[JSON]">
- Each entry has _downloadBtn with PDF link and name with metadata
- PDF download from https://www.ur.ch/_docn/{id}/{filename}.pdf
- No authentication required

Total: ~1,100 decisions
Platform: i-web.ch CMS (custom)
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Iterator

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import (
    Decision,
    detect_language,
    extract_citations,
    make_decision_id,
    parse_date,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.ur.ch"
LIST_URL = f"{BASE_URL}/rechtsprechung"

RE_DATE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
RE_DOCKET = re.compile(r"(OG\s+[A-Z]\s+\d{2}\s+\d+|VGE?\s+\d+[/-]\d+)")
RE_HREF = re.compile(r'href="([^"]+)"')


def _parse_swiss_date(text):
    if not text:
        return None
    m = RE_DATE.search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


class URGerichteScraper(BaseScraper):
    """
    Scraper for Uri court decisions via i-web.ch CMS.

    Strategy: fetch the listing page, parse data-entities JSON from the
    table element, extract PDF links, download and extract text.
    All ~1,100 decisions load in a single page.
    """

    REQUEST_DELAY = 1.5
    TIMEOUT = 30
    MAX_ERRORS = 50

    @property
    def court_code(self):
        return "ur_gerichte"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        total_yielded = 0

        # Fetch listing page with empty search (returns all)
        try:
            params = {"search_publikation_filter[searchText]": ""}
            r = self.get(LIST_URL, params=params)
        except Exception as e:
            logger.error(f"UR: failed to fetch listing page: {e}")
            return

        soup = BeautifulSoup(r.text, "html.parser")

        # Find table with data-entities attribute
        table = soup.find("table", attrs={"data-entities": True})
        if not table:
            logger.error("UR: no data-entities table found")
            return

        data_entities_str = table.get("data-entities", "[]")
        try:
            parsed = json.loads(data_entities_str)
        except json.JSONDecodeError as e:
            logger.error(f"UR: failed to parse data-entities JSON: {e}")
            return

        # data-entities is {"emptyColumns": [...], "data": [...]}
        if isinstance(parsed, dict):
            entities = parsed.get("data", [])
        elif isinstance(parsed, list):
            entities = parsed
        else:
            logger.error(f"UR: unexpected data-entities type: {type(parsed)}")
            return

        logger.info(f"UR: found {len(entities)} entries in data-entities")

        for entity in entities:
            stub = self._parse_entity(entity)
            if not stub:
                continue

            if self.state.is_known(stub["decision_id"]):
                continue

            if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                continue

            total_yielded += 1
            yield stub

        logger.info(f"UR: discovery complete: {total_yielded} new stubs")

    def _parse_entity(self, entity: dict) -> dict | None:
        """Parse a data-entities entry into a stub dict."""
        name = entity.get("name", "")
        download_btn = entity.get("_downloadBtn", "")
        herausgeber = entity.get("herausgeber", "")
        datum = entity.get("datum", "")
        datum_sort = entity.get("datum-sort", "")

        if not download_btn:
            return None

        # Extract URL from _downloadBtn HTML (/_rte/publikation/{id})
        m_href = RE_HREF.search(download_btn)
        if not m_href:
            return None

        href = m_href.group(1)
        if href.startswith("/"):
            pdf_url = f"{BASE_URL}{href}"
        elif href.startswith("http"):
            pdf_url = href
        else:
            return None

        # Extract docket from name (e.g., "2015_OG V 14 24. IV. Art. 7 ...")
        # Name format: YEAR_COURT DOCKET REST
        docket = None
        m_docket = RE_DOCKET.search(name)
        if m_docket:
            docket = m_docket.group(1).strip()

        if not docket:
            # Use name prefix as docket (up to first period)
            parts = name.split(".")
            docket = parts[0].strip()[:60] if parts else name[:60]

        if not docket:
            return None

        # Extract decision date
        decision_date = _parse_swiss_date(datum) or _parse_swiss_date(datum_sort)

        # Title: full name field
        title = name[:200] if name else None

        decision_id = make_decision_id("ur_gerichte", docket)

        return {
            "decision_id": decision_id,
            "docket_number": docket,
            "decision_date": decision_date,
            "title": title,
            "herausgeber": herausgeber,
            "pdf_url": pdf_url,
            "url": pdf_url,
        }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download PDF and extract text."""
        pdf_url = stub.get("pdf_url")
        if not pdf_url:
            return None

        full_text = ""
        try:
            r = self.get(pdf_url, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                full_text = self._extract_pdf_text(r.content)
        except Exception as e:
            logger.warning(f"UR: PDF download failed for {stub['docket_number']}: {e}")

        if not full_text or len(full_text) < 50:
            full_text = stub.get("title", "") or f"[Text extraction failed for {stub['docket_number']}]"

        decision_date = stub.get("decision_date")
        if not decision_date:
            decision_date = date.today()

        language = detect_language(full_text) if len(full_text) > 100 else "de"

        return Decision(
            decision_id=stub["decision_id"],
            court="ur_gerichte",
            canton="UR",
            chamber=stub.get("herausgeber") or None,
            docket_number=stub["docket_number"],
            decision_date=decision_date,
            language=language,
            title=stub.get("title"),
            full_text=full_text,
            source_url=stub.get("url"),
            pdf_url=stub.get("pdf_url"),
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
