"""
Vaud Courts Scraper (VD Gerichte)
==================================
Scrapes court decisions from the Canton de Vaud REST API at
prestations.vd.ch/pub/101623/api/.

Architecture:
- GET /pub/101623/ → session cookies + XSRF-TOKEN
- POST /pub/101623/api/search (JSON) → paginated results (Spring Data Page)
- GET /pub/101623/api/decision/download/{uuid} → PDF

XSRF Protection:
- The XSRF-TOKEN cookie must be sent as X-XSRF-TOKEN header on POST requests.
- Without it, POST returns 403 Forbidden.

Search API:
- Date range is required to get results (empty search returns 0)
- Max pageSize: 100
- sortBy: "DATE_DE_DECISION" or "PERTINENCE"
- queryTarget: "ALL", "DECISION", "RESUME", "CAUSE"
- Date format: {"from": [YYYY, MM, DD], "to": [YYYY, MM, DD]}

Volume: ~3,500-4,000 decisions per year. Total ~10,000+ since 2020.
Platform: Spring Boot REST API with Angular SPA frontend.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
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

BASE_URL = "https://prestations.vd.ch/pub/101623"
API_URL = f"{BASE_URL}/api"

# Monthly iteration to stay under 10,000 result cap
# Earliest year with decisions
START_YEAR = 2007


class VDGerichteScraper(BaseScraper):
    """
    Scraper for Canton de Vaud court decisions via REST API.

    Uses monthly date-range windows, paginating within each month.
    Downloads PDF for each decision.
    """

    REQUEST_DELAY = 1.5
    TIMEOUT = 60
    MAX_ERRORS = 50

    @property
    def court_code(self):
        return "vd_gerichte"

    def _init_session(self) -> bool:
        """Initialize session: get cookies and XSRF token."""
        try:
            r = self.session.get(f"{BASE_URL}/", timeout=self.TIMEOUT)
            r.raise_for_status()
        except Exception as e:
            logger.error(f"VD: failed to init session: {e}")
            return False

        xsrf = self.session.cookies.get("XSRF-TOKEN")
        if not xsrf:
            logger.error("VD: no XSRF-TOKEN cookie received")
            return False

        self.session.headers["X-XSRF-TOKEN"] = xsrf
        self.session.headers["Accept"] = "application/json, text/plain, */*"
        self.session.headers["Origin"] = "https://prestations.vd.ch"
        self.session.headers["Referer"] = f"{BASE_URL}/"
        logger.info(f"VD: session initialized, XSRF token acquired")
        return True

    def _search(self, date_from: list[int], date_to: list[int], page: int = 0) -> dict | None:
        """Execute a search API call. Returns parsed JSON or None."""
        body = {
            "page": page,
            "pageSize": 100,
            "sortBy": "DATE_DE_DECISION",
            "queryTarget": "ALL",
            "query": "",
            "modelesDecision": [],
            "resultatsDecision": [],
            "naturesAffaire": [],
            "compositionsCour": [],
            "autoritesDirectrice": [],
            "juges": [],
            "greffiers": [],
            "resultatsRecours": [],
            "jurivoc": {"inclusions": [], "exclusions": []},
            "articlesDeLoi": {"inclusions": [], "exclusions": []},
            "datePublication": {"from": None, "to": None},
            "dateDecision": {"from": date_from, "to": date_to},
        }

        self._rate_limit()
        try:
            r = self.session.post(
                f"{API_URL}/search",
                json=body,
                headers={"Content-Type": "application/json"},
                timeout=self.TIMEOUT,
            )
            if r.status_code == 403:
                # XSRF token may have expired, refresh
                logger.warning("VD: 403 on search, refreshing session")
                if self._init_session():
                    r = self.session.post(
                        f"{API_URL}/search",
                        json=body,
                        headers={"Content-Type": "application/json"},
                        timeout=self.TIMEOUT,
                    )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"VD: search failed: {e}")
            return None

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        if not self._init_session():
            return

        today = date.today()
        start_year = since_date.year if since_date else START_YEAR
        start_month = since_date.month if since_date else 1

        total_yielded = 0

        # Iterate month by month, newest first
        for year in range(today.year, start_year - 1, -1):
            end_month = today.month if year == today.year else 12
            begin_month = start_month if year == start_year else 1

            for month in range(end_month, begin_month - 1, -1):
                # Last day of month
                if month == 12:
                    last_day = 31
                else:
                    next_month = date(year, month + 1, 1)
                    last_day = (next_month - timedelta(days=1)).day

                date_from = [year, month, 1]
                date_to = [year, month, last_day]

                logger.info(f"VD: searching {year}-{month:02d}")

                data = self._search(date_from, date_to, page=0)
                if not data:
                    continue

                response = data.get("response", {})
                total_elements = response.get("totalElements", 0)
                total_pages = response.get("totalPages", 0)

                if total_elements == 0:
                    logger.debug(f"VD: {year}-{month:02d}: no results")
                    continue

                logger.info(f"VD: {year}-{month:02d}: {total_elements} decisions, {total_pages} pages")

                # Process page 0
                for stub in self._parse_search_page(response):
                    if not self.state.is_known(stub["decision_id"]):
                        total_yielded += 1
                        yield stub

                # Process remaining pages
                for page in range(1, total_pages):
                    data = self._search(date_from, date_to, page=page)
                    if not data:
                        break
                    response = data.get("response", {})
                    for stub in self._parse_search_page(response):
                        if not self.state.is_known(stub["decision_id"]):
                            total_yielded += 1
                            yield stub

        logger.info(f"VD: discovery complete: {total_yielded} new stubs")

    def _parse_search_page(self, response: dict) -> Iterator[dict]:
        """Parse decisions from a search response page."""
        content = response.get("content", [])
        for item in content:
            try:
                stub = self._parse_search_item(item)
                if stub:
                    yield stub
            except Exception as e:
                logger.debug(f"VD: parse error: {e}")

    def _parse_search_item(self, item: dict) -> dict | None:
        """Parse a single search result item into a stub dict."""
        hit = item.get("decisionHit", {})
        if not hit:
            return None

        uuid = hit.get("id")
        if not uuid:
            return None

        affaire = hit.get("affaireHit", {})
        docket = affaire.get("numero", "")
        if not docket:
            # Use decision number as fallback
            docket = hit.get("numero", uuid)

        # Parse decision date
        date_str = hit.get("dateDecision", "")
        decision_date = parse_date(date_str)

        # Parse publication date
        pub_str = hit.get("datePublication", "")
        publication_date = parse_date(pub_str)

        # Authority info
        autorite = affaire.get("autoriteDirectrice", "")
        chamber = affaire.get("autoritePremiereInstance", "")

        # Judges
        judges_list = affaire.get("jugesAbreviation", [])
        judges = ", ".join(judges_list) if judges_list else None

        # Clerks
        clerks_list = affaire.get("greffiersAbreviation", [])
        clerks = ", ".join(clerks_list) if clerks_list else None

        # Legal area / nature
        nature = hit.get("natureAffaire", "")

        # Résumé
        resume = hit.get("resume", "")

        # Articles de loi
        articles = hit.get("articlesDeLoi", {})
        articles_str = ""
        if articles:
            parts = []
            for law, arts in articles.items():
                for art in arts:
                    parts.append(f"{art} {law}")
            articles_str = "; ".join(parts)

        # Outcome
        resultats = hit.get("resultats", [])
        outcome = "; ".join(resultats) if resultats else None

        # Jurivoc concepts
        concepts = hit.get("conceptsJurivoc", [])

        decision_id = make_decision_id("vd_gerichte", docket)

        return {
            "decision_id": decision_id,
            "docket_number": docket,
            "uuid": uuid,
            "decision_date": decision_date,
            "publication_date": publication_date,
            "autorite": autorite,
            "chamber": chamber,
            "judges": judges,
            "clerks": clerks,
            "nature": nature,
            "resume": resume,
            "articles": articles_str,
            "outcome": outcome,
            "concepts": concepts,
            "url": f"{API_URL}/decision/download/{uuid}",
        }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch PDF and extract text for a single decision."""
        uuid = stub.get("uuid")
        if not uuid:
            logger.warning(f"VD: no UUID for {stub['docket_number']}")
            return None

        # Download PDF
        pdf_url = f"{API_URL}/decision/download/{uuid}"
        try:
            self._rate_limit()
            r = self.session.get(pdf_url, timeout=self.TIMEOUT)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"VD: PDF download failed for {stub['docket_number']}: {e}")
            return None

        content_type = r.headers.get("Content-Type", "")
        if "pdf" not in content_type and len(r.content) < 100:
            logger.warning(f"VD: unexpected content type for {stub['docket_number']}: {content_type}")
            return None

        # Extract text from PDF
        full_text = self._extract_pdf_text(r.content)
        if not full_text:
            # Use résumé as fallback
            full_text = stub.get("resume", "")
            if not full_text:
                full_text = f"[PDF text extraction failed for {stub['docket_number']}]"

        decision_date = stub.get("decision_date")
        if not decision_date:
            decision_date = stub.get("publication_date")
        if not decision_date:
            logger.warning(f"VD: no date for {stub['docket_number']}")

        language = detect_language(full_text) if len(full_text) > 100 else "fr"

        return Decision(
            decision_id=stub["decision_id"],
            court="vd_gerichte",
            canton="VD",
            chamber=stub.get("autorite"),
            docket_number=stub["docket_number"],
            decision_date=decision_date,
            publication_date=stub.get("publication_date"),
            language=language,
            title=stub.get("nature"),
            legal_area=stub.get("nature"),
            regeste=stub.get("resume") or None,
            full_text=full_text,
            outcome=stub.get("outcome"),
            judges=stub.get("judges"),
            clerks=stub.get("clerks"),
            source_url=f"{BASE_URL}/",
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )

    @staticmethod
    def _extract_pdf_text(pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber or PyPDF2."""
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

        try:
            from PyPDF2 import PdfReader
            import io
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n\n".join(pages)
        except ImportError:
            pass

        logger.warning("VD: no PDF extraction library available (install pdfplumber or PyPDF2)")
        return ""
