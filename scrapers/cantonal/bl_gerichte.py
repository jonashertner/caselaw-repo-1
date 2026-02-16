"""
Basel-Landschaft Courts Scraper (BL Gerichte)
==============================================
Scrapes court decisions from the Swisslex Angular SPA at bl.swisslex.ch.

Architecture:
- POST /api/retrieval/postSearch  -> JSON result pages (100 hits per page)
  Returns: { numberOfDocuments, hits: [...], transactionId }
  Each hit: { caseLawNumbers, date, courtDescription, title, description, targetID }
- GET /api/doc/getAsset?id={targetID}&lang=de&...&transactionId={tid}
  Returns: { content: { assetContentAsHtml: "...", facsimile: { fileID } } }

Platform: Swisslex Angular SPA with JSON API
Volume: ~9,129 decisions
Language: de

Source: https://bl.swisslex.ch
"""
from __future__ import annotations

import logging
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

# ============================================================
# Constants
# ============================================================

BASE_URL = "https://bl.swisslex.ch"
SEARCH_URL = f"{BASE_URL}/api/retrieval/postSearch"
DOC_URL = f"{BASE_URL}/api/doc/getAsset"

HITS_PER_PAGE = 100

# Custom headers required by the Swisslex Angular API
API_HEADERS = {
    "Content-Type": "application/json",
    "X-Application": "court",
    "Authenticated": "false",
}

# Search request body template
SEARCH_BODY_TEMPLATE = {
    "paging": {
        "CurrentPage": 1,
        "HitsPerPage": HITS_PER_PAGE,
    },
    "searchFilter": {
        "searchText": None,
        "navigation": None,
        "searchLanguage": 1,
        "law": None,
        "articleNumber": None,
        "paragraph": None,
        "subParagraph": None,
        "dateFrom": None,
        "dateUntil": None,
        "reference": None,
        "author": None,
        "practiceAreaGroupsCriteria": [],
        "assetTypeGroupsCriteria": [],
        "thesaurusType": 1,
        "userSearchFilterId": None,
        "bookmarkSearchFilterId": None,
        "thesaurusInformation": None,
        "nSelected": 0,
        "journalCriteria": [],
        "caseCollectionCriteria": [],
        "bookCriteria": [],
        "paging": {
            "CurrentPage": 1,
            "HitsPerPage": HITS_PER_PAGE,
        },
        "drillDownFilter": {
            "sortOrder": 0,
        },
        "expandedFacettes": [],
        "filterAggregationQuery": False,
        "expandReferences": True,
        "selectedParts": 31,
        "portalLanguage": "de",
    },
    "refineFilter": {
        "aggregationsFilter": [],
        "transformationFilter": [],
        "retrievalSortBy": 0,
        "excludedDocumentIds": [],
    },
    "reRunTransactionID": None,
    "sourceTransactionID": None,
    "isLexCampus": False,
}


def _build_search_body(page: int) -> dict:
    """Build a search POST body for the given page number."""
    import copy
    body = copy.deepcopy(SEARCH_BODY_TEMPLATE)
    body["paging"]["CurrentPage"] = page
    body["searchFilter"]["paging"]["CurrentPage"] = page
    return body


# ============================================================
# Scraper
# ============================================================


class BLGerichteScraper(BaseScraper):
    """
    Scraper for Basel-Landschaft court decisions via Swisslex Angular API.

    Uses a JSON search API with pagination (100 hits/page) and a separate
    document endpoint for full HTML text.

    Total: ~9,129 decisions.
    """

    REQUEST_DELAY = 1.5
    TIMEOUT = 60
    MAX_ERRORS = 50

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # transactionId from the latest search response, needed for doc fetches
        self._transaction_id: str | None = None

    @property
    def court_code(self):
        return "bl_gerichte"

    # ----------------------------------------------------------
    # Discovery
    # ----------------------------------------------------------

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = parse_date(since_date)

        total_yielded = 0
        page = 1

        # First request to get total count and transactionId
        data = self._search_page(page)
        if not data:
            logger.error("BL: initial search failed, aborting discovery")
            return

        total_docs = data.get("numberOfDocuments", 0)
        self._transaction_id = data.get("transactionId")
        total_pages = (total_docs + HITS_PER_PAGE - 1) // HITS_PER_PAGE

        logger.info(
            f"BL: {total_docs} total decisions, {total_pages} pages, "
            f"transactionId={self._transaction_id}"
        )

        # Process page 1
        for stub in self._parse_hits(data, since_date):
            if not self.state.is_known(stub["decision_id"]):
                total_yielded += 1
                yield stub

        # Remaining pages
        for page in range(2, total_pages + 1):
            data = self._search_page(page)
            if not data:
                logger.error(f"BL: search page {page} failed, stopping pagination")
                break

            # Update transactionId if it changes
            tid = data.get("transactionId")
            if tid:
                self._transaction_id = tid

            page_yielded = 0
            for stub in self._parse_hits(data, since_date):
                if not self.state.is_known(stub["decision_id"]):
                    total_yielded += 1
                    page_yielded += 1
                    yield stub

            logger.info(f"BL: page {page}/{total_pages}: {page_yielded} new stubs")

        logger.info(f"BL: discovery complete: {total_yielded} new stubs")

    def _search_page(self, page: int) -> dict | None:
        """Execute a search request for the given page number."""
        body = _build_search_body(page)
        try:
            resp = self.post(
                SEARCH_URL,
                json=body,
                headers=API_HEADERS,
                params={"sourceDetails": "search-button"},
            )
            return resp.json()
        except Exception as e:
            logger.error(f"BL: search page {page} error: {e}")
            return None

    def _parse_hits(self, data: dict, since_date=None) -> Iterator[dict]:
        """Parse hits from a search response into stub dicts."""
        hits = data.get("hits", [])
        for hit in hits:
            try:
                stub = self._parse_hit(hit)
                if not stub:
                    continue
                # Filter by since_date if provided
                if since_date and stub.get("decision_date"):
                    if stub["decision_date"] < since_date:
                        continue
                yield stub
            except Exception as e:
                logger.debug(f"BL: hit parse error: {e}")

    def _parse_hit(self, hit: dict) -> dict | None:
        """Parse a single search hit into a stub dict."""
        target_id = hit.get("targetID")
        if not target_id:
            logger.debug("BL: hit missing targetID, skipping")
            return None

        # Docket number: caseLawNumbers may be comma-separated, use first
        raw_docket = hit.get("caseLawNumbers", "").strip()
        if not raw_docket:
            logger.debug(f"BL: hit {target_id} missing caseLawNumbers, skipping")
            return None

        # Take first docket number if comma-separated
        docket = raw_docket.split(",")[0].strip()
        if not docket:
            return None

        # Date: ISO format "2025-12-23T00:00:00" -> take first 10 chars
        raw_date = hit.get("date", "")
        decision_date = None
        if raw_date and len(raw_date) >= 10:
            decision_date = parse_date(raw_date[:10])

        # Court description
        court_description = hit.get("courtDescription", "").strip() or None

        # Title and description (used as regeste)
        title = hit.get("title", "").strip() or None
        description = hit.get("description", "").strip() or None

        decision_id = make_decision_id("bl_gerichte", docket)

        return {
            "decision_id": decision_id,
            "docket_number": docket,
            "raw_docket": raw_docket if raw_docket != docket else None,
            "decision_date": decision_date,
            "court_description": court_description,
            "title": title,
            "description": description,
            "target_id": target_id,
            "url": f"{BASE_URL}/de/doc/{target_id}",
        }

    # ----------------------------------------------------------
    # Fetch full decision
    # ----------------------------------------------------------

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch the full HTML text of a decision via the document API."""
        target_id = stub.get("target_id")
        if not target_id:
            logger.warning(f"BL: no target_id for {stub['docket_number']}")
            return None

        transaction_id = self._transaction_id or ""

        params = {
            "id": target_id,
            "lang": "de",
            "queryLang": "De",
            "source": "hitlist-search",
            "transactionId": transaction_id,
        }

        try:
            resp = self.get(DOC_URL, params=params, headers=API_HEADERS)
            doc_data = resp.json()
        except Exception as e:
            logger.warning(f"BL: doc fetch failed for {stub['docket_number']}: {e}")
            return None

        # Extract HTML content
        content = doc_data.get("content", {})
        html_text = content.get("assetContentAsHtml", "")

        if not html_text:
            logger.warning(f"BL: empty HTML for {stub['docket_number']}")
            return None

        # Parse HTML to plain text
        full_text = self._html_to_text(html_text)
        if not full_text or len(full_text) < 50:
            logger.warning(
                f"BL: text too short for {stub['docket_number']}: "
                f"{len(full_text or '')} chars"
            )
            if not full_text:
                full_text = f"[Text extraction failed for {stub['docket_number']}]"

        # Decision date: stub -> today as fallback
        decision_date = stub.get("decision_date")
        if not decision_date:
            logger.warning(f"BL: no date for {stub['docket_number']}")

        # Language detection (almost always German for BL)
        language = detect_language(full_text) if len(full_text) > 200 else "de"

        # PDF URL from facsimile if available
        pdf_url = None
        facsimile = content.get("facsimile")
        if facsimile and isinstance(facsimile, dict):
            file_id = facsimile.get("fileID")
            if file_id:
                pdf_url = f"{BASE_URL}/api/doc/getFile?id={file_id}"

        # Regeste: use description from search hit
        regeste = stub.get("description")

        # Chamber from court description
        chamber = stub.get("court_description")

        return Decision(
            decision_id=stub["decision_id"],
            court="bl_gerichte",
            canton="BL",
            chamber=chamber,
            docket_number=stub["docket_number"],
            docket_number_2=stub.get("raw_docket"),
            decision_date=decision_date,
            language=language,
            title=stub.get("title"),
            regeste=regeste,
            full_text=full_text,
            source_url=stub.get("url", BASE_URL),
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )

    # ----------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------

    @staticmethod
    def _html_to_text(html: str) -> str:
        """Convert assetContentAsHtml to plain text using BeautifulSoup."""
        soup = BeautifulSoup(html, "html.parser")

        # Remove script and style elements
        for tag in soup.find_all(["script", "style"]):
            tag.decompose()

        # Get text with newline separation
        text = soup.get_text(separator="\n", strip=True)

        # Normalize whitespace: collapse triple+ newlines to double
        import re
        text = re.sub(r"\n{3,}", "\n\n", text)

        return text.strip()
