"""
Bundesstrafgericht (BStGer) — Federal Criminal Court Scraper
==============================================================

Scrapes decisions from bstger.weblaw.ch (Weblaw Lawsearch v4 JSON API).

Architecture:
- Uses Weblaw-hosted JSON API at bstger.weblaw.ch
- POST JSON to /api/getDocuments (returns structured data)
- Date-range windowing: start with 64-day ranges, halve if >100 results
- PDF available via /api/getDocumentFile/{leid}
- userID and sessionDuration randomized per request
- Results contain: docket number, ruling date, publication date, content summary

Coverage: 2005–present
Rate limiting: 3 seconds
"""

from __future__ import annotations

import io
import json
import logging
import random
import time
from datetime import date, datetime, timedelta, timezone
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


# ============================================================
# Constants
# ============================================================

HOST = "https://bstger.weblaw.ch"
DOCUMENTS_URL = f"{HOST}/api/getDocuments?withAggregations=false"
PDF_URL = f"{HOST}/api/getDocumentFile/"

# Starting date range (days) — halved adaptively if >100 results
INITIAL_WINDOW_DAYS = 64
START_DATE = "2005-01-01"
MAX_RESULTS_PER_WINDOW = 100

# Request headers
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Origin": HOST,
    "Referer": f"{HOST}/?sort-field=relevance&sort-direction=relevance",
}


def _extract_pdf_text(data: bytes) -> str:
    """Extract text from PDF bytes using fitz (PyMuPDF) with pdfplumber fallback."""
    try:
        import fitz
        doc = fitz.open(stream=data, filetype="pdf")
        return "\n\n".join(p.get_text() for p in doc)
    except ImportError:
        pass
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n\n".join(p.extract_text() or "" for p in pdf.pages)
    except ImportError:
        pass
    return ""

# JSON search template
BASE_JSON = {
    "guiLanguage": "de",
    "aggs": {
        "fields": ["year", "language", "court", "rulingDate"],
        "size": "10",
    },
}


def _random_user_id() -> str:
    """Generate random userID for API sessions."""
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    return "_" + "".join(random.choice(chars) for _ in range(8))


class BStGerScraper(BaseScraper):
    """
    Scraper for Bundesstrafgericht decisions.

    Uses adaptive date windowing to stay under 100 results per request.
    """

    REQUEST_DELAY = 3.0
    TIMEOUT = 60

    @property
    def court_code(self) -> str:
        return "bstger"

    def _build_request_body(
        self,
        ab_date: date,
        bis_date: date,
        from_offset: int = 0,
    ) -> dict:
        """Build JSON request body for a date range."""
        body = dict(BASE_JSON)
        body["userID"] = _random_user_id()
        body["sessionDuration"] = str(int(time.time()))
        body["metadataDateMap"] = {
            "rulingDate": {
                "from": ab_date.strftime("%Y-%m-%dT00:00:00.000Z"),
                "to": bis_date.strftime("%Y-%m-%dT23:59:59.999Z"),
            }
        }
        if from_offset > 0:
            body["from"] = from_offset
        return body

    def _fetch_window(
        self, ab_date: date, window_days: int, from_offset: int = 0
    ) -> tuple[list[dict], int, bool, date]:
        """
        Fetch a single date-range window.

        Returns: (documents, total_count, has_more, bis_date)
        If total > MAX_RESULTS_PER_WINDOW, caller should halve window.
        """
        bis_date = ab_date + timedelta(days=window_days)
        body = self._build_request_body(ab_date, bis_date, from_offset)

        response = self.post(
            DOCUMENTS_URL,
            headers=HEADERS,
            json=body,
        )

        data = response.json()
        if data.get("status") != "success":
            logger.error(f"BStGer API error: {json.dumps(data)[:500]}")
            return [], 0, False, bis_date

        result = data["data"]
        total = result.get("totalNumberOfDocuments", 0)
        documents = result.get("documents", [])
        has_more = result.get("hasMoreResults", False)

        return documents, total, has_more, bis_date

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """
        Discover BStGer decisions using adaptive date windowing.

        Strategy:
        - Start with 64-day windows
        - If >100 results, halve the window and retry
        - Move forward through time until today
        """
        start = date.fromisoformat(START_DATE)
        if since_date:
            if isinstance(since_date, str):
                since_date = parse_date(since_date) or start
            start = max(since_date, start)

        window_days = INITIAL_WINDOW_DAYS
        current_date = start
        today = date.today()

        while current_date < today:
            documents, total, has_more, bis_date = self._fetch_window(
                current_date, window_days
            )

            if total > MAX_RESULTS_PER_WINDOW:
                # Too many results — halve window and retry
                window_days = max(1, window_days // 2)
                logger.info(
                    f"Window {current_date}–{bis_date} had {total} results. "
                    f"Reducing to {window_days} days."
                )
                continue

            logger.info(
                f"BStGer {current_date}–{bis_date}: {total} decisions, "
                f"{len(documents)} on this page"
            )

            user_id = _random_user_id()

            for doc in documents:
                stub = self._parse_document(doc, user_id)
                if stub:
                    decision_id = make_decision_id("bstger", stub["docket_number"])
                    if not self.state.is_known(decision_id):
                        yield stub

            # Handle pagination within window
            offset = len(documents)
            while has_more and offset < total:
                documents, _, has_more, _ = self._fetch_window(
                    current_date, window_days, offset
                )
                for doc in documents:
                    stub = self._parse_document(doc, user_id)
                    if stub:
                        decision_id = make_decision_id("bstger", stub["docket_number"])
                        if not self.state.is_known(decision_id):
                            yield stub
                offset += len(documents)

            # Move to next window
            current_date = bis_date + timedelta(days=1)

    def _parse_document(self, doc: dict, user_id: str) -> dict | None:
        """Parse a single document from the API response."""
        try:
            meta_kw = doc.get("metadataKeywordTextMap", {})
            meta_date = doc.get("metadataDateMap", {})

            title_list = meta_kw.get("title", [])
            if not title_list:
                return None

            num_raw = title_list[0]
            nums = num_raw.split(", ")
            docket = nums[0]

            leid = doc.get("leid", "")
            content = doc.get("content", "")

            # Parse dates
            ruling_date = None
            if "rulingDate" in meta_date:
                ruling_date = meta_date["rulingDate"][:10]

            pub_date = None
            if "publicationDate" in meta_date:
                pub_date = meta_date["publicationDate"][:10]

            # Decision type
            decision_type = None
            if "tipoSentenza" in meta_kw:
                decision_type = meta_kw["tipoSentenza"][0]

            # PDF URL
            file_name = meta_kw.get("fileName", [""])[0]
            pdf_url = f"{PDF_URL}{leid}?locale=de&userID={user_id}"

            return {
                "docket_number": docket,
                "docket_numbers": nums,
                "decision_date": ruling_date,
                "publication_date": pub_date,
                "headnote": content,
                "decision_type": decision_type,
                "doc_id": leid,
                "pdf_url": pdf_url,
                "file_name": file_name,
            }
        except Exception as e:
            logger.error(f"Failed to parse BStGer document: {e}")
            return None

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Build Decision from BStGer API data, fetching full text when available."""
        try:
            docket = stub["docket_number"]
            decision_date_parsed = parse_date(stub.get("decision_date", ""))
            if not decision_date_parsed:
                logger.warning(f"[bstger] No date for {docket}")

            pub_date_parsed = parse_date(stub.get("publication_date", ""))
            headnote = stub.get("headnote", "")

            # Fetch full text by downloading PDF and extracting text
            full_text = ""
            leid = stub.get("doc_id", "")
            if leid:
                try:
                    resp = self.get(
                        f"{HOST}/api/getDocumentContent/{leid}",
                        timeout=30,
                    )
                    if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
                        full_text = _extract_pdf_text(resp.content)
                        if full_text.strip():
                            logger.debug(f"Extracted {len(full_text)} chars from PDF for {docket}")
                        else:
                            logger.debug(f"PDF text extraction empty for {docket}")
                    elif resp.status_code == 200 and len(resp.text) > 100:
                        # Non-PDF text response (future-proofing)
                        full_text = resp.text
                except Exception as e:
                    logger.debug(f"Content fetch failed for {leid}: {e}")

            if not full_text.strip():
                full_text = headnote or ""

            # Deduplicate multilingual regeste (e.g. "de text;;fr text;;it text")
            if headnote and ";;" in headnote:
                parts = [p.strip() for p in headnote.split(";;") if p.strip()]
                # Take unique parts only
                seen = set()
                unique = []
                for p in parts:
                    if p not in seen:
                        seen.add(p)
                        unique.append(p)
                headnote = "\n\n".join(unique)

            lang = detect_language(full_text) if full_text else "de"

            decision = Decision(
                decision_id=make_decision_id("bstger", docket),
                court="bstger",
                canton="CH",
                docket_number=docket,
                decision_date=decision_date_parsed,
                publication_date=pub_date_parsed,
                language=lang,
                regeste=headnote or None,
                full_text=self.clean_text(full_text) if full_text.strip() else "(metadata only — PDF available)",
                decision_type=stub.get("decision_type"),
                appeal_info=stub.get("decision_type"),
                source_url=f"{HOST}/cache?id={leid}&guiLanguage={lang}" if leid else f"{HOST}/",
                pdf_url=stub.get("pdf_url"),
                cited_decisions=extract_citations(full_text) if full_text else [],
                scraped_at=datetime.now(timezone.utc),
            )
            return decision

        except Exception as e:
            logger.error(f"Failed to process BStGer {stub.get('docket_number', '?')}: {e}", exc_info=True)
            return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape BStGer decisions")
    parser.add_argument("--since", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--max", type=int, default=20, help="Max decisions")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    since = date.fromisoformat(args.since) if args.since else None
    scraper = BStGerScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    print(f"Scraped {len(decisions)} BStGer decisions")