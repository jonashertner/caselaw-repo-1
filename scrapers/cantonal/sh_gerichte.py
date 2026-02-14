"""
Schaffhausen Courts Scraper (SH Gerichte)
==========================================
Scrapes court decisions from the KSD Backend CMS at
obergerichtsentscheide.sh.ch.

Architecture:
- GET /CMS/content/list?filter_customposttypeid_int=402&... → JSON list
- GET /CMS/get/file/{UUID} → PDF download
- No authentication required
- JSON API with excerpts; full text via PDF

Total: ~709 decisions (2000-present)
Platform: KSD Backend v1.0 (custom CMS)
"""
from __future__ import annotations

import logging
import re
from datetime import date
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

BASE_URL = "https://obergerichtsentscheide.sh.ch"
LIST_URL = f"{BASE_URL}/CMS/content/list"
FILE_URL = f"{BASE_URL}/CMS/get/file"

# Root content ID for all decisions
ROOT_PATH_ID = "2272926"
CONTENT_TYPE_DECISION = 402
PAGE_SIZE = 100

RE_DATE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
RE_NR = re.compile(r"Nr\.\s*(\d+/\d{4}/\d+)")


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


class SHGerichteScraper(BaseScraper):
    """
    Scraper for Schaffhausen Obergericht decisions via KSD CMS API.

    Strategy: paginate through JSON list API, extract metadata + excerpt,
    optionally download PDF for full text.
    """

    REQUEST_DELAY = 1.5
    TIMEOUT = 30
    MAX_ERRORS = 50

    @property
    def court_code(self):
        return "sh_gerichte"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        total_yielded = 0
        offset = 0

        while True:
            try:
                params = {
                    "filter_customposttypeid_int": CONTENT_TYPE_DECISION,
                    "filter_approvedpaths_string": f"*{ROOT_PATH_ID}*",
                    "rows": PAGE_SIZE,
                    "start": offset,
                    "status": "published",
                }
                r = self.get(LIST_URL, params=params)
                items = r.json()
            except Exception as e:
                logger.error(f"SH: list failed at offset {offset}: {e}")
                break

            if not items or not isinstance(items, list):
                break

            if offset == 0:
                logger.info(f"SH: fetched first batch of {len(items)} items")

            for item in items:
                stub = self._parse_item(item)
                if not stub:
                    continue

                if self.state.is_known(stub["decision_id"]):
                    continue

                if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                    continue

                total_yielded += 1
                yield stub

            if len(items) < PAGE_SIZE:
                break

            offset += len(items)

        logger.info(f"SH: discovery complete: {total_yielded} new stubs")

    def _parse_item(self, item: dict) -> dict | None:
        """Parse a CMS list item into a stub dict."""
        content_id = item.get("contentid")
        if not content_id:
            return None

        # Docket number from kachellabel (e.g., "Nr. 60/2017/43")
        kachellabel = item.get("kachellabel", "")
        docket = kachellabel.strip()
        if not docket:
            docket = f"SH-{content_id}"

        # Title/summary
        headline = item.get("articleHeadline", "")
        listlabel = item.get("listlabel", "")

        # Decision date from custom_publication_date_date
        decision_date_str = item.get("custom_publication_date_date", "")
        decision_date = _parse_swiss_date(decision_date_str)

        # Publication date
        pub_date_str = item.get("publication_date", "")
        pub_date = _parse_swiss_date(pub_date_str)

        # Excerpt text (HTML)
        post_content = item.get("post_content", "")

        # PDF UUID from sliderguid
        pdf_uuid = item.get("sliderguid", "")

        # Permalink
        permalink = item.get("permalink", "")

        decision_id = make_decision_id("sh_gerichte", docket)

        return {
            "decision_id": decision_id,
            "content_id": content_id,
            "docket_number": docket,
            "decision_date": decision_date,
            "publication_date": pub_date,
            "title": headline,
            "listlabel": listlabel,
            "post_content": post_content,
            "pdf_uuid": pdf_uuid,
            "permalink": permalink,
            "url": f"{BASE_URL}{permalink}" if permalink else f"{BASE_URL}/CMS/content/{content_id}",
        }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Extract decision from CMS data + optional PDF."""
        docket = stub["docket_number"]

        # Start with the excerpt from post_content
        full_text = ""
        post_content = stub.get("post_content", "")
        if post_content:
            # Strip HTML tags from post_content
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(post_content, "html.parser")
            full_text = soup.get_text(separator="\n", strip=True)

        # Try to download PDF for full text
        pdf_uuid = stub.get("pdf_uuid", "")
        if pdf_uuid:
            try:
                r = self.get(f"{FILE_URL}/{pdf_uuid}", timeout=30)
                if r.status_code == 200 and len(r.content) > 1000:
                    pdf_text = self._extract_pdf_text(r.content)
                    if pdf_text and len(pdf_text) > len(full_text):
                        full_text = pdf_text
            except Exception as e:
                logger.debug(f"SH: PDF download failed for {docket}: {e}")

        if not full_text or len(full_text) < 20:
            # Build text from available metadata
            parts = []
            if stub.get("title"):
                parts.append(stub["title"])
            if stub.get("listlabel"):
                parts.append(stub["listlabel"])
            full_text = "\n\n".join(parts) if parts else f"[Text extraction failed for {docket}]"

        decision_date = stub.get("decision_date")
        if not decision_date:
            decision_date = stub.get("publication_date") or date.today()

        language = detect_language(full_text) if len(full_text) > 100 else "de"

        pdf_url = f"{FILE_URL}/{pdf_uuid}" if pdf_uuid else None

        return Decision(
            decision_id=stub["decision_id"],
            court="sh_gerichte",
            canton="SH",
            chamber="Obergericht",
            docket_number=docket,
            decision_date=decision_date,
            publication_date=stub.get("publication_date"),
            language=language,
            title=stub.get("title"),
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
