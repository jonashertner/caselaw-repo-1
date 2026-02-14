"""
ZH Baurekursgericht Scraper — baurekursgericht-zh.ch
=====================================================
POST-based form search with 10 results per page, PDF downloads.

Coverage: Building/planning/environmental law decisions, ~2,000+ decisions.
"""

from __future__ import annotations

import io
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

HOST = "https://www.baurekursgericht-zh.ch"
SEARCH_URL = HOST + "/rechtsprechung/entscheiddatenbank/volltextsuche/"
TREFFER_PRO_SEITE = 10


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes."""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction error: {e}")
        return ""


MONATE = {
    "Januar": 1, "Februar": 2, "März": 3, "April": 4,
    "Mai": 5, "Juni": 6, "Juli": 7, "August": 8,
    "September": 9, "Oktober": 10, "November": 11, "Dezember": 12,
}


def _parse_date_mixed(text: str) -> date | None:
    """Parse date in DD.MM.YYYY or DD. Monat YYYY format."""
    if not text:
        return None
    # Try German month name first: "15. März 2019"
    m = re.search(r"(\d{1,2})\.\s*(\w+)\s+(\d{4})", text)
    if m:
        day = int(m.group(1))
        month = MONATE.get(m.group(2))
        year = int(m.group(3))
        if month:
            try:
                return date(year, month, day)
            except ValueError:
                pass
    # Fallback: numeric DD.MM.YYYY
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


class ZHBaurekursgerichtScraper(BaseScraper):
    """
    Scraper for ZH Baurekursgericht via POST form search.

    Strategy:
    1. POST with empty keywords, source=2, search_type=2 → all decisions
    2. Paginate (10 per page, start=page*10)
    3. Each result: docket number + date, title, Leitsatz, Weiterzug, PDF URL
    4. Download PDFs, extract text
    """

    REQUEST_DELAY = 2.0
    TIMEOUT = 60
    MAX_ERRORS = 50

    @property
    def court_code(self) -> str:
        return "zh_baurekursgericht"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Search all decisions with POST form, paginate."""
        if since_date and isinstance(since_date, str):
            since_date = parse_date(since_date)

        datefrom = ""
        if since_date:
            datefrom = since_date.strftime("%d.%m.%Y")

        page = 0
        total_treffer = None
        total_new = 0

        while True:
            formdata = {
                "keywords": "",
                "source": "2",
                "datefrom": datefrom,
                "dateto": "",
            }
            if page == 0:
                formdata["search_type"] = "2"
            else:
                formdata["start"] = str(TREFFER_PRO_SEITE * page)

            logger.info(f"BRG ZH: fetching page {page + 1}")

            try:
                resp = self.post(SEARCH_URL, data=formdata)
            except Exception as e:
                logger.error(f"BRG ZH page {page + 1} failed: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Parse total count from: <div class="search-listing-head">...<div class="col-6">N Entscheide</div>
            if total_treffer is None:
                head = soup.find("div", class_="search-listing-head")
                if head:
                    cols = head.find_all("div", class_="col-6")
                    for col in cols:
                        text = col.get_text(strip=True)
                        if "Entscheide" in text:
                            m = re.match(r"(\d+)", text)
                            if m:
                                total_treffer = int(m.group(1))
                                logger.info(f"BRG ZH: {total_treffer} total decisions")

            # Parse entries: div.search-listing-item
            items = soup.find_all("div", class_="search-listing-item")
            if not items:
                logger.info(f"BRG ZH: no items on page {page + 1}")
                break

            for item_div in items:
                stub = self._parse_item(item_div)
                if stub and not self.state.is_known(stub["decision_id"]):
                    total_new += 1
                    yield stub

            page += 1
            if total_treffer and page * TREFFER_PRO_SEITE >= total_treffer:
                break

        logger.info(f"BRG ZH discovery complete: {total_new} new")

    def _parse_item(self, item_div) -> dict | None:
        """Parse a single search-listing-item div."""
        # Docket number + date: <div class="search-listing-item-number">NUM vom DD.MM.YYYY</div>
        meta_div = item_div.find("div", class_="search-listing-item-number")
        if not meta_div:
            return None

        meta_text = meta_div.get_text(strip=True)
        parts = meta_text.split(" vom ")
        if len(parts) != 2:
            logger.warning(f"BRG ZH unparseable meta: {meta_text!r}")
            return None

        num = parts[0].strip()
        edatum = _parse_date_mixed(parts[1].strip())
        if not edatum:
            logger.warning(f"BRG ZH unparseable date in: {meta_text!r}")
            return None

        # Title: <h4>
        h4 = item_div.find("h4")
        titel = h4.get_text(strip=True) if h4 else ""

        # Leitsatz: <div class="search-listing-item-summary"><p>
        leitsatz = ""
        summary_div = item_div.find("div", class_="search-listing-item-summary")
        if summary_div:
            paragraphs = summary_div.find_all("p")
            leitsatz = " ".join(p.get_text(strip=True) for p in paragraphs)

        # Weiterzug: <div class="search-listing-item-legal">
        weiterzug = ""
        legal_div = item_div.find("div", class_="search-listing-item-legal")
        if legal_div:
            weiterzug = legal_div.get_text(strip=True)

        # PDF URL: <div class="search-listing-item-download"><a href="...">
        pdf_url = None
        dl_div = item_div.find("div", class_="search-listing-item-download")
        if dl_div:
            a = dl_div.find("a", href=True)
            if a:
                href = a["href"]
                pdf_url = HOST + href if href.startswith("/") else href

        if not pdf_url:
            logger.warning(f"BRG ZH no PDF for {num}")
            return None

        decision_id = make_decision_id("zh_baurekursgericht", num)

        return {
            "decision_id": decision_id,
            "docket_number": num,
            "decision_date": edatum,
            "title": titel,
            "leitsatz": leitsatz,
            "weiterzug": weiterzug,
            "pdf_url": pdf_url,
            "source_url": pdf_url,
        }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download PDF, extract text."""
        pdf_url = stub["pdf_url"]
        num = stub["docket_number"]

        try:
            resp = self.get(pdf_url, timeout=self.TIMEOUT)
        except Exception as e:
            logger.warning(f"BRG ZH PDF download failed for {num}: {e}")
            return None

        if len(resp.content) < 100:
            logger.warning(f"BRG ZH tiny PDF for {num}: {len(resp.content)} bytes")
            return None

        full_text = _extract_text_from_pdf(resp.content)
        if not full_text or len(full_text) < 30:
            if not full_text:
                full_text = f"[PDF extraction failed for {num}]"

        language = detect_language(full_text) if len(full_text) > 100 else "de"

        return Decision(
            decision_id=stub["decision_id"],
            court="zh_baurekursgericht",
            canton="ZH",
            chamber=None,
            docket_number=num,
            decision_date=stub["decision_date"],
            language=language,
            title=stub.get("title") or f"BRG ZH — {num}",
            regeste=stub.get("leitsatz") or None,
            full_text=full_text,
            source_url=stub["source_url"],
            pdf_url=pdf_url,
            decision_type=None,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
            external_id=f"zh_brg_{num}",
        )
