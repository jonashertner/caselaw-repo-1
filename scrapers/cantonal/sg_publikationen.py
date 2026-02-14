"""
St. Gallen Publications Scraper (SG Publikationen)
===================================================
Scrapes court decisions from publikationen.sg.ch via TYPO3 AJAX endpoint.

Platform: TYPO3 CMS with tx_diamjudicalsg plugin
Coverage: ~12,727 decisions across all SG courts
  (Kantonsgericht, Verwaltungsgericht, Versicherungsgericht,
   Verwaltungsrekurskommission, Handelsgericht, Kreisgerichte)
Language: de
Source: https://publikationen.sg.ch/rechtsprechung-gerichte/

Architecture:
- AJAX pagination: 10 items/page, ~1,273 pages
  GET /rechtsprechung-gerichte/?filter[timerangeType]=-1&page=N&sortorder=1&...
  Returns HTML fragments with decision stubs
- Detail pages: /rechtsprechung-gerichte-detail/{id}/
  Most have inline full text in publication-detail__content div
  Recent ones (2026+) may have PDF only
- PDF fallback: /fileadmin/ekab/judical_sg/YYYY/MM/DOCKET/attachments/*.pdf
"""
from __future__ import annotations

import html as html_module
import io
import logging
import re
from datetime import date
from typing import Iterator
from urllib.parse import urljoin

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

BASE_URL = "https://publikationen.sg.ch"
LIST_URL = f"{BASE_URL}/rechtsprechung-gerichte/"
AJAX_PARAMS = {
    "filter[timerangeType]": "-1",
    "sortorder": "1",
    "tx_diamjudicalsg_judicalpublicationpublicuserportalrenderlist[action]": "resultAjax",
    "tx_diamjudicalsg_judicalpublicationpublicuserportalrenderlist[controller]": "JudicalPublicationPublicUserPortal",
}

RE_DATE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")

# Map publishing court names to our court sub-codes
COURT_MAP = {
    "Kantonsgericht": "sg_kantonsgericht",
    "Verwaltungsgericht": "sg_verwaltungsgericht",
    "Versicherungsgericht": "sg_versicherungsgericht",
    "Verwaltungsrekurskommission": "sg_verwaltungsrekurskommission",
    "Handelsgericht": "sg_handelsgericht",
    "Kreisgericht": "sg_kreisgericht",
}


def _parse_swiss_date(text: str) -> date | None:
    if not text:
        return None
    m = RE_DATE.search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


class SGPublikationenScraper(BaseScraper):
    """
    Scraper for St. Gallen court decisions from publikationen.sg.ch.

    Paginates through TYPO3 AJAX endpoint, fetches detail pages for full text.
    Falls back to PDF extraction when inline text is not available.
    """

    REQUEST_DELAY = 1.5
    TIMEOUT = 45
    MAX_ERRORS = 50

    @property
    def court_code(self):
        return "sg_publikationen"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        page = 1
        total_yielded = 0
        empty_pages = 0

        while True:
            params = dict(AJAX_PARAMS)
            params["page"] = str(page)

            try:
                self._rate_limit()
                r = self.session.get(LIST_URL, params=params, timeout=self.TIMEOUT)
            except Exception as e:
                logger.error(f"SG pub: page {page} request failed: {e}")
                empty_pages += 1
                if empty_pages > 5:
                    break
                page += 1
                continue

            if r.status_code != 200 or len(r.text.strip()) < 100:
                empty_pages += 1
                if empty_pages > 3:
                    logger.info(f"SG pub: {empty_pages} consecutive empty pages at page {page}, stopping")
                    break
                page += 1
                continue

            empty_pages = 0
            stubs = self._parse_listing_page(r.text)

            if not stubs:
                logger.info(f"SG pub: no items on page {page}, stopping")
                break

            for stub in stubs:
                decision_id = stub["decision_id"]
                if self.state.is_known(decision_id):
                    continue

                if since_date and stub.get("decision_date"):
                    if stub["decision_date"] < since_date:
                        continue

                total_yielded += 1
                yield stub

            if page % 50 == 0:
                logger.info(f"SG pub: scanned {page} pages, yielded {total_yielded} new stubs")

            page += 1

        logger.info(f"SG pub: discovery complete: {total_yielded} new stubs from {page - 1} pages")

    def _parse_listing_page(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        stubs = []

        for item in soup.find_all("div", class_="publication-list__item"):
            try:
                stub = self._parse_listing_item(item)
                if stub:
                    stubs.append(stub)
            except Exception as e:
                logger.debug(f"SG pub: failed to parse listing item: {e}")

        return stubs

    def _parse_listing_item(self, item) -> dict | None:
        detail_url = item.get("data-detailurl", "")
        if not detail_url:
            return None

        # Extract numeric ID from URL
        m = re.search(r"/(\d+)/?$", detail_url)
        if not m:
            return None
        pub_id = m.group(1)

        # Title and link
        title_link = item.find("a", href=re.compile(r"rechtsprechung-gerichte-detail"))
        title = title_link.get_text(strip=True) if title_link else ""

        # Metadata from dl/dt/dd pairs
        metadata = {}
        for dl in item.find_all("dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if dt and dd:
                key = dt.get_text(strip=True).rstrip(":")
                val = dd.get_text(strip=True)
                metadata[key] = val

        case_number = metadata.get("Fall-Nr.", "")
        rubrik = metadata.get("Rubrik", "")
        court_name = metadata.get("Publizierende Stelle", "")

        # Dates from the list items
        dates = {}
        for li in item.find_all("li"):
            text = li.get_text(strip=True)
            if "Entscheiddatum" in text:
                dates["decision"] = _parse_swiss_date(text)
            elif "Publikationsdatum" in text:
                dates["publication"] = _parse_swiss_date(text)

        decision_date = dates.get("decision") or dates.get("publication")

        # Summary paragraph
        summary_p = item.find("p")
        summary = summary_p.get_text(strip=True) if summary_p else ""

        # PDF link
        pdf_link = item.find("a", class_="pdf-btn")
        pdf_href = pdf_link.get("href", "") if pdf_link else ""
        if pdf_href:
            pdf_href = html_module.unescape(pdf_href)

        # Build decision_id
        if case_number:
            decision_id = make_decision_id("sg_publikationen", case_number)
        else:
            decision_id = f"sg_publikationen_{pub_id}"

        return {
            "decision_id": decision_id,
            "pub_id": pub_id,
            "docket_number": case_number or f"SG-PUB-{pub_id}",
            "decision_date": decision_date,
            "publication_date": dates.get("publication"),
            "title": title,
            "court_name": court_name,
            "rubrik": rubrik,
            "summary": summary,
            "detail_url": urljoin(BASE_URL, detail_url),
            "pdf_url": urljoin(BASE_URL, pdf_href) if pdf_href else "",
        }

    def fetch_decision(self, stub: dict) -> Decision | None:
        detail_url = stub.get("detail_url", "")
        if not detail_url:
            return None

        # Fetch detail page
        try:
            self._rate_limit()
            r = self.session.get(detail_url, timeout=self.TIMEOUT)
        except Exception as e:
            logger.warning(f"SG pub: detail fetch failed for {stub['docket_number']}: {e}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Try inline text first
        full_text = self._extract_inline_text(soup)

        # If no inline text, try PDF
        if not full_text or len(full_text) < 100:
            pdf_url = self._find_pdf_url(soup, stub)
            if pdf_url:
                full_text = self._extract_pdf_text(pdf_url)

        if not full_text or len(full_text) < 50:
            # Use summary as fallback
            full_text = stub.get("summary", "")
            if not full_text:
                logger.warning(f"SG pub: no text for {stub['docket_number']}")
                return None

        decision_date = stub.get("decision_date") or date.today()
        language = detect_language(full_text) if len(full_text) > 100 else "de"

        # Map court name to court code
        court_name = stub.get("court_name", "")
        court = "sg_publikationen"
        for name_prefix, code in COURT_MAP.items():
            if name_prefix.lower() in court_name.lower():
                court = code
                break

        return Decision(
            decision_id=stub["decision_id"],
            court=court,
            canton="SG",
            chamber=court_name or None,
            docket_number=stub["docket_number"],
            decision_date=decision_date,
            publication_date=stub.get("publication_date"),
            language=language,
            title=stub.get("title") or None,
            legal_area=stub.get("rubrik") or None,
            regeste=stub.get("summary") or None,
            full_text=full_text,
            source_url=stub.get("detail_url", ""),
            pdf_url=stub.get("pdf_url") or None,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )

    @staticmethod
    def _extract_inline_text(soup) -> str:
        content_div = soup.find("div", class_="publication-detail__content")
        if not content_div:
            return ""

        text = content_div.get_text(separator="\n", strip=True)

        # Skip if it's just "Entscheid als PDF"
        if len(text) < 100 and "PDF" in text:
            return ""

        # Clean up HTML entities and whitespace
        text = html_module.unescape(text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _find_pdf_url(soup, stub: dict) -> str:
        # Try footer PDF link (direct fileadmin URL)
        footer = soup.find("div", class_="publication-detail__footer")
        if footer:
            link = footer.find("a", href=re.compile(r"fileadmin.*\.pdf"))
            if link:
                href = link.get("href", "")
                return urljoin(BASE_URL, href)

        # Fall back to the listing PDF URL
        if stub.get("pdf_url"):
            return stub["pdf_url"]

        return ""

    def _extract_pdf_text(self, pdf_url: str) -> str:
        try:
            import pdfplumber
        except ImportError:
            logger.warning("pdfplumber not installed, cannot extract PDF text")
            return ""

        try:
            self._rate_limit()
            r = self.session.get(pdf_url, timeout=60)
            if r.status_code != 200:
                return ""

            with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                return "\n\n".join(pages)
        except Exception as e:
            logger.warning(f"SG pub: PDF extraction failed for {pdf_url}: {e}")
            return ""
