"""
Thurgau Courts Scraper (TG Gerichte)
=====================================
Scrapes court decisions from the Scroll Viewport / Confluence portal at
rechtsprechung.tg.ch.

Architecture:
- GET /og/rbog-{year} → year listing page with decision links
- GET /og/rbog-{year}-nr-{nn} → full decision HTML page
- No authentication, no AJAX — pure server-rendered HTML
- Full text inline in HTML

Content: RBOG (Rechenschaftsbericht des Obergerichts) series
Total: ~1,200 decisions (1994-present)
Platform: Atlassian Confluence Cloud + Scroll Viewport
"""
from __future__ import annotations

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

BASE_URL = "https://rechtsprechung.tg.ch"

# Regex patterns
RE_RBOG_LINK = re.compile(r"/og/rbog-(\d{4})-nr-(\d+)")
RE_DOCKET = re.compile(r"([A-Z]+\.\d{4}\.\d+)")
RE_DATE = re.compile(r"(\d{1,2})\.\s*(\w+)\s+(\d{4})")
RE_DATE_NUMERIC = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")

# Month name to number (German)
MONTH_MAP = {
    "januar": 1, "februar": 2, "märz": 3, "april": 4, "mai": 5, "juni": 6,
    "juli": 7, "august": 8, "september": 9, "oktober": 10, "november": 11, "dezember": 12,
}


def _parse_date_text(text):
    """Parse date from German text like '15. Oktober 2023'."""
    if not text:
        return None
    m = RE_DATE_NUMERIC.search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    m = RE_DATE.search(text)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = MONTH_MAP.get(month_name)
        if month:
            try:
                return date(year, month, day)
            except ValueError:
                pass
    return None


class TGGerichteScraper(BaseScraper):
    """
    Scraper for Thurgau Obergericht decisions via Scroll Viewport HTML.

    Strategy: iterate year pages (1994-present), extract decision links,
    fetch each decision page for full text.
    """

    REQUEST_DELAY = 2.0
    TIMEOUT = 30
    MAX_ERRORS = 50

    @property
    def court_code(self):
        return "tg_gerichte"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        total_yielded = 0
        today = date.today()
        start_year = since_date.year if since_date else 1994

        for year in range(today.year, start_year - 1, -1):
            logger.info(f"TG: scanning {year}")
            count = 0
            for stub in self._discover_year(year):
                if not self.state.is_known(stub["decision_id"]):
                    if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                        continue
                    total_yielded += 1
                    count += 1
                    yield stub
            if count > 0:
                logger.info(f"TG: {year}: {count} new stubs")

        logger.info(f"TG: discovery complete: {total_yielded} new stubs")

    def _discover_year(self, year: int) -> Iterator[dict]:
        """Fetch year listing page and extract decision links."""
        url = f"{BASE_URL}/og/rbog-{year}"
        try:
            r = self.get(url)
        except Exception as e:
            logger.error(f"TG: failed to fetch year page {year}: {e}")
            return

        if r.status_code != 200:
            logger.debug(f"TG: year page {year} returned {r.status_code}")
            return

        soup = BeautifulSoup(r.text, "html.parser")

        # Find all links to individual decision pages
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            m = RE_RBOG_LINK.search(href)
            if not m:
                continue

            link_year = int(m.group(1))
            nr = int(m.group(2))
            key = (link_year, nr)
            if key in seen:
                continue
            seen.add(key)

            link_text = a.get_text(strip=True)
            docket = f"RBOG-{link_year}-{nr}"

            decision_id = make_decision_id("tg_gerichte", docket)

            yield {
                "decision_id": decision_id,
                "docket_number": docket,
                "year": link_year,
                "nr": nr,
                "title": link_text[:200] if link_text else None,
                "url": f"{BASE_URL}/og/rbog-{link_year}-nr-{nr}",
            }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch full decision page and extract content."""
        url = stub.get("url")
        if not url:
            return None

        try:
            r = self.get(url)
        except Exception as e:
            logger.warning(f"TG: fetch failed for {stub['docket_number']}: {e}")
            return None

        if r.status_code != 200:
            logger.warning(f"TG: {stub['docket_number']} returned {r.status_code}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Extract main content
        main = soup.find("main") or soup.find("article") or soup.find("div", class_="content-body")
        if not main:
            # Fallback: find the largest content div
            best = None
            best_len = 0
            for div in soup.find_all("div"):
                tlen = len(div.get_text(strip=True))
                if tlen > best_len:
                    best = div
                    best_len = tlen
            if best and best_len > 200:
                main = best

        if not main:
            logger.warning(f"TG: no content found for {stub['docket_number']}")
            return None

        # Remove nav, footer, breadcrumbs
        for tag in main.find_all(["nav", "footer", "header"]):
            tag.decompose()
        for tag in main.find_all(class_=re.compile(r"breadcrumb|sidebar|footer|nav")):
            tag.decompose()

        full_text = main.get_text(separator="\n", strip=True)

        if not full_text or len(full_text) < 50:
            logger.warning(f"TG: text too short for {stub['docket_number']}: {len(full_text or '')} chars")
            if not full_text:
                full_text = f"[Text extraction failed for {stub['docket_number']}]"

        # Try to extract docket number from content
        docket = stub["docket_number"]
        m_docket = RE_DOCKET.search(full_text[:1000])
        if m_docket:
            docket = m_docket.group(1)

        # Extract decision date from content
        decision_date = _parse_date_text(full_text[:500])
        if not decision_date:
            decision_date = stub.get("decision_date")
        if not decision_date and stub.get("year"):
            decision_date = date(stub["year"], 1, 1)
        if not decision_date:
            logger.warning(f"TG: no date for {stub.get('docket_number', '?')}")

        # Extract chamber/division from content
        chamber = None
        m_abt = re.search(r"(Obergericht[,\s]*\d+\.\s*Abteilung)", full_text[:500])
        if m_abt:
            chamber = m_abt.group(1)

        language = detect_language(full_text) if len(full_text) > 100 else "de"
        decision_id = make_decision_id("tg_gerichte", docket)

        return Decision(
            decision_id=decision_id,
            court="tg_gerichte",
            canton="TG",
            chamber=chamber,
            docket_number=docket,
            decision_date=decision_date,
            language=language,
            title=stub.get("title"),
            full_text=full_text,
            source_url=url,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )
