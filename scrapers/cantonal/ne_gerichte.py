"""
Neuchâtel Courts Scraper (NE Gerichte)
=======================================
Scrapes court decisions from the FindinfoWeb / Omnis platform at
jurisprudence.ne.ch.

Architecture:
- POST/GET to /scripts/omnisapi.dll (search with form data) → result list HTML
- GET with Aufruf=getMarkupDocument&nF30_KEY=... → full decision HTML
- No authentication required
- Full text in HTML (inline rendering, not PDF)

Key parameters:
- Schema: NE_WEB
- Parametername: NEWEB
- OmnisServer: JURISWEB,localhost:7000
- Language: FRE (French)
- Wildcard search works via empty cSuchstring

Court instances: 24+ (1e Cour civile, Chambre d'arbitrage, etc.)
Total: ~7,391 decisions (1989-present)
Platform: Omnis/FindInfo (JurisWeb)
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

HOST = "https://jurisprudence.ne.ch"
CGI_PATH = "/scripts/omnisapi.dll"
CGI_URL = HOST + CGI_PATH

RESULTS_PER_PAGE = 20  # Server default; higher values may not work

# Fixed CGI parameters
BASE_PARAMS = {
    "OmnisPlatform": "WINDOWS",
    "WebServerUrl": "",
    "WebServerScript": "/scripts/omnisapi.dll",
    "OmnisLibrary": "JURISWEB",
    "OmnisClass": "rtFindinfoWebHtmlService",
    "OmnisServer": "JURISWEB,7000",
    "Schema": "NE_WEB",
    "Parametername": "NEWEB",
}

# Regex patterns
RE_TOTAL = re.compile(r"von\s+(\d+)\s+gefundenen", re.IGNORECASE)
RE_TOTAL_FR = re.compile(r"de\s+(\d+)\s+", re.IGNORECASE)
RE_NF30_KEY = re.compile(r"nF30_KEY=(\d+)")
RE_W10_KEY = re.compile(r"W10_KEY=(\d+)")
RE_DATE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
RE_DOCKET = re.compile(r"([A-Z]{2,5}[\._]\d{4}[\._]\d+(?:/\d+)?)")


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


def _extract_document_text(soup):
    """Extract full text from decision HTML."""
    content = soup.find("div", class_="WordSection1")
    if not content:
        content = soup.find("div", class_="Section1")
    if not content:
        # Find largest div
        best = None
        best_len = 0
        for div in soup.find_all("div"):
            tlen = len(div.get_text(strip=True))
            if tlen > best_len:
                best = div
                best_len = tlen
        if best and best_len > 500:
            content = best

    if not content:
        return ""

    paragraphs = []
    for p in content.find_all(["p", "div"]):
        text = p.get_text(strip=True)
        if text and len(text) > 1:
            paragraphs.append(text)

    if paragraphs:
        return "\n\n".join(paragraphs)

    return content.get_text(separator="\n", strip=True)


class NEGerichteScraper(BaseScraper):
    """
    Scraper for Neuchâtel court decisions via FindinfoWeb / Omnis.

    Strategy: iterate year-by-year with date filter, paginate within
    each year using W10_KEY session key.
    """

    REQUEST_DELAY = 2.0
    TIMEOUT = 60
    MAX_ERRORS = 100

    _session_initialized = False

    @property
    def court_code(self):
        return "ne_gerichte"

    def _init_session(self):
        """Load the search template to initialize the Omnis session."""
        if self._session_initialized:
            return
        try:
            self._rate_limit()
            self.session.get(
                CGI_URL,
                params={
                    **BASE_PARAMS,
                    "Aufruf": "loadTemplate",
                    "cTemplate": "search.html",
                    "cSprache": "FRE",
                },
                timeout=self.TIMEOUT,
            )
            self._session_initialized = True
            logger.info("NE: session initialized")
        except Exception as e:
            logger.warning(f"NE: session init failed: {e}")

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        self._init_session()

        # Single search for all decisions (no year filter needed)
        formdata = dict(BASE_PARAMS)
        formdata.update({
            "Aufruf": "validate",
            "cTemplate": "search_resulttable.html",
            "cTemplate_ValidationError": "search.html",
            "cSprache": "FRE",
            "nSeite": "1",
            "nAnzahlTrefferProSeite": str(RESULTS_PER_PAGE),
        })

        try:
            r = self.post(CGI_URL, data=formdata)
        except Exception as e:
            logger.error(f"NE: initial search failed: {e}")
            return

        html = r.text
        total_hits = self._parse_total(html)
        if not total_hits:
            logger.warning("NE: could not determine total hits")
            return

        logger.info(f"NE: {total_hits} total decisions, {(total_hits + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE} pages")

        session_key = self._extract_session_key(html)
        total_yielded = 0

        # Parse page 1
        for stub in self._parse_result_page(html):
            if not self.state.is_known(stub["decision_id"]):
                if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                    continue
                total_yielded += 1
                yield stub

        # Paginate through remaining pages
        if total_hits > RESULTS_PER_PAGE and session_key:
            total_pages = (total_hits + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
            for page in range(2, total_pages + 1):
                try:
                    params = dict(BASE_PARAMS)
                    params.update({
                        "Aufruf": "validate",
                        "cTemplate": "search_resulttable.html",
                        "cSprache": "FRE",
                        "nSeite": str(page),
                        "nAnzahlTrefferProSeite": str(RESULTS_PER_PAGE),
                        "W10_KEY": session_key,
                        "nAnzahlTreffer": str(total_hits),
                    })
                    r = self.get(CGI_URL, params=params)
                    for stub in self._parse_result_page(r.text):
                        if not self.state.is_known(stub["decision_id"]):
                            if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                                continue
                            total_yielded += 1
                            yield stub
                except Exception as e:
                    logger.error(f"NE: page {page} failed: {e}")
                    break

                if page % 20 == 0:
                    logger.info(f"NE: scanned {page}/{total_pages} pages, yielded {total_yielded} new stubs")

        logger.info(f"NE: discovery complete: {total_yielded} new stubs")

    def _parse_total(self, html: str) -> int | None:
        """Extract total hit count from result page."""
        # Pattern: "de 7439 fiche(s) trouvée(s)"
        m = re.search(r"de\s+(\d+)\s+fiche", html)
        if m:
            return int(m.group(1))
        # Pattern: "nAnzahlTreffer=7439"
        m = re.search(r"nAnzahlTreffer=(\d+)", html)
        if m:
            return int(m.group(1))
        # German: "von N gefundenen"
        m = RE_TOTAL.search(html)
        if m:
            return int(m.group(1))
        return None

    def _extract_session_key(self, html: str) -> str | None:
        m = RE_W10_KEY.search(html)
        return m.group(1) if m else None

    def _parse_result_page(self, html: str) -> Iterator[dict]:
        """Parse result entries from search result HTML."""
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            m_key = RE_NF30_KEY.search(href)
            if not m_key:
                continue

            nf30_key = m_key.group(1)

            # Extract docket from link text or title
            link_text = a.get_text(strip=True)
            title_attr = a.get("title", "")

            docket = None
            m_docket = RE_DOCKET.search(link_text) or RE_DOCKET.search(title_attr)
            if m_docket:
                docket = m_docket.group(1)

            # Try to extract metadata from parent row
            parent = a.find_parent("tr") or a.find_parent("div")
            decision_date = None
            if parent:
                parent_text = parent.get_text()
                m_date = RE_DATE.search(parent_text)
                if m_date:
                    decision_date = _parse_swiss_date(parent_text)

            if not docket:
                docket = f"NE-{nf30_key}"

            title = link_text[:200] if link_text else None
            decision_id = make_decision_id("ne_gerichte", docket)

            yield {
                "decision_id": decision_id,
                "docket_number": docket,
                "nf30_key": nf30_key,
                "decision_date": decision_date,
                "title": title,
                "url": self._build_doc_url(nf30_key),
            }

    @staticmethod
    def _build_doc_url(nf30_key: str) -> str:
        """Build URL to fetch individual decision document."""
        return (
            f"{CGI_URL}?"
            f"OmnisPlatform=WINDOWS"
            f"&WebServerUrl="
            f"&WebServerScript=/scripts/omnisapi.dll"
            f"&OmnisLibrary=JURISWEB"
            f"&OmnisClass=rtFindinfoWebHtmlService"
            f"&OmnisServer=JURISWEB,7000"
            f"&Parametername=NEWEB"
            f"&Schema=NE_WEB"
            f"&Aufruf=getMarkupDocument"
            f"&cSprache=FRE"
            f"&nF30_KEY={nf30_key}"
            f"&Template=search_result_document.html"
        )

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch full decision document and extract text."""
        url = stub.get("url")
        if not url:
            return None

        try:
            r = self.get(url)
        except Exception as e:
            logger.warning(f"NE: fetch failed for {stub['docket_number']}: {e}")
            return None

        html = r.text
        if len(html) < 500:
            logger.warning(f"NE: short doc for {stub['docket_number']}: {len(html)} chars")
            return None

        soup = BeautifulSoup(html, "html.parser")

        # Extract full text
        full_text = _extract_document_text(soup)
        if not full_text or len(full_text) < 50:
            logger.warning(f"NE: text too short for {stub['docket_number']}: {len(full_text or '')} chars")
            if not full_text:
                full_text = f"[Text extraction failed for {stub['docket_number']}]"

        decision_date = stub.get("decision_date")
        if not decision_date:
            # Try to extract from document
            for td in soup.find_all("td"):
                text = td.get_text(strip=True)
                if "date" in text.lower() and "décision" in text.lower():
                    next_td = td.find_next("td")
                    if next_td:
                        decision_date = _parse_swiss_date(next_td.get_text(strip=True))
                        break

        if not decision_date:
            decision_date = date.today()

        language = detect_language(full_text) if len(full_text) > 100 else "fr"
        decision_id = make_decision_id("ne_gerichte", stub["docket_number"])

        return Decision(
            decision_id=decision_id,
            court="ne_gerichte",
            canton="NE",
            docket_number=stub["docket_number"],
            decision_date=decision_date,
            language=language,
            title=stub.get("title"),
            full_text=full_text,
            source_url=url,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )
