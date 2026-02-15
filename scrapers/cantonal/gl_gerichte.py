"""
Glarus Courts Scraper (GL Gerichte)
====================================
Scrapes court decisions from the FindInfo / Omnis CGI platform at
findinfo.gl.ch.

Architecture:
- POST /cgi-bin/nph-omniscgi.exe (search form data) → result list HTML
- GET with Aufruf=getMarkupDocument&nF30_KEY=... → full decision HTML
- No authentication required
- Full text in HTML (div.WordSection1 / div.Section1)

Key parameters:
- Schema: GLWEB
- Parametername: WEB
- OmnisServer: JURISWEB,7000
- Language: DEU (German)
- Session key: W10_KEY for pagination

Court instances: OG, VG, SG, ZG
Total: ~693 decisions
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

HOST = "https://findinfo.gl.ch"
CGI_PATH = "/cgi-bin/nph-omniscgi.exe"
CGI_URL = HOST + CGI_PATH

RESULTS_PER_PAGE = 10  # Server default

# Fixed CGI parameters
BASE_PARAMS = {
    "OmnisPlatform": "WINDOWS",
    "WebServerUrl": "findinfo.gl.ch",
    "WebServerScript": "/cgi-bin/nph-omniscgi.exe",
    "OmnisLibrary": "JURISWEB",
    "OmnisClass": "rtFindinfoWebHtmlService",
    "OmnisServer": "JURISWEB,7000",
    "Schema": "GLWEB",
    "Parametername": "WEB",
}

# Regex patterns
RE_TOTAL = re.compile(r"von\s+(\d+)\s+gefundenen", re.IGNORECASE)
RE_NF30_KEY = re.compile(r"nF30_KEY=(\d+)")
RE_W10_KEY = re.compile(r"W10_KEY=(\d+)")
RE_DATE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
RE_DOCKET = re.compile(r"([A-Z]{2,4}\s+\d{4}\s+\d+)")


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
    for p in content.find_all(["p"]):
        text = p.get_text(strip=True)
        if text:
            paragraphs.append(text)

    if paragraphs:
        return "\n\n".join(paragraphs)

    return content.get_text(separator="\n", strip=True)


class GLGerichteScraper(BaseScraper):
    """
    Scraper for Glarus court decisions via FindInfo / Omnis CGI.

    Strategy: search all decisions with empty query, paginate using W10_KEY.
    Small volume (~693 decisions, ~70 pages at 10/page).
    """

    REQUEST_DELAY = 2.0
    TIMEOUT = 60
    MAX_ERRORS = 50

    @property
    def court_code(self):
        return "gl_gerichte"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        total_yielded = 0
        self._session_key = None  # Track session key for document fetching

        # Initial search — empty query returns all decisions
        formdata = dict(BASE_PARAMS)
        formdata.update({
            "Aufruf": "search",
            "cTemplate": "simple/search_result.fiw",
            "cSprache": "DEU",
            "cSuchstring": "",
            "bSelectAll": "1",
            "nSeite": "1",
            "nAnzahlTrefferProSeite": str(RESULTS_PER_PAGE),
        })

        if since_date:
            formdata["dPublikationsdatumVon"] = since_date.strftime("%d.%m.%Y")

        try:
            r = self.post(CGI_URL, data=formdata)
        except Exception as e:
            logger.error(f"GL: initial search failed: {e}")
            return

        html = r.text
        total_hits = self._parse_total(html)
        if not total_hits:
            logger.info("GL: no results found")
            return

        logger.info(f"GL: {total_hits} total decisions")

        session_key = self._extract_session_key(html)
        self._session_key = session_key

        # Parse page 1
        for stub in self._parse_result_page(html, session_key):
            if not self.state.is_known(stub["decision_id"]):
                if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                    continue
                total_yielded += 1
                yield stub

        # Paginate
        if total_hits > RESULTS_PER_PAGE and session_key:
            total_pages = (total_hits + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
            for page in range(2, total_pages + 1):
                try:
                    params = dict(BASE_PARAMS)
                    params.update({
                        "Aufruf": "search",
                        "cTemplate": "simple/search_result.fiw",
                        "cSprache": "DEU",
                        "nSeite": str(page),
                        "nAnzahlTrefferProSeite": str(RESULTS_PER_PAGE),
                        "W10_KEY": session_key,
                        "nAnzahlTreffer": str(total_hits),
                    })
                    r = self.get(CGI_URL, params=params)
                    # Update session key if changed
                    new_key = self._extract_session_key(r.text)
                    if new_key:
                        self._session_key = new_key
                    for stub in self._parse_result_page(r.text, self._session_key):
                        if not self.state.is_known(stub["decision_id"]):
                            if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                                continue
                            total_yielded += 1
                            yield stub
                except Exception as e:
                    logger.error(f"GL: page {page} failed: {e}")
                    break

                if page % 20 == 0:
                    logger.info(f"GL: page {page}/{total_pages}, {total_yielded} new stubs")

        logger.info(f"GL: discovery complete: {total_yielded} new stubs")

    def _parse_total(self, html: str) -> int | None:
        """Extract total from 'von N gefundenen Entscheid'."""
        m = RE_TOTAL.search(html)
        if m:
            return int(m.group(1))
        return None

    def _extract_session_key(self, html: str) -> str | None:
        m = RE_W10_KEY.search(html)
        return m.group(1) if m else None

    def _parse_result_page(self, html: str, session_key: str | None = None) -> Iterator[dict]:
        """Parse result entries from search result HTML."""
        soup = BeautifulSoup(html, "html.parser")

        # Extract nF30_KEY links from anchor tags
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            m_key = RE_NF30_KEY.search(href)
            if not m_key:
                continue

            nf30_key = m_key.group(1)
            link_text = a.get_text(strip=True)

            # Extract W10_KEY from the link itself (more accurate than page-level)
            m_w10 = RE_W10_KEY.search(href)
            w10_key = m_w10.group(1) if m_w10 else session_key

            # Extract docket from link text
            docket = None
            m_docket = RE_DOCKET.search(link_text)
            if m_docket:
                docket = m_docket.group(1)

            # Try parent for metadata
            parent = a.find_parent("tr") or a.find_parent("div")
            decision_date = None
            if parent:
                parent_text = parent.get_text()
                decision_date = _parse_swiss_date(parent_text)

            if not docket:
                docket = f"GL-{nf30_key}"

            title = link_text[:200] if link_text else None
            decision_id = make_decision_id("gl_gerichte", docket)

            yield {
                "decision_id": decision_id,
                "docket_number": docket,
                "nf30_key": nf30_key,
                "w10_key": w10_key,
                "decision_date": decision_date,
                "title": title,
                "url": self._build_doc_url(nf30_key, w10_key),
            }

    @staticmethod
    def _build_doc_url(nf30_key: str, w10_key: str | None = None) -> str:
        """Build URL to fetch individual decision document."""
        url = (
            f"{CGI_URL}?"
            f"OmnisPlatform=WINDOWS"
            f"&WebServerUrl=findinfo.gl.ch"
            f"&WebServerScript=/cgi-bin/nph-omniscgi.exe"
            f"&OmnisLibrary=JURISWEB"
            f"&OmnisClass=rtFindinfoWebHtmlService"
            f"&OmnisServer=JURISWEB,7000"
            f"&Parametername=WEB"
            f"&Schema=GLWEB"
            f"&Aufruf=getMarkupDocument"
            f"&cSprache=DEU"
            f"&nF30_KEY={nf30_key}"
            f"&Template=simple/search_result_document.html"
        )
        if w10_key:
            url += f"&W10_KEY={w10_key}"
        return url

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch full decision document and extract text."""
        url = stub.get("url")
        if not url:
            return None

        try:
            r = self.get(url)
        except Exception as e:
            logger.warning(f"GL: fetch failed for {stub['docket_number']}: {e}")
            return None

        html = r.text
        if len(html) < 500:
            logger.warning(f"GL: short doc for {stub['docket_number']}: {len(html)} chars")
            return None

        soup = BeautifulSoup(html, "html.parser")
        full_text = _extract_document_text(soup)

        if not full_text or len(full_text) < 50:
            logger.warning(f"GL: text too short for {stub['docket_number']}: {len(full_text or '')} chars")
            if not full_text:
                full_text = f"[Text extraction failed for {stub['docket_number']}]"

        decision_date = stub.get("decision_date")
        if not decision_date:
            logger.warning(f"[gl_gerichte] No date for {stub['docket_number']}")

        language = detect_language(full_text) if len(full_text) > 100 else "de"
        decision_id = make_decision_id("gl_gerichte", stub["docket_number"])

        return Decision(
            decision_id=decision_id,
            court="gl_gerichte",
            canton="GL",
            docket_number=stub["docket_number"],
            decision_date=decision_date,
            language=language,
            title=stub.get("title"),
            full_text=full_text,
            source_url=url,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )
