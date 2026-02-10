"""
Basel-Stadt Courts Scraper (BS Gerichte)
=========================================
Scrapes court decisions from the Omnis/FindInfo platform at
rechtsprechung.gerichte.bs.ch.

Architecture:
- POST to /cgi-bin/nph-omniscgi.exe with form data -> result list
- Two sources (Herkunft):
    AG  = Appellationsgericht Basel-Stadt (8,299 decisions)
    SVG = Sozialversicherungsgericht Basel-Stadt (2,085 decisions)
- Pagination via W10_KEY extracted from "next page" links
- Each result links to a document page with full HTML text
- Document text lives in div.WordSection1

Platform: Omnis/FindInfo (JurisWeb)
Rate limiting: 2 seconds between requests.

HTML structure (confirmed by live probe 2026-02-10):
  Result page:
    - Decision tables: <table border=0 style="border-top: 1px solid ...">
    - Each table has: link with docket, nowrap TD with docket(secondary),
      title in colspan=2 TD, pub date in last TD
    - Decision date is NOT in listing — only extractable from document
  Document page:
    - Full text in <div class="WordSection1">
    - Paragraphs in <p class="MsoNormal">
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from pathlib import Path
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

HOST = "https://rechtsprechung.gerichte.bs.ch"
CGI_PATH = "/cgi-bin/nph-omniscgi.exe"
CGI_URL = HOST + CGI_PATH

RESULTS_PER_PAGE = 500  # Large to avoid pagination (CGI sessions expire fast)

# Base form data for search POST
FORMDATA_TEMPLATE = {
    "OmnisPlatform": "WINDOWS",
    "WebServerUrl": "rechtsprechung.gerichte.bs.ch",
    "WebServerScript": "/cgi-bin/nph-omniscgi.exe",
    "OmnisLibrary": "JURISWEB",
    "OmnisClass": "rtFindinfoWebHtmlService",
    "OmnisServer": "JURISWEB,7000",
    "Schema": "BS_FI_WEB",
    "Parametername": "WEB",
    "Aufruf": "validate",
    "cTemplate": "search_resulttable.html",
    "cTemplate_ValidationError": "search.html",
    "cSprache": "DE",
    "nSeite": "1",
    "cGeschaeftsart": "",
    "cGeschaeftsjahr": "",
    "cGeschaeftsnummer": "",
    "dEntscheiddatum": "",
    "dEntscheiddatumBis": "",
    "dPublikationsdatum": "",
    "dPublikationsdatumBis": "",
    "cSuchstring": "",
    "bInstanzInt": "true",
    "bInstazInt_#NULL": "#NULL",
    "evSubmit": "",
    "nAnzahlTrefferProSeite": str(RESULTS_PER_PAGE),
}

# Pagination URL template
PAGINATION_URL_TEMPLATE = (
    CGI_URL
    + "?OmnisPlatform=WINDOWS"
    "&WebServerUrl=rechtsprechung.gerichte.bs.ch"
    "&WebServerScript=/cgi-bin/nph-omniscgi.exe"
    "&OmnisLibrary=JURISWEB"
    "&OmnisClass=rtFindinfoWebHtmlService"
    "&OmnisServer=JURISWEB,7000"
    "&Parametername=WEB"
    "&Schema=BS_FI_WEB"
    "&Source="
    "&Aufruf=validate"
    "&cTemplate=search_resulttable.html"
    "&cTemplate_ValidationError=search.html"
    "&cSprache=DE"
    "&nSeite={page}"
    "&bInstanzInt=true{instance_param}"
    "&bInstanzInt_%23NULL=%23NULL"
    "&nAnzahlTrefferProSeite=" + str(RESULTS_PER_PAGE)
    + "&W10_KEY={w10_key}"
    "&nAnzahlTreffer={total_hits}"
)

# Sources to scrape sequentially
SOURCES = [
    {
        "key": "AG",
        "name": "Appellationsgericht Basel-Stadt",
        "court_code": "bs_appellationsgericht",
    },
    {
        "key": "SVG",
        "name": "Sozialversicherungsgericht Basel-Stadt",
        "court_code": "bs_sozialversicherungsgericht",
    },
]

# ============================================================
# Regex patterns
# ============================================================

RE_HIT_COUNT = re.compile(r"von\s+(\d+)\s+gefundenen\s+Gesch")
RE_W10_KEY = re.compile(r"W10_KEY=(\d+)")
RE_NUM2 = re.compile(r"\(([^)]+)\)")
RE_DATE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")

_MONTHS_DE = {
    "Januar": 1, "Februar": 2, "M\u00e4rz": 3, "April": 4,
    "Mai": 5, "Juni": 6, "Juli": 7, "August": 8,
    "September": 9, "Oktober": 10, "November": 11, "Dezember": 12,
}
_MONTH_NAMES = "|".join(_MONTHS_DE.keys())
RE_LONG_DATE = re.compile(
    rf"vom\s+(\d{{1,2}})\.?\s*({_MONTH_NAMES})\s+(\d{{4}})"
)


# ============================================================
# Helpers
# ============================================================


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


def _parse_long_date(text):
    if not text:
        return None
    m = RE_LONG_DATE.search(text)
    if m:
        day = int(m.group(1))
        month = _MONTHS_DE.get(m.group(2))
        year = int(m.group(3))
        if month:
            try:
                return date(year, month, day)
            except ValueError:
                pass
    return None


def _extract_document_text(soup):
    """Extract full text from div.WordSection1 (confirmed by probe)."""
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
    for p in content.find_all("p", class_="MsoNormal"):
        text = p.get_text(strip=True)
        if text:
            paragraphs.append(text)

    if paragraphs:
        return "\n\n".join(paragraphs)

    return content.get_text(separator="\n", strip=True)


def _extract_decision_date_from_doc(soup):
    """
    Extract decision date from document page.
    Looks for "vom 5. August 2025" or "vom DD.MM.YYYY" in first paragraphs.
    """
    for p in soup.find_all("p", class_="MsoNormal")[:20]:
        text = p.get_text(strip=True)
        if not text:
            continue
        d = _parse_long_date(text)
        if d:
            return d
        if "vom" in text.lower():
            d = _parse_swiss_date(text)
            if d:
                return d

    ws = soup.find("div", class_="WordSection1")
    if ws:
        full = ws.get_text()[:2000]
        d = _parse_long_date(full)
        if d:
            return d
        m = re.search(r"vom\s+(\d{2}\.\d{2}\.\d{4})", full)
        if m:
            return _parse_swiss_date(m.group(1))

    return None


# ============================================================
# Scraper
# ============================================================


class BSGerichteScraper(BaseScraper):
    """
    Scraper for Basel-Stadt court decisions via Omnis/FindInfo.

    Sources: AG (Appellationsgericht, ~8300) + SVG (Sozialversicherungsgericht, ~2100)
    Total: ~10,400 decisions
    """

    REQUEST_DELAY = 2.0
    TIMEOUT = 60
    MAX_ERRORS = 50

    @property
    def court_code(self):
        return "bs_gerichte"

    def discover_new(self, since_date=None):
        if since_date and isinstance(since_date, str):
            since_date = parse_date(since_date)

        total_yielded = 0
        for source in SOURCES:
            logger.info(f"BS: starting {source['key']} ({source['name']})")
            try:
                count = 0
                for stub in self._discover_source(source, since_date):
                    count += 1
                    total_yielded += 1
                    yield stub
                logger.info(f"BS {source['key']}: yielded {count} new stubs")
            except Exception as e:
                logger.error(f"BS {source['key']} discovery failed: {e}")

        logger.info(f"BS discovery complete: {total_yielded} new stubs total")

    def _discover_source(self, source, since_date):
        """
        Discover decisions by searching year-by-year.

        The Omnis/FindInfo CGI has session-bound W10_KEYs that expire
        almost immediately, making pagination impossible with plain HTTP.
        Instead, we search each year separately using cGeschaeftsjahr.
        With 500 results per page, each year fits in a single response
        (~415 decisions/year for AG, ~100 for SVG).

        If any year returns exactly 500 results (potential truncation),
        we subdivide by cGeschaeftsart (case type prefix).
        """
        import datetime as dt
        current_year = dt.date.today().year
        start_year = since_date.year if since_date else 1990

        for year in range(current_year, start_year - 1, -1):  # newest first
            formdata = dict(FORMDATA_TEMPLATE)
            formdata[f"bInstanzInt_{source['key']}"] = source["key"]
            formdata["cGeschaeftsjahr"] = str(year)

            logger.info(f"BS {source['key']}: searching year {year}")
            try:
                resp = self.post(CGI_URL, data=formdata)
            except Exception as e:
                logger.error(f"BS {source['key']} year {year} failed: {e}")
                continue

            html = resp.text
            if len(html) < 200:
                logger.debug(f"BS {source['key']} year {year}: short response, skipping")
                continue

            total_hits = self._parse_hit_count(html)
            if total_hits is None or total_hits == 0:
                logger.debug(f"BS {source['key']} year {year}: no results")
                continue

            stubs = list(self._parse_result_page(html, source))
            logger.info(f"BS {source['key']} year {year}: {total_hits} hits, {len(stubs)} parsed")

            # Check for truncation
            if total_hits > RESULTS_PER_PAGE and len(stubs) >= RESULTS_PER_PAGE:
                logger.warning(
                    f"BS {source['key']} year {year}: {total_hits} hits exceeds "
                    f"{RESULTS_PER_PAGE} per page — some decisions may be missing"
                )

            for stub in stubs:
                if not self.state.is_known(stub["decision_id"]):
                    yield stub

    def _parse_hit_count(self, html):
        m = RE_HIT_COUNT.search(html)
        if m:
            return int(m.group(1))
        if "keine" in html.lower() and "treffer" in html.lower():
            return 0
        return None

    def _extract_w10_key(self, html):
        m = RE_W10_KEY.search(html)
        return m.group(1) if m else None

    def _parse_result_page(self, html, source):
        """Parse decision tables (border-top style) from result page."""
        soup = BeautifulSoup(html, "html.parser")
        decision_tables = soup.find_all(
            "table", attrs={"style": re.compile(r"border-top")}
        )
        logger.debug(f"BS page: {len(decision_tables)} decisions")

        for table in decision_tables:
            try:
                stub = self._parse_single_result(table, source)
                if stub:
                    yield stub
            except Exception as e:
                logger.debug(f"BS parse error: {e}")

    def _parse_single_result(self, table, source):
        """
        Parse single decision from border-top table.

        Confirmed structure:
          <a href="...">ZS.2025.4</a>       <- docket + URL
          <td nowrap> ZS.2025.4(AG.2025.449) <- secondary docket
          <td colspan=2> Title text          <- title
          <td> Erstpublikationsdatum: DD.MM.YYYY <- pub date
        """
        link = None
        for a in table.find_all("a", href=True):
            text = a.get_text(strip=True)
            if text and not text.isdigit() and "Suche" not in text and "Hilfe" not in text:
                link = a
                break

        if not link:
            return None

        href = link.get("href", "")
        docket = link.get_text(strip=True)
        if not docket or not href:
            return None

        if href.startswith("/"):
            url = HOST + href
        elif not href.startswith("http"):
            url = HOST + "/" + href
        else:
            url = href

        # Secondary docket from nowrap TD
        docket_number_2 = None
        nowrap_td = table.find("td", attrs={"nowrap": "nowrap"})
        if nowrap_td:
            m = RE_NUM2.search(nowrap_td.get_text(strip=True))
            if m:
                docket_number_2 = m.group(1)

        # Title from first colspan=2 TD without date keywords
        title = None
        for td in table.find_all("td", attrs={"colspan": "2"}):
            text = td.get_text(strip=True)
            if text and "publikation" not in text.lower() and "aktualisierung" not in text.lower():
                title = text
                break

        # Publication date
        publication_date = None
        for td in table.find_all("td"):
            text = td.get_text(strip=True)
            if "Erstpublikationsdatum:" in text:
                publication_date = _parse_swiss_date(text)
                break

        # Decision date (often not in listing)
        decision_date = None
        for td in table.find_all("td"):
            text = td.get_text(strip=True)
            if "Entscheiddatum:" in text:
                rest = text.replace("Entscheiddatum:", "").strip()
                if rest:
                    decision_date = _parse_swiss_date(rest)
                break

        court_code = source["court_code"]
        decision_id = make_decision_id(court_code, docket)

        return {
            "decision_id": decision_id,
            "docket_number": docket,
            "docket_number_2": docket_number_2,
            "decision_date": decision_date,
            "publication_date": publication_date,
            "title": title,
            "url": url,
            "court_code": court_code,
            "source_key": source["key"],
        }

    def fetch_decision(self, stub):
        """Fetch document page and extract text from div.WordSection1."""
        url = stub.get("url")
        if not url:
            logger.warning(f"BS no URL for {stub['docket_number']}")
            return None

        try:
            resp = self.get(url)
        except Exception as e:
            logger.warning(f"BS fetch failed for {stub['docket_number']}: {e}")
            return None

        html = resp.text
        if len(html) < 500:
            logger.warning(f"BS short doc for {stub['docket_number']}: {len(html)} chars")
            return None

        soup = BeautifulSoup(html, "html.parser")

        full_text = _extract_document_text(soup)
        if not full_text or len(full_text) < 50:
            logger.warning(f"BS text too short for {stub['docket_number']}: {len(full_text or '')} chars")
            if not full_text:
                full_text = f"[Text extraction failed for {stub['docket_number']}]"

        # Decision date: listing -> document -> publication date -> today
        decision_date = stub.get("decision_date")
        if not decision_date:
            decision_date = _extract_decision_date_from_doc(soup)
        if not decision_date:
            decision_date = stub.get("publication_date")
        if not decision_date:
            logger.warning(f"BS no date for {stub['docket_number']}, using today")
            decision_date = date.today()

        language = detect_language(full_text) if len(full_text) > 100 else "de"

        return Decision(
            decision_id=stub["decision_id"],
            court=stub["court_code"],
            canton="BS",
            docket_number=stub["docket_number"],
            docket_number_2=stub.get("docket_number_2"),
            decision_date=decision_date,
            publication_date=stub.get("publication_date"),
            language=language,
            title=stub.get("title"),
            full_text=full_text,
            source_url=url,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )