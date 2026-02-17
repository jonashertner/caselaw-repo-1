"""
BGE Leitentscheide (Leading Decisions) Scraper
===============================================

Separate spider from BGer (which scrapes regular AZA decisions).
This scrapes the officially published Leitentscheide collection via the CLIR endpoint.

Key differences from BGer scraper:
- No Proof-of-Work required (CLIR has simpler anti-scraping than AZA)
- Volume-based iteration (Volumes I-V per year, since 1954)
- Trilingual Regesten (de/fr/it) per decision
- EGMR (European Court) integration via separate endpoint

Architecture:
1. Initial request to get session cookie
2. For each year from 1954 to present:
   - Request volumes I-V from CLIR endpoint
   - Parse decision listing from <ol><li> elements
   - For each decision:
     a. Fetch de/fr/it Regesten (3 subrequests)
     b. Fetch full text HTML
     c. Parse metadata from //div[@class='paraatf']
3. Additionally: fetch EGMR decisions (European Court references)

Endpoints (direct to search.bger.ch):
- CLIR: /ext/eurospider/live/de/php/clir/http/index_atf.php
- EGMR: /ext/eurospider/live/de/php/clir/http/index_cedh.php

Rate limiting: 3 seconds between requests.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

import requests
from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import (
    Decision,
    detect_language,
    extract_citations,
    make_decision_id,
    parse_date,
)
from incapsula_bypass import IncapsulaCookieManager

logger = logging.getLogger(__name__)


# ============================================================
# Constants
# ============================================================

# The BGE collection starts in 1954 (year-based indexing)
AUFSETZ_JAHR = 1954

# Official BGE volumes per year
VOLUMES = ["I", "II", "III", "IV", "V"]

# Volume-to-band offset: band = year - 1874
BAND_OFFSET = 1874

# Languages for trilingual Regesten
SPRACHEN = {"de": "D", "fr": "F", "it": "I"}

# Direct CLIR endpoint on search.bger.ch
HOST = "https://search.bger.ch"

# Direct CLIR URL template
# year = band number (year - 1874), volume = I/II/III/IV/V
SUCH_URL_TEMPLATE = (
    "https://search.bger.ch/ext/eurospider/live/de/php/clir/http/index_atf.php"
    "?year={band}&volume={volume}&lang=de&zoom=&system=clir"
)

# EGMR (European Court) endpoint
EGMR_URL = (
    "https://search.bger.ch/ext/eurospider/live/de/php/clir/http/index_cedh.php"
    "?lang=de"
)

# Initial URL to establish session cookie
INITIAL_URL = (
    "https://search.bger.ch/ext/eurospider/live/de/php/clir/http/index_atf.php"
    "?lang=de"
)

# Metadata regex: matches BGE decision header lines
# Pattern: "NNN. ... Urteil der {Kammer} i.S. {parties} {docket} vom {date}"
_MONATE_DE = [
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember",
]
_MONATE_FR = [
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
]
_ALL_MONTHS = "|".join(_MONATE_DE + _MONATE_FR)

RE_META = re.compile(
    r"^\d+\.\s+(?P<formal>.+(?:Urteil der|arrêt (?:de la|du))\s+"
    r"(?P<VKammer>.+)\s+(?:i\.S\.|dans la cause)\s+[^_]+\s+"
    r"(?P<Num2>\d+[A-F]?(?:_|\.)\d+/(?:19|20)\d\d)\s+(?:[^_]+\s+)?"
    r"(?:vom|du)\s+(?P<Datum>\d\d?\.?(?:er)?\s*(?:" + _ALL_MONTHS + r")\s+(?:19|20)\d\d))$"
)

RE_META_OHNE_GN = re.compile(
    r"^\d+\.\s+(?P<formal>.+(?:Urteil der|arrêt (?:de la|du))\s+"
    r"(?P<VKammer>.+)\s+(?:i\.S\.|dans la cause)\s+[^_]+\s+"
    r"(?:vom|du)\s+(?P<Datum>\d\d?\.?(?:er)?\s*(?:" + _ALL_MONTHS + r")\s+(?:19|20)\d\d))$"
)

RE_META_SIMPLE = re.compile(r"^\d+\s?\.\s+(?P<Rest>.+)$")

# HTML cleanup regex: removes div/span/a/artref tags and dangling <br>
RE_REMOVE_DIVS = re.compile(
    r"(</(?:div|span|a|artref)>)|"
    r"(<(?:div|span|a|artref)[^>]+>)|"
    r"(?:^<br>)|"
    r"(?:<br>(?:(?=<br>)|$))"
)
RE_DOUBLE_SPACES = re.compile(r"\s\s+")


# ============================================================
# BGE Leitentscheide Scraper
# ============================================================


class BGELeitentscheideScraper(BaseScraper):
    """
    Scraper for the officially published BGE Leitentscheide collection.

    Coverage: 1954–present, Volumes I–V per year, trilingual Regesten.
    """

    REQUEST_DELAY = 3.0  # Generous rate limit for CLIR
    TIMEOUT = 60         # CLIR can be slow

    @property
    def court_code(self) -> str:
        return "bge"

    def __init__(
        self,
        state_dir: Path = Path("state"),
        include_egmr: bool = True,
    ):
        """
        Args:
            state_dir: Directory for scraper state files.
            include_egmr: If True, also scrape EGMR decisions.
        """
        super().__init__(state_dir)
        self.include_egmr = include_egmr
        self._session_cookies: dict = {}
        self._incapsula = IncapsulaCookieManager(cache_dir=state_dir)

    # ---------------------------------------------------------------
    # URL construction
    # ---------------------------------------------------------------

    def _make_url(self, direct_url: str) -> str:
        """Return URL as-is (direct access to search.bger.ch)."""
        return direct_url

    def _volume_url(self, year: int, volume: str) -> str:
        """Build URL for a specific year/volume."""
        band = year - BAND_OFFSET
        direct = SUCH_URL_TEMPLATE.format(band=band, volume=volume)
        return self._make_url(direct)

    def _regeste_url(self, base_url: str, sprache: str) -> str:
        """
        Build URL for a trilingual Regeste.

        Build CLIR volume URL for a specific year.
        url = basisurl.replace("%3Ade&lang=de", "%3A{sprache}%3Aregeste&lang=de")
        """
        url = base_url.replace("%3Ade&lang=de", f"%3A{sprache}%3Aregeste&lang=de")
        return url

    # ---------------------------------------------------------------
    # Session management
    # ---------------------------------------------------------------

    def _establish_session(self) -> None:
        """
        Establish session: Incapsula bypass + CLIR session cookie.

        Flow:
        1. Harvest Incapsula cookies via Playwright (search.bger.ch)
        2. Apply to requests.Session
        3. Hit CLIR initial URL to get session cookies
        """
        # Step 1: Incapsula cookies
        try:
            incap_cookies = self._incapsula.get_cookies("search.bger.ch")
            self.session.cookies.update(incap_cookies)
            logger.info(f"Applied {len(incap_cookies)} Incapsula cookies for search.bger.ch")
        except Exception as e:
            logger.warning(f"Incapsula cookie harvest failed: {e}")

        # Step 2: CLIR session
        logger.info("Establishing CLIR session...")
        url = self._make_url(INITIAL_URL)
        response = self.get(url)

        # Check for Incapsula block
        if self._incapsula.is_incapsula_blocked(response.text):
            logger.warning("Incapsula block on CLIR, force-refreshing cookies")
            try:
                incap_cookies = self._incapsula.refresh_cookies("search.bger.ch")
                self.session.cookies.update(incap_cookies)
                response = self.get(url)
            except Exception as e:
                logger.error(f"Incapsula refresh failed: {e}")

        # Store any cookies from the response
        self._session_cookies = dict(response.cookies)
        # Merge Incapsula cookies into session cookies so they're sent on every request
        self._session_cookies.update(dict(self.session.cookies))
        logger.info(
            f"Session established. Cookies: {list(self._session_cookies.keys())}"
        )

    def _safe_get(self, url: str, retry: int = 0, max_retries: int = 3, **kwargs) -> "requests.Response":
        """
        GET with Incapsula detection and auto-refresh.

        Wraps self.get() and checks if the response is an Incapsula block.
        If blocked, refreshes cookies via Playwright and retries.
        """
        kwargs.setdefault("cookies", self._session_cookies)
        resp = self.get(url, **kwargs)

        if self._incapsula.is_incapsula_blocked(resp.text) and retry < max_retries:
            logger.info(f"Incapsula block on BGE request, refreshing ({retry+1}/{max_retries})")
            try:
                incap_cookies = self._incapsula.refresh_cookies("search.bger.ch")
                self.session.cookies.update(incap_cookies)
                self._session_cookies.update(incap_cookies)
            except Exception as e:
                logger.error(f"Incapsula refresh failed: {e}")
            return self._safe_get(url, retry + 1, max_retries, **kwargs)

        return resp

    # ---------------------------------------------------------------
    # Listing parsers
    # ---------------------------------------------------------------

    def _parse_volume_listing(self, html: str, year: int, volume: str) -> list[dict]:
        """
        Parse a volume listing page.

        XPath: //ol/li[a]
        Each <li> contains: <a href="...">BGE 150 I 1</a> followed by text.

        Returns list of stubs: {docket_number, url, bge_reference, year, volume}
        """
        soup = BeautifulSoup(html, "html.parser")
        stubs = []

        ol = soup.find("ol")
        if not ol:
            logger.debug(f"No <ol> found for {year} Volume {volume}")
            return stubs

        for li in ol.find_all("li", recursive=False):
            link = li.find("a")
            if not link:
                continue

            bge_ref = link.get_text(strip=True)
            href = link.get("href", "")

            if not href:
                continue

            stub = {
                "docket_number": bge_ref,      # e.g., "BGE 150 III 264"
                "bge_reference": bge_ref,
                "url": href,                    # Original URL for subrequests
                "year": year,
                "volume": volume,
            }
            stubs.append(stub)

        logger.info(f"Year {year} Volume {volume}: {len(stubs)} decisions listed")
        return stubs

    def _parse_egmr_listing(self, html: str) -> list[dict]:
        """
        Parse EGMR listing page.

        XPath: //table[@width='75%' and @style='border: 0px; ...']/tr[td]
        Each row: td[1]=date, td[2]=link(num), td[4]=case_name

        Returns list of stubs: {docket_number, decision_date, url, case_name}
        """
        soup = BeautifulSoup(html, "html.parser")
        stubs = []

        # Find the specific table (width='75%' and collapse style)
        tables = soup.find_all(
            "table",
            attrs={"width": "75%", "style": re.compile("border.*collapse")},
        )

        if not tables:
            logger.debug("No EGMR table found")
            return stubs

        for table in tables:
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                link = cells[1].find("a") if len(cells) > 1 else None
                if not link:
                    continue

                datum_str = cells[0].get_text(strip=True) if cells[0] else ""
                num = link.get_text(strip=True)
                href = link.get("href", "")
                case_name = cells[3].get_text(strip=True) if len(cells) > 3 else None

                stub = {
                    "docket_number": num,
                    "decision_date": datum_str,
                    "url": href,
                    "case_name": case_name,
                    "is_egmr": True,
                }
                stubs.append(stub)

        logger.info(f"EGMR listing: {len(stubs)} decisions")
        return stubs

    # ---------------------------------------------------------------
    # Detail parsers
    # ---------------------------------------------------------------

    def _fetch_regeste(self, base_url: str, sprache: str) -> str | None:
        """
        Fetch a single Regeste in a given language.

        XPath: //div[@id='highlight_content']
        Then clean with RE_REMOVE_DIVS (applied twice, per original).
        """
        url = self._regeste_url(base_url, sprache)
        try:
            response = self._safe_get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            content = soup.find("div", id="highlight_content")
            if not content:
                return None

            text = str(content)
            # Apply cleaning twice for thorough tag removal
            text = RE_REMOVE_DIVS.sub("", text).strip()
            text = RE_REMOVE_DIVS.sub("", text).strip()
            text = RE_DOUBLE_SPACES.sub(" ", text)
            return text if text else None
        except Exception as e:
            logger.warning(f"Failed to fetch {sprache} Regeste: {e}")
            return None

    def _fetch_trilingual_regesten(self, base_url: str) -> dict[str, str | None]:
        """Fetch Regesten in all three languages."""
        regesten = {}
        for sprache in SPRACHEN:
            regesten[sprache] = self._fetch_regeste(base_url, sprache)
        return regesten

    def _parse_document_metadata(self, html: str, year: int) -> dict:
        """
        Extract metadata from a decision document page.

        XPath: //div[@class='paraatf']/text()
        Uses three regex levels:
        1. RE_META: full match (chamber, docket, date)
        2. RE_META_OHNE_GN: match without docket number
        3. RE_META_SIMPLE: minimal match

        Also: //div[@id='highlight_content']/div[@class='content'] for full text.
        """
        soup = BeautifulSoup(html, "html.parser")
        meta = {}

        # Parse metadata from paraatf div
        paraatf = soup.find("div", class_="paraatf")
        if paraatf:
            meta_string = paraatf.get_text(strip=True)

            m = RE_META.search(meta_string)
            if m:
                meta["chamber"] = m.group("VKammer")
                meta["docket_2"] = m.group("Num2").replace(".", "_")
                meta["decision_date"] = parse_date(m.group("Datum"))
            else:
                m2 = RE_META_OHNE_GN.search(meta_string)
                if m2:
                    meta["chamber"] = m2.group("VKammer")
                    meta["decision_date"] = parse_date(m2.group("Datum"))
                else:
                    m3 = RE_META_SIMPLE.search(meta_string)
                    if m3:
                        meta["formal"] = m3.group("Rest")
                    meta["decision_date"] = date(year, 1, 1)

        # Extract full text HTML
        content = soup.find("div", id="highlight_content")
        if content:
            content_div = content.find("div", class_="content")
            if content_div:
                meta["html"] = str(content_div)
                meta["text"] = content_div.get_text(separator="\n", strip=True)
            else:
                meta["html"] = str(content)
                meta["text"] = content.get_text(separator="\n", strip=True)

        return meta

    def _parse_egmr_document(self, html: str) -> dict:
        """Parse an EGMR document page."""
        soup = BeautifulSoup(html, "html.parser")
        meta = {"chamber": "EGMR"}

        content = soup.find("div", id="highlight_content")
        if content:
            content_div = content.find("div", class_="content")
            if content_div:
                meta["html"] = str(content_div)
                meta["text"] = content_div.get_text(separator="\n", strip=True)
            else:
                meta["html"] = str(content)
                meta["text"] = content.get_text(separator="\n", strip=True)

        return meta

    # ---------------------------------------------------------------
    # Discovery
    # ---------------------------------------------------------------

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """
        Discover all BGE Leitentscheide.

        Iterates year-by-year from AUFSETZ_JAHR (1954) to current year,
        volumes I-V per year. Optionally also EGMR decisions.
        """
        # Establish session first
        self._establish_session()

        current_year = date.today().year
        start_year = AUFSETZ_JAHR

        if since_date:
            if isinstance(since_date, str):
                since_date = parse_date(since_date) or date(AUFSETZ_JAHR, 1, 1)
            start_year = max(since_date.year, AUFSETZ_JAHR)

        # Iterate years (newest first for incremental scraping)
        for year in range(current_year, start_year - 1, -1):
            for volume in VOLUMES:
                url = self._volume_url(year, volume)
                try:
                    response = self._safe_get(url)
                    stubs = self._parse_volume_listing(response.text, year, volume)
                    for stub in stubs:
                        # Check if already scraped
                        decision_id = make_decision_id("bge", stub["docket_number"])
                        if self.state.is_known(decision_id):
                            continue
                        yield stub
                except Exception as e:
                    logger.error(f"Failed to fetch listing {year}/{volume}: {e}")

        # EGMR decisions
        if self.include_egmr:
            try:
                egmr_url = self._make_url(EGMR_URL)
                response = self._safe_get(egmr_url)
                stubs = self._parse_egmr_listing(response.text)
                for stub in stubs:
                    decision_id = make_decision_id("bge_egmr", stub["docket_number"])
                    if self.state.is_known(decision_id):
                        continue
                    yield stub
            except Exception as e:
                logger.error(f"Failed to fetch EGMR listing: {e}")

    # ---------------------------------------------------------------
    # Full decision fetching
    # ---------------------------------------------------------------

    def fetch_decision(self, stub: dict) -> Decision | None:
        """
        Fetch a single BGE Leitentscheid with trilingual Regesten.

        Steps:
        1. Fetch de/fr/it Regesten from stub URL
        2. Fetch full text document
        3. Parse metadata from paraatf div
        4. Assemble Decision object
        """
        docket = stub["docket_number"]
        base_url = stub["url"]
        year = stub.get("year", date.today().year)
        is_egmr = stub.get("is_egmr", False)

        try:
            # Step 1: Fetch trilingual Regesten
            regesten = self._fetch_trilingual_regesten(base_url)

            # Step 2: Fetch full document
            doc_url = self._make_url(base_url)
            response = self._safe_get(doc_url)

            # Step 3: Parse metadata
            if is_egmr:
                meta = self._parse_egmr_document(response.text)
                court_code = "bge_egmr"
                decision_date = parse_date(stub.get("decision_date", "")) or date(year, 1, 1)
            else:
                meta = self._parse_document_metadata(response.text, year)
                court_code = "bge"
                decision_date = meta.get("decision_date") or date(year, 1, 1)

            full_text = meta.get("text", "")
            if not full_text:
                logger.warning(f"No text content for {docket}")
                return None

            # Step 4: Detect language from full text
            lang = detect_language(full_text)

            # Step 5: Build source URL
            source_url = base_url if base_url.startswith("http") else f"https://search.bger.ch{base_url}"

            # Step 6: Assemble Decision
            decision = Decision(
                decision_id=make_decision_id(court_code, docket),
                court=court_code,
                canton="CH",
                chamber=meta.get("chamber"),
                docket_number=docket,
                docket_number_2=meta.get("docket_2"),
                decision_date=decision_date,
                language=lang,
                title=stub.get("case_name"),
                regeste=regesten.get("de") or regesten.get("fr"),
                abstract_de=regesten.get("de"),
                abstract_fr=regesten.get("fr"),
                abstract_it=regesten.get("it"),
                full_text=self.clean_text(full_text),
                bge_reference=stub.get("bge_reference"),
                collection=stub.get("bge_reference"),
                source_url=source_url,
                cited_decisions=extract_citations(full_text),
                scraped_at=datetime.now(timezone.utc),
            )

            return decision

        except Exception as e:
            logger.error(f"Failed to fetch decision {docket}: {e}", exc_info=True)
            return None


# ============================================================
# CLI
# ============================================================


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape BGE Leitentscheide")
    parser.add_argument("--since", type=str, help="Start year (default: 1954)")
    parser.add_argument("--max", type=int, default=10, help="Max decisions to scrape")
    parser.add_argument(
        "--no-egmr", action="store_true", help="Skip EGMR decisions"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    since = None
    if args.since:
        since = date(int(args.since), 1, 1)

    scraper = BGELeitentscheideScraper(
        include_egmr=not args.no_egmr,
    )

    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)

    print(f"\n{'='*60}")
    print(f"Scraped {len(decisions)} BGE Leitentscheide")
    for d in decisions:
        print(f"  {d.decision_id}: {d.bge_reference or d.docket_number} ({d.decision_date})")
        if d.abstract_de:
            print(f"    DE: {d.abstract_de[:80]}...")
        if d.abstract_fr:
            print(f"    FR: {d.abstract_fr[:80]}...")
        if d.abstract_it:
            print(f"    IT: {d.abstract_it[:80]}...")
