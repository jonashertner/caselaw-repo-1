"""
Geneva Courts Scraper (GE Gerichte)
=====================================
Scrapes curated court decisions ("arrêts de principe") from
justice.ge.ch/apps/decis/fr/pjdoc/search.

Architecture:
- GET /apps/decis/fr/pjdoc/search?search_meta=...&sort_by=date&page_size=100&page=N
  → HTML result pages with decision metadata
- Decisions contain: docket, date, court, chamber, descripteurs,
  normes, résumé — but NO full text or PDF
- Content is metadata + résumé only (structured abstracts of leading cases)

Pagination:
- page parameter is 1-indexed (page=0 behaves like page=1)
- page_size: 20, 50, or 100
- Total from: <strong>N</strong> enregistrements trouvés

Volume: ~2,259 curated decisions (1974-2026)
Most years have <100 decisions, so one request per year usually suffices.
Platform: Drupal + Solr/Elasticsearch
"""
from __future__ import annotations

import logging
import re
from datetime import date
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

BASE_URL = "https://justice.ge.ch"
SEARCH_URL = f"{BASE_URL}/apps/decis/fr/pjdoc/search"

RE_TOTAL = re.compile(r"<strong>(\d+)</strong>\s*enregistrement")
RE_DATE_FR = re.compile(r"du\s+(\d{2})\.(\d{2})\.(\d{4})")

# Start year for decisions
START_YEAR = 1974


class GEGerichteScraper(BaseScraper):
    """
    Scraper for Geneva curated court decisions (arrêts de principe).

    Note: This platform only provides metadata + résumé, not full decision text.
    The full_text field will contain the résumé and any available structured content.
    Total: ~2,259 decisions.
    """

    REQUEST_DELAY = 2.0
    TIMEOUT = 30
    MAX_ERRORS = 30

    @property
    def court_code(self):
        return "ge_gerichte"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        total_yielded = 0
        today = date.today()
        start_year = since_date.year if since_date else START_YEAR

        for year in range(today.year, start_year - 1, -1):
            logger.info(f"GE: searching year {year}")
            count = 0
            for stub in self._discover_year(year):
                if not self.state.is_known(stub["decision_id"]):
                    if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                        continue
                    total_yielded += 1
                    count += 1
                    yield stub
            logger.info(f"GE: year {year}: {count} new stubs")

        logger.info(f"GE: discovery complete: {total_yielded} new stubs")

    def _discover_year(self, year: int) -> Iterator[dict]:
        """Discover all decisions for a given year."""
        search_meta = f"dt_decision:[01.01.{year} TO 31.12.{year}]"
        params = {
            "search_meta": search_meta,
            "sort_by": "date",
            "page_size": "100",
            "page": "1",
        }

        try:
            r = self.get(SEARCH_URL, params=params)
        except Exception as e:
            logger.error(f"GE: search failed for year {year}: {e}")
            return

        html = r.text
        total = self._parse_total(html)
        if not total:
            return

        logger.debug(f"GE: year {year}: {total} decisions")

        # Parse page 1
        for stub in self._parse_result_page(html):
            yield stub

        # Paginate if needed
        if total > 100:
            total_pages = (total + 99) // 100
            for page in range(2, total_pages + 1):
                params["page"] = str(page)
                try:
                    r = self.get(SEARCH_URL, params=params)
                    for stub in self._parse_result_page(r.text):
                        yield stub
                except Exception as e:
                    logger.error(f"GE: page {page} for year {year} failed: {e}")
                    break

    def _parse_total(self, html: str) -> int | None:
        m = RE_TOTAL.search(html)
        if m:
            return int(m.group(1))
        return None

    def _parse_result_page(self, html: str) -> Iterator[dict]:
        """Parse decision blocks from search result HTML."""
        soup = BeautifulSoup(html, "html.parser")

        results_div = soup.find("div", id="lstDecis")
        if not results_div:
            return

        blocks = results_div.find_all("div", class_="list-block")

        for block in blocks:
            try:
                stub = self._parse_block(block)
                if stub:
                    yield stub
            except Exception as e:
                logger.debug(f"GE: parse error: {e}")

    def _parse_block(self, block) -> dict | None:
        """Parse a single decision block."""
        # Fiche ID
        flag_div = block.find("div", class_="decis-block__flag")
        fiche_id = None
        if flag_div:
            text = flag_div.get_text(strip=True)
            m = re.search(r"Fiche\s+(\d+)", text)
            if m:
                fiche_id = m.group(1)

        # Title/docket from h3 link
        h3 = block.find("h3", class_="list-block__title")
        if not h3:
            return None

        link = h3.find("a")
        if not link:
            return None

        docket = link.get_text(strip=True)
        if not docket:
            return None

        # Decision date from "du DD.MM.YYYY" text after the link
        h3_text = h3.get_text(strip=True)
        decision_date = None
        m = RE_DATE_FR.search(h3_text)
        if m:
            try:
                decision_date = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass

        # Court and chamber
        court_div = block.find("div", class_="col-lg-12")
        court_info = ""
        chamber = None
        if court_div:
            inner_div = court_div.find("div")
            if inner_div:
                court_info = inner_div.get_text(strip=True)
                # Parse: "CJ , CABL" -> court=CJ, chamber=CABL
                parts = [p.strip() for p in court_info.split(",")]
                if len(parts) >= 2:
                    chamber = parts[1]

        # Descripteurs
        descripteurs = ""
        normes = ""
        resume = ""

        for div in block.find_all("div"):
            text = div.get_text(strip=True)
            if text.startswith("Descripteurs"):
                descripteurs = text.replace("Descripteurs :", "").strip()
            elif text.startswith("Normes"):
                normes = text.replace("Normes :", "").strip()
            elif text.startswith("Résumé") or text.startswith("Resume"):
                resume = text.replace("Résumé :", "").replace("Resume :", "").strip()

        decision_id = make_decision_id("ge_gerichte", docket)

        return {
            "decision_id": decision_id,
            "docket_number": docket,
            "fiche_id": fiche_id,
            "decision_date": decision_date,
            "court_info": court_info,
            "chamber": chamber,
            "descripteurs": descripteurs,
            "normes": normes,
            "resume": resume,
            "url": f"{SEARCH_URL}?query={docket}",
        }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Build decision from stub metadata (no additional fetch needed)."""
        docket = stub["docket_number"]

        # Build full text from available metadata
        parts = []
        if stub.get("resume"):
            parts.append(stub["resume"])
        if stub.get("descripteurs"):
            parts.append(f"Descripteurs: {stub['descripteurs']}")
        if stub.get("normes"):
            parts.append(f"Normes: {stub['normes']}")

        full_text = "\n\n".join(parts) if parts else f"[Metadata only for {docket}]"

        decision_date = stub.get("decision_date")
        if not decision_date:
            logger.warning(f"[ge_gerichte] No date for {stub.get('docket_number', '?')}")

        language = "fr"  # Geneva is French-speaking

        return Decision(
            decision_id=stub["decision_id"],
            court="ge_gerichte",
            canton="GE",
            chamber=stub.get("chamber"),
            docket_number=docket,
            decision_date=decision_date,
            language=language,
            regeste=stub.get("resume") or None,
            full_text=full_text,
            source_url=stub.get("url", SEARCH_URL),
            external_id=stub.get("fiche_id"),
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )
