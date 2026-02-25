"""
FINMA Kasuistik Scraper
========================

Scrapes enforcement case reports ("Kasuistik") from the Swiss Financial Market
Supervisory Authority (FINMA) at finma.ch.

Architecture:
- Sitecore CMS with a POST search API that returns all ~406 cases at once
- POST to /de/api/search/getresult with dataset ID
- Each item links to a detail page with a structured HTML table
- Detail table has: Partei, Bereich, Thema, Zusammenfassung, Massnahmen,
  Rechtskraft, Kommunikation, Entscheiddatum

Coverage: 2014–present (~406 decisions)
Rate limiting: 1.5 seconds
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
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

# Sitecore search API
SEARCH_URL = "https://www.finma.ch/de/api/search/getresult"
DATASET_ID = "{2FBD0DFE-112F-4176-BE8D-07C2D0BE0903}"
BASE_URL = "https://www.finma.ch"

# Map FINMA Bereich to legal_area
BEREICH_MAP = {
    "Bewilligte": "Finanzmarktaufsicht",
    "Unbewilligte": "Finanzmarktaufsicht",
    "Marktaufsicht": "Marktaufsicht",
    "Prüfgesellschaften": "Prüfgesellschaften",
}


class FINMAScraper(BaseScraper):
    """Scraper for FINMA enforcement case reports (Kasuistik)."""

    REQUEST_DELAY = 1.5
    TIMEOUT = 30

    @property
    def court_code(self) -> str:
        return "finma"

    def _fetch_listing(self) -> list[dict]:
        """Fetch all cases from Sitecore search API."""
        response = self.post(
            SEARCH_URL,
            data=f"ds={DATASET_ID}&Order=4",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://www.finma.ch/de/dokumentation/enforcementberichterstattung/kasuistik/",
            },
        )
        data = response.json()
        items = data.get("Items", [])
        logger.info(f"[finma] Listing returned {len(items)} cases")
        return items

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover FINMA enforcement cases."""
        items = self._fetch_listing()

        for item in items:
            title = item.get("Title", "").strip()
            if not title:
                continue

            decision_id = make_decision_id("finma", title)
            if self.state.is_known(decision_id):
                continue

            link = item.get("Link", "")
            if link and not link.startswith("http"):
                link = BASE_URL + link

            # Parse date from listing (format: DD.MM.YYYY or YYYY-MM-DD)
            item_date = item.get("Date", "")

            # Filter by since_date if provided
            if since_date:
                parsed = parse_date(item_date)
                if parsed and parsed < since_date:
                    continue

            stub = {
                "docket_number": title,
                "decision_date": item_date,
                "url": link,
                "facet": item.get("FacetColumn", ""),
            }
            yield stub

    def _parse_detail_table(self, soup: BeautifulSoup) -> dict[str, str]:
        """Extract fields from the vertical e-table on the detail page."""
        fields = {}
        table = soup.find("table", class_="e-table")
        if not table:
            # Try broader search
            table = soup.find("table")
        if not table:
            return fields

        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                key = th.get_text(strip=True)
                # Preserve paragraph breaks in value
                for br in td.find_all("br"):
                    br.replace_with("\n")
                for p in td.find_all("p"):
                    p.insert_before("\n")
                    p.insert_after("\n")
                value = td.get_text(separator=" ").strip()
                # Clean up multiple whitespace/newlines
                value = re.sub(r"[ \t]+", " ", value)
                value = re.sub(r"\n{3,}", "\n\n", value)
                value = value.strip()
                fields[key] = value
        return fields

    def _extract_abbr_refs(self, soup: BeautifulSoup) -> list[str]:
        """Extract law references from <abbr> tags."""
        refs = []
        for abbr in soup.find_all("abbr"):
            title = abbr.get("title", "").strip()
            text = abbr.get_text(strip=True)
            if title:
                refs.append(title)
            elif text:
                refs.append(text)
        return refs

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch and parse a single FINMA enforcement case detail page."""
        url = stub.get("url", "")
        docket = stub["docket_number"]

        if not url:
            logger.warning(f"[finma] No URL for {docket}")
            return None

        try:
            response = self.get(url, allow_redirects=False)
            # Non-existent pages return 302 redirect
            if response.status_code in (301, 302):
                logger.warning(f"[finma] Redirect for {docket} — page not found")
                return None
        except Exception as e:
            logger.error(f"[finma] Failed to fetch {docket}: {e}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        fields = self._parse_detail_table(soup)

        if not fields:
            logger.warning(f"[finma] No table data for {docket} at {url}")
            return None

        # Extract structured fields
        partei = fields.get("Partei", fields.get("Partie", ""))
        bereich = fields.get("Bereich", fields.get("Domaine", ""))
        thema = fields.get("Thema", fields.get("Thème", ""))
        zusammenfassung = fields.get("Zusammenfassung", fields.get("Résumé", ""))
        massnahmen = fields.get("Massnahmen", fields.get("Mesures", ""))
        rechtskraft = fields.get("Rechtskraft", fields.get("Force de chose jugée", ""))
        kommunikation = fields.get("Kommunikation", fields.get("Communication", ""))
        entscheiddatum = fields.get("Entscheiddatum", fields.get("Date de la décision", ""))

        # Build full text from all substantive fields
        text_parts = []
        if partei:
            text_parts.append(f"Partei: {partei}")
        if bereich:
            text_parts.append(f"Bereich: {bereich}")
        if thema:
            text_parts.append(f"Thema: {thema}")
        if zusammenfassung:
            text_parts.append(f"Zusammenfassung:\n{zusammenfassung}")
        if massnahmen:
            text_parts.append(f"Massnahmen:\n{massnahmen}")
        if rechtskraft:
            text_parts.append(f"Rechtskraft: {rechtskraft}")
        if kommunikation and kommunikation != "-":
            text_parts.append(f"Kommunikation: {kommunikation}")

        full_text = "\n\n".join(text_parts)
        if not full_text.strip():
            logger.warning(f"[finma] Empty text for {docket}")
            return None

        # Date: prefer Entscheiddatum field, fall back to listing date
        decision_date = parse_date(entscheiddatum) or parse_date(stub.get("decision_date", ""))

        # Language detection
        lang = detect_language(full_text)

        # Title from Partei + Thema
        title_parts = [p for p in [partei, thema] if p]
        title = " — ".join(title_parts) if title_parts else docket

        # Legal area from Bereich
        legal_area = BEREICH_MAP.get(bereich, bereich) if bereich else None

        # Citations from text
        citations = extract_citations(full_text)
        # Also add any abbr references
        abbr_refs = self._extract_abbr_refs(soup)
        for ref in abbr_refs:
            if ref not in citations:
                citations.append(ref)

        decision = Decision(
            decision_id=make_decision_id("finma", docket),
            court="finma",
            canton="CH",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=title,
            legal_area=legal_area,
            full_text=self.clean_text(full_text),
            outcome=massnahmen or None,
            source_url=url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )
        return decision


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape FINMA Kasuistik")
    parser.add_argument("--since", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--max", type=int, default=5, help="Max decisions")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    since = date.fromisoformat(args.since) if args.since else None
    scraper = FINMAScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  {d.title[:60]}")
    print(f"\nScraped {len(decisions)} FINMA decisions")
