"""
Zurich Obergericht Scraper — Model for Custom Cantonal Scrapers
================================================================

Scrapes decisions from obergericht.zh.ch.

Architecture:
- livesearch.php API at gerichte-zh.ch (not a standard web UI)
- Date-range windowing: 500-day steps from 01.01.1980 to today
- Returns JSON-like results list
- PDF links on base URL

This serves as a model for implementing other custom cantonal scrapers.
Each canton is different, but the pattern of date-windowed discovery + detail parsing
is common across many implementations.

Adapt by:
1. Changing RESULT_PAGE_URL to the canton's search endpoint
2. Adjusting the response parser (_parse_listing_page)
3. Adjusting the date step size and start year
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
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

RESULT_PAGE_URL = (
    "https://www.gerichte-zh.ch/typo3conf/ext/"
    "frp_entscheidsammlung_extended/res/php/livesearch.php"
    "?q=&geschaeftsnummer=&gericht=gerichtTitel&kammer=kammerTitel"
    "&entscheiddatum_von={datum_ab}&entscheiddatum_bis={datum_bis}"
    "&erweitert=1&usergroup=0&sysOrdnerPid=0"
    "&sucheErlass=Erlass&sucheArt=Art.&sucheAbs=Abs.&sucheZiff=Ziff./lit."
    "&sucheErlass2=Erlass&sucheArt2=Art.&sucheAbs2=Abs.&sucheZiff2=Ziff./lit."
    "&sucheErlass3=Erlass&sucheArt3=Art.&sucheAbs3=Abs.&sucheZiff3=Ziff./lit."
    "&suchfilter=1"
)

PDF_BASE = "https://www.gerichte-zh.ch"
TAG_SCHRITTE = 500  # Day window size
AUFSETZ_TAG = date(1980, 1, 1)


class ZHObergerichtScraper(BaseScraper):
    """
    Scraper for Zürich Obergericht decisions.

    Uses date-window iteration over livesearch.php endpoint.
    """

    REQUEST_DELAY = 2.0
    TIMEOUT = 30

    @property
    def court_code(self) -> str:
        return "zh_obergericht"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """
        Discover decisions using 500-day windows.

        Iterate from AUFSETZ_TAG (01.01.1980) to today
        in TAG_SCHRITTE (500) day steps.
        """
        start = AUFSETZ_TAG
        if since_date:
            if isinstance(since_date, str):
                since_date = parse_date(since_date) or AUFSETZ_TAG
            start = since_date

        delta = timedelta(days=TAG_SCHRITTE)
        one_day = timedelta(days=1)
        current = start
        today = date.today()

        while current <= today:
            end = min(current + delta, today)
            datum_ab = current.strftime("%d.%m.%Y")
            datum_bis = end.strftime("%d.%m.%Y")

            url = RESULT_PAGE_URL.format(datum_ab=datum_ab, datum_bis=datum_bis)
            try:
                response = self.get(url)
                stubs = self._parse_listing_page(response.text)
                logger.info(f"ZH OG {datum_ab}–{datum_bis}: {len(stubs)} decisions")

                for stub in stubs:
                    decision_id = make_decision_id("zh_obergericht", stub["docket_number"])
                    if not self.state.is_known(decision_id):
                        yield stub

            except Exception as e:
                logger.error(f"Failed to fetch ZH OG listing {datum_ab}–{datum_bis}: {e}")

            current = end + one_day

    def _parse_listing_page(self, html: str) -> list[dict]:
        """
        Parse the livesearch.php response.

        The response format varies — it may be HTML with decision entries.
        This is a template; adjust selectors based on actual response format.
        """
        stubs = []
        soup = BeautifulSoup(html, "html.parser")

        # Typical pattern: each decision is in a result item container
        # Adjust selectors based on actual DOM structure
        for item in soup.find_all("div", class_="result") or soup.find_all("li"):
            link = item.find("a")
            if not link:
                continue

            href = link.get("href", "")
            text = link.get_text(strip=True)

            # Extract docket number from text or attributes
            # Format varies: "AB.2023.123" or similar
            stub = {
                "docket_number": text.split(" - ")[0].strip() if " - " in text else text,
                "url": f"{PDF_BASE}{href}" if not href.startswith("http") else href,
                "listing_text": text,
            }
            stubs.append(stub)

        return stubs

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch a single ZH Obergericht decision."""
        try:
            docket = stub["docket_number"]

            # For ZH Obergericht, the listing may already contain sufficient metadata
            # Or we may need to fetch the detail/PDF page
            full_text = stub.get("listing_text", "")
            lang = detect_language(full_text) if full_text else "de"

            decision = Decision(
                decision_id=make_decision_id("zh_obergericht", docket),
                court="zh_obergericht",
                canton="ZH",
                docket_number=docket,
                decision_date=parse_date(stub.get("decision_date", "")),
                language=lang,
                full_text=self.clean_text(full_text) if full_text else "(detail fetch needed)",
                source_url=stub.get("url", RESULT_PAGE_URL),
                cited_decisions=extract_citations(full_text) if full_text else [],
                scraped_at=datetime.now(timezone.utc),
            )
            return decision

        except Exception as e:
            logger.error(f"Failed to fetch ZH OG {stub.get('docket_number', '?')}: {e}", exc_info=True)
            return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape ZH Obergericht decisions")
    parser.add_argument("--since", type=str, help="Start date DD.MM.YYYY")
    parser.add_argument("--max", type=int, default=10)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    since = parse_date(args.since) if args.since else None
    scraper = ZHObergerichtScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    print(f"Scraped {len(decisions)} ZH Obergericht decisions")
