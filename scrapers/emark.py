"""
EMARK Scraper (Asylrekurskommission / ARK)
==========================================

Scrapes published EMARK decisions from the Swiss Asylum Appeals Commission
archive at ark-cra.rekurskommissionen.ch.

The ARK (Schweizerische Asylrekurskommission) was replaced by the
Bundesverwaltungsgericht (BVGer) on January 1, 2007.

Architecture:
- Static HTML archive organized by year (1993-2006)
- Individual decisions at /assets/resources/ark/emark/{year}/{nr}.htm
- Decision numbers are sequential per year (1-42 max)
- Text is in German, French, or Italian with trilingual summaries
- Keyword indices at stichw-98.htm (1993-1998) and stichw_ab99.htm (1999-2006)

Coverage: ~430 published decisions (Grundsatzentscheide + selected decisions)
Rate limiting: 1.0 seconds (static archive, no server load concern)
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

BASE_URL = "https://ark-cra.rekurskommissionen.ch/assets/resources/ark/emark"

# Year range and max decision numbers per year (empirically determined)
YEAR_RANGES = {
    1993: 39, 1994: 29, 1995: 25, 1996: 42, 1997: 27, 1998: 34,
    1999: 29, 2000: 30, 2001: 27, 2002: 23, 2003: 30, 2004: 40,
    2005: 25, 2006: 33,
}

# Date pattern in EMARK decisions: "24. Januar 2006" or "24 janvier 2006"
DATE_PATTERN_DE = re.compile(
    r"(\d{1,2})\.?\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|"
    r"September|Oktober|November|Dezember)\s+(\d{4})",
    re.IGNORECASE,
)
DATE_PATTERN_FR = re.compile(
    r"(\d{1,2})\.?\s*(?:er)?\s*(janvier|février|mars|avril|mai|juin|juillet|"
    r"août|septembre|octobre|novembre|décembre)\s+(\d{4})",
    re.IGNORECASE,
)
DATE_PATTERN_IT = re.compile(
    r"(\d{1,2})\.?\s*(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|"
    r"agosto|settembre|ottobre|novembre|dicembre)\s+(\d{4})",
    re.IGNORECASE,
)


class EMARKScraper(BaseScraper):
    """Scraper for EMARK (Swiss Asylum Appeals Commission) published decisions."""

    REQUEST_DELAY = 1.0
    TIMEOUT = 30
    MAX_ERRORS = 50

    @property
    def court_code(self) -> str:
        return "emark"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Enumerate all year/number combinations, newest first."""
        found = 0
        for year in sorted(YEAR_RANGES.keys(), reverse=True):
            if since_date and year < since_date.year:
                continue

            max_nr = YEAR_RANGES[year]
            for nr in range(max_nr, 0, -1):
                docket = f"EMARK-{year}-{nr}"
                decision_id = make_decision_id("emark", docket)
                if self.state.is_known(decision_id):
                    continue

                url = f"{BASE_URL}/{year}/{nr:02d}.htm"
                found += 1
                yield {
                    "docket_number": docket,
                    "year": year,
                    "nr": nr,
                    "url": url,
                }

        logger.info(f"[emark] Found {found} new decisions to fetch")

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download and parse an EMARK decision HTML page."""
        url = stub["url"]
        docket = stub["docket_number"]

        try:
            response = self.get(url)
        except Exception as e:
            if hasattr(e, "response") and getattr(e.response, "status_code", 0) == 404:
                logger.debug(f"[emark] {docket}: 404 (not published)")
                return None
            logger.warning(f"[emark] Failed to fetch {docket}: {e}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Extract full text
        full_text = soup.get_text(separator="\n", strip=True)
        if not full_text or len(full_text) < 100:
            logger.debug(f"[emark] {docket}: too short ({len(full_text)} chars)")
            return None

        full_text = self.clean_text(full_text)

        # Extract decision date from text
        decision_date = None
        for pattern in [DATE_PATTERN_DE, DATE_PATTERN_FR, DATE_PATTERN_IT]:
            m = pattern.search(full_text[:2000])
            if m:
                date_str = f"{m.group(1)} {m.group(2)} {m.group(3)}"
                decision_date = parse_date(date_str)
                if decision_date:
                    break

        if not decision_date:
            # Fallback: use year from docket
            decision_date = date(stub["year"], 1, 1)

        # Detect language
        lang = detect_language(full_text)

        # Extract title/regeste from first meaningful lines
        lines = [l.strip() for l in full_text.split("\n") if l.strip()]
        title = None
        regeste = None
        for line in lines[:20]:
            if re.match(r"^(Art\.|Asyl|Flüchtling|Wegweisung|Vollzug|Nichteintret)", line):
                title = line[:200]
                break

        # Build regeste from summary paragraphs (typically first few substantive lines)
        regeste_lines = []
        capture = False
        for line in lines:
            if re.match(r"^\d+\.\s", line) and not capture:
                capture = True
            if capture:
                regeste_lines.append(line)
                if len(regeste_lines) >= 5:
                    break
        if regeste_lines:
            regeste = "\n".join(regeste_lines)

        return Decision(
            decision_id=make_decision_id("emark", docket),
            court="emark",
            canton="CH",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=title,
            legal_area="Asylrecht",
            regeste=regeste,
            full_text=full_text,
            source_url=url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape EMARK decisions")
    parser.add_argument("--since", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--max", type=int, default=5, help="Max decisions")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    since = date.fromisoformat(args.since) if args.since else None
    scraper = EMARKScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  {len(d.full_text)} chars  {d.language}")
    print(f"\nScraped {len(decisions)} EMARK decisions")
