"""
Bern Directorate Beschwerdeentscheide Scraper (BE Direktionen)
==============================================================
Scrapes the administrative-appeal decisions (Beschwerdeentscheide) of Bernese
cantonal directorates that publish their own decision tables — the executive
administrative-recourse layer entscheidsuche does not aggregate.

Covered directorates (chamber = directorate abbreviation):
- GSI — Gesundheits-, Sozial- und Integrationsdirektion
- KAIO — Beschaffungswesen / öffentliche Vergaben (public-procurement appeals)

Each portal is a single static HTML table: Datum | Nummer (docket, links the PDF) |
Gegenstand (subject). Pattern = static index → direct PDF (like scrapers/esbk.py).
Our only pre-existing BE directorate is be_bvd (Bau- und Verkehrsdirektion); these
are net-new. robots Crawl-delay:10 → REQUEST_DELAY 10s.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import Decision, detect_language, extract_citations, make_decision_id, parse_date
from scrapers.elcom import _extract_pdf_text

logger = logging.getLogger(__name__)

# (directorate abbreviation, listing URL)
LISTINGS = [
    ("GSI", "https://www.gsi.be.ch/de/start/ueber-uns/generalsekretariat/rechtsabteilung/rechtsprechung.html"),
    ("KAIO", "https://www.kaio.fin.be.ch/de/start/themen/oeffentliches-beschaffungswesen/rechtliches/beschwerdeentscheide.html"),
]

DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
# Directorate docket: 2025.GSI.2700, 2026.KAIO.12, …  (year.DIR.number)
DOCKET_RE = re.compile(r"\b(\d{4}\.[A-Za-zÄÖÜ]{2,6}\.\d+)\b")


class BEDirektionenScraper(BaseScraper):
    """Scraper for Bernese cantonal directorate Beschwerdeentscheide (GSI, KAIO)."""

    REQUEST_DELAY = 10.0  # robots Crawl-delay:10
    TIMEOUT = 120

    @property
    def court_code(self) -> str:
        return "be_direktionen"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        seen_urls: set[str] = set()
        found = 0
        for directorate, listing_url in LISTINGS:
            try:
                resp = self.get(listing_url)
            except Exception as e:
                logger.error(f"[be_direktionen] {directorate} listing failed: {e}")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if ".pdf" not in href.lower():
                    continue
                link_text = a.get_text(" ", strip=True)
                m = DOCKET_RE.search(link_text) or DOCKET_RE.search(href)
                if not m:
                    continue  # only docket-numbered decision PDFs (skips forms etc.)
                docket = m.group(1)

                pdf_url = urljoin(listing_url, href)
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                decision_date_str, title = None, None
                row = a.find_parent("tr")
                if row:
                    cells = row.find_all(["th", "td"])
                    for c in cells:
                        dm = DATE_RE.search(c.get_text(strip=True))
                        if dm:
                            decision_date_str = f"{dm.group(1)}.{dm.group(2)}.{dm.group(3)}"
                            break
                    if len(cells) >= 3:
                        gegenstand = cells[-1].get_text(" ", strip=True)
                        if gegenstand and not DATE_RE.search(gegenstand) and docket not in gegenstand:
                            title = gegenstand

                decision_id = make_decision_id("be_direktionen", docket)
                if self.state.is_known(decision_id):
                    continue
                if since_date and decision_date_str:
                    parsed = parse_date(decision_date_str)
                    if parsed and parsed < since_date:
                        continue

                # The true issuing directorate is the docket prefix (YYYY.DIR.N), which can
                # differ from the listing page (e.g. a BKD decision listed on the KAIO portal).
                parts = docket.split(".")
                chamber = parts[1] if len(parts) >= 3 and parts[1].isalpha() else directorate

                found += 1
                yield {
                    "docket_number": docket,
                    "decision_date": decision_date_str or "",
                    "pdf_url": pdf_url,
                    "title": title,
                    "chamber": chamber,
                }
        logger.info(f"[be_direktionen] Found {found} new decisions ({len(seen_urls)} PDFs)")

    def fetch_decision(self, stub: dict) -> Decision | None:
        pdf_url = stub["pdf_url"]
        docket = stub["docket_number"]
        try:
            resp = self.get(pdf_url)
        except Exception as e:
            logger.error(f"[be_direktionen] Failed to download PDF for {docket}: {e}")
            return None
        full_text = _extract_pdf_text(resp.content)
        if not full_text or len(full_text.strip()) < 50:
            logger.warning(f"[be_direktionen] No text extracted from {docket}")
            return None
        full_text = self.clean_text(full_text)
        return Decision(
            decision_id=make_decision_id("be_direktionen", docket),
            court="be_direktionen",
            canton="BE",
            chamber=stub.get("chamber"),
            docket_number=docket,
            decision_date=parse_date(stub.get("decision_date", "")),
            language=detect_language(full_text),
            title=stub.get("title"),
            decision_type="Beschwerdeentscheid",
            full_text=full_text,
            source_url=pdf_url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape BE directorate Beschwerdeentscheide")
    parser.add_argument("--since", type=str)
    parser.add_argument("--max", type=int, default=5)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    for noisy in ("pdfminer", "pdfplumber", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    since = date.fromisoformat(args.since) if args.since else None
    scraper = BEDirektionenScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  [{d.chamber}]  {len(d.full_text)} chars  {(d.title or '')[:45]}")
    print(f"\nScraped {len(decisions)} BE directorate decisions")
