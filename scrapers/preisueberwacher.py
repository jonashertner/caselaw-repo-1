"""
Preisüberwacher Scraper (Eidg. Preisüberwacher / Swiss Price Supervisor)
========================================================================
Scrapes the Price Supervisor's own decisions from preisueberwacher.admin.ch
(…/dokumentation/publikationen/{formelle-entscheide,einvernehmliche-regelungen,empfehlungen}.html).

Each entry is a plain <a> to a /dam/pue/… PDF, with the date + description in the link text:
  "20.05.2025 - Verfügung gegen Booking.com (PDF, 2 MB, …)"

IMPORTANT — the "formelle Entscheide" page also lists the *subsequent appellate rulings*
(BGer/BVGer/cantonal) reviewing the Preisüberwacher decision. Those courts are already in
our corpus, so ingesting them here would duplicate AND mislabel them under court=preisueberwacher.
COURT_RULING_RE filters them out; we keep only the Preisüberwacher's own output. Verified
beyond-es gap (not aggregated by entscheidsuche).
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Iterator
from urllib.parse import unquote, urljoin

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import Decision, detect_language, extract_citations, make_decision_id, parse_date
from scrapers.elcom import PUB_DATE_PATTERN, _extract_pdf_text, _slugify

logger = logging.getLogger(__name__)

BASE_URL = "https://www.preisueberwacher.admin.ch"
LISTING_PAGES = [
    ("https://www.preisueberwacher.admin.ch/pue/de/home/dokumentation/publikationen/formelle-entscheide.html", "Verfügung"),
    ("https://www.preisueberwacher.admin.ch/pue/de/home/dokumentation/publikationen/einvernehmliche-regelungen.html", "Einvernehmliche Regelung"),
    ("https://www.preisueberwacher.admin.ch/pue/de/home/dokumentation/publikationen/empfehlungen.html", "Empfehlung"),
]

TEXT_DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
PDF_SIZE_SUFFIX_RE = re.compile(r"\s*\(PDF,[^)]*\)\s*$", re.I)
# Other authorities' decisions listed alongside the Preisüberwacher's own (appellate
# rulings reviewing a PUE decision + Federal Council decisions on regulated prices). They
# belong to their own court, so exclude them here to avoid duplication + mislabeling.
COURT_RULING_RE = re.compile(
    r"gerichtsurteil|gerichtsentscheid|bundesgericht|bundesverwaltungsgericht"
    r"|kantonsgericht|verwaltungsgericht|obergericht|\bdtf\b|\barrêt\b|\burteil\b"
    r"|\bsentenza\b|bezirksrat|tribunal"
    r"|bundesratsbeschluss|bundesratsentscheid|conseil f[ée]d[ée]ral|consiglio federale",
    re.I,
)


class PreisueberwacherScraper(BaseScraper):
    """Scraper for the Swiss Price Supervisor's own decisions/recommendations."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 120

    @property
    def court_code(self) -> str:
        return "preisueberwacher"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        seen_urls: set[str] = set()
        found = 0
        for listing_url, decision_type in LISTING_PAGES:
            try:
                resp = self.get(listing_url)
            except Exception as e:
                logger.error(f"[preisueberwacher] {listing_url} failed: {e}")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if ".pdf" not in href.lower():
                    continue
                text = PDF_SIZE_SUFFIX_RE.sub("", re.sub(r"\s+", " ", a.get_text(" ", strip=True))).strip()
                fn = unquote(href.split("/")[-1])
                # drop appellate rulings (already in corpus under their own court)
                if COURT_RULING_RE.search(text) or COURT_RULING_RE.search(fn):
                    continue

                pdf_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                decision_date_str = None
                tm = TEXT_DATE_RE.search(text)
                if tm:
                    decision_date_str = f"{tm.group(1)}.{tm.group(2)}.{tm.group(3)}"
                else:
                    pm = PUB_DATE_PATTERN.search(fn)
                    if pm:
                        decision_date_str = f"{pm.group(1)}. {pm.group(2)} {pm.group(3)}"

                stem = fn.rsplit(".pdf", 1)[0]
                docket = _slugify(stem) or _slugify(text) or "pue-decision"

                decision_id = make_decision_id("preisueberwacher", docket)
                if self.state.is_known(decision_id):
                    continue
                if since_date and decision_date_str:
                    parsed = parse_date(decision_date_str)
                    if parsed and parsed < since_date:
                        continue

                found += 1
                yield {
                    "docket_number": docket,
                    "decision_date": decision_date_str or "",
                    "pdf_url": pdf_url,
                    "title": text or stem,
                    "decision_type": decision_type,
                }
        logger.info(f"[preisueberwacher] Found {found} new decisions ({len(seen_urls)} PDFs)")

    def fetch_decision(self, stub: dict) -> Decision | None:
        pdf_url = stub["pdf_url"]
        docket = stub["docket_number"]
        try:
            resp = self.get(pdf_url)
        except Exception as e:
            logger.error(f"[preisueberwacher] Failed to download PDF for {docket}: {e}")
            return None
        full_text = _extract_pdf_text(resp.content)
        if not full_text or len(full_text.strip()) < 50:
            logger.warning(f"[preisueberwacher] No text extracted from {docket}")
            return None
        full_text = self.clean_text(full_text)
        return Decision(
            decision_id=make_decision_id("preisueberwacher", docket),
            court="preisueberwacher",
            canton="CH",
            docket_number=docket,
            decision_date=parse_date(stub.get("decision_date", "")),
            language=detect_language(full_text),
            title=stub.get("title"),
            legal_area="Preisüberwachungsrecht",
            decision_type=stub.get("decision_type") or "Verfügung",
            full_text=full_text,
            source_url=pdf_url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Preisüberwacher decisions")
    parser.add_argument("--since", type=str)
    parser.add_argument("--max", type=int, default=5)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    for noisy in ("pdfminer", "pdfplumber", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    since = date.fromisoformat(args.since) if args.since else None
    scraper = PreisueberwacherScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  {len(d.full_text)} chars  {(d.title or '')[:50]}")
    print(f"\nScraped {len(decisions)} Preisüberwacher decisions")
