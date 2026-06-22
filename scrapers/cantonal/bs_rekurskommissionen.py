"""
Basel-Stadt Recourse Commissions Scraper (Steuerrekurs- + Personalrekurskommission)
===================================================================================
Scrapes the Basel-Stadt Steuerrekurskommission (STRK) and Personalrekurskommission
(PRK) decisions from the unified www.bs.ch portal (Nuxt SSR; PDFs on media.bs.ch).
These specialised commissions publish on their own pages and are NOT aggregated by
entscheidsuche (which has only bs_appellationsgericht / bs_sozialversicherungsgericht
for Basel-Stadt) — verified beyond-es gaps.

One scraper, two distinct corpus courts (build_fts5 indexes by each row's `court`):
- bs_steuerrekurskommission  — STRK.YYYY.N  (tax appeals)
- bs_personalrekurskommission — PRK-N / RRB-N / YYYY-NN  (personnel-law recourse;
  the page also carries the Regierungsrat appeal-instance decisions, flagged via chamber)

Static index → direct PDF (esbk.py pattern). The STRK listing carries no date (parsed
from the PDF); the PRK listing carries the date in the link text. robots Crawl-delay:10.
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

SOURCES = [
    {
        "court": "bs_steuerrekurskommission",
        "url": "https://www.bs.ch/organisation/rechtsprechung-der-steuerrekurskommission",
        "docket_re": re.compile(r"(STRK\.\d{4}\.\d+)", re.I),
        "legal_area": "Steuerrecht",
    },
    {
        "court": "bs_personalrekurskommission",
        "url": "https://www.bs.ch/entscheide-der-personalrekurskommission",
        "docket_re": re.compile(r"((?:PRK|RRB)[-.]?\d+|\d{4}-\d{1,3})", re.I),
        "legal_area": "Personalrecht",
    },
]

DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
BOILERPLATE_RE = re.compile(r"\s*(Externer Link|, wird in einem neuen Fenster geöffnet).*", re.S)
GERMAN_MONTHS = "Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember"


def _parse_head_date(text: str) -> str | None:
    """Decision date from the PDF header — 'Entscheid vom 20. Januar 2022' (German month) or
    DD.MM.YYYY. Anchored on 'Entscheid/Urteil/Beschluss vom' so it doesn't grab the case dates
    scattered through the body."""
    head = text[:2500]
    for anchor in (r"(?:Entscheid|Urteil|Beschluss)\s+vom\s+", r"\bvom\s+"):
        m = re.search(anchor + r"(\d{1,2})\.?\s*(" + GERMAN_MONTHS + r")\s+(\d{4})", head)
        if m:
            return f"{m.group(1)}. {m.group(2)} {m.group(3)}"
        m = re.search(anchor + r"(\d{1,2})\.(\d{1,2})\.(\d{4})", head)
        if m:
            return f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
    return None


class BSRekurskommissionenScraper(BaseScraper):
    """Scraper for the Basel-Stadt recourse commissions (STRK + PRK)."""

    REQUEST_DELAY = 10.0  # robots Crawl-delay:10
    TIMEOUT = 120

    @property
    def court_code(self) -> str:
        return "bs_rekurskommissionen"  # storage/state key; rows carry the per-commission court

    def discover_new(self, since_date=None) -> Iterator[dict]:
        seen_urls: set[str] = set()
        found = 0
        for src in SOURCES:
            try:
                resp = self.get(src["url"])
            except Exception as e:
                logger.error(f"[bs_rekurs] {src['court']} listing failed: {e}")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "media.bs.ch" not in href or ".pdf" not in href.lower():
                    continue
                text = a.get_text(" ", strip=True)
                slug = href.split("/")[-1]
                m = src["docket_re"].search(text) or src["docket_re"].search(slug)
                if not m:
                    continue
                docket = m.group(1).upper().replace("RRB-", "RRB-").replace("PRK-", "PRK-")

                pdf_url = urljoin(src["url"], href)
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                dm = DATE_RE.search(text)
                decision_date_str = f"{dm.group(1)}.{dm.group(2)}.{dm.group(3)}" if dm else None
                title = BOILERPLATE_RE.sub("", text).strip().rstrip(",") or None
                chamber = "Regierungsrat" if docket.startswith("RRB") else None

                decision_id = make_decision_id(src["court"], docket)
                if self.state.is_known(decision_id):
                    continue
                if since_date and decision_date_str:
                    parsed = parse_date(decision_date_str)
                    if parsed and parsed < since_date:
                        continue

                found += 1
                yield {
                    "court": src["court"],
                    "docket_number": docket,
                    "decision_date": decision_date_str or "",
                    "pdf_url": pdf_url,
                    "title": title,
                    "legal_area": src["legal_area"],
                    "chamber": chamber,
                }
        logger.info(f"[bs_rekurs] Found {found} new decisions ({len(seen_urls)} PDFs)")

    def fetch_decision(self, stub: dict) -> Decision | None:
        pdf_url = stub["pdf_url"]
        court = stub["court"]
        docket = stub["docket_number"]
        try:
            resp = self.get(pdf_url)
        except Exception as e:
            logger.error(f"[bs_rekurs] Failed to download PDF for {docket}: {e}")
            return None
        full_text = _extract_pdf_text(resp.content)
        if not full_text or len(full_text.strip()) < 50:
            logger.warning(f"[bs_rekurs] No text extracted from {docket}")
            return None
        full_text = self.clean_text(full_text)

        decision_date_str = stub.get("decision_date") or _parse_head_date(full_text)

        return Decision(
            decision_id=make_decision_id(court, docket),
            court=court,
            canton="BS",
            chamber=stub.get("chamber"),
            docket_number=docket,
            decision_date=parse_date(decision_date_str) if decision_date_str else None,
            language=detect_language(full_text),
            title=stub.get("title"),
            legal_area=stub.get("legal_area"),
            decision_type="Entscheid",
            full_text=full_text,
            source_url=pdf_url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape BS recourse-commission decisions")
    parser.add_argument("--since", type=str)
    parser.add_argument("--max", type=int, default=5)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    for noisy in ("pdfminer", "pdfplumber", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    since = date.fromisoformat(args.since) if args.since else None
    scraper = BSRekurskommissionenScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.court}/{d.docket_number}  {d.decision_date}  {len(d.full_text)} chars  {(d.title or '')[:42]}")
    print(f"\nScraped {len(decisions)} BS recourse-commission decisions")
