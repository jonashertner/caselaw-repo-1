"""
WEKO Scraper (Wettbewerbskommission)
======================================

Scrapes published decisions from the Swiss Competition Commission (WEKO/COMCO)
at weko.admin.ch.

Architecture:
- Single static HTML listing page with ~84 PDF links
- All decisions are PDF-only (some 20+ MB)
- Adobe AEM CMS, no API
- PDF links under /dam/weko/ path

Coverage: ~84 decisions
Rate limiting: 2.0 seconds (large PDFs)
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Iterator
from urllib.parse import urljoin

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

LISTING_URL = "https://www.weko.admin.ch/weko/de/home/praxis/publizierte-entscheide.html"
BASE_URL = "https://www.weko.admin.ch"

# Parse title: "Name: Type vom DD. Monat YYYY (PDF, size, DD.MM.YYYY)"
TITLE_PATTERN = re.compile(
    r"^(.+?):\s*(Verfügung|Schlussbericht|Stellungnahme|Beratung|Gutachten|Empfehlung|Sanktionsverfügung|Einstellungsverfügung|Genehmigung|Prüfung|Abklärung|Untersuchung|Vorsorgliche Massnahme|Zwischenverfügung)\s+vom\s+(.+?)\s*\(",
    re.IGNORECASE,
)

# Fallback: extract date from "(PDF, size, DD.MM.YYYY)" suffix
PUB_DATE_PATTERN = re.compile(r"\(PDF,\s*[\d.,]+\s*[kKmMgG][bB],?\s*(\d{2}\.\d{2}\.\d{4})\)")

# Date from "vom DD. Monat YYYY" in title (German)
VOM_DATE_PATTERN = re.compile(
    r"vom\s+(\d{1,2})\.?\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+(\d{4})",
    re.IGNORECASE,
)

# Date from "du DD mois YYYY" in title (French)
DU_DATE_PATTERN = re.compile(
    r"du\s+(\d{1,2})\.?\s*(?:er)?\s*(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})",
    re.IGNORECASE,
)


def _slugify(text: str) -> str:
    """Create a filesystem-safe slug from text."""
    text = text.lower().strip()
    text = re.sub(r"[äÄ]", "ae", text)
    text = re.sub(r"[öÖ]", "oe", text)
    text = re.sub(r"[üÜ]", "ue", text)
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return text[:80]


def _extract_pdf_text(data: bytes) -> str:
    """Extract text from PDF bytes using fitz (PyMuPDF) with pdfplumber fallback."""
    try:
        import fitz

        doc = fitz.open(stream=data, filetype="pdf")
        return "\n\n".join(p.get_text() for p in doc)
    except ImportError:
        pass
    try:
        import pdfplumber
        import io

        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n\n".join(p.extract_text() or "" for p in pdf.pages)
    except ImportError:
        pass
    return ""


class WEKOScraper(BaseScraper):
    """Scraper for WEKO (Swiss Competition Commission) published decisions."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 120  # Large PDFs

    @property
    def court_code(self) -> str:
        return "weko"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover WEKO decisions from the listing page."""
        response = self.get(LISTING_URL)
        soup = BeautifulSoup(response.text, "html.parser")

        links = soup.find_all("a", href=True)
        found = 0

        for a in links:
            href = a["href"]
            # Only PDF links under /dam/weko/
            if "/dam/weko/" not in href or not href.endswith(".pdf"):
                continue

            pdf_url = urljoin(BASE_URL, href)
            link_text = a.get_text(strip=True)
            if not link_text:
                continue

            # Clean title: strip "(PDF, size, DD.MM.YYYY)" suffix
            title = re.sub(r"\s*\(PDF,\s*[\d.,]+\s*[kKmMgG][bB],?\s*\d{2}\.\d{2}\.\d{4}\)\s*$", "", link_text).strip()
            decision_date_str = None
            doc_type = None

            m = TITLE_PATTERN.match(link_text)
            if m:
                case_name = m.group(1).strip()
                doc_type = m.group(2).strip()
                date_part = m.group(3).strip()
                decision_date_str = date_part
            else:
                case_name = link_text.split("(")[0].strip()

            # Try to extract date from "vom DD. Monat YYYY" (DE) or "du DD mois YYYY" (FR)
            vom_m = VOM_DATE_PATTERN.search(link_text)
            if vom_m:
                decision_date_str = f"{vom_m.group(1)}. {vom_m.group(2)} {vom_m.group(3)}"
            else:
                du_m = DU_DATE_PATTERN.search(link_text)
                if du_m:
                    decision_date_str = f"{du_m.group(1)} {du_m.group(2)} {du_m.group(3)}"

            # Fallback: publication date from "(PDF, size, DD.MM.YYYY)"
            pub_m = PUB_DATE_PATTERN.search(link_text)
            pub_date_str = pub_m.group(1) if pub_m else None

            # Build docket from case name + date
            date_suffix = ""
            if decision_date_str:
                parsed = parse_date(decision_date_str)
                if parsed:
                    date_suffix = f"-{parsed.isoformat()}"
            docket = _slugify(case_name) + date_suffix

            decision_id = make_decision_id("weko", docket)
            if self.state.is_known(decision_id):
                continue

            # Filter by since_date
            if since_date and decision_date_str:
                parsed = parse_date(decision_date_str)
                if parsed and parsed < since_date:
                    continue

            found += 1
            yield {
                "docket_number": docket,
                "decision_date": decision_date_str or pub_date_str or "",
                "pdf_url": pdf_url,
                "title": title,
                "case_name": case_name,
                "doc_type": doc_type,
                "pub_date": pub_date_str,
            }

        logger.info(f"[weko] Found {found} new decisions on listing page")

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download PDF and extract decision text."""
        pdf_url = stub["pdf_url"]
        docket = stub["docket_number"]

        try:
            response = self.get(pdf_url)
        except Exception as e:
            logger.error(f"[weko] Failed to download PDF for {docket}: {e}")
            return None

        full_text = _extract_pdf_text(response.content)
        if not full_text or len(full_text.strip()) < 50:
            logger.warning(
                f"[weko] No text extracted from {docket} "
                f"({len(response.content)} bytes PDF)"
            )
            return None

        full_text = self.clean_text(full_text)
        lang = detect_language(full_text)
        decision_date = parse_date(stub.get("decision_date", ""))

        return Decision(
            decision_id=make_decision_id("weko", docket),
            court="weko",
            canton="CH",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=stub.get("title"),
            legal_area="Wettbewerbsrecht",
            decision_type=stub.get("doc_type"),
            full_text=full_text,
            source_url=pdf_url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape WEKO decisions")
    parser.add_argument("--since", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--max", type=int, default=5, help="Max decisions")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    for noisy in ("pdfminer", "pdfplumber", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    since = date.fromisoformat(args.since) if args.since else None
    scraper = WEKOScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  {len(d.full_text)} chars  {d.title[:60]}")
    print(f"\nScraped {len(decisions)} WEKO decisions")
