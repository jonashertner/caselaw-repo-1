"""
EDÖB Scraper (Datenschutzbeauftragter)
========================================

Scrapes published decisions and recommendations from the Swiss Federal Data
Protection and Information Commissioner (EDÖB/PFPDT) at edoeb.admin.ch.

Architecture:
- Nuxt SSR app, no API
- 3 listing pages with direct PDF download links
- All documents are PDFs, no HTML detail pages

Categories:
1. Verfügungen (formal decisions): ~3 documents
2. Schlussberichte/Empfehlungen old DSG: ~57 documents
3. BGÖ Empfehlungen (freedom of information): ~150+ documents

Coverage: ~210 total documents
Rate limiting: 1.5 seconds
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

BASE_URL = "https://www.edoeb.admin.ch"

LISTING_PAGES = [
    {
        "url": f"{BASE_URL}/de/verfuegungen",
        "category": "Verfügung",
        "legal_area": "Datenschutz",
    },
    {
        "url": f"{BASE_URL}/de/schlussberichte-empfehlungen-bis-31082023",
        "category": "Schlussbericht/Empfehlung",
        "legal_area": "Datenschutz",
    },
    {
        "url": f"{BASE_URL}/de/empfehlungen-nach-bgo",
        "category": "BGÖ-Empfehlung",
        "legal_area": "Öffentlichkeitsprinzip",
    },
]

# Date patterns in titles: "Empfehlung vom DD. Monat YYYY" or "Schlussbericht vom DD. Monat YYYY"
VOM_DATE_PATTERN = re.compile(
    r"vom\s+(\d{1,2})\.?\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+(\d{4})",
    re.IGNORECASE,
)

# Also try French date pattern
VOM_DATE_FR = re.compile(
    r"du\s+(\d{1,2})\.?\s*(?:er)?\s*(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})",
    re.IGNORECASE,
)

# DD.MM.YYYY in parentheses or text
DATE_DOTTED = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")


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


def _extract_date_from_text(text: str) -> str | None:
    """Try to extract a date string from title/link text."""
    m = VOM_DATE_PATTERN.search(text)
    if m:
        return f"{m.group(1)}. {m.group(2)} {m.group(3)}"
    m = VOM_DATE_FR.search(text)
    if m:
        return f"{m.group(1)} {m.group(2)} {m.group(3)}"
    m = DATE_DOTTED.search(text)
    if m:
        return m.group(0)
    return None


class EDOEBScraper(BaseScraper):
    """Scraper for EDÖB (Data Protection Commissioner) decisions."""

    REQUEST_DELAY = 1.5
    TIMEOUT = 60

    @property
    def court_code(self) -> str:
        return "edoeb"

    def _scrape_listing(self, page_info: dict, since_date=None) -> Iterator[dict]:
        """Scrape a single listing page for PDF links."""
        url = page_info["url"]
        category = page_info["category"]
        legal_area = page_info["legal_area"]

        response = self.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        seen_urls = set()
        found = 0

        for a in soup.find_all("a", href=True):
            href = a["href"]

            # Only PDF links — two hosting patterns:
            # 1. /dam/de/sd-web/{key}/{filename}.pdf (old CMS)
            # 2. https://backend.edoeb.admin.ch/fileservice/.../{uuid}.pdf (new backend)
            if not href.endswith(".pdf"):
                continue
            # Skip "geschwärzt" (redacted) versions if unredacted exists
            if "geschwaerzt" in href.lower() or "geschwärzt" in href.lower():
                continue

            if href.startswith("http"):
                pdf_url = href
            else:
                pdf_url = urljoin(BASE_URL, href)
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            # Get title from link text, or parent element
            link_text = a.get_text(strip=True)
            if not link_text or len(link_text) < 5:
                # Try parent h4 or surrounding text
                parent = a.find_parent(["h4", "h3", "li", "p"])
                if parent:
                    link_text = parent.get_text(strip=True)

            if not link_text or len(link_text) < 5:
                # Use filename as fallback
                link_text = href.split("/")[-1].replace(".pdf", "").replace("-", " ")

            # Extract date from title text
            date_str = _extract_date_from_text(link_text)

            # Build docket
            slug = _slugify(link_text)
            date_suffix = ""
            if date_str:
                parsed = parse_date(date_str)
                if parsed:
                    date_suffix = f"-{parsed.isoformat()}"
            docket = slug + date_suffix

            decision_id = make_decision_id("edoeb", docket)
            if self.state.is_known(decision_id):
                continue

            # Filter by since_date
            if since_date and date_str:
                parsed = parse_date(date_str)
                if parsed and parsed < since_date:
                    continue

            found += 1
            yield {
                "docket_number": docket,
                "decision_date": date_str or "",
                "pdf_url": pdf_url,
                "title": link_text,
                "category": category,
                "legal_area": legal_area,
            }

        logger.info(f"[edoeb] {category}: found {found} new PDFs from {url}")

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover EDÖB decisions across all listing pages."""
        for page_info in LISTING_PAGES:
            yield from self._scrape_listing(page_info, since_date)

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download PDF and extract decision text."""
        pdf_url = stub["pdf_url"]
        docket = stub["docket_number"]

        try:
            response = self.get(pdf_url)
        except Exception as e:
            logger.error(f"[edoeb] Failed to download PDF for {docket}: {e}")
            return None

        full_text = _extract_pdf_text(response.content)
        if not full_text or len(full_text.strip()) < 50:
            logger.warning(
                f"[edoeb] No text extracted from {docket} "
                f"({len(response.content)} bytes PDF)"
            )
            return None

        full_text = self.clean_text(full_text)
        lang = detect_language(full_text)
        decision_date = parse_date(stub.get("decision_date", ""))

        return Decision(
            decision_id=make_decision_id("edoeb", docket),
            court="edoeb",
            canton="CH",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=stub.get("title"),
            legal_area=stub.get("legal_area"),
            decision_type=stub.get("category"),
            full_text=full_text,
            source_url=pdf_url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape EDÖB decisions")
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
    scraper = EDOEBScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  {len(d.full_text)} chars  {d.title[:60]}")
    print(f"\nScraped {len(decisions)} EDÖB decisions")
