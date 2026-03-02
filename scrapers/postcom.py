"""
PostCom Scraper (Eidgenössische Postkommission)
=================================================

Scrapes published Verfügungen from the Swiss Federal Postal Commission
(PostCom) at postcom.admin.ch.

Architecture:
- Static HTML page, no JavaScript framework
- Single listing page with all Verfügungen from 2013 to present
- Year sections marked with <strong> tags (2025, 2024, 2023)
- Each entry is a <p> tag containing PDF link(s) and status text
- PDF links under /inhalte/PDF/Verfuegungen/ path
- Inconsistent HTML formatting across years (improved since 2024)

Entry formats (varying by era):
  New (2024+): "DD.MM.YYYY_Verfügung NN_YYYY_betreffend_Subject (status)"
  Older (pre-2024): "DD.MM.YYYY - Verfügung NN/YYYY betreffend Subject - status"
  French: "DD.MM.YYYY - Décision NN/YYYY concernant Subject - status"
  Italian: "DD.MM.YYYY - Decisione NN/YYYY ... - status"

HTML quirks:
- Some <p> tags have multiple <a> links where a second link wrapping just
  "(" is an editing error (always ignore links with text length <= 3)
- "Beilage" (appendix) entries are separate PDFs — scraped as standalone entries
- Status is outside the <a> tag: "(rechtskräftig)" or "nicht rechtskräftig"
- Some titles use underscores instead of spaces

Coverage: ~224 Verfügungen (2013-2025)
Rate limiting: 2.0 seconds (PDF downloads)
"""
from __future__ import annotations

import io
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

LISTING_URL = "https://www.postcom.admin.ch/de/dokumentation/verfuegungen"
BASE_URL = "https://www.postcom.admin.ch"

# Extract Verfügung/Décision/Decisione number and year
VFG_NUMBER_PATTERN = re.compile(
    r"(?:Verfügung|Décision|Decisione)\s*[_\s]*"
    r"(?:Nr?\.?\s*|n[°o]?\s*)?"
    r"(\d+)\s*[/_-]\s*(\d{4})",
    re.IGNORECASE,
)

# Leading date: "DD.MM.YYYY" at start of paragraph or link text
LEADING_DATE = re.compile(r"^(\d{1,2}\.\d{2}\.\d{4})")

# Status patterns
STATUS_PATTERN = re.compile(
    r"(rechtskräftig|nicht\s+rechtskräftig|noch\s+nicht\s+rechtskräftig"
    r"|nicht\s+rechtkräftig)",
    re.IGNORECASE,
)


def _slugify(text: str) -> str:
    """Create a filesystem-safe slug from text."""
    text = text.lower().strip()
    text = re.sub(r"[äÄ]", "ae", text)
    text = re.sub(r"[öÖ]", "oe", text)
    text = re.sub(r"[üÜ]", "ue", text)
    text = re.sub(r"[éèê]", "e", text)
    text = re.sub(r"[àâ]", "a", text)
    text = re.sub(r"[ùû]", "u", text)
    text = re.sub(r"[ôò]", "o", text)
    text = re.sub(r"[îì]", "i", text)
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

        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n\n".join(p.extract_text() or "" for p in pdf.pages)
    except ImportError:
        pass
    return ""


def _clean_title(raw: str) -> str:
    """Clean a raw PostCom title string.

    - Replace underscores with spaces
    - Remove leading date
    - Remove trailing status info
    - Normalize whitespace
    """
    # Replace underscores used as separators
    text = raw.replace("_", " ")
    # Remove leading date
    text = re.sub(r"^\d{1,2}\.\d{2}\.\d{4}\s*[-–]\s*", "", text)
    text = re.sub(r"^\d{1,2}\.\d{2}\.\d{4}\s*", "", text)
    # Remove trailing status
    text = re.sub(
        r"\s*[-–]\s*(rechtskräftig|nicht\s+rechtskräftig|noch\s+nicht\s+rechtskräftig)\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    )
    # Remove trailing parenthesized status
    text = re.sub(
        r"\s*\(\s*(rechtskräftig|nicht\s+rechtskräftig|noch\s+nicht\s+rechtskräftig)\s*\)\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    )
    # Remove language notes like "(en langue française)"
    text = re.sub(r"\s*\((?:en|in)\s+(?:langue\s+)?(?:français|französisch|italienisch|italiana)e?\)\s*", "", text, flags=re.IGNORECASE)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


class PostComScraper(BaseScraper):
    """Scraper for PostCom (Swiss Federal Postal Commission) Verfügungen."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 60

    @property
    def court_code(self) -> str:
        return "postcom"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover PostCom Verfügungen from the listing page.

        Iterates over all <p> tags containing PDF links. Each paragraph
        represents one Verfügung entry. Uses the first meaningful link
        (text length > 3) as the primary PDF, ignoring HTML editing errors.
        """
        response = self.get(LISTING_URL)
        soup = BeautifulSoup(response.text, "html.parser")

        seen_hrefs = set()
        found = 0

        p_tags = soup.find_all("p")

        for p in p_tags:
            links = p.find_all("a", href=lambda h: h and ".pdf" in h)
            if not links:
                continue

            # Get the first meaningful link (skip broken ones with text="(" etc.)
            main_link = None
            for link in links:
                link_text = link.get_text(strip=True)
                if len(link_text) > 3:
                    main_link = link
                    break

            if not main_link:
                # All links are broken/short, skip this paragraph entirely
                continue

            href = main_link["href"]

            # Deduplicate by PDF path
            if href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            pdf_url = href if href.startswith("http") else urljoin(BASE_URL, href)

            # Use the full paragraph text to extract date, title, and status
            p_text = p.get_text(" ", strip=True)
            link_text = main_link.get_text(strip=True)

            # Extract leading date from paragraph or link text
            decision_date_str = None
            date_m = LEADING_DATE.match(p_text)
            if date_m:
                decision_date_str = date_m.group(1)
            else:
                date_m = LEADING_DATE.match(link_text)
                if date_m:
                    decision_date_str = date_m.group(1)

            # Extract Verfügung/Décision number
            vfg_m = VFG_NUMBER_PATTERN.search(p_text)
            if vfg_m:
                vfg_number = vfg_m.group(1)
                vfg_year = vfg_m.group(2)
                docket = f"VFG-{vfg_number}-{vfg_year}"
                # Disambiguate Beilage/Liste appendices from main Verfügung
                lower_text = p_text.lower()
                if "beilage" in lower_text:
                    docket += "-beilage"
                elif "liste" in lower_text and "dienstleistung" in lower_text:
                    docket += "-liste"
            else:
                # No standard number: use slug of cleaned title
                clean = _clean_title(link_text or p_text)
                slug = _slugify(clean)
                date_suffix = ""
                if decision_date_str:
                    parsed = parse_date(decision_date_str)
                    if parsed:
                        date_suffix = f"-{parsed.isoformat()}"
                docket = (slug or "unknown") + date_suffix

            # Clean title
            raw_title = link_text if len(link_text) > len(p_text) * 0.3 else p_text
            title = _clean_title(raw_title)

            # Extract status from paragraph text
            status = None
            status_m = STATUS_PATTERN.search(p_text)
            if status_m:
                status = status_m.group(0).strip()

            decision_id = make_decision_id("postcom", docket)
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
                "decision_date": decision_date_str or "",
                "pdf_url": pdf_url,
                "title": title,
                "status": status,
            }

        logger.info(
            f"[postcom] Found {found} new Verfügungen "
            f"({len(seen_hrefs)} unique PDFs on page)"
        )

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download PDF and extract decision text."""
        pdf_url = stub["pdf_url"]
        docket = stub["docket_number"]

        try:
            response = self.get(pdf_url)
        except Exception as e:
            logger.error(f"[postcom] Failed to download PDF for {docket}: {e}")
            return None

        full_text = _extract_pdf_text(response.content)
        if not full_text or len(full_text.strip()) < 50:
            logger.warning(
                f"[postcom] No text extracted from {docket} "
                f"({len(response.content)} bytes PDF)"
            )
            return None

        full_text = self.clean_text(full_text)
        lang = detect_language(full_text)
        decision_date = parse_date(stub.get("decision_date", ""))

        return Decision(
            decision_id=make_decision_id("postcom", docket),
            court="postcom",
            canton="CH",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=stub.get("title"),
            legal_area="Postrecht",
            decision_type="Verfügung",
            full_text=full_text,
            source_url=pdf_url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape PostCom Verfügungen")
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
    scraper = PostComScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  {len(d.full_text)} chars  {d.title[:60]}")
    print(f"\nScraped {len(decisions)} PostCom Verfügungen")
