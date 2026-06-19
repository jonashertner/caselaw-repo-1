"""
Federal Council (Bundesrat) Complaint Decisions Scraper
========================================================

Scrapes Beschwerdeentscheide des Bundesrates from the Federal Office of
Justice (BJ) at bj.admin.ch. These are administrative complaint decisions
where the Federal Council acts as an appellate authority.

Architecture:
- Listing page at bj.admin.ch with ~20 entries (JS-paginated, all in DOM)
- Each entry links to a detail page with PDF download
- PDFs contain the full decision text

Coverage: ~20 decisions, 2012–present, ~2-4 new per year.
Rate limiting: 2 seconds.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
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

BASE_URL = "https://www.bj.admin.ch"

# bj.admin.ch retired the legacy /bj/de/home/... tree in its 2026 CMS
# migration; the Beschwerdeentscheide now live at this flat path.
LISTING_URL = f"{BASE_URL}/de/beschwerdeentscheide-des-bundesrates"

# Detail pages are flat descriptive slugs, e.g.
#   /de/entscheid-des-bundesrates-vom-13-juni-2025-eda
_DETAIL_RE = re.compile(r"^/de/entscheid-des-bundesrates-vom-", re.IGNORECASE)

# German month names as they appear (transliterated, lowercase) in the slug.
_SLUG_MONTHS = {
    "januar": 1, "februar": 2, "maerz": 3, "marz": 3, "april": 4, "mai": 5,
    "juni": 6, "juli": 7, "august": 8, "september": 9, "oktober": 10,
    "november": 11, "dezember": 12,
}
_SLUG_DATE_RE = re.compile(
    r"vom-(\d{1,2})-([a-z]+)-(\d{4})(?:-(.+))?$", re.IGNORECASE
)


def _parse_bundesrat_slug(slug: str) -> tuple[str | None, str]:
    """Parse a detail slug into (ISO decision date, descriptor).

    ``entscheid-des-bundesrates-vom-13-juni-2025-eda`` -> ("2025-06-13", "eda").
    Returns (None, descriptor-or-"") when the date can't be parsed, so the
    caller can fall back to the raw slug for the docket.
    """
    m = _SLUG_DATE_RE.search(slug)
    if not m:
        return None, ""
    day, month_name, year, descriptor = m.groups()
    month = _SLUG_MONTHS.get(month_name.lower())
    descriptor = descriptor or ""
    if not month:
        return None, descriptor
    return f"{int(year):04d}-{month:02d}-{int(day):02d}", descriptor

# Date pattern in titles: "Entscheid des Bundesrates vom DD. Monat YYYY"
DATE_PATTERN = re.compile(
    r"vom\s+(\d{1,2})\.?\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|"
    r"September|Oktober|November|Dezember)\s+(\d{4})",
    re.IGNORECASE,
)

DATE_FR = re.compile(
    r"du\s+(\d{1,2})\.?\s*(?:er)?\s*(janvier|février|mars|avril|mai|juin|"
    r"juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})",
    re.IGNORECASE,
)


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


class CHBundesratScraper(BaseScraper):
    """Scraper for Federal Council complaint decisions."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 60

    @property
    def court_code(self) -> str:
        return "ch_bundesrat"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover decisions from the BJ listing page."""
        try:
            response = self.get(LISTING_URL)
        except Exception as e:
            # A 404 / transport error here means the portal moved again (as in
            # the 2026 CMS migration that broke the old URL). Fail loudly rather
            # than silently reporting zero new decisions.
            logger.error(f"[ch_bundesrat] Listing fetch failed: {e}")
            raise

        soup = BeautifulSoup(response.text, "html.parser")

        # Detail links are flat slugs: /de/entscheid-des-bundesrates-vom-...
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not _DETAIL_RE.match(href):
                continue

            full_url = urljoin(BASE_URL, href)
            if full_url in seen:
                continue
            seen.add(full_url)

            # Title from the link text, falling back to the enclosing block.
            title = a.get_text(strip=True)
            if not title or len(title) < 5:
                parent = a.find_parent(["h3", "h4", "li", "div"])
                if parent:
                    title = parent.get_text(strip=True)

            # The real Entscheid date + a disambiguating descriptor live in the
            # slug. Docket = "YYYY-MM-DD[-descriptor]" so same-day decisions
            # (e.g. three 2025-06-13 entries) stay distinct; fall back to the
            # raw slug if the date can't be parsed.
            slug = href.rstrip("/").split("/")[-1]
            date_str, descriptor = _parse_bundesrat_slug(slug)
            if date_str:
                docket = f"{date_str}-{descriptor}" if descriptor else date_str
            else:
                docket = slug

            decision_id = make_decision_id("ch_bundesrat", docket)
            if self.state.is_known(decision_id):
                continue

            if since_date and date_str:
                parsed = parse_date(date_str)
                if parsed and parsed < since_date:
                    continue

            yield {
                "docket_number": docket,
                "decision_date": date_str or "",
                "detail_url": full_url,
                "title": title,
            }

        logger.info(f"[ch_bundesrat] Found {len(seen)} entries on listing page")

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch detail page, find PDF link, download and extract text."""
        detail_url = stub["detail_url"]
        docket = stub["docket_number"]

        try:
            response = self.get(detail_url)
        except Exception as e:
            logger.error(f"[ch_bundesrat] Failed to fetch detail page {docket}: {e}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Find PDF link on detail page
        pdf_url = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.endswith(".pdf"):
                pdf_url = urljoin(BASE_URL, href)
                break

        if not pdf_url:
            logger.warning(f"[ch_bundesrat] No PDF found on {detail_url}")
            return None

        # Extract keywords/Stichwörter if present
        keywords = None
        for el in soup.find_all(["p", "div", "span"]):
            text = el.get_text(strip=True)
            if text.startswith("Stichwörter") or text.startswith("Mots-clés"):
                keywords = text.split(":", 1)[-1].strip() if ":" in text else None
                break

        # Download PDF
        try:
            pdf_response = self.get(pdf_url)
        except Exception as e:
            logger.error(f"[ch_bundesrat] Failed to download PDF for {docket}: {e}")
            return None

        full_text = _extract_pdf_text(pdf_response.content)
        if not full_text or len(full_text.strip()) < 50:
            logger.warning(
                f"[ch_bundesrat] No text extracted from {docket} "
                f"({len(pdf_response.content)} bytes PDF)"
            )
            return None

        full_text = self.clean_text(full_text)
        lang = detect_language(full_text)
        decision_date = parse_date(stub.get("decision_date", ""))

        # Try to extract date from PDF text if not found in title
        if not decision_date:
            for pattern in (DATE_PATTERN, DATE_FR):
                m = pattern.search(full_text[:2000])
                if m:
                    decision_date = parse_date(f"{m.group(1)}. {m.group(2)} {m.group(3)}")
                    break

        return Decision(
            decision_id=make_decision_id("ch_bundesrat", docket),
            court="ch_bundesrat",
            canton="CH",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=stub.get("title"),
            legal_area=keywords,
            decision_type="Beschwerdeentscheid",
            full_text=full_text,
            source_url=detail_url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )
