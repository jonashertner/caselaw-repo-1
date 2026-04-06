"""
SAV Kantone Scraper
===================

Scrapes cantonal attorney supervisory decisions (Anwaltsaufsicht) published
by the Swiss Bar Association (SAV/FSA) at sav-fsa.ch/kantone.

The Kantone page is a Liferay AssetPublisher listing with ~40 entries, each
linking to a PDF containing a cantonal disciplinary decision. Pages may also
include intermediate HTML detail pages that embed the PDF link.

Coverage: ~40 decisions, multiple cantons, German/French
Rate limiting: 2.0 seconds (PDF downloads)
"""
from __future__ import annotations

import io
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from urllib.parse import urljoin, urlparse

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

LISTING_URL = "https://www.sav-fsa.ch/de/kantone"
BASE_URL = "https://www.sav-fsa.ch"

# SAV Kantone detail page URL pattern (Liferay AssetPublisher)
DETAIL_URL_RE = re.compile(r"/kantone/-/asset_publisher/")

# Canton name → abbreviation mapping (German + French names)
CANTON_MAP = {
    "aargau": "AG", "argovia": "AG",
    "appenzell ausserrhoden": "AR", "appenzell rhodes-extérieures": "AR",
    "appenzell innerrhoden": "AI", "appenzell rhodes-intérieures": "AI",
    "basel-landschaft": "BL", "bâle-campagne": "BL",
    "basel-stadt": "BS", "bâle-ville": "BS",
    "bern": "BE", "berne": "BE",
    "freiburg": "FR", "fribourg": "FR",
    "genf": "GE", "genève": "GE", "geneva": "GE",
    "glarus": "GL", "glaris": "GL",
    "graubünden": "GR", "grisons": "GR", "grigioni": "GR",
    "jura": "JU",
    "luzern": "LU", "lucerne": "LU",
    "neuenburg": "NE", "neuchâtel": "NE",
    "nidwalden": "NW", "nidwald": "NW",
    "obwalden": "OW", "obwald": "OW",
    "schaffhausen": "SH", "schaffhouse": "SH",
    "schwyz": "SZ",
    "solothurn": "SO", "soleure": "SO",
    "st. gallen": "SG", "saint-gall": "SG", "st gallen": "SG",
    "tessin": "TI", "ticino": "TI",
    "thurgau": "TG", "thurgovie": "TG",
    "uri": "UR",
    "waadt": "VD", "vaud": "VD",
    "wallis": "VS", "valais": "VS",
    "zug": "ZG", "zoug": "ZG",
    "zürich": "ZH", "zurich": "ZH",
}

# Canton abbreviation detection (word-boundary match)
CANTON_ABBREV_RE = re.compile(
    r"\b(AG|AI|AR|BE|BL|BS|FR|GE|GL|GR|JU|LU|NE|NW|OW|SG|SH|SO|SZ|TG|TI|UR|VD|VS|ZG|ZH)\b"
)

# Date patterns in titles / text
DATE_RE = re.compile(
    r"\b(\d{1,2})[.\s](\d{1,2})[.\s](\d{4})\b"        # DD.MM.YYYY or D M YYYY
    r"|(\d{4})-(\d{2})-(\d{2})\b"                        # YYYY-MM-DD
)


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes; pdfplumber first, fitz as fallback."""
    try:
        import pdfplumber

        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
            text = "\n\n".join(pages)
            if text.strip():
                return text
    except Exception:
        pass
    try:
        import fitz

        doc = fitz.open(stream=content, filetype="pdf")
        pages = [page.get_text() for page in doc]
        doc.close()
        return "\n\n".join(pages)
    except Exception:
        pass
    return ""


def _detect_canton(text: str) -> str | None:
    """Try to extract a canton abbreviation from text (title or body)."""
    lower = text.lower()
    # Try full canton names first (longer names take priority)
    for name in sorted(CANTON_MAP, key=len, reverse=True):
        if name in lower:
            return CANTON_MAP[name]
    # Try two-letter abbreviation (uppercase, word boundary)
    m = CANTON_ABBREV_RE.search(text)
    if m:
        return m.group(1)
    return None


def _url_to_slug(url: str) -> str:
    """Derive a stable slug from a URL path."""
    path = urlparse(url).path
    # Take last non-empty path segment
    parts = [p for p in path.split("/") if p]
    slug = parts[-1] if parts else "unknown"
    # Strip common extensions
    slug = re.sub(r"\.(pdf|html?|aspx?)$", "", slug, flags=re.IGNORECASE)
    # Normalize separators
    slug = re.sub(r"[^\w-]", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug[:80] or "unknown"


class SAVKantoneScraper(BaseScraper):
    """Scraper for SAV/FSA cantonal attorney discipline decisions."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 60

    @property
    def court_code(self) -> str:
        return "sav_kantone"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_pdf_url(self, url: str) -> bool:
        path = urlparse(url).path.lower()
        return path.endswith(".pdf") or "/documents/" in path

    def _is_detail_url(self, url: str) -> bool:
        return bool(DETAIL_URL_RE.search(url))

    def _find_pdf_in_detail_page(self, page_url: str) -> str | None:
        """Fetch a Liferay detail page and locate the embedded PDF link."""
        try:
            resp = self.get(page_url)
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup.find_all("a", href=True):
                href = tag["href"]
                full = href if href.startswith("http") else urljoin(BASE_URL, href)
                if self._is_pdf_url(full):
                    return full
        except Exception as e:
            logger.warning(f"[sav_kantone] Could not fetch detail page {page_url}: {e}")
        return None

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover decisions from the SAV Kantone listing page."""
        try:
            resp = self.get(LISTING_URL)
        except Exception as e:
            logger.error(f"[sav_kantone] Failed to fetch listing: {e}")
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        seen_slugs: set[str] = set()
        found = 0

        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            full_url = href if href.startswith("http") else urljoin(BASE_URL, href)

            # Only follow PDF links or Liferay detail pages
            if not (self._is_pdf_url(full_url) or self._is_detail_url(full_url)):
                continue

            # Skip external domains
            parsed = urlparse(full_url)
            if parsed.netloc and "sav-fsa.ch" not in parsed.netloc:
                continue

            slug = _url_to_slug(full_url)
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)

            title = tag.get_text(strip=True) or slug

            decision_id = make_decision_id("sav_kantone", slug)
            if self.state.is_known(decision_id):
                continue

            found += 1
            yield {
                "slug": slug,
                "title": title,
                "url": full_url,
                "is_pdf": self._is_pdf_url(full_url),
            }

        logger.info(
            f"[sav_kantone] Found {found} new entries "
            f"({len(seen_slugs)} unique links on page)"
        )

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download and parse a single SAV Kantone decision."""
        slug = stub["slug"]
        url = stub["url"]

        try:
            # Resolve detail pages to their PDF
            if not stub.get("is_pdf", True):
                pdf_url = self._find_pdf_in_detail_page(url)
                if not pdf_url:
                    logger.warning(
                        f"[sav_kantone] No PDF found in detail page for {slug}"
                    )
                    return None
            else:
                pdf_url = url

            resp = self.get(pdf_url)
            full_text = _extract_pdf_text(resp.content)

            if not full_text or len(full_text.strip()) < 30:
                logger.warning(
                    f"[sav_kantone] No text extracted for {slug} "
                    f"({len(resp.content)} bytes)"
                )
                return None

            full_text = self.clean_text(full_text)
            lang = detect_language(full_text)

            title = stub.get("title") or slug
            canton = _detect_canton(title + "\n" + full_text[:500]) or "CH"

            # Try to extract a date from the title or early text
            decision_date = None
            for candidate in [title, full_text[:500]]:
                m = DATE_RE.search(candidate)
                if m:
                    if m.group(1):  # DD.MM.YYYY form
                        date_str = f"{int(m.group(1)):02d}.{int(m.group(2)):02d}.{m.group(3)}"
                    else:  # YYYY-MM-DD form
                        date_str = f"{m.group(4)}-{m.group(5)}-{m.group(6)}"
                    decision_date = parse_date(date_str)
                    if decision_date:
                        break

            return Decision(
                decision_id=make_decision_id("sav_kantone", slug),
                court="sav_kantone",
                canton=canton,
                docket_number=slug,
                decision_date=decision_date,
                language=lang,
                title=title,
                legal_area="Anwaltsrecht",
                full_text=full_text,
                source_url=url,
                pdf_url=pdf_url,
                cited_decisions=extract_citations(full_text),
                scraped_at=datetime.now(timezone.utc),
            )

        except Exception as e:
            logger.error(
                f"[sav_kantone] Failed to fetch {slug}: {e}", exc_info=True
            )
            return None


if __name__ == "__main__":
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(
        description="Scrape SAV Kantone attorney discipline decisions"
    )
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
    scraper = SAVKantoneScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(
            f"  {d.decision_id}  {d.decision_date}  {d.canton}  "
            f"{len(d.full_text)} chars  {d.title[:60]}"
        )
    print(f"\nScraped {len(decisions)} SAV Kantone decisions")
