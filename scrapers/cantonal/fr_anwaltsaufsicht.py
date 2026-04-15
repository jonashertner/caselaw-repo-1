"""
FR Anwaltsaufsicht Scraper
==========================

Scrapes Fribourg attorney discipline decisions (Commission du barreau)
from fr.ch.

Covers: ~3 decisions, French/German
Rate limiting: 2.0 seconds (PDF downloads)
"""
from __future__ import annotations

import io
import logging
import re
from datetime import datetime, timezone
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

LISTING_URL = "https://www.fr.ch/etat-et-droit/justice/commission-du-barreau"
BASE_URL = "https://www.fr.ch"

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


def _is_relevant_pdf(title: str, url: str) -> bool:
    """Filter for relevant PDFs by keywords: jurisprudence, décision, disciplin, barreau, anonymi."""
    keywords = ("jurisprudence", "décision", "disciplin", "barreau", "anonymi")
    search_text = (title + " " + url).lower()
    return any(kw in search_text for kw in keywords)


class FRAnwaltsaufsichtScraper(BaseScraper):
    """Scraper for Fribourg attorney discipline decisions (Commission du barreau)."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 60

    @property
    def court_code(self) -> str:
        return "fr_anwaltsaufsicht"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_pdf_url(self, url: str) -> bool:
        path = urlparse(url).path.lower()
        return path.endswith(".pdf")

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover decisions from the FR Commission du barreau page."""
        try:
            resp = self.get(LISTING_URL)
        except Exception as e:
            logger.error(f"[fr_anwaltsaufsicht] Failed to fetch listing: {e}")
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        seen_slugs: set[str] = set()
        found = 0

        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            full_url = href if href.startswith("http") else urljoin(BASE_URL, href)

            # Only follow PDF links
            if not self._is_pdf_url(full_url):
                continue

            # Skip external domains
            parsed = urlparse(full_url)
            if parsed.netloc and "fr.ch" not in parsed.netloc:
                continue

            slug = _url_to_slug(full_url)
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)

            title = tag.get_text(strip=True) or slug

            # Filter for relevant PDFs by keywords
            if not _is_relevant_pdf(title, full_url):
                continue

            decision_id = make_decision_id("fr_anwaltsaufsicht", slug)
            if self.state.is_known(decision_id):
                continue

            found += 1
            yield {
                "slug": slug,
                "title": title,
                "url": full_url,
            }

        logger.info(
            f"[fr_anwaltsaufsicht] Found {found} new entries "
            f"({len(seen_slugs)} unique links on page)"
        )

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download and parse a single FR Anwaltsaufsicht decision."""
        slug = stub["slug"]
        url = stub["url"]

        try:
            resp = self.get(url)
            full_text = _extract_pdf_text(resp.content)

            if not full_text or len(full_text.strip()) < 50:
                logger.warning(
                    f"[fr_anwaltsaufsicht] No text extracted for {slug} "
                    f"({len(resp.content)} bytes)"
                )
                return None

            full_text = self.clean_text(full_text)
            lang = detect_language(full_text)

            title = stub.get("title") or slug

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
                decision_id=make_decision_id("fr_anwaltsaufsicht", slug),
                court="fr_anwaltsaufsicht",
                canton="FR",
                docket_number=slug,
                decision_date=decision_date,
                language=lang,
                title=title,
                legal_area="Anwaltsrecht",
                full_text=full_text,
                source_url=url,
                pdf_url=url,
                cited_decisions=extract_citations(full_text),
                scraped_at=datetime.now(timezone.utc),
            )

        except Exception as e:
            logger.error(
                f"[fr_anwaltsaufsicht] Failed to fetch {slug}: {e}", exc_info=True
            )
            return None


if __name__ == "__main__":
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(
        description="Scrape FR Anwaltsaufsicht attorney discipline decisions"
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
    scraper = FRAnwaltsaufsichtScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(
            f"  {d.decision_id}  {d.decision_date}  {d.canton}  "
            f"{len(d.full_text)} chars  {d.title[:60]}"
        )
    print(f"\nScraped {len(decisions)} FR Anwaltsaufsicht decisions")
