"""
Thurgau Courts Scraper (TG Gerichte)
=====================================
Scrapes court decisions from the Scroll Viewport / Confluence portal at
rechtsprechung.tg.ch.

Architecture:
- GET /og/rbog-{year} → OG year listing page with decision links
- GET /og/rbog-{year}-nr-{nn} → OG full decision HTML page
- GET /vg/tvr-{year} → VG year listing page with decision links
- GET /vg/tvr-{year}-nr-{nn} → VG full decision HTML page
- No authentication, no AJAX — pure server-rendered HTML
- Full text inline in HTML

Additional sections (Medienschaffende + pending publication):
- GET /og/entscheide-fur-medienschaffende-anonymisiert → year links
- GET /og/{year} → decision links (PDF-only pages)
- GET /vg/entscheide-fur-medienschaffende → year links
- GET /vg/{year} → decision links (PDF-only pages)
- GET /og/zur-publikation-in-rbog-vorgesehen → decision links (inline HTML)
- GET /vg/zur-publikation-in-tvr-vorgesehen → decision links (inline HTML)

Content: RBOG (Obergericht, ~1,200) + TVR (Verwaltungsgericht, ~900) series
  + Medienschaffende (~12) + pending publication (~1-5)
Total: ~2,200+ decisions (1994/2000-present)
Platform: Atlassian Confluence Cloud + Scroll Viewport
"""
from __future__ import annotations

import io
import logging
import re
from datetime import date
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import (
    Decision,
    detect_language,
    extract_citations,
    make_decision_id,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://rechtsprechung.tg.ch"

# Regex patterns
RE_DECISION_LINK = re.compile(r"/(og/rbog|vg/tvr)-(\d{4})-nr-(\d+)")
RE_DOCKET = re.compile(r"([A-Z]+\.\d{4}\.\d+)")
RE_DATE = re.compile(r"(\d{1,2})\.\s*(\w+)\s+(\d{4})")
RE_DATE_NUMERIC = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")

# Court sections: (path_prefix, docket_prefix, start_year)
SECTIONS = [
    ("og", "rbog", "RBOG", 1994),   # Obergericht
    ("vg", "tvr", "TVR", 2000),      # Verwaltungsgericht
]

# Additional sections: Medienschaffende (PDF-only) + pending publication (inline HTML)
# (section_path, index_url_suffix, label)
MEDIA_SECTIONS = [
    ("og", "entscheide-fur-medienschaffende-anonymisiert", "OG/Media"),
    ("vg", "entscheide-fur-medienschaffende", "VG/Media"),
]
PENDING_SECTIONS = [
    ("og", "zur-publikation-in-rbog-vorgesehen", "OG/Pending"),
    ("vg", "zur-publikation-in-tvr-vorgesehen", "VG/Pending"),
]

# Match decision slugs like "sbr-2023-51", "zbr-2024-35", "vg-2023-9", "zr-2025-16"
RE_EXTRA_SLUG = re.compile(r"/(?:og|vg)/([a-z]+-\d{4}-\d+)")

# Match year links like "/og/2024", "/vg/2025"
RE_YEAR_LINK = re.compile(r"/(?:og|vg)/(\d{4})$")

# Month name to number (German)
MONTH_MAP = {
    "januar": 1, "februar": 2, "märz": 3, "april": 4, "mai": 5, "juni": 6,
    "juli": 7, "august": 8, "september": 9, "oktober": 10, "november": 11, "dezember": 12,
}


def _extract_pdf_text(data: bytes) -> str:
    """Extract text from PDF bytes using fitz (PyMuPDF) with pdfplumber fallback."""
    try:
        import fitz

        with fitz.open(stream=data, filetype="pdf") as doc:
            return "\n\n".join(page.get_text() for page in doc)
    except Exception:
        import pdfplumber

        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n\n".join(p.extract_text() or "" for p in pdf.pages)


def _slug_to_docket(slug: str) -> str:
    """Convert URL slug like 'sbr-2023-51' to docket 'SBR.2023.51'."""
    # Strip trailing suffixes like "-zur-publikation-vorgesehen"
    m = re.match(r"([a-z]+)-(\d{4})-(\d+)", slug)
    if m:
        return f"{m.group(1).upper()}.{m.group(2)}.{m.group(3)}"
    return slug.upper()


def _parse_date_text(text):
    """Parse date from German text like '15. Oktober 2023'."""
    if not text:
        return None
    m = RE_DATE_NUMERIC.search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    m = RE_DATE.search(text)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = MONTH_MAP.get(month_name)
        if month:
            try:
                return date(year, month, day)
            except ValueError:
                pass
    return None


RE_SIGNATURE_DATE = re.compile(
    r"(?:Obergericht|Verwaltungsgericht),\s*(?:\d+\.\s*)?(?:Zivil)?[Aa]bteilung,?\s*"
    r"(\d{1,2})\.\s*(\w+)\s+(\d{4})"
)


def _extract_signature_date(text: str, docket_year: int | None = None) -> date | None:
    """
    Extract the decision date from the TG signature block.

    Strategy 1 (RBOG): Match "Obergericht, 3. Abteilung, 19. Februar 2024, ZR.2024.9"
    Strategy 2 (TVR fallback): Take the last German date in the text that's
    within ±1 year of the docket year — TVR decisions end with the decision date.
    """
    # Strategy 1: explicit signature pattern (RBOG)
    matches = list(RE_SIGNATURE_DATE.finditer(text))
    if matches:
        m = matches[-1]
        d = _parse_german_date(m.group(1), m.group(2), m.group(3))
        if d:
            return d

    # Strategy 2: last date near the docket year (TVR)
    all_dates = list(RE_DATE.finditer(text))
    if not all_dates:
        return None

    # Work backwards — the last date matching the docket year is usually correct
    for m in reversed(all_dates):
        d = _parse_german_date(m.group(1), m.group(2), m.group(3))
        if d and docket_year and abs(d.year - docket_year) <= 1:
            return d

    # Last resort: just use the very last date found
    m = all_dates[-1]
    return _parse_german_date(m.group(1), m.group(2), m.group(3))


def _parse_german_date(day_str: str, month_str: str, year_str: str) -> date | None:
    """Parse a German date from captured groups."""
    month = MONTH_MAP.get(month_str.lower())
    if not month:
        return None
    try:
        return date(int(year_str), month, int(day_str))
    except ValueError:
        return None


class TGGerichteScraper(BaseScraper):
    """
    Scraper for Thurgau Obergericht decisions via Scroll Viewport HTML.

    Strategy: iterate year pages (1994-present), extract decision links,
    fetch each decision page for full text.
    """

    REQUEST_DELAY = 2.0
    TIMEOUT = 30
    MAX_ERRORS = 50

    @property
    def court_code(self):
        return "tg_gerichte"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        total_yielded = 0
        today = date.today()
        seen_ids: set[str] = set()  # Run-level dedup across all sections

        # 1. Main RBOG/TVR series
        for section_path, series_slug, series_prefix, default_start in SECTIONS:
            start_year = since_date.year if since_date else default_start
            section_count = 0

            for year in range(today.year, start_year - 1, -1):
                for stub in self._discover_year(section_path, series_slug, series_prefix, year):
                    did = stub["decision_id"]
                    if did in seen_ids or self.state.is_known(did):
                        continue
                    seen_ids.add(did)
                    if since_date and stub.get("decision_date") and stub["decision_date"] < since_date:
                        continue
                    total_yielded += 1
                    section_count += 1
                    yield stub

            if section_count > 0:
                logger.info(f"TG/{series_prefix}: {section_count} new stubs")

        # 2. Medienschaffende sections (PDF-only decisions)
        for section_path, index_suffix, label in MEDIA_SECTIONS:
            section_count = 0
            for stub in self._discover_extra_section(section_path, index_suffix, label, since_date):
                did = stub["decision_id"]
                if did in seen_ids or self.state.is_known(did):
                    continue
                seen_ids.add(did)
                total_yielded += 1
                section_count += 1
                yield stub
            if section_count > 0:
                logger.info(f"TG/{label}: {section_count} new stubs")

        # 3. Pending publication sections (inline HTML)
        for section_path, index_suffix, label in PENDING_SECTIONS:
            section_count = 0
            for stub in self._discover_pending_section(section_path, index_suffix, label, since_date):
                did = stub["decision_id"]
                if did in seen_ids or self.state.is_known(did):
                    continue
                seen_ids.add(did)
                total_yielded += 1
                section_count += 1
                yield stub
            if section_count > 0:
                logger.info(f"TG/{label}: {section_count} new stubs")

        logger.info(f"TG: discovery complete: {total_yielded} new stubs")

    def _discover_year(self, section_path: str, series_slug: str,
                       series_prefix: str, year: int) -> Iterator[dict]:
        """Fetch year listing page and extract decision links."""
        url = f"{BASE_URL}/{section_path}/{series_slug}-{year}"
        try:
            r = self.get(url)
        except Exception as e:
            logger.error(f"TG/{series_prefix}: failed to fetch year page {year}: {e}")
            return

        if r.status_code != 200:
            logger.debug(f"TG/{series_prefix}: year page {year} returned {r.status_code}")
            return

        soup = BeautifulSoup(r.text, "html.parser")

        # Find all links to individual decision pages
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            m = RE_DECISION_LINK.search(href)
            if not m:
                continue

            link_year = int(m.group(2))
            nr = int(m.group(3))
            key = (series_prefix, link_year, nr)
            if key in seen:
                continue
            seen.add(key)

            link_text = a.get_text(strip=True)
            docket = f"{series_prefix}-{link_year}-{nr}"

            decision_id = make_decision_id("tg_gerichte", docket)

            # Use the actual href from the page (portal switched to
            # zero-padded URLs like -nr-01 in 2024).
            # Hrefs may be relative (e.g. "../og/rbog-2024-nr-01").
            raw_href = urljoin(url, href)

            yield {
                "decision_id": decision_id,
                "docket_number": docket,
                "year": link_year,
                "nr": nr,
                "title": link_text[:200] if link_text else None,
                "url": raw_href,
            }

    def _discover_extra_section(self, section_path: str, index_suffix: str,
                                label: str, since_date=None) -> Iterator[dict]:
        """Discover decisions from Medienschaffende index → year pages."""
        index_url = f"{BASE_URL}/{section_path}/{index_suffix}"
        try:
            r = self.get(index_url)
        except Exception as e:
            logger.error(f"TG/{label}: failed to fetch index: {e}")
            return

        if r.status_code != 200:
            logger.debug(f"TG/{label}: index returned {r.status_code}")
            return

        soup = BeautifulSoup(r.text, "html.parser")

        # Find year links (e.g. /og/2024, /og/2025), deduped
        years = set()
        for a in soup.find_all("a", href=True):
            m = RE_YEAR_LINK.search(a["href"])
            if m:
                years.add(int(m.group(1)))

        for year in sorted(years, reverse=True):
            if since_date and year < since_date.year - 1:
                continue
            yield from self._discover_extra_year(section_path, year, label)

    def _discover_extra_year(self, section_path: str, year: int,
                             label: str) -> Iterator[dict]:
        """Fetch a Medienschaffende year page and extract decision links."""
        url = f"{BASE_URL}/{section_path}/{year}"
        try:
            r = self.get(url)
        except Exception as e:
            logger.error(f"TG/{label}: failed to fetch year {year}: {e}")
            return

        if r.status_code != 200:
            logger.debug(f"TG/{label}: year {year} returned {r.status_code}")
            return

        soup = BeautifulSoup(r.text, "html.parser")
        seen = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = RE_EXTRA_SLUG.search(href)
            if not m:
                continue
            slug = m.group(1)
            if slug in seen:
                continue
            # Skip RBOG/TVR links that might appear in nav
            if slug.startswith(("rbog-", "tvr-")):
                continue
            seen.add(slug)

            docket = _slug_to_docket(slug)
            decision_id = make_decision_id("tg_gerichte", docket)
            link_url = urljoin(url, href)
            link_text = a.get_text(strip=True)

            yield {
                "decision_id": decision_id,
                "docket_number": docket,
                "title": link_text[:200] if link_text else None,
                "url": link_url,
                "pdf_mode": True,  # Signal that this is a PDF-only page
            }

    def _discover_pending_section(self, section_path: str, index_suffix: str,
                                  label: str, since_date=None) -> Iterator[dict]:
        """Discover decisions from 'Zur Publikation vorgesehen' pages."""
        url = f"{BASE_URL}/{section_path}/{index_suffix}"
        try:
            r = self.get(url)
        except Exception as e:
            logger.error(f"TG/{label}: failed to fetch pending page: {e}")
            return

        if r.status_code != 200:
            logger.debug(f"TG/{label}: pending page returned {r.status_code}")
            return

        soup = BeautifulSoup(r.text, "html.parser")
        seen = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = RE_EXTRA_SLUG.search(href)
            if not m:
                continue
            slug = m.group(1)
            if slug in seen:
                continue
            if slug.startswith(("rbog-", "tvr-")):
                continue
            seen.add(slug)

            docket = _slug_to_docket(slug)
            decision_id = make_decision_id("tg_gerichte", docket)
            link_url = urljoin(url, href)
            link_text = a.get_text(strip=True)

            yield {
                "decision_id": decision_id,
                "docket_number": docket,
                "title": link_text[:200] if link_text else None,
                "url": link_url,
                "pdf_mode": False,  # Inline HTML like regular decisions
            }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch full decision page and extract content."""
        url = stub.get("url")
        if not url:
            return None

        try:
            r = self.get(url)
        except Exception as e:
            logger.warning(f"TG: fetch failed for {stub['docket_number']}: {e}")
            return None

        if r.status_code != 200:
            logger.warning(f"TG: {stub['docket_number']} returned {r.status_code}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Check for PDF attachment (Medienschaffende pages have PDF, not inline text)
        full_text = None
        pdf_url = None
        pdf_link = soup.find("a", href=re.compile(r"__attachments/.*\.pdf"))
        if pdf_link:
            pdf_url = urljoin(url, pdf_link["href"])
            full_text = self._fetch_pdf_text(pdf_url, stub["docket_number"])

        # If no PDF or PDF extraction failed, extract inline HTML text
        if not full_text or len(full_text) < 50:
            full_text = self._extract_inline_text(soup, stub["docket_number"])

        if not full_text or len(full_text) < 50:
            logger.warning(f"TG: text too short for {stub['docket_number']}: {len(full_text or '')} chars")
            if not full_text:
                full_text = f"[Text extraction failed for {stub['docket_number']}]"

        # Extract docket number from content (for metadata, not for decision_id)
        docket = stub["docket_number"]
        m_docket = RE_DOCKET.search(full_text[:1000])
        if m_docket:
            docket = m_docket.group(1)

        # Extract decision date — try docket year from docket number
        docket_year = stub.get("year")
        if not docket_year:
            m_yr = re.search(r"\.(\d{4})\.", docket)
            if m_yr:
                docket_year = int(m_yr.group(1))
        decision_date = _extract_signature_date(full_text, docket_year)
        if not decision_date:
            decision_date = _parse_date_text(full_text[:500])
        if not decision_date:
            decision_date = stub.get("decision_date")
        if not decision_date and docket_year:
            decision_date = date(docket_year, 1, 1)
        if not decision_date:
            logger.warning(f"TG: no date for {stub.get('docket_number', '?')}")

        # Extract chamber/division from signature block
        chamber = None
        m_abt = re.search(
            r"((?:Obergericht|Verwaltungsgericht),\s*(?:\d+\.\s*)?(?:Zivil)?[Aa]bteilung)",
            full_text,
        )
        if m_abt:
            chamber = m_abt.group(1)

        language = detect_language(full_text) if len(full_text) > 100 else "de"

        decision_id = stub["decision_id"]

        return Decision(
            decision_id=decision_id,
            court="tg_gerichte",
            canton="TG",
            chamber=chamber,
            docket_number=docket,
            decision_date=decision_date,
            language=language,
            title=stub.get("title"),
            full_text=full_text,
            source_url=url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )

    def _fetch_pdf_text(self, pdf_url: str, docket: str) -> str | None:
        """Download PDF and extract text."""
        try:
            self._rate_limit()
            r = self.session.get(pdf_url, timeout=self.TIMEOUT)
            if r.status_code != 200:
                logger.warning(f"TG: PDF download failed for {docket}: {r.status_code}")
                return None
            return _extract_pdf_text(r.content)
        except Exception as e:
            logger.warning(f"TG: PDF extraction failed for {docket}: {e}")
            return None

    @staticmethod
    def _extract_inline_text(soup, docket: str) -> str:
        """Extract decision text from inline HTML content."""
        main = soup.find("main") or soup.find("article") or soup.find("div", class_="content-body")
        if not main:
            best = None
            best_len = 0
            for div in soup.find_all("div"):
                tlen = len(div.get_text(strip=True))
                if tlen > best_len:
                    best = div
                    best_len = tlen
            if best and best_len > 200:
                main = best

        if not main:
            return ""

        for tag in main.find_all(["nav", "footer", "header"]):
            tag.decompose()
        for tag in main.find_all(class_=re.compile(r"breadcrumb|sidebar|footer|nav")):
            tag.decompose()

        return main.get_text(separator="\n", strip=True)
