"""
BAZG (Bundesamt für Zoll und Grenzsicherheit) publications scraper.

Scrapes Merkblätter, Formulare and Publikationen from
https://www.bazg.admin.ch/de/formulare-merkblaetter-und-publikationen-bazg.

Architecture:
- Hub page lists ~13 publication categories (h3 sections with "Mehr über"
  links to /de/publikationen-* subpages).
- Each category subpage lists 10–60 PDF documents under
  /dam/de/sd-web/<id>/<filename>.pdf with the title, file size, and
  publication date inline in the link text (e.g. "52.15 Mehrwertsteuersätze
  PDF 22.56 kB 19. September 2023").
- Each PDF carries an official BAZG publication code (e.g. "52.01") used
  as docket_number.

Coverage (2026-05-19): ~13 categories × ~16 docs ≈ 200+ publications,
covering VAT, customs, vehicle tax, alcohol/tobacco, free-trade
agreements, heavy-vehicle fees, transit, tariff (Tares), and more.

Output: Decision objects with court="bazg", canton="CH", chamber=
category label, docket_number=publication code, full_text=extracted
PDF text. Treated as administrative-guidance via the existing decisions
pipeline (no separate corpus DB), per minimum-viable-integration choice.
"""
from __future__ import annotations

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

BASE_URL = "https://www.bazg.admin.ch"
HUB_PATH = "/de/formulare-merkblaetter-und-publikationen-bazg"

# Official BAZG publication code at the start of every link text:
# "52.01 Steuer ...", "18.85 Zoll- ...", "52.66.1.x …"
RE_CODE = re.compile(r"^(\d+\.\d+(?:\.\d+)?)\b")

# Publication date in German full-text format: "19. September 2023".
RE_PUB_DATE = re.compile(
    r"(\d{1,2})\.\s*"
    r"(Januar|Februar|M(?:ä|ae)rz|April|Mai|Juni|Juli|"
    r"August|September|Oktober|November|Dezember)\s+(\d{4})"
)
MONTHS_DE = {
    "Januar": 1, "Februar": 2, "März": 3, "Maerz": 3, "April": 4, "Mai": 5,
    "Juni": 6, "Juli": 7, "August": 8, "September": 9, "Oktober": 10,
    "November": 11, "Dezember": 12,
}

# Trailing "PDF <size> kB <date>" decoration on every link text.
RE_TRAILER = re.compile(r"PDF\s*[\d.,]+\s*[kKmM]?B.*$")

# Identify category subpages on the hub. Each lives under /de/ and carries
# one of these keyword markers. Excludes generic site links (Kontakt, FAQ).
CATEGORY_KEYWORDS = ("publikation", "merkblaetter", "schwerverkehr")


def _parse_german_date(text: str) -> date | None:
    if not text:
        return None
    m = RE_PUB_DATE.search(text)
    if not m:
        return None
    try:
        day = int(m.group(1))
        month_key = m.group(2).replace("ä", "ae") if "ae" not in m.group(2) else m.group(2)
        month = MONTHS_DE.get(m.group(2)) or MONTHS_DE.get(month_key)
        year = int(m.group(3))
        if not month:
            return None
        return date(year, month, day)
    except ValueError:
        return None


class BAZGScraper(BaseScraper):
    """Scraper for BAZG (Federal Office for Customs and Border Security) publications."""

    REQUEST_DELAY = 1.5
    TIMEOUT = 60
    MAX_ERRORS = 50

    @property
    def court_code(self) -> str:
        return "bazg"

    def _discover_categories(self) -> list[str]:
        """Return absolute URLs for every publication-category subpage on the hub."""
        try:
            r = self.get(BASE_URL + HUB_PATH)
        except Exception as e:
            logger.error(f"[bazg] hub fetch failed: {e}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        seen: set[str] = set()
        cats: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("/de/"):
                continue
            low = href.lower()
            if not any(k in low for k in CATEGORY_KEYWORDS):
                continue
            if href in seen:
                continue
            seen.add(href)
            cats.append(BASE_URL + href)
        logger.info(f"[bazg] discovered {len(cats)} category subpages")
        return cats

    def _discover_category_docs(self, category_url: str) -> Iterator[dict]:
        """Yield document stubs for every PDF link on a category subpage."""
        try:
            r = self.get(category_url)
        except Exception as e:
            logger.warning(f"[bazg] category fetch failed for {category_url}: {e}")
            return
        soup = BeautifulSoup(r.text, "html.parser")
        category_label = ""
        h1 = soup.find("h1")
        if h1:
            category_label = h1.get_text(strip=True)

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower():
                continue
            pdf_url = href if href.startswith("http") else urljoin(category_url, href)

            link_text = a.get_text(" ", strip=True)
            code_m = RE_CODE.match(link_text)
            code = code_m.group(1) if code_m else None
            pub_date = _parse_german_date(link_text)
            # Strip the trailing PDF/size/date decoration to recover the title.
            title = RE_TRAILER.sub("", link_text).strip()

            # Docket: prefer the official code; fall back to the filename stem
            # so we still have a stable per-doc identifier.
            if code:
                docket = code
            else:
                docket = pdf_url.rsplit("/", 1)[-1].removesuffix(".pdf")

            decision_id = make_decision_id("bazg", docket)
            yield {
                "decision_id": decision_id,
                "docket_number": docket,
                "title": title[:300] if title else None,
                "category": category_label or None,
                "url": pdf_url,
                "publication_date": pub_date,
                "code": code,
            }

    def discover_new(self, since_date=None) -> Iterator[dict]:
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        categories = self._discover_categories()
        # Some Merkblätter are linked from more than one category page
        # (e.g. cross-listed VAT/customs items). De-dup in-run by
        # decision_id so we only yield each once per discovery pass.
        seen_ids: set[str] = set()
        total_yielded = 0
        for cat_url in categories:
            for stub in self._discover_category_docs(cat_url):
                did = stub["decision_id"]
                if did in seen_ids:
                    continue
                seen_ids.add(did)
                if self.state.is_known(did):
                    continue
                if since_date and stub.get("publication_date"):
                    if stub["publication_date"] < since_date:
                        continue
                total_yielded += 1
                yield stub
        logger.info(f"[bazg] discovery complete: {total_yielded} new stubs")

    def fetch_decision(self, stub: dict) -> Decision | None:
        url = stub["url"]
        try:
            r = self.get(url)
        except Exception as e:
            logger.warning(f"[bazg] fetch failed for {stub['docket_number']}: {e}")
            return None

        try:
            import fitz  # PyMuPDF
            pdf = fitz.open(stream=r.content, filetype="pdf")
            pages = [page.get_text() for page in pdf]
            pdf.close()
            full_text = "\n\n".join(pages).strip()
        except Exception as e:
            logger.warning(
                f"[bazg] PDF extraction failed for {stub['docket_number']}: {e}"
            )
            return None

        if len(full_text) < 100:
            logger.warning(
                f"[bazg] short text for {stub['docket_number']}: "
                f"{len(full_text)} chars"
            )
            return None

        language = detect_language(full_text) if len(full_text) > 100 else "de"
        decision_id = make_decision_id("bazg", stub["docket_number"])

        return Decision(
            decision_id=decision_id,
            court="bazg",
            canton="CH",
            chamber=stub.get("category"),
            docket_number=stub["docket_number"],
            decision_date=stub.get("publication_date"),
            publication_date=stub.get("publication_date"),
            language=language,
            title=stub.get("title"),
            full_text=full_text,
            source_url=url,
            cited_decisions=(
                extract_citations(full_text) if len(full_text) > 200 else []
            ),
        )
