"""
ARE — Bundesamt für Raumentwicklung
====================================

Federal spatial-planning Vollzugshilfen, Erläuterungen RPG/RPV,
Praxis-Empfehlungen.

Index URL (best entry point):
  https://www.are.admin.ch/are/de/home/raumentwicklung-und-raumplanung.html

Sub-pages:
  /are/de/home/raumentwicklung-und-raumplanung/grundlagen-und-daten/grundlagen.html
  /are/de/home/raumentwicklung-und-raumplanung/raumplanungsrecht/erlaeuterungen.html

PDFs are typically under /dam/are/de/sd-web/HASH/*.pdf with descriptive
filenames.

STATUS: stub — needs real-page inspection. Smaller corpus than BAFU
(estimated 30-80 documents).
"""
from __future__ import annotations

import logging
import re
from typing import Iterator
from urllib.parse import urljoin, unquote

from bs4 import BeautifulSoup

from .base import PracticeScraper

logger = logging.getLogger(__name__)

_BASE = "https://www.are.admin.ch"
INDEX_CANDIDATES = (
    f"{_BASE}/are/de/home/raumentwicklung-und-raumplanung/raumplanungsrecht/"
    f"erlaeuterungen-rpg-rpv.html",
    f"{_BASE}/are/de/home/raumentwicklung-und-raumplanung/grundlagen-und-daten/"
    f"grundlagen.html",
    f"{_BASE}/are/de/home/raumentwicklung-und-raumplanung.html",
)

_YEAR = re.compile(r"\b(19|20)\d{2}\b")


class AreVollzugshilfenScraper(PracticeScraper):
    SOURCE_KEY = "are_vollzug"
    ISSUING_AUTHORITY = "ARE"
    DEFAULT_DOC_TYPE = "vollzugshilfe"

    def discover_documents(self) -> Iterator[dict]:
        seen: set[str] = set()
        for index_url in INDEX_CANDIDATES:
            try:
                r = self.get(index_url)
                if r.status_code != 200:
                    continue
            except Exception as e:
                logger.warning("ARE index probe failed: %s", e)
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.lower().endswith(".pdf"):
                    continue
                if "/dam/" not in href:
                    continue
                pdf_url = urljoin(_BASE, href)
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)

                title = (re.sub(r"\s+", " ", a.get_text(" ", strip=True))
                         or unquote(pdf_url.rsplit("/", 1)[-1]).replace(".pdf", ""))
                ym = _YEAR.search(title)
                date_iso = f"{ym.group(0)}-01-01" if ym else ""

                yield {
                    "pdf_url": pdf_url,
                    "url": index_url,
                    "title": title,
                    "doc_number": pdf_url.rsplit("/", 1)[-1].replace(".pdf", "")[:80],
                    "date": date_iso,
                    "language": "de",
                    "doc_type": "vollzugshilfe",
                    "topics": ["Raumplanung", "RPG/RPV"],
                }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    AreVollzugshilfenScraper().run(max_new=5)
