"""
EPA — Eidgenössisches Personalamt
==================================

Bundespersonalrecht: BPG/BPV-Weisungen, Erläuterungen, Praxis-
Mitteilungen.

Index URL (best entry point):
  https://www.epa.admin.ch/epa/de/home/themen/rechtliche_grundlagen.html
  https://www.epa.admin.ch/epa/de/home/dokumentation.html

STATUS: stub. EPA's site is smaller than the others — likely <30 docs.
Discover/fetch logic is a defensive scaffold; needs first-run validation.
"""
from __future__ import annotations

import logging
import re
from typing import Iterator
from urllib.parse import urljoin, unquote

from bs4 import BeautifulSoup

from .base import PracticeScraper

logger = logging.getLogger(__name__)

_BASE = "https://www.epa.admin.ch"
INDEX_CANDIDATES = (
    f"{_BASE}/epa/de/home/themen/rechtliche_grundlagen.html",
    f"{_BASE}/epa/de/home/dokumentation.html",
)


class EpaPersonalrechtScraper(PracticeScraper):
    SOURCE_KEY = "epa_personalrecht"
    ISSUING_AUTHORITY = "EPA"
    DEFAULT_DOC_TYPE = "weisung"

    def discover_documents(self) -> Iterator[dict]:
        seen: set[str] = set()
        for index_url in INDEX_CANDIDATES:
            try:
                r = self.get(index_url)
                if r.status_code != 200:
                    continue
            except Exception as e:
                logger.warning("EPA index probe failed: %s", e)
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.lower().endswith(".pdf"):
                    continue
                pdf_url = urljoin(_BASE, href)
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)

                title = (re.sub(r"\s+", " ", a.get_text(" ", strip=True))
                         or unquote(pdf_url.rsplit("/", 1)[-1]).replace(".pdf", ""))
                yield {
                    "pdf_url": pdf_url,
                    "url": index_url,
                    "title": title,
                    "doc_number": pdf_url.rsplit("/", 1)[-1].replace(".pdf", "")[:80],
                    "date": "",
                    "language": "de",
                    "doc_type": "weisung",
                    "topics": ["Bundespersonalrecht", "BPG/BPV"],
                }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    EpaPersonalrechtScraper().run(max_new=5)
