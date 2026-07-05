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

# Site relaunched (2025/26): old /epa/de/home/... redirects to a Nuxt SPA.
# /de/personalrecht carries real PDF anchors (Weisungen des Bundesrates,
# Erläuterungen) plus Nuxt-payload documents; sub-pages are crawled one
# level deep for more. Validated 2026-07-06.
_BASE = "https://www.epa.admin.ch"
INDEX_CANDIDATES = (
    f"{_BASE}/de/personalrecht",
)
_SUBPAGE = re.compile(r"^/de/[a-z0-9-]+$")
_MAX_SUBPAGES = 15


class EpaPersonalrechtScraper(PracticeScraper):
    SOURCE_KEY = "epa_personalrecht"
    ISSUING_AUTHORITY = "EPA"
    DEFAULT_DOC_TYPE = "weisung"

    def discover_documents(self) -> Iterator[dict]:
        from .are_vollzugshilfen import extract_nuxt_pdfs

        seen: set[str] = set()
        visited: set[str] = set()
        to_visit = list(INDEX_CANDIDATES)
        while to_visit and len(visited) <= _MAX_SUBPAGES:
            index_url = to_visit.pop(0)
            if index_url in visited:
                continue
            visited.add(index_url)
            try:
                r = self.get(index_url)
                if r.status_code != 200:
                    continue
            except Exception as e:
                logger.warning("EPA index probe failed: %s", e)
                continue

            soup = BeautifulSoup(r.text, "html.parser")

            # Crawl one level of same-site /de/ sub-pages from the seed page.
            if index_url in INDEX_CANDIDATES:
                for a in soup.find_all("a", href=True):
                    if _SUBPAGE.match(a["href"]):
                        to_visit.append(urljoin(_BASE, a["href"]))

            # Nuxt-payload documents (fileservice PDFs without anchors).
            for doc in extract_nuxt_pdfs(r.text, "backend.epa.admin.ch"):
                if doc["pdf_url"] in seen:
                    continue
                seen.add(doc["pdf_url"])
                yield {
                    "pdf_url": doc["pdf_url"], "url": index_url,
                    "title": doc["title"],
                    "doc_number": (doc["filename"] or
                                   doc["pdf_url"].rsplit("/", 1)[-1])[:80],
                    "date": doc["date"], "language": doc["language"],
                    "doc_type": "weisung",
                    "topics": ["Bundespersonalrecht", "BPG/BPV"],
                }

            # Plain PDF anchors (incl. federal-gazette links on admin.ch).
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
