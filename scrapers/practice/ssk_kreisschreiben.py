"""
SSK Kreisschreiben — Schweizerische Steuerkonferenz
====================================================

Inter-cantonal tax-administration coordination circulars. Cited
alongside ESTV-KS in tax practice. Hosted at tax-admin.ch (the SSK's
own site), separate from estv.admin.ch.

Index URLs to probe (best guesses based on the SSK site structure):
  https://www.tax-admin.ch/de/dokumentationen/kreisschreiben.html
  https://www.tax-admin.ch/de/grundlagen-themen/dokumentationen.html

Each KS is a PDF, naming pattern roughly ``ks-NN-DDDDDDDD.pdf`` or
``KS_##_topic.pdf``.

STATUS: stub — needs real-page inspection before first run. The
discover_documents() implementation here is a defensive scaffold:
it'll find PDF anchors on the index page if the URL is correct,
but the title/number/date heuristics may need tuning to match the
SSK's actual HTML markup. A first run with --max-new 5 is the
recommended way to validate before enabling on the production timer.
"""
from __future__ import annotations

import logging
import re
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import PracticeScraper

logger = logging.getLogger(__name__)

# Validated 2026-07-05: the SSK lives at ssk-csi.ch (steuerkonferenz.ch 301s
# there; the tax-admin.ch guesses were dead). TYPO3 news plugin: the index is
# paginated (4 pages) via tx_news_pi1[currentPage] links WITH cHash — the
# pagination URLs must be harvested from the page, never constructed.
_BASE = "https://www.ssk-csi.ch"
INDEX_CANDIDATES = (
    f"{_BASE}/de/themen/kreisschreiben",
)

# Matches "KS 35", "KS_35_d.pdf" and "Kreisschreiben 31a" alike.
_KS_NUM = re.compile(r"(?:KS|Kreisschreiben)[_\s]*(?:Nr\.?\s*)?(\d{1,3}[a-z]?)",
                     re.IGNORECASE)
_DATE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
_PAGINATION = re.compile(r"tx_news_pi1(?:%5B|\[)currentPage")


class SskKreisschreibenScraper(PracticeScraper):
    SOURCE_KEY = "ssk_ks"
    ISSUING_AUTHORITY = "SSK"
    DEFAULT_DOC_TYPE = "kreisschreiben"

    def discover_documents(self) -> Iterator[dict]:
        index_url = self._find_live_index()
        if not index_url:
            logger.warning("SSK: no live index found — needs URL update")
            return

        # Walk the paginated index: start at page 1, follow every
        # tx_news_pi1[currentPage] link found in fetched pages (cHash-protected,
        # so harvested — not constructed). Order is stable enough for a scrape.
        to_visit = [index_url]
        visited: set[str] = set()
        seen: set[str] = set()
        while to_visit:
            page_url = to_visit.pop(0)
            if page_url in visited:
                continue
            visited.add(page_url)
            try:
                r = self.get(page_url)
                r.raise_for_status()
            except Exception as e:
                logger.warning("SSK index fetch failed: %s — %s", page_url, e)
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if _PAGINATION.search(href):
                    nxt = urljoin(page_url, href.replace("&amp;", "&"))
                    if nxt not in visited:
                        to_visit.append(nxt)
            yield from self._extract_pdfs(soup, page_url, seen)

    def _extract_pdfs(self, soup, index_url: str, seen: set) -> Iterator[dict]:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith(".pdf"):
                continue
            pdf_url = urljoin(index_url, href)
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            title = re.sub(r"\s+", " ", a.get_text(" ", strip=True))
            m = _KS_NUM.search(title) or _KS_NUM.search(pdf_url)
            doc_number = f"SSK-KS Nr. {m.group(1)}" if m else pdf_url.rsplit("/", 1)[-1][:80]
            d = _DATE.search(title)
            date_iso = (f"{d.group(3)}-{int(d.group(2)):02d}-{int(d.group(1)):02d}"
                        if d else "")

            yield {
                "pdf_url": pdf_url,
                "url": index_url,
                "title": title or doc_number,
                "doc_number": doc_number,
                "date": date_iso,
                "language": "de",
                "doc_type": "kreisschreiben",
                "topics": ["Inter-cantonal", "Steuerkonferenz"],
            }

    def _find_live_index(self) -> str | None:
        for u in INDEX_CANDIDATES:
            try:
                r = self.session.head(u, timeout=10, allow_redirects=True)
                if r.status_code == 200:
                    return u
            except Exception:
                continue
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    SskKreisschreibenScraper().run(max_new=5)
