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

# Site relaunched (2025/26): the old /are/de/home/... tree redirects to a
# Nuxt SPA at /de/... . Publication lists are NOT plain <a> anchors anymore —
# they live in the __NUXT_DATA__ devalue payload embedded in the page HTML.
# extract_nuxt_pdfs() below parses that; validated 2026-07-06.
_BASE = "https://www.are.admin.ch"
INDEX_CANDIDATES = (
    f"{_BASE}/de/publikationen-planung-recht",
)

_YEAR = re.compile(r"\b(19|20)\d{2}\b")
_ISO_DATE = re.compile(r"^(\d{4}-\d{2}-\d{2})T")
# filename suffixes carry the language: "...-d.pdf" / "_f.pdf" / "-i.pdf"
_LANG_SUFFIX = re.compile(r"[-_]([dfie])\.pdf$", re.IGNORECASE)
_LANG_MAP = {"d": "de", "f": "fr", "i": "it", "e": "en"}


def extract_nuxt_pdfs(html: str, host_filter: str) -> list[dict]:
    """Parse a Nuxt 3 __NUXT_DATA__ payload (flat devalue array) for PDF
    entries. Serialization pattern observed on are.admin.ch (2026-07-06):
        ..., "<category>", "<human date>", "<ISO date>",
        "https://backend...fileservice/....pdf", {meta}, "PDF", "<size>",
        "<original filename>.pdf", ...
    We anchor on the URL string and scan a small window for the filename
    (→ title + language) and the preceding ISO date."""
    import json as _json

    from bs4 import BeautifulSoup as _BS
    soup = _BS(html, "html.parser")
    script = soup.find("script", id="__NUXT_DATA__")
    if not script or not script.string:
        return []
    try:
        arr = _json.loads(script.string)
    except Exception:
        return []
    if not isinstance(arr, list):
        return []

    out = []
    for i, v in enumerate(arr):
        if not (isinstance(v, str) and v.lower().endswith(".pdf")
                and v.startswith("http") and host_filter in v):
            continue
        filename = next(
            (x for x in arr[i + 1: i + 9]
             if isinstance(x, str) and x.lower().endswith(".pdf")
             and not x.startswith("http")), "")
        date_iso = ""
        for x in arr[max(0, i - 5): i]:
            if isinstance(x, str):
                m = _ISO_DATE.match(x)
                if m:
                    date_iso = m.group(1)
        lang = "de"
        lm = _LANG_SUFFIX.search(filename or v)
        if lm:
            lang = _LANG_MAP.get(lm.group(1).lower(), "de")
        title = re.sub(r"[_-]+", " ",
                       (filename or v.rsplit("/", 1)[-1])[:-4]).strip()
        out.append({"pdf_url": v, "filename": filename,
                    "title": title, "date": date_iso, "language": lang})
    return out


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

            # Primary path: publications embedded in the Nuxt payload.
            for doc in extract_nuxt_pdfs(r.text, "backend.are.admin.ch"):
                if doc["pdf_url"] in seen:
                    continue
                seen.add(doc["pdf_url"])
                yield {
                    "pdf_url": doc["pdf_url"],
                    "url": index_url,
                    "title": doc["title"],
                    "doc_number": (doc["filename"] or
                                   doc["pdf_url"].rsplit("/", 1)[-1])[:80],
                    "date": doc["date"],
                    "language": doc["language"],
                    "doc_type": "vollzugshilfe",
                    "topics": ["Raumplanung", "RPG/RPV"],
                }

            # Fallback: any plain PDF anchors that do exist.
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
                ym = _YEAR.search(title)
                yield {
                    "pdf_url": pdf_url,
                    "url": index_url,
                    "title": title,
                    "doc_number": pdf_url.rsplit("/", 1)[-1].replace(".pdf", "")[:80],
                    "date": f"{ym.group(0)}-01-01" if ym else "",
                    "language": "de",
                    "doc_type": "vollzugshilfe",
                    "topics": ["Raumplanung", "RPG/RPV"],
                }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    AreVollzugshilfenScraper().run(max_new=5)
