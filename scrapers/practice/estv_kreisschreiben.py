"""
ESTV Kreisschreiben — Federal Direct Tax (DBG)
==============================================

Source page (DE):
  https://www.estv.admin.ch/de/kreisschreiben-direkten-bundessteuer

Each Kreisschreiben (KS) is a PDF under ``estv.admin.ch/dam/de/sd-web/{ID}/``.
The listing HTML is server-rendered, no JS required. Each entry has the
form:

    <a href="/dam/de/sd-web/HASH/dbst-ks-YYYY-1-NNN-X-de.pdf">
      KS Nr. NNN — Title
      Datum: DD.MM.YYYY
    </a>

Coverage: ~73 KS + Anhänge currently active (DE). FR+IT versions exist on
parallel ``/fr/`` and ``/it/`` index pages.

Cited PDF naming convention:
    dbst-ks-{YYYY}-1-{NNN}{suffix}-{lang}.pdf
        suffix: a/b/c (revisions), -anhang1, -faq, etc.

Why this matters: KS bind cantonal tax authorities applying DBG/StHG.
Every Swiss tax practitioner cites them; current MCP corpus has zero.
"""
from __future__ import annotations

import logging
import re
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import PracticeScraper

logger = logging.getLogger(__name__)

INDEX_URLS = {
    "de": "https://www.estv.admin.ch/de/kreisschreiben-direkten-bundessteuer",
    "fr": "https://www.estv.admin.ch/fr/circulaires-impot-federal-direct",
    "it": "https://www.estv.admin.ch/it/circolari-imposta-federale-diretta",
}

# Match KS number from PDF URL or anchor text.
# URL pattern: dbst-ks-YYYY-1-NNN[suffix]-lang.pdf
# Anchor text: "KS Nr. 28" or "KS Nr. 50a"
_KS_NUM_FROM_URL = re.compile(r"dbst-ks-(\d{4})-1-(\d{1,3})([a-z]?)", re.IGNORECASE)
_KS_NUM_FROM_TEXT = re.compile(r"KS\s+Nr\.\s+(\d{1,3}[a-z]?)", re.IGNORECASE)
_DATE_PATTERN = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
_BASE = "https://www.estv.admin.ch"


class EstvKreisschreibenScraper(PracticeScraper):
    SOURCE_KEY = "estv_ks"
    ISSUING_AUTHORITY = "ESTV"
    DEFAULT_DOC_TYPE = "kreisschreiben"
    REQUEST_DELAY = 1.0

    def __init__(self, languages: tuple[str, ...] = ("de",)):
        super().__init__()
        self.languages = languages

    def discover_documents(self) -> Iterator[dict]:
        for lang in self.languages:
            url = INDEX_URLS.get(lang)
            if not url:
                continue
            try:
                r = self.get(url)
                r.raise_for_status()
            except Exception as e:
                logger.warning("ESTV-KS [%s] index fetch failed: %s", lang, e)
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            seen_pdfs: set[str] = set()

            # Listing has anchor tags whose href points to /dam/.../*.pdf
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/dam/" not in href or not href.lower().endswith(".pdf"):
                    continue
                pdf_url = urljoin(_BASE, href)
                if pdf_url in seen_pdfs:
                    continue
                seen_pdfs.add(pdf_url)

                # Title: text content of the anchor (strip + collapse whitespace)
                title = re.sub(r"\s+", " ", a.get_text(" ", strip=True)) or "(untitled)"

                # KS number — from URL primarily, anchor text as fallback
                m_url = _KS_NUM_FROM_URL.search(pdf_url)
                m_txt = _KS_NUM_FROM_TEXT.search(title)
                if m_url:
                    ks_year, ks_num, ks_rev = m_url.group(1), m_url.group(2), m_url.group(3)
                    doc_number = f"KS Nr. {int(ks_num)}{ks_rev}"
                elif m_txt:
                    doc_number = f"KS Nr. {m_txt.group(1)}"
                else:
                    # Mitteilungen / Wegleitungen from same index
                    fname = pdf_url.rsplit("/", 1)[-1]
                    doc_number = fname.replace(".pdf", "")[:60]

                # Date — anchor + immediate sibling/parent text often contain DD.MM.YYYY
                surrounding = (a.get_text(" ", strip=True) + " "
                               + (a.parent.get_text(" ", strip=True) if a.parent else ""))
                d = _DATE_PATTERN.search(surrounding)
                date_iso = (f"{d.group(3)}-{int(d.group(2)):02d}-{int(d.group(1)):02d}"
                            if d else "")

                yield {
                    "pdf_url": pdf_url,
                    "url": url,
                    "title": title,
                    "doc_number": doc_number,
                    "date": date_iso,
                    "language": lang,
                    "doc_type": "kreisschreiben",
                    "topics": ["DBG", "Direkte Bundessteuer"],
                }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    s = EstvKreisschreibenScraper(languages=("de",))
    s.run()
