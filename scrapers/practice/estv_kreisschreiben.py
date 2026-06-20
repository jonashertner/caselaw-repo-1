"""
ESTV Kreisschreiben — Federal tax administrative practice (DBST / VST / STA)
===========================================================================

Source pages (server-rendered HTML, no JS):
  Direkte Bundessteuer (DBST):  /de/kreisschreiben-direkten-bundessteuer  (+ fr/it)
  Verrechnungssteuer (VST):     /de/kreisschreiben-verrechnungssteuer     (+ fr/it)
  Stempelabgaben (STA):         /de/kreisschreiben-stempelabgaben         (+ fr/it)

Each Kreisschreiben (KS) is a PDF under ``estv.admin.ch/dam/.../*.pdf``. Every
file uses the ``dbst-ks-`` prefix regardless of tax type; the applicable tax(es)
are encoded in the suffix letters before the language code, e.g.

    dbst-ks-{YYYY}-1-{NNN}{rev}-{suffix}-{lang}.pdf
        suffix:  d = Direkte Bundessteuer
                 v = Verrechnungssteuer
                 s = Stempelabgaben      (combinations: dv, dvs, …)

A single KS can therefore apply to several taxes and appears on several tax-type
pages — we iterate all of them and dedup by (language-keyed) doc_id.

This is the SINGLE home for ESTV Kreisschreiben. The decisions-pipeline
duplicate (PR #26, ``scrapers/estv.py``) was retired (issue #16): Kreisschreiben
are administrative practice, not court decisions, and belong in practice.db /
search_practice — not in the decision corpus + citation graph.

Why this matters: KS bind cantonal tax authorities applying DBG / VStG / StG.
Every Swiss tax practitioner cites them.
"""
from __future__ import annotations

import logging
import re
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import PracticeScraper

logger = logging.getLogger(__name__)

_BASE = "https://www.estv.admin.ch"

# Per (tax type, language) index page. Every page lists dbst-ks-*.pdf files;
# the union across tax pages (deduped) is the full KS set for a language.
_TAX_PAGES = {
    "DBST": {
        "de": "/de/kreisschreiben-direkten-bundessteuer",
        "fr": "/fr/circulaires-impot-federal-direct",
        "it": "/it/circolari-imposta-federale-diretta",
    },
    "VST": {
        "de": "/de/kreisschreiben-verrechnungssteuer",
        "fr": "/fr/circulaires-impot-anticipe",
        "it": "/it/circolari-imposta-preventiva",
    },
    "STA": {
        "de": "/de/kreisschreiben-stempelabgaben",
        "fr": "/fr/circulaires-droits-de-timbre",
        "it": "/it/circolari-tasse-di-bollo",
    },
}

INDEX_SOURCES = [
    {"tax_type": tax, "lang": lang, "url": _BASE + slug}
    for tax, pages in _TAX_PAGES.items()
    for lang, slug in pages.items()
]

# KS number from PDF URL (primary) or anchor text (fallback).
_KS_NUM_FROM_URL = re.compile(r"dbst-ks-(\d{4})-1-(\d{1,3})([a-z]?)", re.IGNORECASE)
_KS_NUM_FROM_TEXT = re.compile(r"KS\s+Nr\.\s+(\d{1,3}[a-z]?)", re.IGNORECASE)
_DATE_PATTERN = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
# Tax-type suffix: the letters between the KS number and the language code.
_SUFFIX_RE = re.compile(r"dbst-ks-\d{4}-1-\d{1,3}[a-z]?-([dvs]+)-(?:de|fr|it)\.pdf",
                        re.IGNORECASE)
_TAX_LABEL = {"d": "Direkte Bundessteuer", "v": "Verrechnungssteuer", "s": "Stempelabgaben"}


def _topics_from_filename(filename: str) -> list[str]:
    """Applicable taxes from the suffix letters (d/v/s) → ordered labels."""
    m = _SUFFIX_RE.search(filename)
    if not m:
        return []
    seen: list[str] = []
    for ch in m.group(1).lower():
        label = _TAX_LABEL.get(ch)
        if label and label not in seen:
            seen.append(label)
    return seen


class EstvKreisschreibenScraper(PracticeScraper):
    SOURCE_KEY = "estv_ks"
    ISSUING_AUTHORITY = "ESTV"
    DEFAULT_DOC_TYPE = "kreisschreiben"
    REQUEST_DELAY = 1.0

    def __init__(self, languages: tuple[str, ...] = ("de", "fr", "it")):
        super().__init__()
        self.languages = languages

    def _make_doc_id(self, stub: dict) -> str:
        # Key by the PDF filename stem (unique per PDF): this distinguishes
        # languages (-de/-fr/-it) AND annexes (one KS ships as main + Anhänge +
        # FAQ sharing a number). A doc_number-based id would collapse those and
        # silently drop the FR/IT versions + the annexes.
        from .base import slugify
        stem = stub["pdf_url"].rsplit("/", 1)[-1].rsplit(".", 1)[0]
        return f"{self.SOURCE_KEY}_{slugify(stem)}"

    def discover_documents(self) -> Iterator[dict]:
        for src in INDEX_SOURCES:
            lang = src["lang"]
            if lang not in self.languages:
                continue
            url = src["url"]
            try:
                r = self.get(url, headers={"Accept-Language": f"{lang}-CH,{lang};q=0.9"})
                r.raise_for_status()
            except Exception as e:
                logger.warning("ESTV-KS [%s/%s] index fetch failed: %s",
                               src["tax_type"], lang, e)
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/dam/" not in href or not href.lower().endswith(".pdf"):
                    continue
                pdf_url = urljoin(_BASE, href)
                filename = pdf_url.rsplit("/", 1)[-1]

                title = re.sub(r"\s+", " ", a.get_text(" ", strip=True)) or "(untitled)"

                m_url = _KS_NUM_FROM_URL.search(pdf_url)
                m_txt = _KS_NUM_FROM_TEXT.search(title)
                if m_url:
                    doc_number = f"KS Nr. {int(m_url.group(2))}{m_url.group(3)}"
                elif m_txt:
                    doc_number = f"KS Nr. {m_txt.group(1)}"
                else:
                    doc_number = filename.replace(".pdf", "")[:60]

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
                    "topics": _topics_from_filename(filename) or ["Bundessteuern"],
                }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    EstvKreisschreibenScraper().run()
