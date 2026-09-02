"""
SEM Handbuch Asyl und Rückkehr
==============================

The State Secretariat for Migration's procedural handbook for the asylum and
return process — 46 articles in nine chapters (A völkerrechtliche Grundlagen,
B Verfahrensgrundsätze, C Asylverfahren, D Flüchtlingseigenschaft, E Entscheid
und Wegweisung, F besondere Verfahren, G Vollzug und Rückkehr, H Rechtsmittel,
I Sprache und Stil), each a separately dated PDF.

  {lang} https://www.sem.admin.ch/sem/{lang}/home/asyl/asylverfahren/nationale-verfahren/handbuch-asyl-rueckkehr.html
  DE and FR carry the PDFs; the IT page links none (tolerated: 0 stubs).

Not covered by sem_weisungen.py, which reads the Weisungen/Kreisschreiben hub
only (a different path). The article code for the doc_id comes from the
filename segment (hb-art-c61) because the FR page labels the last article
"Article I2" where DE says "Artikel I1" — both link hb-art-i1.

The PDF URL is STABLE across re-issues (…/hb-art-c10.pdf.download.pdf/…), so
two things differ from the DAM-hashed sources: REVISION_FIELD is the anchor
date, and CACHE_PDFS is off (a cache hit would re-index the superseded text
after a re-issue). An anchor with no parseable date never counts as a
re-issue, so a page regression cannot blank a stored date.
"""
from __future__ import annotations

import logging
import re
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import PracticeScraper

logger = logging.getLogger(__name__)

_BASE = "https://www.sem.admin.ch"
PAGE = "/sem/{lang}/home/asyl/asylverfahren/nationale-verfahren/handbuch-asyl-rueckkehr.html"
LANGUAGES = ("de", "fr", "it")

_FILE_CODE = re.compile(r"hb-art-([a-i]\d+)", re.IGNORECASE)
_LABEL_CODE = re.compile(r"(?:Artikel|Article|Articolo)\s+([A-I]\d+(?:\.\d+)?)", re.IGNORECASE)
_DATE = re.compile(r"\(PDF,\s*[\d.,']+\s*[kM]B,\s*(\d{1,2})\.(\d{1,2})\.(\d{4})\)")
_SUFFIX = re.compile(r"\s*\(PDF,[^)]*\)\s*$")

# Descriptive chapter labels (the page itself has no chapter headings).
CHAPTERS = {
    "A": "Völkerrechtliche Grundlagen",
    "B": "Verfahrensgrundsätze",
    "C": "Asylverfahren",
    "D": "Flüchtlingseigenschaft und Asylgründe",
    "E": "Asylentscheid, Wegweisung und vorläufige Aufnahme",
    "F": "Besondere Verfahren",
    "G": "Vollzug und Rückkehr",
    "H": "Rechtsmittel",
    "I": "Sprache und Stil",
}


def _clean(text: str) -> str:
    return " ".join((text or "").split())


def _display_code(file_code: str, label_code: str) -> str:
    """'c61' + 'C6.1' -> 'C6.1'; 'i1' + 'I2' (FR outlier) -> 'I1'."""
    fc = file_code.upper()
    if label_code and re.sub(r"\.", "", label_code.upper()) == fc:
        return label_code.upper()
    # Without a label, "c10" and "c61" are ambiguous (C10 vs C6.1): keep the
    # file code as-is rather than guessing a dotted form.
    return fc


def parse_page(html: str, lang: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    stubs: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "hb-art" not in href or not href.lower().endswith(".pdf"):
            continue
        pdf_url = urljoin(_BASE, href)
        if pdf_url in seen:
            continue
        seen.add(pdf_url)
        m = _FILE_CODE.search(href)
        if not m:
            continue
        file_code = m.group(1).lower()
        raw = _clean(a.get_text(" ", strip=True))
        lm = _LABEL_CODE.search(raw)
        code = _display_code(file_code, lm.group(1) if lm else "")
        dm = _DATE.search(raw)
        date = f"{dm.group(3)}-{int(dm.group(2)):02d}-{int(dm.group(1)):02d}" if dm else ""
        chapter = code[0]
        stubs.append({
            "pdf_url": pdf_url,
            "url": page_url,
            "title": _SUFFIX.sub("", raw).strip(),
            "doc_number": f"Art. {code}",
            "date": date,
            "language": lang,
            "doc_type": "handbuch",
            "topics": ["Handbuch Asyl und Rückkehr", "Asyl",
                       f"Kapitel {chapter}: {CHAPTERS.get(chapter, '')}".rstrip(": "),
                       f"Artikel {code}"],
            "hb_code": file_code,
        })
    return stubs


class SemHandbuchAsylScraper(PracticeScraper):
    SOURCE_KEY = "sem_handbuch_asyl"
    ISSUING_AUTHORITY = "SEM"
    DEFAULT_DOC_TYPE = "handbuch"
    REVISION_FIELD = "date"
    CACHE_PDFS = False
    REQUEST_DELAY = 1.5
    LANGUAGES = LANGUAGES

    def _make_doc_id(self, stub: dict) -> str:
        return f"{self.SOURCE_KEY}_{stub['hb_code']}_{stub.get('language', 'de')}"

    def _is_reissue(self, doc_id: str, stub: dict) -> bool:
        # An anchor whose date failed to parse is "unknown", not "revised".
        if not stub.get("date"):
            return False
        return super()._is_reissue(doc_id, stub)

    def discover_documents(self) -> Iterator[dict]:
        for lang in self.LANGUAGES:
            url = _BASE + PAGE.format(lang=lang)
            try:
                r = self.get(url)
                r.raise_for_status()
            except Exception as e:
                logger.warning("[%s] %s fetch failed: %s", self.SOURCE_KEY, url, e)
                continue
            stubs = parse_page(r.text, lang, url)
            logger.info("[%s] %s: %d Handbuch articles", self.SOURCE_KEY, lang, len(stubs))
            yield from stubs
