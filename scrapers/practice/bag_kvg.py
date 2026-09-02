"""
BAG Kreisschreiben zur Krankenversicherung (KVG / KVAG)
=======================================================

  de  https://www.bag.admin.ch/de/krankenversicherung-kreisschreiben-schweiz
  fr  https://www.bag.admin.ch/fr/assurance-maladie-circulaires-suisse
  (the IT page exists but links no PDFs)

19 Kreisschreiben (Nr. 1.1 … 7.10) on insurer obligations, premiums, special
insurance forms, revision/reporting, data protection, Akteneinsicht,
observation. NOTE: nothing here is about individuelle Prämienverbilligung
(IPV) — that is cantonal execution and remains uncovered; the tool
description says so.

`date`: the anchor text sometimes states the issuance date ("vom
2014.10.14") and always the file date ("17. Dezember 2021", a site-migration
date shared by many files). first_date_iso takes the issuance date when
present, else the file date. PDF URLs are DAM-hashed and change on re-issue
-> REVISION_FIELD="pdf_url".
"""
from __future__ import annotations

import logging
import re
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import PracticeScraper, first_date_iso, slugify

logger = logging.getLogger(__name__)

_BASE = "https://www.bag.admin.ch"
PAGES = {
    "de": "/de/krankenversicherung-kreisschreiben-schweiz",
    "fr": "/fr/assurance-maladie-circulaires-suisse",
}
# The number must follow the document word or "Nr." — a date-first anchor
# ("… vom 1.7.2026 …") would otherwise yield a bogus "KS 1.7".
_NUM = re.compile(r"(?:Kreisschreiben|Circulaire|Circolare)\s+(?:(?:Nr\.?|n[o°]\.?|n\.)\s*)?(\d{1,2}\.\d{1,2})\b",
                  re.IGNORECASE)
_TAIL = re.compile(r"\s*PDF\s+[\d.,']+\s*[kM]B.*$", re.IGNORECASE)


def _clean(text: str) -> str:
    return " ".join((text or "").split())


def parse_page(html: str, lang: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    stubs: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/dam/" not in href or not href.lower().endswith(".pdf"):
            continue
        pdf_url = urljoin(_BASE, href)
        if pdf_url in seen:
            continue
        seen.add(pdf_url)
        raw = _clean(a.get_text(" ", strip=True))
        m = _NUM.search(raw)
        if not m:
            logger.info("[bag_kvg] no KS number in %r — skipped", raw[:80])
            continue
        num = m.group(1)
        title = _TAIL.sub("", raw).strip()
        stubs.append({
            "pdf_url": pdf_url,
            "url": page_url,
            "title": title,
            "doc_number": f"KS {num}",
            "date": first_date_iso(raw),
            "language": lang,
            "doc_type": "kreisschreiben",
            "topics": ["KVG", "Krankenversicherung", f"KS {num}"],
        })
    return stubs


class BagKvgScraper(PracticeScraper):
    SOURCE_KEY = "bag_kvg"
    ISSUING_AUTHORITY = "BAG"
    DEFAULT_DOC_TYPE = "kreisschreiben"
    REVISION_FIELD = "pdf_url"
    REQUEST_DELAY = 1.5
    LANGUAGES = ("de", "fr")
    NO_TEXT_LAYER_BODY = "[Textlayer fehlt: gescanntes PDF]"

    def _make_doc_id(self, stub: dict) -> str:
        return f"{self.SOURCE_KEY}_{slugify(stub['doc_number'])}_{stub.get('language', 'de')}"

    def discover_documents(self) -> Iterator[dict]:
        for lang in self.LANGUAGES:
            url = _BASE + PAGES[lang]
            try:
                r = self.get(url)
                r.raise_for_status()
            except Exception as e:
                logger.warning("[%s] %s fetch failed: %s", self.SOURCE_KEY, url, e)
                continue
            stubs = parse_page(r.text, lang, url)
            logger.info("[%s] %s: %d Kreisschreiben", self.SOURCE_KEY, lang, len(stubs))
            yield from stubs
