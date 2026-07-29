"""
SECO Wegleitungen zum Arbeitsgesetz (ArG + ArGV 1-5)
=====================================================

SECO's article-by-article commentary on the Arbeitsgesetz and its five
ordinances — the operative interpretation of Swiss employment-protection
law, cited constantly in practice and, until now, absent from the corpus.

Structure: eight index pages per language, each a flat list of per-article
PDFs, plus the consolidated Gesamtdokumente.

  /{lang-slug}/wegleitungen                  4 Gesamtdokumente + 3 Änderungslisten
  /{lang-slug}/arbeitsgesetz                 ArG Art. 1-71
  /{lang-slug}/wegleitung-argv-{1..5}        per-ordinance articles
  /{lang-slug}/wegleitung-gefaehrliche-...   WBF-VO gefährliche Arbeiten

Anchor text carries everything we need:
  "ArG Artikel 2: Ausnahmen vom betrieblichen Geltungsbereich
   PDF 726.59 kB 18. November 2025"

PDFs live at /dam/{lang}/sd-web/{HASH}/{Erlass}-Artikel-{NN}-SECO-AB-{YYYY}-{LANG}.pdf
and — usefully — the DAM hash is IDENTICAL across languages, so the three
language versions of one article are trivially linkable.

Re-issues: when SECO revises an article both the year in the filename and
the DAM hash change, while our doc_id (erlass + article + language) stays
stable. REVISION_FIELD="pdf_url" makes the base class re-fetch instead of
skipping the revision forever.

robots.txt (verified 2026-07-29): "User-agent: * / Disallow:" — fully
permissive, sitemap advertised.
"""
from __future__ import annotations

import logging
import re
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import PracticeScraper

logger = logging.getLogger(__name__)

_BASE = "https://www.seco.admin.ch"

# Index pages per language. The slugs are NOT machine-translatable — note
# FR is "loi-sur-travail" (no article), which is easy to get wrong, and the
# law-text pages ("...-et-ordonnances") are a DIFFERENT thing we must not
# scrape as commentary.
INDEX_PAGES: dict[str, list[tuple[str, str]]] = {
    "de": [
        ("wegleitungen", "Gesamtdokumente"),
        ("arbeitsgesetz", "ArG"),
        ("wegleitung-argv-1", "ArGV 1"),
        ("wegleitung-argv-2", "ArGV 2"),
        ("wegleitung-argv-3", "ArGV 3"),
        ("wegleitung-argv-4", "ArGV 4"),
        ("wegleitung-argv-5", "ArGV 5"),
        ("wegleitung-gefaehrliche-arbeiten-fuer-jugendliche", "WBF-VO"),
    ],
    "fr": [
        ("commentaires", "Gesamtdokumente"),
        ("loi-sur-travail", "ArG"),
        ("commentaire-olt-1", "ArGV 1"),
        ("commentaire-olt-2", "ArGV 2"),
        ("commentaire-olt-3", "ArGV 3"),
        ("commentaire-olt-4", "ArGV 4"),
        ("commentaire-olt-5", "ArGV 5"),
        ("articles-travaux-dangereux-jeunes", "WBF-VO"),
    ],
    "it": [
        ("indicazioni", "Gesamtdokumente"),
        ("legge-sul-lavoro", "ArG"),
        ("indicazioni-oll-1", "ArGV 1"),
        ("indicazioni-oll-2", "ArGV 2"),
        ("indicazioni-oll-3", "ArGV 3"),
        ("indicazioni-oll-4", "ArGV 4"),
        ("indicazioni-oll-5", "ArGV 5"),
        ("articolo-lavori-pericolosi-giovani", "WBF-VO"),
    ],
}

# Localized long-form dates in the anchor text ("18. November 2025",
# "18 novembre 2025"). FR and IT share several month names, which is fine —
# we look up per language.
_MONTHS: dict[str, dict[str, int]] = {
    "de": {"januar": 1, "februar": 2, "märz": 3, "maerz": 3, "april": 4, "mai": 5,
           "juni": 6, "juli": 7, "august": 8, "september": 9, "oktober": 10,
           "november": 11, "dezember": 12},
    "fr": {"janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
           "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
           "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12},
    "it": {"gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5,
           "giugno": 6, "luglio": 7, "agosto": 8, "settembre": 9, "ottobre": 10,
           "novembre": 11, "dicembre": 12},
}
_DATE_RE = re.compile(r"(\d{1,2})\.?\s+([A-Za-zÀ-ÿ]+)\s+(\d{4})")

# Filename shapes:
#   ArG-Artikel-02-SECO-AB-2025-DE.pdf      (current convention)
#   ArGV-1-2-SECO-AB-2026-DE.pdf            (Gesamtdokument)
#   ArGV1_art02_de.pdf                      (legacy)
_ART_FROM_NAME = re.compile(r"Artikel[-_]?(\d{1,3}[a-z]?)", re.IGNORECASE)
_ART_LEGACY = re.compile(r"art\.?[-_]?(\d{1,3}[a-z]?)", re.IGNORECASE)
# "ArG Artikel 2:" / "LTr Article 2:" / "LL Articolo 2:" in the anchor text
# Annex marker in the filename: "ArGV3_Anhang_Artikel-02-…",
# "ArGV2-Anhang3-2-Einkaufszentren-…", "ArGV3_AnhangArtikel-15-…".
# The optional capture is the annex's OWN number (3-2 -> 3.2).
_ANNEX = re.compile(r"Anhang[-_]?(\d+(?:[-_]\d+)?)?", re.IGNORECASE)
_ART_FROM_TEXT = re.compile(
    r"\b(?:Artikel|Article|Articolo|Art\.)\s*(\d{1,3}[a-z]?)\b", re.IGNORECASE)
# trailing "PDF 726.59 kB 18. November 2025" noise on the title
_TITLE_TAIL = re.compile(
    r"\s*PDF\s+[\d'’.,]+\s*[kKMG]?B.*$", re.IGNORECASE | re.DOTALL)

_ERLASS_SR = {
    "ArG": "822.11", "ArGV 1": "822.111", "ArGV 2": "822.112",
    "ArGV 3": "822.113", "ArGV 4": "822.114", "ArGV 5": "822.115",
    "WBF-VO": "822.115.2",
}


class SecoArgScraper(PracticeScraper):
    """SECO commentary on the Arbeitsgesetz and its ordinances."""

    SOURCE_KEY = "seco_arg"
    ISSUING_AUTHORITY = "SECO"
    DEFAULT_DOC_TYPE = "wegleitung"
    REQUEST_DELAY = 1.5
    # SECO revises articles in place under a new DAM hash; without this the
    # first edition we ever fetched would be the last one we ever hold.
    REVISION_FIELD = "pdf_url"

    LANGUAGES = ("de", "fr", "it")

    # ── helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _parse_date(text: str, lang: str) -> str:
        m = _DATE_RE.search(text or "")
        if not m:
            return ""
        day, month_word, year = m.group(1), m.group(2).lower(), m.group(3)
        month = _MONTHS.get(lang, {}).get(month_word)
        if not month:
            # A French page can carry a German month name and vice versa;
            # fall back across all maps rather than dropping the date.
            for table in _MONTHS.values():
                if month_word in table:
                    month = table[month_word]
                    break
        if not month:
            return year  # year-only is still citable
        return f"{year}-{month:02d}-{int(day):02d}"

    @staticmethod
    def _clean_title(text: str) -> str:
        return _TITLE_TAIL.sub("", " ".join((text or "").split())).strip(" :–-")

    @staticmethod
    def _article_of(href: str, text: str) -> str:
        for rx in (_ART_FROM_NAME, _ART_LEGACY):
            m = rx.search(href or "")
            if m:
                return m.group(1).lstrip("0") or "0"
        m = _ART_FROM_TEXT.search(text or "")
        return (m.group(1).lstrip("0") or "0") if m else ""

    def _make_doc_id(self, stub: dict) -> str:
        # doc_number already encodes erlass + article; language must be part
        # of the id because the same article exists three times.
        from .base import slugify
        return (f"{self.SOURCE_KEY}_{slugify(stub.get('doc_number') or stub.get('title',''))}"
                f"_{stub.get('language', 'de')}")

    # ── discovery ───────────────────────────────────────────────────

    def discover_documents(self) -> Iterator[dict]:
        for lang in self.LANGUAGES:
            for slug, erlass in INDEX_PAGES.get(lang, []):
                page_url = f"{_BASE}/{lang}/{slug}"
                try:
                    r = self.get(page_url)
                    r.raise_for_status()
                except Exception as e:
                    logger.warning("[seco_arg] index fetch failed %s: %s", page_url, e)
                    continue
                yield from self._parse_index(r.text, page_url, lang, erlass)

    def _parse_index(self, html: str, page_url: str, lang: str,
                     erlass: str) -> Iterator[dict]:
        soup = BeautifulSoup(html, "html.parser")
        seen_hrefs: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower():
                continue
            pdf_url = urljoin(_BASE, href)
            if pdf_url in seen_hrefs:
                continue
            seen_hrefs.add(pdf_url)

            raw = a.get_text(" ", strip=True)
            title = self._clean_title(raw)
            if not title:
                continue
            article = self._article_of(href, raw)
            annex = _ANNEX.search(href or "")
            if annex:
                # Annexes are substantive documents in their own right and
                # collide with their parent article: ArGV3-Artikel-02-… and
                # ArGV3_Anhang_Artikel-02-… both yield article "02". Caught in
                # the first full run — 18 annexes across DE/FR/IT would have
                # been swallowed by the doc_id upsert. Prefer the annex's own
                # number (Anhang3-2 -> "Anhang 3.2"); otherwise mark it as the
                # annex to its article.
                num = annex.group(1)
                if num:
                    doc_number = f"{erlass} Anhang {num.replace('-', '.').replace('_', '.')}"
                elif article:
                    doc_number = f"{erlass} Art. {article} Anhang"
                else:
                    doc_number = f"{erlass} Anhang"
            elif article:
                doc_number = f"{erlass} Art. {article}"
            else:
                # Gesamtdokument / Änderungsliste: no article to key on. Title
                # prefixes are NOT distinguishing — "Wegleitung zum
                # Arbeitsgesetz und den Verordnungen 1 und 2" and "… 3 und 4"
                # share their first 40 chars and collided on doc_id (caught in
                # the first live run: the second silently overwrote the first).
                # The PDF filename stem is unique and stable, so key on it.
                stem = pdf_url.rsplit("/", 1)[-1]
                stem = re.sub(r"\.pdf$", "", stem, flags=re.IGNORECASE)
                stem = re.sub(r"-SECO-AB-\d{4}-[A-Z]{2}$", "", stem, flags=re.IGNORECASE)
                doc_number = f"{erlass} {stem}".strip()
            doc_type = "wegleitung"

            topics = [erlass]
            sr = _ERLASS_SR.get(erlass)
            if sr:
                topics.append(f"SR {sr}")

            yield {
                "pdf_url": pdf_url,
                "url": page_url,
                "title": title,
                "doc_number": doc_number,
                "doc_type": doc_type,
                "date": self._parse_date(raw, lang),
                "language": lang,
                "topics": topics,
            }


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    print(SecoArgScraper().run(max_new=5))
