"""
SECO Arbeitslosenversicherung — AVIG-Praxis (Weisungen ALE, KAE, SWE, RVEI,
IE, AMM, AVG öAV, Datenschutzleitfaden, thematic Weisungen YYYY/NN)
==============================================================================

arbeit.swiss publications page, section "Weisungen / AVIG-Praxis / Richtlinien"
(FR "Directives / Bulletins LACI / Lignes directrices", IT "Direttive / Prassi
LADI / Linee guida"). The old secoalv/… URLs 301 to these pages.

  de  https://www.arbeit.swiss/de/informationszentrum/publikationen
  fr  https://www.arbeit.swiss/fr/publications-fr
  it  https://www.arbeit.swiss/it/pubblicazioni

PDF links are UUID media URLs (/api/media/fileservice/…/<uuid>) with no
filename; the UUID changes on every re-issue, so REVISION_FIELD="pdf_url"
detects the half-yearly editions (1 Jan / 1 Jul) and the base cache is safe.
SECO does not keep an archive of superseded editions online, so this corpus
holds every edition from its first ingest onward.

doc_id must be stable across editions: the anchor text carries the validity
clause ("gültig ab 1.7.2026"), which is stripped before any slug is built, and
the stable code (AVIG ALE, LACI IC, LADI ID, Weisung 2026/01, VO 883/2004) is
preferred over the title.
"""
from __future__ import annotations

import logging
import re
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import PracticeScraper, first_date_iso, slugify

logger = logging.getLogger(__name__)

_BASE = "https://www.arbeit.swiss"
PAGES = {
    "de": "/de/informationszentrum/publikationen",
    "fr": "/fr/publications-fr",
    "it": "/it/pubblicazioni",
}
_SECTION_H2 = re.compile(r"AVIG-Praxis|LACI|LADI", re.IGNORECASE)
_TAIL = re.compile(r"\s*PDF(?:\s*\|\s*\d{1,2}\.\d{1,2}\.\d{4})?\s*$")
_VALIDITY = re.compile(
    r"(?:\s*,?\s*\d+[ªa]?\s*(?:edizione|édition|Auflage),?)?"
    r"\s*(?:gültig ab|valable dès|valable des|valida dal|Stand\s*:?|État\s*:|Stato\s*:?)\s*"
    r"\d{1,2}\.\d{1,2}\.\d{4}.*$",
    re.IGNORECASE,
)
# The code token must look like a code (SWE, RVEI, öAV, SPC), not a
# connective: "… in den Bereichen AVIG und AVG" must not yield "AVIG und".
# Anchored to the document word: "Weisung AVIG ALE (…)" / "Directive LACI IC" /
# "Direttiva LADI ID". Unanchored, a thematic Weisung that merely mentions
# "AVIG Art. 65" in its title would mint the id "AVIG Art" and collide.
_CODE = re.compile(r"^(?:Weisung|Directive|Direttiva)\s+(AVIG|AVG|LACI|LSE|LADI|LC)\s+"
                   r"((?:[A-ZÖÄÜ]|ö)[A-Za-zÀ-ÿ]{1,5})\b")
_LEITFADEN = re.compile(r"Datenschutzleitfaden|Guide relatif à la protection des données"
                        r"|Guida alla protezione dei dati|Leitfaden zur Bearbeitung von Personendaten"
                        r"|Guide pour le traitement des données|Guida al trattamento dei dati", re.I)
_NUMBERED = re.compile(r"\b(?:Weisung|Directive|Direttiva)\s+(\d{4}/\d{2})\b", re.IGNORECASE)
_VO = re.compile(r"\b(\d{3,4}/\d{4})\b")
_FAMILY = {"AVIG": "AVIG", "LACI": "AVIG", "LADI": "AVIG", "AVG": "AVG", "LSE": "AVG", "LC": "AVG"}


def _clean(text: str) -> str:
    return " ".join((text or "").split())


def stable_code(title: str) -> str:
    """'Weisung AVIG ALE (Arbeitslosenentschädigung) (AVIG-Praxis ALE) gültig
    ab 1.7.2026' -> 'AVIG ALE'; 'Weisung 2026/01: …' -> 'Weisung 2026/01';
    'Weisung über die Auswirkungen der Verordnungen (EG) Nr. 883/2004 …' ->
    'VO 883/2004'; else the title without its validity clause."""
    t = _clean(title)
    if _LEITFADEN.search(t):
        return "Datenschutzleitfaden AVIG AVG"
    m = _NUMBERED.search(t)          # "Weisung 2026/01: …" before any code
    if m:
        return f"Weisung {m.group(1)}"
    m = _CODE.search(t)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    m = _VO.search(t)
    if m:
        return f"VO {m.group(1)}"
    return _VALIDITY.sub("", t).strip(" :–-")


def parse_page(html: str, lang: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    h2 = None
    for cand in soup.find_all("h2"):
        if _SECTION_H2.search(cand.get_text(" ", strip=True)):
            h2 = cand
            break
    if h2 is None:
        logger.warning("[seco_alv] no AVIG-Praxis section on %s", page_url)
        return []
    stubs: list[dict] = []
    seen: set[str] = set()
    node = h2.next_sibling
    while node is not None and getattr(node, "name", None) != "h2":
        if getattr(node, "find_all", None):
            for a in node.find_all("a", href=True):
                href = a["href"]
                if "fileservice" not in href:
                    continue
                pdf_url = urljoin(_BASE, href)
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                raw = _clean(a.get_text(" ", strip=True))
                title = _TAIL.sub("", raw).strip()
                code = stable_code(title)
                fam = _FAMILY.get(code.split(" ", 1)[0], "AVIG")
                stubs.append({
                    "pdf_url": pdf_url,
                    "url": page_url,
                    "title": title,
                    "doc_number": code,
                    "date": first_date_iso(raw),
                    "language": lang,
                    "doc_type": "weisung",
                    "topics": [fam, code, "Arbeitslosenversicherung"],
                })
        node = node.next_sibling
    return stubs


class SecoAlvScraper(PracticeScraper):
    SOURCE_KEY = "seco_alv"
    ISSUING_AUTHORITY = "SECO"
    DEFAULT_DOC_TYPE = "weisung"
    REVISION_FIELD = "pdf_url"
    REQUEST_DELAY = 1.5
    LANGUAGES = ("de", "fr", "it")

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
            logger.info("[%s] %s: %d AVIG-Praxis documents", self.SOURCE_KEY, lang, len(stubs))
            yield from stubs
