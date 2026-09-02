"""
BJ Oberaufsicht SchKG — Weisungen, kantonale Kreisschreiben, Kreisschreiben
des Bundes
==========================================================================

The Federal Office of Justice's high-supervision pages for debt enforcement:

  Weisungen (Nr. 1-11 + Anhänge)   /de/weisungen-schkg  /fr/instructions-lp  /it/istruzioni-lef
  kantonale Kreisschreiben          /de/kreisschreiben-schkg  /fr/circulaires-lp  /it/circolari-lef (no PDFs)
      42 DE + 12 FR files named NN-xx-ks-l.pdf (xx = canton) — Obergerichts-
      Kreisschreiben, Existenzminimum-Richtlinien (ZH, BL, GL, LU, NW, OW, SH,
      SZ, AI), a few cantonal SchKG-Erlasse and a Konkordat
  Kreisschreiben des Bundes         /de/kreisschreiben-des-bundes  /fr/circulaires-de-la-confederation
      historical Bundesgericht (SchKK) circulars 1892-2004, files NN-ks[-variant].pdf

doc_type and doc_number are read from the ANCHOR TEXT, not the filename: the
cantonal page mixes Weisungen, Richtlinien, Kreisschreiben, an
Einführungsgesetz, a Verordnung and a Konkordat, and get_practice prints the
label. `date` is the FIRST date in the anchor text = the issuance date
("Weisung Nr. 7 vom 16. April 2020 … PDF 200 kB 6. April 2020" -> 2020-04-16);
the trailing file date is ignored. ZIP annexes are skipped and counted.
DAM-hashed URLs -> REVISION_FIELD="pdf_url".
"""
from __future__ import annotations

import logging
import re
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import PracticeScraper, first_date_iso, slugify

logger = logging.getLogger(__name__)

_BASE = "https://www.bj.admin.ch"
# (page_kind, lang, path)
PAGES = (
    ("weisungen", "de", "/de/weisungen-schkg"),
    ("weisungen", "fr", "/fr/instructions-lp"),
    ("weisungen", "it", "/it/istruzioni-lef"),
    ("kreisschreiben_kantone", "de", "/de/kreisschreiben-schkg"),
    ("kreisschreiben_kantone", "fr", "/fr/circulaires-lp"),
    ("kreisschreiben_kantone", "it", "/it/circolari-lef"),
    ("kreisschreiben_bund", "de", "/de/kreisschreiben-des-bundes"),
    ("kreisschreiben_bund", "fr", "/fr/circulaires-de-la-confederation"),
)
_PAGE_DEFAULT_TYPE = {"weisungen": "weisung", "kreisschreiben_kantone": "kreisschreiben",
                      "kreisschreiben_bund": "kreisschreiben"}

_CANTONAL_STEM = re.compile(r"^(\d{2})-([a-z]{2})-ks-([dfi])$", re.IGNORECASE)
_FEDERAL_STEM = re.compile(r"^(\d{2})-ks(?:-([a-z]+))?$", re.IGNORECASE)
_WEISUNG_STEM = re.compile(r"^weisung-(\d+)(-anh[aä]ng[e]?)?$", re.IGNORECASE)
_WEISUNG_NR = re.compile(r"(?:Weisung|Instruction|Istruzione)\s+(?:Nr\.?|n[o°]\.?|n\.)\s*(\d+)", re.IGNORECASE)
_TAIL = re.compile(r"\s*(?:PDF|ZIP)\s+[\d.,']+\s*[kM]B.*$", re.IGNORECASE)
_ISSUER = re.compile(r"(?:erlassen von|édicté(?:e)? par|emanat[oa] da)\s*:\s*([^|]+?)(?:\s+PDF|\s*$)", re.IGNORECASE)
_EXISTENZMINIMUM = re.compile(r"Existenzminimum|Notbedarf|minimum vital|minimo vitale", re.IGNORECASE)
_OBSOLET = re.compile(r"\(obsol[eè]t[aeo]?\)", re.IGNORECASE)

_TYPE_RULES = (
    # Anchored first-word rules first: "Weisung vom … betr. Anwendung der
    # Richtlinien" is a Weisung, "KS der VK des OG … Richtlinien für die
    # Berechnung …" is a Kreisschreiben. Unanchored keyword rules follow.
    (re.compile(r"^(?:Weisung|Instruction|Istruzion|Directive|Direttiv)", re.I), "weisung"),
    (re.compile(r"^(?:Kreisschreiben|KS\b|Circulaire|Circolare)", re.I), "kreisschreiben"),
    (re.compile(r"^(?:Richtlinien?|Lignes directrices|Linee guida)", re.I), "richtlinie"),
    (re.compile(r"^(?:Anhang|Annexe|Allegato)\b", re.I), "weisung_anhang"),
    (re.compile(r"Konkordat|Beitritt .* Konkordat|concordat", re.I), "konkordat"),
    (re.compile(r"Einführungsgesetz|Verordnung zum|Gesetz über|Loi d'application|Legge di applicazione", re.I), "erlass"),
    (re.compile(r"^(?:Beschluss|Décision|Decisione)", re.I), "kreisschreiben"),
    (re.compile(r"^(?:Mitteilung|Communication|Comunicazione)", re.I), "mitteilung"),
    (re.compile(r"Richtlinien?|Lignes directrices|Linee guida", re.I), "richtlinie"),
    (re.compile(r"Kreisschreiben|\bKS\b|Circulaire|Circolare", re.I), "kreisschreiben"),
    (re.compile(r"Mitteilung|Communication|Comunicazione", re.I), "mitteilung"),
)


def _clean(text: str) -> str:
    return " ".join((text or "").split())


def _doc_type(text: str, page_kind: str) -> str:
    for rx, kind in _TYPE_RULES:
        if rx.search(text):
            return kind
    return _PAGE_DEFAULT_TYPE[page_kind]


def parse_page(html: str, page_kind: str, lang: str, page_url: str) -> tuple[list[dict], int]:
    """Return (stubs, zip_count)."""
    soup = BeautifulSoup(html, "html.parser")
    stubs: list[dict] = []
    seen: set[str] = set()
    zips = 0
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/dam/" not in href:
            continue
        low = href.lower()
        if low.endswith(".zip"):
            zips += 1
            continue
        if not low.endswith(".pdf"):
            continue
        pdf_url = urljoin(_BASE, href)
        if pdf_url in seen:
            continue
        seen.add(pdf_url)
        stem = pdf_url.rsplit("/", 1)[-1][:-4]
        raw = _clean(a.get_text(" ", strip=True))
        title = _TAIL.sub("", raw).strip()
        doc_type = _doc_type(title, page_kind)
        topics = ["SchKG", "Schuldbetreibung und Konkurs"]
        canton = "CH"
        mc = _CANTONAL_STEM.match(stem)
        mf = _FEDERAL_STEM.match(stem)
        mw = _WEISUNG_STEM.match(stem)
        if mc:
            canton = mc.group(2).upper()
            label = {"weisung": "Weisung", "richtlinie": "Richtlinien", "konkordat": "Konkordat",
                     "erlass": "Erlass", "mitteilung": "Mitteilung"}.get(doc_type, "KS")
            doc_number = f"{canton} {label} {int(mc.group(1))}"
        elif mw:
            n = int(mw.group(1))
            doc_number = f"Weisung {n}" + (" Anhang" if mw.group(2) else "")
            if mw.group(2):
                doc_type = "weisung_anhang"
        elif mf:
            doc_number = f"BGer KS {int(mf.group(1))}" + (f" ({mf.group(2)})" if mf.group(2) else "")
            topics.append("historisch")
        else:
            mn = _WEISUNG_NR.search(raw)
            doc_number = f"Weisung {mn.group(1)}" if mn else slugify(stem)[:40]
        topics.append(canton)
        mi = _ISSUER.search(raw)
        if mi:
            topics.append(_clean(mi.group(1)))
        if _EXISTENZMINIMUM.search(raw):
            topics.append("Existenzminimum")
        if _OBSOLET.search(raw):
            topics.append("obsolet")
        stubs.append({
            "pdf_url": pdf_url,
            "url": page_url,
            "title": title,
            "doc_number": doc_number,
            "date": first_date_iso(raw),
            "language": lang,
            "doc_type": doc_type,
            "topics": topics,
            "bj_stem": stem,
        })
    return stubs, zips


class BjSchkgScraper(PracticeScraper):
    SOURCE_KEY = "bj_schkg"
    ISSUING_AUTHORITY = "BJ"
    DEFAULT_DOC_TYPE = "weisung"
    REVISION_FIELD = "pdf_url"
    REQUEST_DELAY = 1.5
    NO_TEXT_LAYER_BODY = "[Textlayer fehlt: gescanntes PDF]"

    def _make_doc_id(self, stub: dict) -> str:
        return f"{self.SOURCE_KEY}_{slugify(stub['bj_stem'])}_{stub.get('language', 'de')}"

    def discover_documents(self) -> Iterator[dict]:
        for page_kind, lang, path in PAGES:
            url = _BASE + path
            try:
                r = self.get(url)
                r.raise_for_status()
            except Exception as e:
                logger.warning("[%s] %s fetch failed: %s", self.SOURCE_KEY, url, e)
                continue
            stubs, zips = parse_page(r.text, page_kind, lang, url)
            logger.info("[%s] %s [%s]: %d PDFs, %d ZIP annexes skipped",
                        self.SOURCE_KEY, page_kind, lang, len(stubs), zips)
            yield from stubs
