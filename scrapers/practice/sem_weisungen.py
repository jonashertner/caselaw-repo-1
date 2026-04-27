"""
SEM Weisungen und Kreisschreiben
================================

State Secretariat for Migration. The most comprehensive federal
Verwaltungspraxis corpus — covers AIG, Asyl, BüG (Bürgerrecht), FZA,
Visa, Integration, Datenschutz/Öffentlichkeitsgesetz.

Index page (DE):
  https://www.sem.admin.ch/sem/de/home/publiservice/weisungen-kreisschreiben.html

Section structure:
  I.   Ausländerbereich          → AIG-Weisungen (chapters 0-11)
  II.  Freizügigkeitsabkommen     → FZA-Rundschreiben
  III. Asylgesetz                 → Asyl-Handbuch
  IV.  Integration
  V.   Bürgerrecht                → BüG-Handbuch + Rundschreiben
  VI.  Datenschutz und ÖG
  VII. Visa                       → Visa-Handbuch
  VIII Weitere Weisungen

Each section is a sub-page listing PDFs (some single multi-MB files,
some chapter-split, some thematic Rundschreiben).

PDF naming: /dam/sem/de/data/rechtsgrundlagen/weisungen/{topic}/*.pdf
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
INDEX_URL = "https://www.sem.admin.ch/sem/de/home/publiservice/weisungen-kreisschreiben.html"

SECTIONS = {
    "auslaenderbereich":        ("AIG / Ausländerbereich",  "weisung"),
    "fza":                      ("FZA / Freizügigkeitsabkommen", "rundschreiben"),
    "asylgesetz":               ("Asylgesetz",               "weisung"),
    "integration":              ("Integration",              "weisung"),
    "buergerrecht":             ("Bürgerrecht",              "weisung"),
    "datenschutz_und_oeffentlichkeitsgesetz": ("DSG / ÖG",   "weisung"),
    "visa":                     ("Visa",                     "weisung"),
    "weitere_weisungen":        ("Weitere",                  "weisung"),
}

_DATE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
_VERSION_DATE = re.compile(r"(?:Stand|Version|gültig ab)\s*[:\-]?\s*(\d{1,2})\.(\d{1,2})\.(\d{4})", re.I)


class SemWeisungenScraper(PracticeScraper):
    SOURCE_KEY = "sem_weisungen"
    ISSUING_AUTHORITY = "SEM"
    DEFAULT_DOC_TYPE = "weisung"
    REQUEST_DELAY = 1.5

    def discover_documents(self) -> Iterator[dict]:
        for section_slug, (section_label, doc_type) in SECTIONS.items():
            section_url = (f"{_BASE}/sem/de/home/publiservice/weisungen-kreisschreiben/"
                           f"{section_slug}.html")
            try:
                r = self.get(section_url)
                r.raise_for_status()
            except Exception as e:
                logger.warning("SEM section [%s] fetch failed: %s", section_slug, e)
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            seen: set[str] = set()
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.lower().endswith(".pdf"):
                    continue
                if "/dam/sem/" not in href and "/dam/data/sem/" not in href:
                    continue
                pdf_url = urljoin(_BASE, href)
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)

                title = re.sub(r"\s+", " ", a.get_text(" ", strip=True)) or "(untitled)"
                # Surrounding text often carries "Stand: DD.MM.YYYY"
                ctx = a.get_text(" ", strip=True) + " " + (
                    a.find_parent().get_text(" ", strip=True) if a.find_parent() else "")
                vm = _VERSION_DATE.search(ctx) or _DATE.search(ctx)
                date_iso = (f"{vm.group(3)}-{int(vm.group(2)):02d}-{int(vm.group(1)):02d}"
                            if vm else "")

                # doc_number: use filename minus ".pdf"
                fname = pdf_url.rsplit("/", 1)[-1]
                doc_number = fname.replace(".pdf", "")[:80]

                yield {
                    "pdf_url": pdf_url,
                    "url": section_url,
                    "title": f"{section_label} — {title}",
                    "doc_number": doc_number,
                    "date": date_iso,
                    "language": "de",
                    "doc_type": doc_type,
                    "topics": [section_label],
                }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    SemWeisungenScraper().run()
