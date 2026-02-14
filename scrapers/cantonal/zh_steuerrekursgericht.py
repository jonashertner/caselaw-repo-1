"""
ZH Steuerrekursgericht Scraper — strgzh.ch
============================================
GET-based paginated search, 10 results per page, PDF downloads.

Coverage: Tax law decisions from 2009-2010 (Steuerrekurskommissionen)
and 2011+ (Steuerrekursgericht), ~1,000+ decisions.
"""

from __future__ import annotations

import io
import logging
import re
from datetime import date
from typing import Iterator

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import (
    Decision,
    detect_language,
    extract_citations,
    make_decision_id,
    parse_date,
)

logger = logging.getLogger(__name__)

HOST = "https://www.strgzh.ch"
SEARCH_URL = HOST + "/entscheide/datenbank/verfahrensnummersuche"
TREFFER_PRO_SEITE = 10

# German month names for date parsing
MONATE = {
    "Januar": 1, "Februar": 2, "März": 3, "April": 4,
    "Mai": 5, "Juni": 6, "Juli": 7, "August": 8,
    "September": 9, "Oktober": 10, "November": 11, "Dezember": 12,
}

# Regex for "NUM / DD. Monat YYYY" metadata line
# Handles multiple docket numbers separated by commas
RE_META = re.compile(
    r"^(?P<Num>[A-Z][^/,]+[0-9, +-]*)"
    r"(?:,\s+(?P<Num2>[A-Z][^/,]+[0-9, +-]*))?"
    r"(?:,\s+(?P<Num3>[A-Z][^/,]+[0-9, +-]*))?"
    r"(?:,\s+(?P<Num4>[A-Z][^/,]+[0-9, +-]*))?"
    r"\s+/\s+(?P<Datum>\d+\.\s+(?:" + "|".join(MONATE.keys()) + r")\s+\d{4})$"
)


def _parse_german_date(text: str) -> date | None:
    """Parse 'DD. Monat YYYY' format."""
    if not text:
        return None
    m = re.match(r"(\d+)\.\s+(\w+)\s+(\d{4})", text.strip())
    if m:
        day = int(m.group(1))
        month = MONATE.get(m.group(2))
        year = int(m.group(3))
        if month:
            try:
                return date(year, month, day)
            except ValueError:
                pass
    return None


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes."""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction error: {e}")
        return ""


class ZHSteuerrekursgerichtScraper(BaseScraper):
    """
    Scraper for ZH Steuerrekursgericht via paginated GET search.

    Strategy:
    1. GET search page with empty params → all decisions
    2. Paginate (10 per page, page=N)
    3. Each result: cit-title (NUM / Date), ruling__title (PDF link + title),
       legal_foundation (Normen), Leitsatz, note (Weiterzug)
    4. Download PDFs, extract text
    """

    REQUEST_DELAY = 2.0
    TIMEOUT = 60
    MAX_ERRORS = 50

    @property
    def court_code(self) -> str:
        return "zh_steuerrekursgericht"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Paginate through all search results."""
        page = 1
        total_treffer = None
        total_new = 0

        while True:
            url = f"{SEARCH_URL}?subject=&year=&number=&submit=Suchen&page={page}"
            logger.info(f"STRG ZH: fetching page {page}")

            try:
                resp = self.get(url)
            except Exception as e:
                logger.error(f"STRG ZH page {page} failed: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Total count: <div class="box ruling"><p>N Entscheide gefunden</p>
            if total_treffer is None:
                for p in soup.find_all("p"):
                    text = p.get_text(strip=True)
                    if "Entscheide gefunden" in text:
                        m = re.match(r"(\d+)", text)
                        if m:
                            total_treffer = int(m.group(1))
                            logger.info(f"STRG ZH: {total_treffer} total decisions")
                        break

            # Parse entries: div.box.ruling that contain p.cit-title
            entries = soup.find_all("div", class_="box ruling")
            found_any = False

            for entry in entries:
                cit = entry.find("p", class_="cit-title")
                if not cit:
                    continue

                found_any = True
                stub = self._parse_entry(entry, cit)
                if stub:
                    if since_date:
                        sd = parse_date(since_date) if isinstance(since_date, str) else since_date
                        if sd and stub["decision_date"] < sd:
                            continue

                    if not self.state.is_known(stub["decision_id"]):
                        total_new += 1
                        yield stub

            if not found_any:
                break

            page += 1
            if total_treffer and page * TREFFER_PRO_SEITE > total_treffer + TREFFER_PRO_SEITE:
                break

        logger.info(f"STRG ZH discovery complete: {total_new} new")

    def _parse_entry(self, entry, cit) -> dict | None:
        """Parse a single ruling entry."""
        meta_text = cit.get_text(strip=True)

        # Try regex match
        match = RE_META.search(meta_text)
        if match:
            num = match.group("Num").strip()
            edatum = _parse_german_date(match.group("Datum"))
            # Secondary docket numbers
            nums = [num]
            for key in ["Num2", "Num3", "Num4"]:
                val = match.group(key)
                if val:
                    nums.append(val.strip())
        else:
            # Fallback: try to split on " / "
            parts = meta_text.split(" / ")
            if len(parts) >= 2:
                num = parts[0].strip()
                edatum = _parse_german_date(parts[-1].strip())
                nums = [num]
            else:
                logger.warning(f"STRG ZH unparseable meta: {meta_text!r}")
                return None

        if not edatum:
            logger.warning(f"STRG ZH no date for: {meta_text!r}")
            return None

        # PDF URL + Title: <h2 class="ruling__title"><a href="...">Title</a></h2>
        h2 = entry.find("h2", class_="ruling__title")
        pdf_url = None
        titel = ""
        if h2:
            a = h2.find("a", href=True)
            if a:
                href = a["href"]
                pdf_url = HOST + href if href.startswith("/") else href
                titel = a.get_text(strip=True)

        if not pdf_url:
            logger.warning(f"STRG ZH no PDF for {num}")
            return None

        # Normen: <p class="legal_foundation">
        normen = ""
        nf = entry.find("p", class_="legal_foundation")
        if nf:
            normen = nf.get_text(strip=True)

        # Leitsatz: first <p> without class after legal_foundation
        leitsatz = ""
        if nf:
            next_p = nf.find_next_sibling("p")
            if next_p and not next_p.get("class"):
                leitsatz = next_p.get_text(strip=True)

        # Weiterzug: <p class="note">
        weiterzug = ""
        note = entry.find("p", class_="note")
        if note:
            weiterzug = note.get_text(strip=True)

        decision_id = make_decision_id("zh_steuerrekursgericht", num)

        return {
            "decision_id": decision_id,
            "docket_number": num,
            "docket_numbers": nums,
            "decision_date": edatum,
            "title": titel,
            "normen": normen,
            "leitsatz": leitsatz,
            "weiterzug": weiterzug,
            "pdf_url": pdf_url,
            "source_url": pdf_url,
        }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download PDF, extract text."""
        pdf_url = stub["pdf_url"]
        num = stub["docket_number"]

        try:
            resp = self.get(pdf_url, timeout=self.TIMEOUT)
        except Exception as e:
            logger.warning(f"STRG ZH PDF download failed for {num}: {e}")
            return None

        if len(resp.content) < 100:
            logger.warning(f"STRG ZH tiny PDF for {num}: {len(resp.content)} bytes")
            return None

        full_text = _extract_text_from_pdf(resp.content)
        if not full_text or len(full_text) < 30:
            if not full_text:
                full_text = f"[PDF extraction failed for {num}]"

        language = detect_language(full_text) if len(full_text) > 100 else "de"

        # Secondary docket number
        docket_2 = None
        if len(stub.get("docket_numbers", [])) > 1:
            docket_2 = ", ".join(stub["docket_numbers"][1:])

        return Decision(
            decision_id=stub["decision_id"],
            court="zh_steuerrekursgericht",
            canton="ZH",
            chamber=None,
            docket_number=num,
            docket_number_2=docket_2,
            decision_date=stub["decision_date"],
            language=language,
            title=stub.get("title") or f"STRG ZH — {num}",
            regeste=stub.get("leitsatz") or None,
            full_text=full_text,
            source_url=stub["source_url"],
            pdf_url=pdf_url,
            decision_type=None,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
            external_id=f"zh_strg_{num}",
        )
