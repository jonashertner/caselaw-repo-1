"""
Luzern Courts Scraper (LU Gerichte)
=====================================
Scrapes court decisions from gerichte.lu.ch via AJAX endpoint.

Architecture:
- GET /recht_sprechung/lgve/Ajax?EnId={id} → HTML fragment with decision
- No authentication, no session required
- Sequential ID enumeration across two segments:
    Segment 1: IDs 684-3843 (decisions ~1996-2012)
    Segment 2: IDs 10001-11200+ (decisions ~2012-present)
- Invalid IDs return exactly 1995 bytes with error message

Decision HTML fragment contains:
- CSS + JS + <div id="JurisdictionPrintArea">
- Table with th/td pairs: Instanz, Abteilung, Rechtsgebiet,
  Entscheiddatum (DD.MM.YYYY), Fallnummer, LGVE, Gesetzesartikel,
  Leitsatz, Rechtskraft, Entscheid (full text with <br/>)

Total: ~4,200 decisions (1996-2025)
Platform: Custom ASP.NET AJAX
"""
from __future__ import annotations

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
)

logger = logging.getLogger(__name__)

BASE_URL = "https://gerichte.lu.ch"
AJAX_URL = f"{BASE_URL}/recht_sprechung/lgve/Ajax"

# ID segments (empirically determined Feb 2026)
# Segment 1: 684-3843 (decisions ~1996-2012)
# Segment 2: 10001-11102+ (decisions ~2012-present, growing)
SEGMENT_1 = (684, 3843)
SEGMENT_2 = (10001, 11500)  # Upper bound with margin for growth

# Invalid response size (error page)
INVALID_SIZE = 1995

RE_DATE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")


def _parse_swiss_date(text):
    if not text:
        return None
    m = RE_DATE.search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


class LUGerichteScraper(BaseScraper):
    """
    Scraper for Luzern court decisions via AJAX endpoint.

    Enumerates sequential IDs across two segments, skipping invalid IDs.
    Each valid response contains the complete decision with full text.
    """

    REQUEST_DELAY = 1.0  # Lighter rate limit since responses are small
    TIMEOUT = 30
    MAX_ERRORS = 100  # Many IDs will be invalid (gaps)

    @property
    def court_code(self):
        return "lu_gerichte"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """
        Enumerate IDs in both segments, newest first.
        Since we get full decisions from the AJAX endpoint,
        discover_new yields stubs with the ID, and fetch_decision
        re-fetches (or we could cache — but keeping it simple).
        """
        if since_date and isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        total_yielded = 0
        consecutive_invalid = 0

        # Segment 2 first (newer decisions)
        logger.info(f"LU: scanning segment 2 (IDs {SEGMENT_2[0]}-{SEGMENT_2[1]})")
        for en_id in range(SEGMENT_2[1], SEGMENT_2[0] - 1, -1):
            decision_id = f"lu_gerichte_{en_id}"
            if self.state.is_known(decision_id):
                consecutive_invalid = 0  # Known IDs reset the counter
                continue

            # Probe the ID
            stub = self._probe_id(en_id)
            if stub is None:
                consecutive_invalid += 1
                # Allow up to 500 consecutive invalids (segment has sparse IDs at edges)
                if consecutive_invalid > 500:
                    logger.info(f"LU: 500 consecutive invalid IDs at {en_id}, moving on")
                    break
                continue

            consecutive_invalid = 0

            if since_date and stub.get("decision_date"):
                if stub["decision_date"] < since_date:
                    continue

            total_yielded += 1
            yield stub

        # Segment 1 (older decisions)
        if not since_date or since_date.year < 2013:
            consecutive_invalid = 0
            logger.info(f"LU: scanning segment 1 (IDs {SEGMENT_1[0]}-{SEGMENT_1[1]})")
            for en_id in range(SEGMENT_1[1], SEGMENT_1[0] - 1, -1):
                decision_id = f"lu_gerichte_{en_id}"
                if self.state.is_known(decision_id):
                    consecutive_invalid = 0
                    continue

                stub = self._probe_id(en_id)
                if stub is None:
                    consecutive_invalid += 1
                    if consecutive_invalid > 100:
                        logger.info(f"LU: 100 consecutive invalid IDs at {en_id}, done")
                        break
                    continue

                consecutive_invalid = 0

                if since_date and stub.get("decision_date"):
                    if stub["decision_date"] < since_date:
                        continue

                total_yielded += 1
                yield stub

        logger.info(f"LU: discovery complete: {total_yielded} new stubs")

    def _probe_id(self, en_id: int) -> dict | None:
        """
        Probe a single ID. Returns stub dict if valid, None if invalid.
        """
        try:
            self._rate_limit()
            r = self.session.get(
                AJAX_URL, params={"EnId": en_id}, timeout=self.TIMEOUT
            )
        except Exception as e:
            logger.debug(f"LU: request failed for EnId={en_id}: {e}")
            return None

        # Invalid IDs return exactly INVALID_SIZE bytes
        if len(r.content) == INVALID_SIZE:
            return None

        if "Keine oder ung" in r.text:
            return None

        # Valid response — parse metadata
        return self._parse_response(en_id, r.text)

    def _parse_response(self, en_id: int, html: str) -> dict | None:
        """Parse decision metadata from AJAX response HTML fragment."""
        soup = BeautifulSoup(html, "html.parser")

        metadata = {}
        table = soup.find("table", class_="headerleft")
        if not table:
            table = soup.find("table")
        if not table:
            return None

        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                key = th.get_text(strip=True).rstrip(":")
                value = td.get_text(strip=True)
                metadata[key] = value

        instance = metadata.get("Instanz", "")
        abteilung = metadata.get("Abteilung", "")
        rechtsgebiet = metadata.get("Rechtsgebiet", "")
        decision_date_str = metadata.get("Entscheiddatum", "")
        fallnummer = metadata.get("Fallnummer", "")
        lgve = metadata.get("LGVE", "")
        gesetzesartikel = metadata.get("Gesetzesartikel", "")

        decision_date = _parse_swiss_date(decision_date_str)

        # Use fallnummer as docket, or construct from LGVE
        docket = fallnummer or lgve or f"LU-{en_id}"

        decision_id = f"lu_gerichte_{en_id}"

        return {
            "decision_id": decision_id,
            "en_id": en_id,
            "docket_number": docket,
            "decision_date": decision_date,
            "instance": instance,
            "abteilung": abteilung,
            "rechtsgebiet": rechtsgebiet,
            "lgve": lgve,
            "gesetzesartikel": gesetzesartikel,
            "url": f"{AJAX_URL}?EnId={en_id}",
            "html": html,  # Cache the HTML to avoid re-fetching
        }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Parse full decision from cached or re-fetched HTML."""
        en_id = stub.get("en_id")
        html = stub.get("html")

        if not html:
            # Re-fetch if not cached
            try:
                self._rate_limit()
                r = self.session.get(
                    AJAX_URL, params={"EnId": en_id}, timeout=self.TIMEOUT
                )
                html = r.text
            except Exception as e:
                logger.warning(f"LU: fetch failed for EnId={en_id}: {e}")
                return None

        soup = BeautifulSoup(html, "html.parser")

        # Extract full text
        full_text = self._extract_full_text(soup)
        if not full_text or len(full_text) < 20:
            # Try leitsatz as fallback
            full_text = self._extract_leitsatz(soup)
            if not full_text:
                full_text = f"[Text extraction failed for LU EnId={en_id}]"

        decision_date = stub.get("decision_date")
        if not decision_date:
            logger.warning(f"[lu_gerichte] No date for EnId={stub.get('en_id', '?')}")

        language = detect_language(full_text) if len(full_text) > 100 else "de"

        # Build chamber from instance + abteilung
        chamber_parts = []
        if stub.get("instance"):
            chamber_parts.append(stub["instance"])
        if stub.get("abteilung") and stub["abteilung"] != "-":
            chamber_parts.append(stub["abteilung"])
        chamber = " / ".join(chamber_parts) if chamber_parts else None

        return Decision(
            decision_id=stub["decision_id"],
            court="lu_gerichte",
            canton="LU",
            chamber=chamber,
            docket_number=stub["docket_number"],
            decision_date=decision_date,
            language=language,
            title=stub.get("rechtsgebiet") or None,
            legal_area=stub.get("rechtsgebiet") or None,
            regeste=self._extract_leitsatz(soup) or None,
            full_text=full_text,
            source_url=stub.get("url", f"{AJAX_URL}?EnId={en_id}"),
            collection=stub.get("lgve") or None,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
        )

    @staticmethod
    def _extract_full_text(soup) -> str:
        """Extract the Entscheid (full decision text) field."""
        for tr in soup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                key = th.get_text(strip=True).rstrip(":")
                if key == "Entscheid":
                    # The text has <br/> line breaks
                    text = td.get_text(separator="\n", strip=True)
                    return text
        return ""

    @staticmethod
    def _extract_leitsatz(soup) -> str:
        """Extract the Leitsatz (headnote/summary) field."""
        for tr in soup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                key = th.get_text(strip=True).rstrip(":")
                if key == "Leitsatz":
                    return td.get_text(separator="\n", strip=True)
        return ""
