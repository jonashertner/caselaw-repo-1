"""
ZH Sozialversicherungsgericht Scraper — Findex API
====================================================
JSON-based API at api.findex.webgate.cloud returning decision metadata,
plus HTML decision pages at findex.webgate.cloud/entscheide/{num}.html.

Coverage: Social insurance court decisions since ~Feb 2003, ~5,000+ decisions.
"""

from __future__ import annotations

import json
import logging
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

SEARCH_URL = "https://api.findex.webgate.cloud/api/search/*"
RESULT_BASE = "https://findex.webgate.cloud/entscheide/"

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://findex.webgate.cloud/",
    "Referer": "https://findex.webgate.cloud/",
}


def _extract_html_text(html: str) -> str:
    """Extract decision text from HTML page."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # Try the specific content div first (from NeueScraper)
    content = soup.find("div", class_="cell small-12 contentContainer printArea")
    if content:
        for tag in content(["script", "style"]):
            tag.decompose()
        return content.get_text(separator="\n").strip()
    # Fallback to full page
    for tag in soup(["script", "style", "meta", "link"]):
        tag.decompose()
    return soup.get_text(separator="\n").strip()


class ZHSozialversicherungsgerichtScraper(BaseScraper):
    """
    Scraper for ZH Sozialversicherungsgericht via Findex JSON API.
    
    Single POST returns ALL decisions as JSON array — no pagination needed.
    Then fetch each HTML page individually.
    """

    REQUEST_DELAY = 1.5
    TIMEOUT = 60
    MAX_ERRORS = 50

    @property
    def court_code(self) -> str:
        return "zh_sozialversicherungsgericht"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """POST to search API → get all decisions as JSON array."""
        datum = ""
        if since_date:
            if isinstance(since_date, str):
                since_date = parse_date(since_date)
            if since_date:
                datum = since_date.strftime("%Y-%m-%d")

        payload = json.dumps({
            "Rechtsgebiet": "",
            "datum": datum,
            "operation": ">",
            "prozessnummer": "",
        })

        logger.info(f"SVG ZH: searching all decisions (datum>{datum or 'all'})")

        try:
            resp = self.session.post(
                SEARCH_URL,
                data=payload,
                headers={**self.session.headers, **HEADERS},
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"SVG ZH search failed: {e}")
            return

        try:
            data = json.loads(resp.text)
        except json.JSONDecodeError as e:
            logger.error(f"SVG ZH JSON parse error: {e}")
            return

        logger.info(f"SVG ZH: {len(data)} decisions returned")

        total_new = 0
        for entry in data:
            num = entry.get("prozessnummer", "")
            if not num:
                continue

            edatum_raw = entry.get("entscheiddatum", "")
            if edatum_raw:
                edatum = parse_date(edatum_raw[:10])
            else:
                edatum = None

            if not edatum:
                logger.warning(f"SVG ZH no date for {num}")
                continue

            # Build title with BGE ref and Weiterzug
            titel = entry.get("betreff", "")
            bge = entry.get("bge", "")
            weiterzug = entry.get("weiterzug", "")
            if bge and bge.strip():
                titel += f" (BGE {bge.strip()})"
            if weiterzug and weiterzug.strip():
                titel += f" ({weiterzug.strip()})"

            rechtsgebiet = entry.get("rechtsgebiet", "")
            url = f"{RESULT_BASE}{num}.html"
            decision_id = make_decision_id("zh_sozialversicherungsgericht", num)

            if self.state.is_known(decision_id):
                continue

            total_new += 1
            yield {
                "decision_id": decision_id,
                "docket_number": num,
                "decision_date": edatum,
                "title": titel,
                "rechtsgebiet": rechtsgebiet,
                "weiterzug": weiterzug,
                "url": url,
                "source_url": url,
            }

        logger.info(f"SVG ZH: {total_new} new decisions to scrape")

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch individual HTML decision page."""
        url = stub["url"]
        num = stub["docket_number"]

        try:
            resp = self.get(url, timeout=self.TIMEOUT)
        except Exception as e:
            logger.warning(f"SVG ZH fetch failed for {num}: {e}")
            return None

        full_text = _extract_html_text(resp.text)
        if not full_text or len(full_text) < 30:
            logger.warning(f"SVG ZH short text for {num}: {len(full_text or '')} chars")
            if not full_text:
                full_text = f"[HTML extraction failed for {num}]"

        language = detect_language(full_text) if len(full_text) > 100 else "de"

        return Decision(
            decision_id=stub["decision_id"],
            court="zh_sozialversicherungsgericht",
            canton="ZH",
            chamber=None,
            docket_number=num,
            decision_date=stub["decision_date"],
            language=language,
            title=stub.get("title") or f"SVG ZH — {num}",
            regeste=None,
            full_text=full_text,
            source_url=stub["source_url"],
            pdf_url=None,
            decision_type=None,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
            external_id=f"zh_svg_{num}",
        )
