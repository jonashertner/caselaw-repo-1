"""
HUDOC Scraper (ECHR Swiss Cases)
=================================

Scrapes European Court of Human Rights (ECHR/EGMR) judgments and decisions
concerning Switzerland from the HUDOC database.

Architecture:
- HUDOC has an undocumented JSON API at hudoc.echr.coe.int/app/query/results
- Filter: respondent=CHE, documentcollectionid2=JUDGMENTS or DECISIONS
- Returns JSON with metadata + document ID
- Full text available at hudoc.echr.coe.int/app/conversion/docx/html/body/{itemid}
- Also: PDF at hudoc.echr.coe.int/app/conversion/pdf/?library=ECHR&id={itemid}

Coverage: ~800-1,500 judgments + decisions against Switzerland
Rate limiting: 2.0 seconds (public ECHR server)
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Iterator
from urllib.parse import quote

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

# HUDOC API endpoint (reverse-engineered from browser network tab)
HUDOC_API = "https://hudoc.echr.coe.int/app/query/results"

# Query for Swiss cases — judgments and decisions
# NOTE: Do NOT put quotes around filter values — HUDOC API ignores quoted values
QUERY_TEMPLATE = (
    'contentsitename:ECHR AND '
    '(NOT (doctype:PR OR doctype:HFCOMOLD OR doctype:HECOMOLD)) AND '
    'respondent:{respondent} AND '
    'documentcollectionid2:{collection}'
)

COLLECTIONS = ["JUDGMENTS", "DECISIONS"]

# Full text URL — item_id goes as query parameter, NOT path segment
FULLTEXT_URL = "https://hudoc.echr.coe.int/app/conversion/docx/html/body?library=ECHR&id={item_id}"


class HUDOCScraper(BaseScraper):
    """Scraper for ECHR/HUDOC decisions concerning Switzerland."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 60
    MAX_ERRORS = 30

    @property
    def court_code(self) -> str:
        return "hudoc_ch"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Query HUDOC API for Swiss cases."""
        # Establish session cookies by visiting main page first
        try:
            self.session.get("https://hudoc.echr.coe.int/eng", timeout=30)
        except Exception:
            pass

        found = 0
        seen_appnos = set()  # HUDOC returns same case in multiple languages

        for collection in COLLECTIONS:
            query = QUERY_TEMPLATE.format(respondent="CHE", collection=collection)
            start = 0
            page_size = 500

            while True:
                params = {
                    "query": query,
                    "select": (
                        "itemid,applicability,appno,article,conclusion,"
                        "docname,doctypebranch,ecli,importance,"
                        "judgementdate,kpdate,languageisocode,"
                        "originatingbody,respondent,separateopinion,"
                        "typedescription,violation,nonviolation"
                    ),
                    "sort": "",
                    "start": start,
                    "length": page_size,
                }

                try:
                    self._rate_limit()
                    r = self.session.get(HUDOC_API, params=params, timeout=self.TIMEOUT)
                    r.raise_for_status()
                except Exception as e:
                    logger.error(f"[hudoc_ch] API query failed: {e}")
                    break

                try:
                    data = r.json()
                except json.JSONDecodeError:
                    logger.error(f"[hudoc_ch] Invalid JSON response")
                    break

                results = data.get("results", [])
                if not results:
                    break

                for item in results:
                    columns = item.get("columns", {})
                    item_id = columns.get("itemid", "")
                    if not item_id:
                        continue

                    appno = columns.get("appno", "")
                    docname = columns.get("docname", "")
                    judgement_date = columns.get("judgementdate", "")
                    lang_iso = columns.get("languageisocode", "")
                    doc_type = columns.get("typedescription", "")
                    ecli = columns.get("ecli", "")
                    article = columns.get("article", "")
                    conclusion = columns.get("conclusion", "")
                    violation = columns.get("violation", "")
                    nonviolation = columns.get("nonviolation", "")
                    importance = columns.get("importance", "")

                    # Build docket from application number
                    docket = appno.replace(";", "_") if appno else item_id
                    decision_id = make_decision_id("hudoc_ch", docket)

                    if self.state.is_known(decision_id):
                        continue

                    # Skip duplicate language versions (keep first encountered)
                    if appno and appno in seen_appnos:
                        continue
                    if appno:
                        seen_appnos.add(appno)

                    # Parse date — HUDOC format: "19/02/2026 00:00:00"
                    decision_date = None
                    if judgement_date:
                        parts = judgement_date.split(" ")[0]  # "19/02/2026"
                        # Convert DD/MM/YYYY to DD.MM.YYYY for parse_date
                        decision_date = parse_date(parts.replace("/", "."))

                    if since_date and decision_date and decision_date < since_date:
                        continue

                    found += 1
                    yield {
                        "docket_number": docket,
                        "decision_date": decision_date,
                        "item_id": item_id,
                        "appno": appno,
                        "docname": docname,
                        "doc_type": doc_type,
                        "lang_iso": lang_iso,
                        "ecli": ecli,
                        "article": article,
                        "conclusion": conclusion,
                        "violation": violation,
                        "nonviolation": nonviolation,
                        "importance": importance,
                        "collection": collection,
                    }

                # Pagination
                total = data.get("resultcount", 0)
                start += page_size
                if start >= total:
                    break

                logger.info(
                    f"[hudoc_ch] {collection}: fetched {start}/{total} metadata entries"
                )

        logger.info(f"[hudoc_ch] Found {found} new decisions to fetch")

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch full text of an ECHR decision."""
        item_id = stub["item_id"]
        docket = stub["docket_number"]

        url = FULLTEXT_URL.format(item_id=item_id)
        try:
            response = self.get(url)
        except Exception as e:
            logger.warning(f"[hudoc_ch] Failed to fetch {docket}: {e}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        full_text = soup.get_text(separator="\n", strip=True)

        if not full_text or len(full_text) < 100:
            logger.debug(f"[hudoc_ch] {docket}: too short ({len(full_text)} chars)")
            return None

        full_text = self.clean_text(full_text)

        # Map HUDOC language codes to our codes
        # ECHR decisions are in ENG/FRE (official languages) — detect from text
        lang_map = {"FRE": "fr", "GER": "de", "ITA": "it"}
        lang = lang_map.get(stub.get("lang_iso", ""), None)
        if not lang:
            lang = detect_language(full_text)

        # Build title
        title = stub.get("docname", "")
        if not title:
            title = f"ECHR {stub['appno']}"

        # Build regeste from conclusion
        regeste = None
        conclusion = stub.get("conclusion", "")
        violation = stub.get("violation", "")
        nonviolation = stub.get("nonviolation", "")
        if conclusion or violation:
            parts = []
            if conclusion:
                parts.append(conclusion)
            if violation:
                parts.append(f"Violation: {violation}")
            if nonviolation:
                parts.append(f"No violation: {nonviolation}")
            regeste = "; ".join(parts)

        source_url = f"https://hudoc.echr.coe.int/eng?i={item_id}"

        return Decision(
            decision_id=make_decision_id("hudoc_ch", docket),
            court="hudoc_ch",
            canton="CH",
            docket_number=docket,
            decision_date=stub.get("decision_date"),
            language=lang,
            title=title,
            legal_area="EMRK / Menschenrechte",
            regeste=regeste,
            decision_type=stub.get("doc_type"),
            full_text=full_text,
            source_url=source_url,
            external_id=stub.get("ecli") or stub.get("item_id"),
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
            scraped_at=datetime.now(timezone.utc),
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape HUDOC Swiss cases")
    parser.add_argument("--since", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--max", type=int, default=5, help="Max decisions")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    since = date.fromisoformat(args.since) if args.since else None
    scraper = HUDOCScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  {len(d.full_text)} chars  {d.title[:60]}")
    print(f"\nScraped {len(decisions)} HUDOC Swiss decisions")
