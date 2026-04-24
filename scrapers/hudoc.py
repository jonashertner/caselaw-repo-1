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
from datetime import date, datetime, timezone
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
    # HUDOC sometimes 404s on individual case documents (missing HTML even
    # though the metadata entry exists). Cache those for the weekly TTL.
    CACHE_NONE_AS_GAP = True

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
        seen_keys = set()  # Dedup by appno+collection (not just appno)

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
                    # Key on appno+collection so both judgments and decisions
                    # for the same application number are retained
                    dedup_key = f"{appno}|{collection}" if appno else None
                    if dedup_key and dedup_key in seen_keys:
                        continue
                    if dedup_key:
                        seen_keys.add(dedup_key)

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


# ============================================================
# Generalized HUDOC scraper — all ECtHR judgments
# ============================================================
#
# Covers Grand Chamber + Chamber + Committee *judgments* (tier 1+2) from
# every Council of Europe respondent state. Skips admissibility decisions
# (~900k Committee DECs add bulk without practitioner value), press
# releases, and old European Commission documents. Skips third-party
# translations (copyright held by translators/ministries, not the Court —
# redistribution requires separate permission).
#
# Language scope (v1): French only (FRE). Our Decision schema pattern is
# `^(de|fr|it|rm)$` and does not include `en`. English ingest ships in v2
# alongside a coordinated pattern change + FTS5 validation.
#
# Copyright: © ECHR-CEDH. Permitted reuse per
# https://www.echr.coe.int/copyright-and-disclaimer conditional on
# attribution (© ECHR-CEDH) and free-of-charge information/education use.
# A registry permission letter has been sent to secure bulk + commercial
# redistribution; until it returns, ingest runs but HF publication of
# ECtHR content is gated to a separate repository (not CC0).
# ============================================================

# doctypebranch (HUDOC) -> our court code
_BRANCH_TO_COURT = {
    "GRANDCHAMBER": "ecthr_grand_chamber",
    "CHAMBER": "ecthr_chamber",
    "COMMITTEE": "ecthr_committee",
}

# Authoritative languages we accept. HUDOC lists translations separately
# under the same itemid with different languageisocode; those are not
# owned by the Court and must not be redistributed without each
# translator's permission.
_AUTHORITATIVE_LANGS = {"FRE"}  # v1: FR only. EN in v2 once schema extended.

# HUDOC respondent state ISO-3 → our canton-like code. Swiss respondent
# keeps canton='CH' to preserve compatibility with the existing
# hudoc_ch filter and the `canton='CH'` federal-level search facet.
_RESPONDENT_TO_CANTON = {"CHE": "CH"}

# Generalized full-corpus query. No respondent filter; all 46 member
# states. documentcollectionid2:JUDGMENTS excludes admissibility DECs.
_FULL_QUERY = (
    "contentsitename:ECHR AND "
    "(NOT (doctype:PR OR doctype:HFCOMOLD OR doctype:HECOMOLD)) AND "
    "documentcollectionid2:JUDGMENTS"
)


class HUDOCFullScraper(BaseScraper):
    """Full-corpus ECtHR judgment scraper (all respondent states)."""

    REQUEST_DELAY = 1.5
    TIMEOUT = 60
    MAX_ERRORS = 50
    CACHE_NONE_AS_GAP = True

    @property
    def court_code(self) -> str:
        # State-tracking key. Individual decisions carry chamber-specific
        # court codes from _BRANCH_TO_COURT.
        return "ecthr"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        try:
            self.session.get("https://hudoc.echr.coe.int/eng", timeout=30)
        except Exception:
            pass

        start = 0
        page_size = 500
        found = 0
        skipped_translations = 0
        seen_keys: set[str] = set()

        while True:
            params = {
                "query": _FULL_QUERY,
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
                data = r.json()
            except Exception as e:
                logger.error(f"[ecthr] API query failed at start={start}: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            for item in results:
                columns = item.get("columns", {})
                item_id = columns.get("itemid", "") or ""
                if not item_id:
                    continue

                lang_iso = (columns.get("languageisocode") or "").upper()
                if lang_iso not in _AUTHORITATIVE_LANGS:
                    skipped_translations += 1
                    continue

                appno = columns.get("appno", "") or ""
                branch = (columns.get("doctypebranch") or "").upper()
                court = _BRANCH_TO_COURT.get(branch, "ecthr")
                respondent = (columns.get("respondent") or "").upper()

                docket = appno.replace(";", "_") if appno else item_id
                decision_id = make_decision_id(court, docket)
                if self.state.is_known(decision_id):
                    continue

                # Dedup by appno+branch+lang_iso; at FR-only this is
                # effectively (appno, branch).
                dedup_key = f"{appno}|{branch}|{lang_iso}" if appno else None
                if dedup_key and dedup_key in seen_keys:
                    continue
                if dedup_key:
                    seen_keys.add(dedup_key)

                # HUDOC judgementdate format: "19/02/2026 00:00:00"
                decision_date = None
                jd = columns.get("judgementdate", "") or ""
                if jd:
                    decision_date = parse_date(jd.split(" ")[0].replace("/", "."))

                if since_date and decision_date and decision_date < since_date:
                    continue

                found += 1
                yield {
                    "court": court,
                    "docket_number": docket,
                    "decision_date": decision_date,
                    "item_id": item_id,
                    "appno": appno,
                    "docname": columns.get("docname") or "",
                    "doc_type": columns.get("typedescription") or "",
                    "branch": branch,
                    "respondent": respondent,
                    "lang_iso": lang_iso,
                    "ecli": columns.get("ecli") or "",
                    "article": columns.get("article") or "",
                    "conclusion": columns.get("conclusion") or "",
                    "violation": columns.get("violation") or "",
                    "nonviolation": columns.get("nonviolation") or "",
                    "importance": columns.get("importance") or "",
                }

            total = int(data.get("resultcount") or 0)
            logger.info(
                f"[ecthr] page start={start} /{total} found_new={found} "
                f"skipped_translations={skipped_translations}"
            )
            start += page_size
            if start >= total:
                break

        logger.info(
            f"[ecthr] discovered {found} new judgments "
            f"(skipped {skipped_translations} non-authoritative-language entries)"
        )

    def fetch_decision(self, stub: dict) -> Decision | None:
        item_id = stub["item_id"]
        docket = stub["docket_number"]
        court = stub["court"]

        url = FULLTEXT_URL.format(item_id=item_id)
        try:
            response = self.get(url)
        except Exception as e:
            logger.warning(f"[ecthr] fetch {docket}: {e}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        full_text = soup.get_text(separator="\n", strip=True)
        if not full_text or len(full_text) < 100:
            return None
        full_text = self.clean_text(full_text)

        lang_iso = (stub.get("lang_iso") or "").upper()
        lang_map = {"ENG": "en", "FRE": "fr", "GER": "de", "ITA": "it"}
        lang = lang_map.get(lang_iso)
        if not lang:
            lang = detect_language(full_text)

        title = stub.get("docname") or f"ECtHR {stub.get('appno') or item_id}"

        regeste_parts: list[str] = []
        conclusion = stub.get("conclusion") or ""
        violation = stub.get("violation") or ""
        nonviolation = stub.get("nonviolation") or ""
        if conclusion:
            regeste_parts.append(conclusion)
        if violation:
            regeste_parts.append(f"Violation: {violation}")
        if nonviolation:
            regeste_parts.append(f"No violation: {nonviolation}")
        regeste = "; ".join(regeste_parts) if regeste_parts else None

        lang_seg = "fre" if lang == "fr" else ("eng" if lang == "en" else "eng")
        source_url = f"https://hudoc.echr.coe.int/{lang_seg}?i={item_id}"

        legal_area = (
            "CEDH / Droits humains" if lang == "fr" else "EMRK / Menschenrechte"
        )

        respondent = stub.get("respondent") or ""
        canton = _RESPONDENT_TO_CANTON.get(respondent, "CE")

        return Decision(
            decision_id=make_decision_id(court, docket),
            court=court,
            canton=canton,
            docket_number=docket,
            decision_date=stub.get("decision_date"),
            language=lang,
            title=title,
            legal_area=legal_area,
            regeste=regeste,
            decision_type=stub.get("doc_type"),
            full_text=full_text,
            source_url=source_url,
            external_id=stub.get("ecli") or item_id,
            cited_decisions=extract_citations(full_text) if len(full_text) > 200 else [],
            scraped_at=datetime.now(timezone.utc),
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape HUDOC")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Use full-corpus scraper (all respondent states, tier 1+2 judgments, FR only). "
        "Default: Swiss-respondent only.",
    )
    parser.add_argument("--since", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--max", type=int, default=5, help="Max decisions")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    since = date.fromisoformat(args.since) if args.since else None
    scraper = HUDOCFullScraper() if args.full else HUDOCScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(
            f"  {d.decision_id}  {d.court}  {d.canton}  {d.decision_date}  "
            f"{len(d.full_text)} chars  {d.title[:60]}"
        )
    label = "ECtHR (full)" if args.full else "HUDOC Swiss"
    print(f"\nScraped {len(decisions)} {label} decisions")
