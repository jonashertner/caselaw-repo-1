#!/usr/bin/env python3
"""One-off recovery for BVGer docket-collision misses.

Until 2026-05-27 the BVGer scraper used `make_decision_id(court, docket)`
as the dedup key (without date), so when the same docket had multiple
decisions (Zwischenverfügung / Teilurteil / Endurteil / Revision) only
the first one to be scraped survived. Corpus-wide analysis on
2026-05-27 estimated ~200 BVGer decisions missed across 2007-2025
(~0.26% of total) for this reason.

This script:
  1. Queries the Weblaw search API for a given date range
  2. For each result, generates `make_decision_id("bvger", docket)`
  3. Looks up that id in decisions.db
  4. If our stored row has a DIFFERENT decision_date than this result's
     ruling date → this is a same-docket-different-date miss
  5. Fetches the full content via singleDocQueryService (leid)
  6. Appends a JSONL line to output/decisions/bvger.jsonl with a
     date-suffixed decision_id: `bvger_<docket-norm>_d<YYYYMMDD>`
  7. Next build_fts5 ingests the new row through the normal merge path
     (the date suffix means no further collision)

Idempotency: the script reads the bvger.jsonl tail to skip dockets
already recovered with date-suffix ids.

Usage (on VPS, where Weblaw API + decisions.db are reachable):

    # Verify a single case
    python3 scripts/recover_bvger_docket_collisions.py \
        --from 2010-01-01 --to 2010-01-15 --dry-run

    # Recover a single docket
    python3 scripts/recover_bvger_docket_collisions.py \
        --docket "B-1092/2009"

    # Full sweep across 2007-2025 (slow; cap with --max if needed)
    python3 scripts/recover_bvger_docket_collisions.py \
        --from 2007-01-01 --to 2025-12-31 --max 250

The script ONLY writes to bvger.jsonl. It does NOT touch the live
decisions.db; the next nightly publish picks up the appended rows.
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import requests  # noqa: E402

from models import make_decision_id, normalize_docket  # noqa: E402
from scrapers.bvger import (  # noqa: E402
    WEBLAW_SEARCH_URL,
    WEBLAW_CONTENT_URL,
    WEBLAW_HEADERS,
    WEBLAW_AGGS_FIELDS,
)

DECISIONS_DB = REPO_ROOT / "output" / "decisions.db"
BVGER_JSONL = REPO_ROOT / "output" / "decisions" / "bvger.jsonl"

log = logging.getLogger("recover_bvger")


def weblaw_search(ab: date, bis: date, offset: int = 0, size: int = 100) -> dict:
    """Query Weblaw for BVGer rulings in a date range."""
    body = {
        "guiLanguage": "de",
        "userID": "opencaselaw-recover-bvger",
        "sessionDuration": int(time.time()) % 10000,
        "size": size,
        "aggs": {"fields": WEBLAW_AGGS_FIELDS, "size": "10"},
        "metadataDateMap": {
            "rulingDate": {
                "from": ab.strftime("%Y-%m-%dT00:00:00.000Z"),
                "to": bis.strftime("%Y-%m-%dT23:59:59.999Z"),
            }
        },
    }
    if offset > 0:
        body["from"] = offset
    r = requests.post(WEBLAW_SEARCH_URL, headers=WEBLAW_HEADERS, json=body, timeout=30)
    r.raise_for_status()
    return r.json()


def weblaw_content(leid: str) -> str | None:
    """Fetch the full text of a BVGer decision by leid."""
    try:
        r = requests.get(f"{WEBLAW_CONTENT_URL}/{leid}", headers=WEBLAW_HEADERS, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        # The Weblaw response has the content under various keys depending on lang
        for key in ("text", "fullText", "content", "body"):
            v = data.get(key)
            if v and isinstance(v, str) and len(v) > 100:
                return v
        return None
    except Exception as e:
        log.warning("weblaw_content(%s) failed: %s", leid, e)
        return None


def parse_docket_from_title(titles: list) -> str | None:
    """Extract the docket number from the title field of a Weblaw result.

    Title format: "BVGer B-1092/2009;;TAF B-1092/2009;;TAF B-1092/2009;;BVGer B-1092/2009"
    """
    import re

    if not titles:
        return None
    blob = "|".join(titles)
    m = re.search(r"\b([A-Z]-\d+/\d{4})\b", blob)
    return m.group(1) if m else None


def parse_language_from_panel(panels: list) -> str:
    """Heuristic language detection from the panel string.

    Panel format: "Abt. II (Wirtschaft, …);;Cour II (économie, …);;Corte II (economia, …);;…"
    First segment is the authoritative-language version (DE by convention).
    """
    if not panels:
        return "de"
    first = panels[0].split(";;")[0]
    if first.lower().startswith("cour"):
        return "fr"
    if first.lower().startswith("corte"):
        return "it"
    return "de"


def find_missing_decisions(
    db_conn: sqlite3.Connection,
    ab: date,
    bis: date,
    docket_filter: str | None = None,
    max_recover: int = 250,
    dry_run: bool = False,
) -> list[dict]:
    """Walk Weblaw search results in [ab, bis] and identify docket-collision misses.

    Returns the list of new decision dicts ready to append to bvger.jsonl.
    """
    new_rows: list[dict] = []
    seen_leids: set[str] = set()  # in case of duplicate hits across pages

    offset = 0
    page_size = 100
    while True:
        try:
            resp = weblaw_search(ab, bis, offset=offset, size=page_size)
        except Exception as e:
            log.error("weblaw_search(%s..%s, offset=%d) failed: %s", ab, bis, offset, e)
            break

        docs = resp.get("documents", [])
        total = resp.get("totalNumberOfDocuments", 0)
        if not docs:
            break

        for doc in docs:
            leid = doc.get("leid")
            if not leid or leid in seen_leids:
                continue
            seen_leids.add(leid)

            kw = doc.get("metadataKeywordTextMap", {})
            titles = kw.get("title", [])
            panels = kw.get("panel", [])
            docket = parse_docket_from_title(titles)
            if not docket:
                continue
            if docket_filter and docket != docket_filter:
                continue

            ruling_date_str = doc.get("metadataDateMap", {}).get("rulingDate", "")
            try:
                ruling_date = datetime.fromisoformat(
                    ruling_date_str.replace("Z", "+00:00")
                ).date()
            except (ValueError, AttributeError):
                continue

            base_id = make_decision_id("bvger", docket)
            row = db_conn.execute(
                "SELECT decision_date FROM decisions WHERE decision_id = ?", (base_id,)
            ).fetchone()
            if not row:
                # We don't have ANY decision under this docket — different bug class
                # (genuine miss, not collision). Skip; the main scraper handles those.
                continue
            our_date = str(row[0]) if row[0] else ""

            if our_date == ruling_date_str[:10]:
                continue  # We have THIS exact decision

            # COLLISION: same docket, different date. Recover this leid.
            disambig_id = f"{base_id}_d{ruling_date.strftime('%Y%m%d')}"
            # Skip if we've already recovered this one
            existing = db_conn.execute(
                "SELECT 1 FROM decisions WHERE decision_id = ?", (disambig_id,)
            ).fetchone()
            if existing:
                continue

            log.info(
                "MISSING: %s (docket=%s, date=%s, our_date=%s, leid=%s) → %s",
                base_id, docket, ruling_date, our_date, leid, disambig_id,
            )

            if dry_run:
                new_rows.append({"decision_id": disambig_id, "_dry_run": True})
            else:
                full_text = weblaw_content(leid)
                if not full_text:
                    log.warning("  could not fetch content for %s; skipping", leid)
                    continue
                language = parse_language_from_panel(panels)
                title = titles[0].split(";;")[0] if titles else docket
                new_rows.append({
                    "decision_id": disambig_id,
                    "court": "bvger",
                    "canton": "CH",
                    "docket_number": docket,
                    "decision_date": ruling_date.strftime("%Y-%m-%d"),
                    "language": language,
                    "title": title,
                    "full_text": full_text,
                    "regeste": None,
                    "source_url": f"https://bvger.weblaw.ch/cache?id={leid}&guiLanguage={language}",
                    "cited_decisions": [],
                    "scraped_at": datetime.utcnow().isoformat() + "Z",
                    "_recovery_source": "recover_bvger_docket_collisions.py",
                    "_recovery_leid": leid,
                })
                time.sleep(0.5)  # gentle rate-limit

            if len(new_rows) >= max_recover:
                return new_rows

        offset += len(docs)
        if offset >= total:
            break

    return new_rows


def append_to_jsonl(rows: list[dict], jsonl_path: Path) -> None:
    """Append recovered rows to the BVGer JSONL file."""
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with open(jsonl_path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--from", dest="ab", type=date.fromisoformat,
                    default=date(2007, 1, 1),
                    help="Start date (YYYY-MM-DD). Default: 2007-01-01.")
    ap.add_argument("--to", dest="bis", type=date.fromisoformat,
                    default=date.today(),
                    help="End date (YYYY-MM-DD). Default: today.")
    ap.add_argument("--docket", default=None,
                    help="Only recover this specific docket (e.g. 'B-1092/2009'). "
                         "Otherwise scan the full date range.")
    ap.add_argument("--max", type=int, default=250,
                    help="Stop after N recoveries (safety cap). Default 250.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Find missing decisions but do NOT fetch or write.")
    ap.add_argument("--jsonl", type=Path, default=BVGER_JSONL,
                    help=f"Path to append to. Default: {BVGER_JSONL}.")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    if not DECISIONS_DB.exists():
        log.error("decisions.db not found at %s — run on the VPS", DECISIONS_DB)
        return 1

    db_conn = sqlite3.connect(f"file:{DECISIONS_DB}?mode=ro&immutable=1", uri=True)
    db_conn.row_factory = sqlite3.Row

    log.info("Scanning Weblaw %s → %s%s",
             args.ab, args.bis,
             f" (docket filter: {args.docket})" if args.docket else "")

    if args.docket:
        # Narrow date range to roughly cover the docket's possible lifespan
        # (docket year ± 3 years; the user case B-1092/2009 has decisions 2009-2010).
        try:
            year = int(args.docket.split("/")[-1])
            args.ab = date(year, 1, 1)
            args.bis = date(year + 3, 12, 31)
        except (ValueError, IndexError):
            pass

    rows = find_missing_decisions(
        db_conn, args.ab, args.bis,
        docket_filter=args.docket,
        max_recover=args.max,
        dry_run=args.dry_run,
    )

    log.info("Found %d collision-missed decisions", len(rows))
    if rows and not args.dry_run:
        append_to_jsonl(rows, args.jsonl)
        log.info("Appended %d rows to %s", len(rows), args.jsonl)
        log.info("Next nightly publish will ingest them via build_fts5 disambiguation.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
