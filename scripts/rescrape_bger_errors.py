#!/usr/bin/env python3
"""Re-scrape BGer decisions that have error-page text instead of real content.

Finds decisions with "Document Dienstes ist fehlgeschlagen" in full_text,
re-fetches from relevancy.bger.ch, and overwrites the JSONL entry.
"""
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scrapers.bger import BgerScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rescrape")

DB_PATH = Path("output/decisions.db")
JSONL_PATH = Path("output/decisions/bger.jsonl")
ERROR_SIGNATURE = "Document Dienstes ist fehlgeschlagen"


def main():
    if not DB_PATH.exists():
        log.error(f"DB not found: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT decision_id, docket_number, decision_date, source_url, language
           FROM decisions
           WHERE court = 'bger'
             AND full_text LIKE ?""",
        (f"%{ERROR_SIGNATURE}%",),
    ).fetchall()
    conn.close()

    if not rows:
        log.info("No error decisions found — nothing to do")
        return

    log.info(f"Found {len(rows)} BGer decisions with error text")

    # Load existing JSONL into a dict keyed by decision_id for patching
    existing = {}
    if JSONL_PATH.exists():
        with open(JSONL_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    existing[entry["decision_id"]] = entry
                except (json.JSONDecodeError, KeyError):
                    continue
    log.info(f"Loaded {len(existing)} entries from {JSONL_PATH}")

    scraper = BgerScraper()
    fixed = 0
    still_broken = 0

    for row in rows:
        did = row["decision_id"]
        docket = row["docket_number"]
        stub = {
            "docket_number": docket,
            "decision_date": row["decision_date"],
            "url": row["source_url"] or "",
            "language": row["language"] or "de",
        }

        log.info(f"Re-fetching {docket} ...")
        time.sleep(0.5)

        try:
            decision = scraper.fetch_decision(stub)
        except Exception as e:
            log.warning(f"  Failed: {e}")
            still_broken += 1
            continue

        if not decision:
            log.warning(f"  No result for {docket}")
            still_broken += 1
            continue

        if ERROR_SIGNATURE in (decision.full_text or ""):
            log.warning(f"  Still has error text: {docket}")
            still_broken += 1
            continue

        # Update the JSONL entry
        if did in existing:
            existing[did]["full_text"] = decision.full_text
            existing[did]["regeste"] = decision.regeste or existing[did].get("regeste", "")
            if decision.title:
                existing[did]["title"] = decision.title
            fixed += 1
            log.info(f"  Fixed {docket}: {len(decision.full_text)} chars")
        else:
            log.warning(f"  {did} not in JSONL — skipping")
            still_broken += 1

    # Write back
    if fixed > 0:
        tmp_path = JSONL_PATH.with_suffix(".jsonl.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            for entry in existing.values():
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        tmp_path.replace(JSONL_PATH)
        log.info(f"Wrote {len(existing)} entries to {JSONL_PATH}")

    log.info(f"Done: {fixed} fixed, {still_broken} still broken out of {len(rows)}")


if __name__ == "__main__":
    main()
