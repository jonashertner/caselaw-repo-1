#!/usr/bin/env python3
"""
Audit BGer coverage by comparing Neuheiten pages against ingested JSONL.

Fetches the last N days of BGer Neuheiten pages (?date=YYYYMMDD&mode=news),
extracts all docket numbers, and checks each against the JSONL file.

Usage:
    python3 scripts/audit_bger_coverage.py --days 7
    python3 scripts/audit_bger_coverage.py --days 30 --verbose
    python3 scripts/audit_bger_coverage.py --days 7 --rescrape  # re-scrape missing
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("audit_bger")

SEARCH_HOST = "https://search.bger.ch"
NEUHEITEN_DATE_URL = (
    SEARCH_HOST
    + "/ext/eurospider/live/{lang}/php/aza/http/index_aza.php"
    "?date={date}&lang={lang}&mode=news"
)

DOCKET_RE = re.compile(r"\b(\d{1,2}[A-Z][_ ]\d+/\d{4})\b")
DOCKET_OLD_RE = re.compile(r"\b(\d[A-Z]\.\d+/\d{4})\b")

DEFAULT_JSONL = Path("output/decisions/bger.jsonl")


def extract_docket(text: str) -> str | None:
    """Extract a BGer docket number from text."""
    for pattern in [DOCKET_RE, DOCKET_OLD_RE]:
        m = pattern.search(text)
        if m:
            return m.group(1).replace(" ", "_")
    return None


def load_known_dockets(jsonl_path: Path) -> set[str]:
    """Load all known docket numbers from the JSONL file."""
    dockets = set()
    if not jsonl_path.exists():
        logger.warning(f"JSONL not found: {jsonl_path}")
        return dockets
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                dn = obj.get("docket_number")
                if dn:
                    dockets.add(dn)
            except json.JSONDecodeError:
                continue
    return dockets


def fetch_neuheiten_dockets(check_date: date, session: requests.Session) -> list[str]:
    """Fetch a single day's Neuheiten page and extract docket numbers."""
    date_str = check_date.strftime("%Y%m%d")
    url = NEUHEITEN_DATE_URL.format(lang="de", date=date_str)

    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch Neuheiten for {check_date}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    dockets = []

    ranklist = soup.select_one("div.ranklist_content ol")
    if not ranklist:
        ranklist = soup.find("ol")
    if not ranklist:
        return dockets

    for li in ranklist.find_all("li", recursive=False):
        link = li.select_one("span > a") or li.find("a", href=True)
        if not link:
            continue
        meta_text = link.get_text(strip=True)
        href = link.get("href", "")
        docket = extract_docket(meta_text) or extract_docket(href)
        if docket:
            dockets.append(docket)

    return dockets


def main():
    parser = argparse.ArgumentParser(description="Audit BGer scraper coverage")
    parser.add_argument(
        "--days", type=int, default=7,
        help="Number of days to check (default: 7)",
    )
    parser.add_argument(
        "--jsonl", type=str, default=str(DEFAULT_JSONL),
        help=f"Path to BGer JSONL file (default: {DEFAULT_JSONL})",
    )
    parser.add_argument(
        "--rescrape", action="store_true",
        help="Re-scrape missing decisions (runs run_scraper.py for each)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    jsonl_path = Path(args.jsonl)
    logger.info(f"Loading known dockets from {jsonl_path}")
    known = load_known_dockets(jsonl_path)
    logger.info(f"Loaded {len(known)} known dockets")

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
    })

    today = date.today()
    total_published = 0
    total_captured = 0
    all_missing: list[tuple[date, str]] = []

    for days_ago in range(args.days):
        check_date = today - timedelta(days=days_ago)
        dockets = fetch_neuheiten_dockets(check_date, session)

        if not dockets:
            print(f"{check_date}: no decisions on Neuheiten page")
            continue

        captured = [d for d in dockets if d in known]
        missing = [d for d in dockets if d not in known]

        total_published += len(dockets)
        total_captured += len(captured)
        all_missing.extend((check_date, d) for d in missing)

        pct = len(captured) / len(dockets) * 100 if dockets else 0
        status = "OK" if not missing else "GAPS"
        print(
            f"{check_date}: {len(dockets)} published, "
            f"{len(captured)} captured ({pct:.0f}%) [{status}]"
        )

        if missing and args.verbose:
            for d in missing:
                print(f"  MISSING: {d}")

        time.sleep(1)  # Be polite to the server

    # Summary
    print()
    pct = total_captured / total_published * 100 if total_published else 0
    print(
        f"Total: {total_published} published, {total_captured} captured "
        f"({pct:.1f}%), {len(all_missing)} missing"
    )

    if all_missing:
        print("\nMissing decisions:")
        for dt, docket in all_missing:
            print(f"  {docket} ({dt})")

    if all_missing and args.rescrape:
        print(f"\nRe-scraping {len(all_missing)} missing decisions is not yet implemented.")
        print("Run manually: python3 run_scraper.py bger --max N")

    sys.exit(1 if all_missing else 0)


if __name__ == "__main__":
    main()
