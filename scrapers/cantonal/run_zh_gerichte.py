#!/usr/bin/env python3
"""
Runner for ZH Gerichte scraper.

Usage:
    python run_zh_gerichte.py                    # Full scrape from 1980
    python run_zh_gerichte.py --since 2024-01-01 # Only from 2024
    python run_zh_gerichte.py --max 100          # First 100 only (test)
    python run_zh_gerichte.py --probe             # Probe API only (no scraping)
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent))

from zh_gerichte import ZHGerichteScraper, LIVESEARCH_URL, FIXED_PARAMS, HOST

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("zh_gerichte.log"),
    ],
)

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("output/decisions")
JSONL_FILE = OUTPUT_DIR / "zh_gerichte.jsonl"


def append_jsonl(decision, filepath: Path):
    """Append a single decision to JSONL file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "a") as f:
        f.write(decision.model_dump_json() + "\n")


def probe_api():
    """Quick probe to verify the API is responding."""
    import requests

    logger.info("Probing gerichte-zh.ch livesearch API...")

    # Test with a narrow recent window
    params = dict(FIXED_PARAMS)
    today = date.today()
    month_ago = today - timedelta(days=30)
    params["entscheiddatum_von"] = month_ago.strftime("%d.%m.%Y")
    params["entscheiddatum_bis"] = today.strftime("%d.%m.%Y")

    try:
        resp = requests.get(
            LIVESEARCH_URL,
            params=params,
            headers={
                "User-Agent": "SwissCaselawBot/1.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=30,
        )
        logger.info(f"Status: {resp.status_code}")
        logger.info(f"Response size: {len(resp.text)} chars")

        if resp.status_code == 200 and len(resp.text) > 100:
            from bs4 import BeautifulSoup
            import re

            soup = BeautifulSoup(resp.text, "html.parser")

            # Count
            count_div = soup.find("div", id="entscheideText")
            if count_div:
                strong = count_div.find("strong")
                if strong:
                    logger.info(f"Decisions in last 30 days: {strong.get_text(strip=True)}")

            # Entries
            entries = soup.find_all("div", class_=re.compile(r"^entscheid\s+entscheid_nummer_"))
            details = soup.find_all("div", class_=re.compile(r"^entscheidDetails\s+container_"))
            logger.info(f"Entscheid divs: {len(entries)}, Details divs: {len(details)}")

            # Show first entry
            if entries and details:
                first_detail = details[0]
                for p in first_detail.find_all("p"):
                    spans = p.find_all("span")
                    if len(spans) >= 2:
                        logger.info(f"  {spans[0].get_text(strip=True)}: {spans[1].get_text(strip=True)}")

                # PDF URL
                pdf_link = first_detail.find("a", class_="pdf-icon")
                if pdf_link:
                    logger.info(f"  PDF: {HOST + pdf_link.get('href', '')}")

                # Also check the entscheid div
                first_entry = entries[0]
                for a in first_entry.find_all("a", href=True):
                    href = a.get("href", "")
                    if ".pdf" in href.lower():
                        logger.info(f"  PDF (entscheid): {HOST + href}")

            logger.info("✅ API probe successful!")
            return True
        else:
            logger.error(f"Unexpected response: status={resp.status_code}, length={len(resp.text)}")
            # Show first 500 chars for debugging
            logger.error(f"Response preview: {resp.text[:500]}")
            return False

    except Exception as e:
        logger.error(f"API probe failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="ZH Gerichte scraper")
    parser.add_argument("--since", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--max", type=int, help="Max decisions to scrape")
    parser.add_argument("--probe", action="store_true", help="Probe API only")
    parser.add_argument("--state-dir", default="state", help="State directory")
    parser.add_argument("--output", default=str(JSONL_FILE), help="Output JSONL file")
    args = parser.parse_args()

    if args.probe:
        sys.exit(0 if probe_api() else 1)

    scraper = ZHGerichteScraper(state_dir=Path(args.state_dir))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting ZH Gerichte scraper. Output: {output_path}")
    logger.info(f"Known decisions: {scraper.state.count()}")

    count = 0
    errors = 0

    for stub in scraper.discover_new(since_date=args.since):
        if args.max and count >= args.max:
            logger.info(f"Reached --max={args.max}, stopping.")
            break

        try:
            decision = scraper.fetch_decision(stub)
            if decision:
                append_jsonl(decision, output_path)
                scraper.state.mark_scraped(decision.decision_id)
                count += 1
                if count % 100 == 0:
                    logger.info(f"Progress: {count} decisions scraped")
        except Exception as e:
            errors += 1
            logger.error(f"Error scraping {stub.get('docket_number', '?')}: {e}")
            if errors > scraper.MAX_ERRORS:
                logger.error(f"Too many errors ({errors}), stopping.")
                break

    logger.info(
        f"ZH Gerichte complete. Scraped: {count}, Errors: {errors}, "
        f"Total known: {scraper.state.count()}"
    )


if __name__ == "__main__":
    main()
