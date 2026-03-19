#!/usr/bin/env python3
"""Fix SH decisions with short text by extracting new PDF UUIDs from CMS detail pages.

The SH CMS was migrated — old PDF UUIDs (sliderguid) return 404.
New UUIDs are available on each decision's detail page but require
JavaScript rendering (camoufox) to discover.

Usage:
    nice -n 19 python3 scripts/fix_sh_decisions.py \
        --jsonl output/decisions/sh_gerichte.jsonl \
        --max-chars 200 \
        -v
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
from pathlib import Path

import os
import sys

# Add repo root to path for models import
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import fitz  # PyMuPDF
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://obergerichtsentscheide.sh.ch"
# UUIDs that appear on every page (logo, header images) — skip these
COMMON_UUIDS = set()


def load_short_decisions(jsonl_path: Path, max_chars: int) -> list[dict]:
    """Load decisions with short text from JSONL."""
    short = []
    with open(jsonl_path) as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            text_len = len(row.get("full_text", ""))
            if text_len < max_chars:
                short.append(row)
    return short


def discover_common_uuids(browser) -> set[str]:
    """Visit the homepage to find UUIDs that appear on every page (logo etc)."""
    page = browser.new_page()
    try:
        page.goto(f"{BASE_URL}", timeout=20000)
        page.wait_for_load_state("networkidle", timeout=10000)
        time.sleep(1)
        content = page.content()
        uuids = re.findall(r"/CMS/get/file/([a-f0-9-]{36})", content)
        common = {u for u in uuids if uuids.count(u) >= 2}
        logger.info(f"Found {len(common)} common UUIDs to skip (header/logo)")
        return common
    finally:
        page.close()


def find_pdf_uuid_on_detail_page(browser, permalink: str, common_uuids: set[str]) -> str | None:
    """Navigate to a decision detail page and find the PDF UUID."""
    url = f"{BASE_URL}{permalink}" if permalink.startswith("/") else permalink
    page = browser.new_page()
    try:
        page.goto(url, timeout=20000)
        page.wait_for_load_state("networkidle", timeout=10000)
        time.sleep(1)

        content = page.content()
        all_uuids = re.findall(r"/CMS/get/file/([a-f0-9-]{36})", content)

        # Deduplicate and remove common UUIDs
        unique = []
        seen = set()
        for u in all_uuids:
            if u not in seen and u not in common_uuids:
                seen.add(u)
                unique.append(u)

        if not unique:
            return None

        # Test each UUID — find the one that returns a PDF
        for uuid in unique:
            try:
                r = httpx.head(
                    f"{BASE_URL}/CMS/get/file/{uuid}",
                    timeout=10,
                    follow_redirects=True,
                )
                ct = r.headers.get("content-type", "")
                if "pdf" in ct.lower() or r.status_code == 200 and int(r.headers.get("content-length", "0")) > 5000:
                    return uuid
            except Exception:
                continue

        return None
    except Exception as e:
        logger.debug(f"  Detail page failed: {e}")
        return None
    finally:
        page.close()


def download_and_extract(uuid: str) -> str:
    """Download PDF by UUID and extract text."""
    url = f"{BASE_URL}/CMS/get/file/{uuid}"
    r = httpx.get(url, timeout=30, follow_redirects=True)
    if r.status_code != 200 or len(r.content) < 100:
        return ""

    if not r.content[:5] == b"%PDF-":
        return ""

    try:
        doc = fitz.open(stream=r.content, filetype="pdf")
        text_parts = []
        for page in doc:
            text_parts.append(page.get_text())
        doc.close()
        return "\n\n".join(text_parts).strip()
    except Exception:
        return ""


def update_jsonl(jsonl_path: Path, updates: dict[str, tuple[str, str]]):
    """Update JSONL file with new full_text and pdf_url for given decision_ids.

    updates: {decision_id: (new_full_text, new_pdf_url)}
    """
    lines = []
    updated = 0
    with open(jsonl_path, "rb") as f:
        for line in f:
            if not line.strip():
                lines.append(line)
                continue
            row = json.loads(line)
            did = row.get("decision_id", "")
            if did in updates:
                new_text, new_pdf_url = updates[did]
                row["full_text"] = new_text
                row["pdf_url"] = new_pdf_url
                lines.append(json.dumps(row, ensure_ascii=False).encode("utf-8") + b"\n")
                updated += 1
            else:
                lines.append(line)

    with open(jsonl_path, "wb") as f:
        f.writelines(lines)

    logger.info(f"Updated {updated} decisions in {jsonl_path}")


def main():
    parser = argparse.ArgumentParser(description="Fix SH decisions via CMS detail pages")
    parser.add_argument("--jsonl", type=Path, required=True)
    parser.add_argument("--max-chars", type=int, default=200,
                        help="Process decisions with text shorter than this")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    decisions = load_short_decisions(args.jsonl, args.max_chars)
    logger.info(f"Found {len(decisions)} decisions with <{args.max_chars} chars text")

    if not decisions:
        logger.info("Nothing to fix")
        return

    # Need permalinks — get them from the API
    logger.info("Fetching permalinks from SH CMS API...")
    permalink_map: dict[str, str] = {}
    offset = 0
    while True:
        r = httpx.get(
            f"{BASE_URL}/CMS/content/list",
            params={
                "filter_customposttypeid_int": 402,
                "filter_approvedpaths_string": "*2272926*",
                "rows": 100,
                "start": offset,
                "status": "published",
            },
            timeout=15,
        )
        items = r.json()
        if not items:
            break
        for item in items:
            docket = item.get("kachellabel", "").strip()
            permalink = item.get("permalink", "")
            if docket and permalink:
                from models import make_decision_id
                did = make_decision_id("sh_gerichte", docket)
                permalink_map[did] = permalink
        if len(items) < 100:
            break
        offset += 100

    logger.info(f"Got {len(permalink_map)} permalinks from API")

    # Match short decisions to permalinks
    to_fix = []
    for dec in decisions:
        did = dec["decision_id"]
        permalink = permalink_map.get(did)
        if permalink:
            to_fix.append((did, permalink, dec.get("docket_number", "?")))
        else:
            logger.debug(f"  {did}: no permalink found in API")

    logger.info(f"Matched {len(to_fix)} decisions to permalinks (out of {len(decisions)} short)")

    if not to_fix:
        logger.info("No decisions to fix")
        return

    # Launch camoufox and process
    from camoufox.sync_api import Camoufox

    updates: dict[str, tuple[str, str]] = {}
    success = 0
    fail = 0
    no_pdf = 0
    t0 = time.time()

    with Camoufox(headless=True) as browser:
        common = discover_common_uuids(browser)
        COMMON_UUIDS.update(common)

        for i, (did, permalink, docket) in enumerate(to_fix):
            if (i + 1) % 20 == 0:
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed * 3600
                logger.info(
                    f"Progress: {i+1}/{len(to_fix)} "
                    f"(success={success}, no_pdf={no_pdf}, fail={fail}, "
                    f"rate={rate:.0f}/hr)"
                )

            uuid = find_pdf_uuid_on_detail_page(browser, permalink, COMMON_UUIDS)
            if not uuid:
                no_pdf += 1
                logger.debug(f"  {docket}: no PDF UUID on detail page")
                continue

            text = download_and_extract(uuid)
            if text and len(text) >= 200:
                pdf_url = f"{BASE_URL}/CMS/get/file/{uuid}"
                if not args.dry_run:
                    updates[did] = (text, pdf_url)
                success += 1
                logger.debug(f"  {docket}: extracted {len(text)} chars via new UUID {uuid[:12]}...")
            else:
                fail += 1
                logger.debug(f"  {docket}: PDF found but text too short ({len(text)} chars)")

    elapsed = time.time() - t0
    logger.info(f"\nDone in {elapsed/60:.1f} min")
    logger.info(f"  Success: {success}")
    logger.info(f"  No PDF on detail page: {no_pdf}")
    logger.info(f"  PDF found but extraction failed: {fail}")

    if updates and not args.dry_run:
        update_jsonl(args.jsonl, updates)


if __name__ == "__main__":
    main()
