#!/usr/bin/env python3
"""
Extract individual decisions from AI (Appenzell Innerrhoden) annual
compilation PDFs.

The canton publishes annual "Geschäftsbericht der Gerichte" PDFs containing
10-70 numbered decisions each. This script downloads the available PDFs,
splits them into individual decisions using the TOC, and appends to the
ai_gerichte.jsonl file.

Available PDFs (as of March 2026):
- Recent (2023-2024): Gerichtsentscheide
- 2021: Verwaltungsentscheide (separate from 2021 on)
- Historical (2013-2020): Verwaltungs- und Gerichtsentscheide

Usage:
    python3 scripts/extract_ai_compilations.py [--dry-run] [--year YEAR]
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from models import detect_language, extract_citations, make_decision_id

BASE_URL = "https://www.ai.ch"

COMPILATION_URLS = {
    2024: f"{BASE_URL}/gerichte/gerichtsentscheide/gerichtsentscheide/gerichtsentscheide-2024.pdf/download",
    2023: f"{BASE_URL}/gerichte/gerichtsentscheide/gerichtsentscheide/gerichtsentscheide-2023.pdf/download",
    2021: f"{BASE_URL}/themen/staat-und-recht/veroeffentlichungen/verwaltungs-und-gerichtsentscheide/ftw-simplelayout-filelistingblock/verwaltungsentscheide-2021/download",
    2020: f"{BASE_URL}/themen/staat-und-recht/veroeffentlichungen/verwaltungs-und-gerichtsentscheide/ftw-simplelayout-filelistingblock/verwaltungs-und-gerichtsentscheide-2020/download",
    2015: f"{BASE_URL}/themen/staat-und-recht/veroeffentlichungen/verwaltungs-und-gerichtsentscheide/ftw-simplelayout-filelistingblock/verwaltungs-und-gerichtsentscheide-2015/download",
    2014: f"{BASE_URL}/themen/staat-und-recht/veroeffentlichungen/verwaltungs-und-gerichtsentscheide/ftw-simplelayout-filelistingblock/verwaltungs-und-gerichtsentscheide-2014/download",
    2013: f"{BASE_URL}/themen/staat-und-recht/veroeffentlichungen/verwaltungs-und-gerichtsentscheide/ftw-simplelayout-filelistingblock/verwaltungs-und-gerichtsentscheide-2013/download",
}

RE_TOC_ENTRY = re.compile(r"(\d{1,3})\.\s*\n([^\n]+?)\.{3,}\s*(\d+)")

MONTHS_DE = {
    "Januar": 1, "Februar": 2, "März": 3, "April": 4,
    "Mai": 5, "Juni": 6, "Juli": 7, "August": 8,
    "September": 9, "Oktober": 10, "November": 11, "Dezember": 12,
}
_MONTH_NAMES = "|".join(MONTHS_DE.keys())

# "Urteil vom 31. Januar 2022" or "Entscheid vom 17. Januar 2023"
RE_ENTSCHEID_DATE_SPELLED = re.compile(
    rf"(?:Urteil|Beschluss|Entscheid|Verfügung)\s+vom\s+(\d{{1,2}})\.\s*({_MONTH_NAMES})\s+(\d{{4}})"
)
# "vom 31. Januar 2022" (broader)
RE_VOM_DATE_SPELLED = re.compile(
    rf"vom\s+(\d{{1,2}})\.\s*({_MONTH_NAMES})\s+(\d{{4}})"
)
# Numeric fallback: "31.01.2022"
RE_DATE_NUMERIC = re.compile(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})")


def _parse_date(text: str, year: int) -> date | None:
    """Extract the decision date, preferring dates in the compilation year."""
    # Try "Urteil vom 31. Januar 2024" with matching year first
    for m in RE_ENTSCHEID_DATE_SPELLED.finditer(text[:5000]):
        y = int(m.group(3))
        if abs(y - year) <= 1:  # within 1 year of compilation
            try:
                return date(y, MONTHS_DE[m.group(2)], int(m.group(1)))
            except (ValueError, KeyError):
                pass

    # Try any "vom DD. Month YYYY" with matching year
    for m in RE_VOM_DATE_SPELLED.finditer(text[:5000]):
        y = int(m.group(3))
        if abs(y - year) <= 1:
            try:
                return date(y, MONTHS_DE[m.group(2)], int(m.group(1)))
            except (ValueError, KeyError):
                pass

    # Numeric fallback with year filter
    for m in RE_DATE_NUMERIC.finditer(text[:3000]):
        y = int(m.group(3))
        if abs(y - year) <= 1:
            try:
                return date(y, int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass

    return None


def split_pdf(pdf_bytes: bytes, year: int) -> list[dict]:
    """Split a compilation PDF into individual decisions using the TOC."""
    import fitz

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = [p.get_text() for p in doc]
    doc.close()

    # Parse TOC from first pages (check up to 5 pages for TOC)
    toc_text = "\n".join(pages[:5])
    toc_entries = []
    for m in RE_TOC_ENTRY.finditer(toc_text):
        num = int(m.group(1))
        title = m.group(2).strip().rstrip(".")
        page = int(m.group(3))
        toc_entries.append((num, title, page))

    if not toc_entries:
        logger.warning(f"  {year}: no TOC found, trying fallback split")
        return _fallback_split(pages, year)

    # Find page offset: content page 1 = which doc page?
    offset = None
    for i, page_text in enumerate(pages):
        m = re.search(r"^(\d+)\s*-\s*\d+\s*$", page_text, re.MULTILINE)
        if m:
            content_page = int(m.group(1))
            offset = i - content_page
            break

    if offset is None:
        # Estimate: TOC pages = number of pages before first content page
        offset = max(0, len(pages) - max(e[2] for e in toc_entries) - 1)
        # Common: 2-3 TOC pages
        if offset > 5:
            offset = 2

    decisions = []
    for i, (num, title, start_page) in enumerate(toc_entries):
        end_page = toc_entries[i + 1][2] if i + 1 < len(toc_entries) else len(pages) - offset + 1

        doc_start = start_page + offset
        doc_end = end_page + offset

        text_parts = []
        for p in range(max(0, doc_start), min(doc_end, len(pages))):
            text_parts.append(pages[p])

        text = "\n\n".join(text_parts).strip()
        # Remove page footer lines like "123 - 216"
        text = re.sub(r"\n\d+\s*-\s*\d+\s*\n", "\n", text)
        # Remove repeated header lines
        text = re.sub(
            r"Geschäftsbericht \d{4} der Gerichte [–—-] [^\n]+\n", "", text
        )

        if len(text) < 200:
            continue

        decisions.append({
            "number": num,
            "title": title,
            "text": text,
        })

    return decisions


def _fallback_split(pages: list[str], year: int) -> list[dict]:
    """Fallback: split on numbered headers when no TOC is found."""
    full_text = "\n\n".join(pages)

    # Look for "N.\n" followed by uppercase title (no dots)
    pattern = re.compile(
        r"(?:^|\n)(\d{1,2})\.\s*\n([A-ZÄÖÜ][^\n]{10,})",
        re.MULTILINE,
    )
    positions = []
    for m in pattern.finditer(full_text):
        title = m.group(2).strip()
        if "....." in title:
            continue
        positions.append((m.start(), int(m.group(1)), title))

    decisions = []
    for i, (pos, num, title) in enumerate(positions):
        end = positions[i + 1][0] if i + 1 < len(positions) else len(full_text)
        text = full_text[pos:end].strip()
        if len(text) < 200:
            continue
        decisions.append({
            "number": num,
            "title": title[:200],
            "text": text,
        })

    return decisions


def process_compilation(
    year: int, url: str, existing_ids: set, dry_run: bool
) -> list[dict]:
    """Download and process a single compilation PDF."""
    import requests

    logger.info(f"  Downloading {year}...")
    try:
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            logger.warning(f"  {year}: HTTP {r.status_code}")
            return []
    except Exception as e:
        logger.warning(f"  {year}: download failed: {e}")
        return []

    if len(r.content) < 1000:
        logger.warning(f"  {year}: PDF too small ({len(r.content)} bytes)")
        return []

    entries = split_pdf(r.content, year)
    logger.info(f"  {year}: {len(entries)} decisions found in PDF")

    results = []
    for entry in entries:
        docket = f"AI-{year}/{entry['number']}"
        decision_id = make_decision_id("ai_gerichte", docket)

        if decision_id in existing_ids:
            continue

        decision_date = _parse_date(entry["text"], year)
        if not decision_date:
            decision_date = date(year, 6, 30)  # mid-year fallback

        text = entry["text"]
        lang = detect_language(text) if len(text) > 100 else "de"

        result = {
            "decision_id": decision_id,
            "court": "ai_gerichte",
            "canton": "AI",
            "chamber": "Kantonsgericht",
            "docket_number": docket,
            "decision_date": decision_date.isoformat(),
            "language": lang,
            "title": entry["title"],
            "full_text": text,
            "source_url": url,
            "pdf_url": url,
            "decision_type": "",
            "cited_decisions": json.dumps(
                extract_citations(text) if len(text) > 200 else []
            ),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "has_full_text": True,
            "text_length": len(text),
        }
        results.append(result)

    return results


def main():
    parser = argparse.ArgumentParser(description="Extract AI compilation PDFs")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--year", type=int, help="Process only this year")
    args = parser.parse_args()

    jsonl_path = Path("output/decisions/ai_gerichte.jsonl")

    existing_ids = set()
    if jsonl_path.exists():
        with open(jsonl_path) as f:
            for line in f:
                try:
                    e = json.loads(line)
                    existing_ids.add(e.get("decision_id", ""))
                except json.JSONDecodeError:
                    pass
    logger.info(f"Existing AI decisions: {len(existing_ids)}")

    urls = COMPILATION_URLS
    if args.year:
        if args.year not in urls:
            logger.error(
                f"Year {args.year} not available. Available: {sorted(urls.keys())}"
            )
            return
        urls = {args.year: urls[args.year]}

    all_new = []
    for year in sorted(urls.keys(), reverse=True):
        url = urls[year]
        new_entries = process_compilation(year, url, existing_ids, args.dry_run)
        for e in new_entries:
            existing_ids.add(e["decision_id"])
        all_new.extend(new_entries)
        logger.info(f"  {year}: {len(new_entries)} new decisions")
        time.sleep(2)

    if not all_new:
        logger.info("No new decisions found.")
        return

    if args.dry_run:
        logger.info(f"[dry-run] Would add {len(all_new)} decisions")
        for e in all_new:
            logger.info(
                f"  {e['decision_id']}: {e['decision_date']} "
                f"({e['text_length']:,} chars) — {e['title'][:60]}"
            )
        return

    with open(jsonl_path, "a") as f:
        for entry in all_new:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    logger.info(f"Added {len(all_new)} new decisions to {jsonl_path}")
    logger.info(f"Total AI decisions now: {len(existing_ids)}")


if __name__ == "__main__":
    main()
