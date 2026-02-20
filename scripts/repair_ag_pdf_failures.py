#!/usr/bin/env python3
"""
One-off repair script: re-extract text from AG decisions where PDF extraction
previously failed. Reads ag_gerichte.jsonl, finds entries with the error marker,
re-downloads the PDF, extracts text with pymupdf, and rewrites the JSONL.

Usage:
    python3 scripts/repair_ag_pdf_failures.py [--dry-run]
"""

from __future__ import annotations

import io
import json
import logging
import sys
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

JSONL_DIR = Path("output/decisions")
FAILURE_MARKER = "[PDF text extraction failed for "
REQUEST_DELAY = 1.0  # seconds between PDF downloads


def extract_text_pymupdf(pdf_bytes: bytes) -> str:
    """Extract text using pymupdf (fitz)."""
    import fitz

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = [page.get_text() for page in doc]
    doc.close()
    return "\n\n".join(p for p in pages if p.strip())


def detect_language(text: str) -> str:
    """Simple language detection based on common words."""
    text_lower = text[:3000].lower()
    fr_words = sum(1 for w in ("le ", "la ", "les ", "des ", "une ", "est ", "dans ") if w in text_lower)
    it_words = sum(1 for w in ("il ", "la ", "le ", "dei ", "una ", "che ", "nel ") if w in text_lower)
    de_words = sum(1 for w in ("der ", "die ", "das ", "den ", "ein ", "ist ", "und ") if w in text_lower)
    if fr_words > de_words and fr_words > it_words:
        return "fr"
    if it_words > de_words and it_words > fr_words:
        return "it"
    return "de"


def repair_file(jsonl_path: Path, dry_run: bool = False) -> tuple[int, int, int]:
    """Repair a single JSONL file. Returns (total, failed, fixed)."""
    lines = jsonl_path.read_text(encoding="utf-8").splitlines()
    total = len(lines)
    failed = 0
    fixed = 0
    new_lines = []

    session = requests.Session()
    session.headers.update({
        "Origin": "https://gesetzessammlungen.ag.ch",
        "User-Agent": "Mozilla/5.0 (compatible; Swiss-Caselaw-Repair/1.0)",
    })

    for i, line in enumerate(lines):
        if not line.strip():
            new_lines.append(line)
            continue

        obj = json.loads(line)
        full_text = obj.get("full_text", "")

        if FAILURE_MARKER not in full_text:
            new_lines.append(line)
            continue

        failed += 1
        pdf_url = obj.get("pdf_url", "")
        docket = obj.get("docket_number", "?")

        if not pdf_url:
            logger.warning(f"  [{failed}] No pdf_url for {docket}, skipping")
            new_lines.append(line)
            continue

        if dry_run:
            logger.info(f"  [dry-run] Would re-extract: {docket} from {pdf_url}")
            new_lines.append(line)
            continue

        # Download PDF
        try:
            time.sleep(REQUEST_DELAY)
            resp = session.get(pdf_url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  [{failed}] PDF download failed for {docket}: {e}")
            new_lines.append(line)
            continue

        # Extract text
        try:
            text = extract_text_pymupdf(resp.content)
        except Exception as e:
            logger.warning(f"  [{failed}] pymupdf extraction failed for {docket}: {e}")
            new_lines.append(line)
            continue

        if len(text) < 50:
            logger.warning(f"  [{failed}] Still too short for {docket}: {len(text)} chars")
            new_lines.append(line)
            continue

        # Update the record
        obj["full_text"] = text
        obj["text_length"] = len(text)
        obj["has_full_text"] = True
        if len(text) > 100:
            obj["language"] = detect_language(text)
        new_lines.append(json.dumps(obj, ensure_ascii=False))
        fixed += 1

        if fixed % 50 == 0:
            logger.info(f"  Fixed {fixed}/{failed} so far...")

    if not dry_run and fixed > 0:
        # Write repaired file
        jsonl_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        logger.info(f"  Wrote {jsonl_path} ({fixed} entries repaired)")

    return total, failed, fixed


def main():
    dry_run = "--dry-run" in sys.argv

    # Find all JSONL files with failures
    if not JSONL_DIR.exists():
        logger.error(f"JSONL directory not found: {JSONL_DIR}")
        sys.exit(1)

    target_files = []
    for jsonl_path in sorted(JSONL_DIR.glob("*.jsonl")):
        content = jsonl_path.read_text(encoding="utf-8")
        count = content.count(FAILURE_MARKER)
        if count > 0:
            target_files.append((jsonl_path, count))
            logger.info(f"Found {count} failures in {jsonl_path.name}")

    if not target_files:
        logger.info("No PDF extraction failures found.")
        return

    total_fixed = 0
    total_failed = 0
    for jsonl_path, count in target_files:
        logger.info(f"\nRepairing {jsonl_path.name} ({count} failures)...")
        total, failed, fixed = repair_file(jsonl_path, dry_run=dry_run)
        total_failed += failed
        total_fixed += fixed
        logger.info(f"  {jsonl_path.name}: {total} total, {failed} failed, {fixed} fixed")

    logger.info(f"\n=== Summary ===")
    logger.info(f"  Total failures found: {total_failed}")
    logger.info(f"  Successfully fixed: {total_fixed}")
    logger.info(f"  Still broken: {total_failed - total_fixed}")


if __name__ == "__main__":
    main()
