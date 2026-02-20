#!/usr/bin/env python3
"""
One-off repair script: re-extract text from AG decisions where PDF extraction
previously failed. Streams ag_gerichte.jsonl line by line, re-downloads the PDF
for failed entries, extracts text with pymupdf, writes repaired JSONL.

Usage:
    python3 scripts/repair_ag_pdf_failures.py [--dry-run]
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

JSONL_DIR = Path("output/decisions")
FAILURE_MARKER = "[PDF text extraction failed for "
REQUEST_DELAY = 1.0


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
    """Repair a single JSONL file by streaming. Returns (total, failed, fixed)."""
    total = 0
    failed = 0
    fixed = 0

    session = requests.Session()
    session.headers.update({
        "Origin": "https://gesetzessammlungen.ag.ch",
        "User-Agent": "Mozilla/5.0 (compatible; Swiss-Caselaw-Repair/1.0)",
    })

    # Stream: read input line by line, write to temp file
    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=".jsonl", dir=str(jsonl_path.parent)
    )

    try:
        with open(jsonl_path, "r", encoding="utf-8") as fin, \
             os.fdopen(tmp_fd, "w", encoding="utf-8") as fout:

            for line in fin:
                total += 1
                stripped = line.rstrip("\n")

                if not stripped:
                    fout.write(line)
                    continue

                # Quick check before parsing JSON
                if FAILURE_MARKER not in stripped:
                    fout.write(line)
                    continue

                obj = json.loads(stripped)
                full_text = obj.get("full_text", "")

                if FAILURE_MARKER not in full_text:
                    # False positive from quick check (marker in other field)
                    fout.write(line)
                    continue

                failed += 1
                pdf_url = obj.get("pdf_url", "")
                docket = obj.get("docket_number", "?")

                if dry_run:
                    logger.info(f"  [dry-run] Would re-extract: {docket}")
                    fout.write(line)
                    continue

                if not pdf_url:
                    logger.warning(f"  No pdf_url for {docket}, skipping")
                    fout.write(line)
                    continue

                # Download PDF
                try:
                    time.sleep(REQUEST_DELAY)
                    resp = session.get(pdf_url, timeout=60)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"  PDF download failed for {docket}: {e}")
                    fout.write(line)
                    continue

                # Extract text
                try:
                    text = extract_text_pymupdf(resp.content)
                except Exception as e:
                    logger.warning(f"  pymupdf failed for {docket}: {e}")
                    fout.write(line)
                    continue

                if len(text) < 50:
                    logger.warning(f"  Still too short for {docket}: {len(text)} chars")
                    fout.write(line)
                    continue

                # Update the record
                obj["full_text"] = text
                obj["text_length"] = len(text)
                obj["has_full_text"] = True
                if len(text) > 100:
                    obj["language"] = detect_language(text)
                fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                fixed += 1

                if fixed % 50 == 0:
                    logger.info(f"  Fixed {fixed}/{failed} so far...")

        # Replace original with repaired file
        if not dry_run and fixed > 0:
            os.replace(tmp_path, str(jsonl_path))
            logger.info(f"  Replaced {jsonl_path} ({fixed} entries repaired)")
        else:
            os.unlink(tmp_path)

    except Exception:
        # Clean up temp file on error
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return total, failed, fixed


def main():
    dry_run = "--dry-run" in sys.argv

    if not JSONL_DIR.exists():
        logger.error(f"JSONL directory not found: {JSONL_DIR}")
        sys.exit(1)

    # Scan for files with failures (line-by-line to avoid loading whole file)
    target_files = []
    for jsonl_path in sorted(JSONL_DIR.glob("*.jsonl")):
        count = 0
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                if FAILURE_MARKER in line:
                    count += 1
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
