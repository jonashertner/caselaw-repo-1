#!/usr/bin/env python3
"""
One-time repair script: fix doubled base URLs in BPatGer pdf_url fields,
re-download PDFs, and extract full text.

The BPatGer scraper produced pdf_url values like:
    https://www.bundespatentgericht.chhttps://www.bundespatentgericht.ch/...
This script fixes them to:
    https://www.bundespatentgericht.ch/...
then downloads the PDF and extracts text using fitz (PyMuPDF) with pdfplumber fallback.

Usage:
    python3 scripts/repair_bpatger_pdfs.py [--dry-run]
"""

from __future__ import annotations

import io
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

JSONL_PATH = Path("output/decisions/bpatger.jsonl")
FAILED_LOG = Path("logs/bpatger_pdf_repair_failures.log")

DOUBLED_PREFIX = "https://www.bundespatentgericht.chhttps://www.bundespatentgericht.ch"
CORRECT_PREFIX = "https://www.bundespatentgericht.ch"

REQUEST_DELAY = 2.0
REQUEST_TIMEOUT = 30
MIN_EXTRACTED_LENGTH = 50


def fix_doubled_url(url: str) -> str:
    """Fix the doubled base URL pattern."""
    if url.startswith(DOUBLED_PREFIX):
        return CORRECT_PREFIX + url[len(DOUBLED_PREFIX):]
    return url


def _extract_pdf_text(data: bytes) -> str:
    """Extract text from PDF bytes using fitz (PyMuPDF) with pdfplumber fallback."""
    try:
        import fitz

        with fitz.open(stream=data, filetype="pdf") as doc:
            return "\n\n".join(page.get_text() for page in doc)
    except Exception:
        import pdfplumber

        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n\n".join(p.extract_text() or "" for p in pdf.pages)


def main():
    dry_run = "--dry-run" in sys.argv

    if not JSONL_PATH.exists():
        logger.error(f"Input not found: {JSONL_PATH}")
        sys.exit(1)

    FAILED_LOG.parent.mkdir(parents=True, exist_ok=True)

    # Load all entries
    entries = []
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))

    logger.info(f"Loaded {len(entries)} BPatGer entries")

    # Count entries with doubled URLs
    doubled_url_count = sum(
        1 for e in entries
        if (e.get("pdf_url") or "").startswith(DOUBLED_PREFIX)
    )
    has_pdf_url_count = sum(1 for e in entries if e.get("pdf_url"))
    logger.info(f"{has_pdf_url_count} entries have pdf_url")
    logger.info(f"{doubled_url_count} entries have doubled base URL")

    if dry_run:
        logger.info("[dry-run] Would fix URLs and re-extract PDFs:")
        for e in entries:
            pdf_url = e.get("pdf_url") or ""
            if pdf_url.startswith(DOUBLED_PREFIX):
                fixed = fix_doubled_url(pdf_url)
                docket = e.get("docket_number", "?")
                text_len = len(e.get("full_text", "") or "")
                logger.info(f"  {docket}: text_length={text_len}, url_fix={pdf_url[:80]}... -> {fixed[:80]}...")
        logger.info("[dry-run] No changes made.")
        return

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; Swiss-Caselaw-Repair/1.0)",
    })

    fixed_urls = 0
    extracted = 0
    failed = 0
    skipped = 0
    failures = []
    t0 = time.time()

    for i, entry in enumerate(entries):
        pdf_url = entry.get("pdf_url") or ""
        docket = entry.get("docket_number", "?")

        if not pdf_url:
            skipped += 1
            continue

        # Fix doubled URL if present
        original_url = pdf_url
        pdf_url = fix_doubled_url(pdf_url)
        url_was_fixed = pdf_url != original_url

        if url_was_fixed:
            entry["pdf_url"] = pdf_url
            # Also fix source_url if it has the same problem
            source_url = entry.get("source_url") or ""
            if source_url.startswith(DOUBLED_PREFIX):
                entry["source_url"] = fix_doubled_url(source_url)
            fixed_urls += 1

        # Re-download and extract if URL was fixed or text is short/missing
        existing_text_len = len(entry.get("full_text", "") or "")
        if not url_was_fixed and existing_text_len >= MIN_EXTRACTED_LENGTH:
            skipped += 1
            continue

        # Download PDF
        try:
            time.sleep(REQUEST_DELAY)
            resp = session.get(pdf_url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  [{i+1}/{len(entries)}] PDF download failed for {docket}: {e}")
            failed += 1
            failures.append(f"{docket}\t{pdf_url}\tdownload: {e}")
            continue

        # Verify it's a PDF
        if not resp.content[:5] == b"%PDF-":
            logger.warning(f"  [{i+1}/{len(entries)}] Not a PDF for {docket} (got {resp.content[:20]})")
            failed += 1
            failures.append(f"{docket}\t{pdf_url}\tnot a PDF")
            continue

        # Extract text
        try:
            text = _extract_pdf_text(resp.content)
        except Exception as e:
            logger.warning(f"  [{i+1}/{len(entries)}] Text extraction failed for {docket}: {e}")
            failed += 1
            failures.append(f"{docket}\t{pdf_url}\textraction: {e}")
            continue

        if len(text.strip()) < MIN_EXTRACTED_LENGTH:
            logger.warning(f"  [{i+1}/{len(entries)}] Extracted text too short for {docket}: {len(text)} chars")
            failed += 1
            failures.append(f"{docket}\t{pdf_url}\ttoo short: {len(text)} chars")
            continue

        # Update the entry
        entry["full_text"] = text
        entry["text_length"] = len(text)
        entry["has_full_text"] = True
        extracted += 1

        logger.info(
            f"  [{i+1}/{len(entries)}] {docket}: extracted {len(text)} chars"
            f"{' (URL fixed)' if url_was_fixed else ''}"
        )

    elapsed = time.time() - t0
    logger.info(f"\nDone in {elapsed:.0f}s")
    logger.info(f"  URLs fixed:     {fixed_urls}")
    logger.info(f"  Text extracted: {extracted}")
    logger.info(f"  Failed:         {failed}")
    logger.info(f"  Skipped:        {skipped}")

    # Write to temp file and atomically replace
    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=".jsonl", dir=str(JSONL_PATH.parent)
    )
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fout:
            for entry in entries:
                fout.write(json.dumps(entry, ensure_ascii=False) + "\n")

        if extracted > 0 or fixed_urls > 0:
            os.replace(tmp_path, str(JSONL_PATH))
            logger.info(f"Replaced {JSONL_PATH} ({extracted} texts extracted, {fixed_urls} URLs fixed)")
        else:
            os.unlink(tmp_path)
            logger.warning("No changes made -- original file unchanged")
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    # Write failure log
    if failures:
        with open(FAILED_LOG, "w", encoding="utf-8") as f:
            for line in failures:
                f.write(line + "\n")
        logger.info(f"Failures logged to {FAILED_LOG}")


if __name__ == "__main__":
    main()
