#!/usr/bin/env python3
"""Recover VPB (Verwaltungspraxis des Bundes) texts by downloading and extracting PDFs.

Reads es_ch_vb.jsonl, downloads PDFs from amtsdruckschriften.bar.admin.ch,
extracts text with fitz/PyMuPDF (fast), and writes updated JSONL.
"""

import json
import logging
import os
import signal
import sys
import tempfile
import time
from pathlib import Path

import fitz  # PyMuPDF — much faster than pdfplumber for large docs
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

JSONL_PATH = Path("output/decisions/es_ch_vb.jsonl")
OUTPUT_PATH = Path("output/decisions/es_ch_vb.jsonl.new")
FAILED_LOG = Path("logs/vpb_pdf_failures.log")

REQUEST_TIMEOUT = 30
RETRY_COUNT = 2
RETRY_DELAY = 2
MIN_EXTRACTED_LENGTH = 100
MAX_PDF_SIZE = 15_000_000  # Skip PDFs > 15MB
PROGRESS_INTERVAL = 200


class TimeoutError(Exception):
    pass


def _timeout_handler(signum, frame):
    raise TimeoutError("PDF extraction timed out")


def download_pdf(url: str, dest: str) -> bool:
    """Download PDF to dest path. Returns True on success."""
    for attempt in range(RETRY_COUNT + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
            resp.raise_for_status()
            size = 0
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
                    size += len(chunk)
                    if size > MAX_PDF_SIZE:
                        logger.debug(f"PDF too large (>{MAX_PDF_SIZE}): {url}")
                        return False
            # Verify it's actually a PDF
            with open(dest, "rb") as f:
                header = f.read(5)
            if header != b"%PDF-":
                return False
            return True
        except Exception as e:
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.debug(f"Download failed: {url}: {e}")
                return False
    return False


def extract_text_fitz(pdf_path: str, timeout: int = 60) -> str:
    """Extract text using PyMuPDF with timeout."""
    old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(timeout)
    try:
        doc = fitz.open(pdf_path)
        pages = []
        for page in doc:
            text = page.get_text()
            if text and text.strip():
                pages.append(text.strip())
        doc.close()
        return "\n\n".join(pages)
    except TimeoutError:
        logger.debug(f"Extraction timed out after {timeout}s")
        return ""
    except Exception as e:
        logger.debug(f"fitz failed: {e}")
        return ""
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def main():
    if not JSONL_PATH.exists():
        logger.error(f"Input not found: {JSONL_PATH}")
        sys.exit(1)

    FAILED_LOG.parent.mkdir(parents=True, exist_ok=True)

    # Load all entries
    entries = []
    with open(JSONL_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))

    logger.info(f"Loaded {len(entries)} VPB entries")

    need_extract = sum(1 for e in entries if len(e.get("full_text", "") or "") < 500)
    logger.info(f"{need_extract} need PDF text extraction")

    extracted = 0
    failed = 0
    skipped = 0
    failures = []
    t0 = time.time()

    for i, entry in enumerate(entries):
        existing = len(entry.get("full_text", "") or "")
        if existing >= 500:
            skipped += 1
            if (i + 1) % PROGRESS_INTERVAL == 0:
                elapsed = time.time() - t0
                logger.info(
                    f"[{i+1}/{len(entries)}] extracted={extracted} failed={failed} "
                    f"skipped={skipped} ({elapsed:.0f}s)"
                )
            continue

        pdf_url = entry.get("pdf_url") or entry.get("source_url") or ""
        if not pdf_url or "amtsdruckschriften" not in pdf_url:
            skipped += 1
            continue

        tmp_path = tempfile.mktemp(suffix=".pdf")
        try:
            if not download_pdf(pdf_url, tmp_path):
                failed += 1
                failures.append(pdf_url)
                continue

            text = extract_text_fitz(tmp_path, timeout=60)
            if len(text) >= MIN_EXTRACTED_LENGTH:
                entry["full_text"] = text
                entry["text_length"] = len(text)
                entry["has_full_text"] = True
                extracted += 1
            else:
                failed += 1
                failures.append(pdf_url)
        except Exception as e:
            failed += 1
            failures.append(pdf_url)
            logger.debug(f"Entry {i} error: {e}")
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        if (i + 1) % PROGRESS_INTERVAL == 0:
            elapsed = time.time() - t0
            rate = (extracted + failed) / elapsed if elapsed > 0 else 0
            eta = (need_extract - extracted - failed) / rate if rate > 0 else 0
            logger.info(
                f"[{i+1}/{len(entries)}] extracted={extracted} failed={failed} "
                f"skipped={skipped} ({rate:.1f}/s, ETA {eta/60:.0f}m)"
            )

    elapsed = time.time() - t0
    logger.info(
        f"Done in {elapsed:.0f}s. Extracted: {extracted}, Failed: {failed}, Skipped: {skipped}"
    )

    # Write output
    with open(OUTPUT_PATH, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    # Write failure log
    if failures:
        with open(FAILED_LOG, "w") as f:
            for url in failures:
                f.write(url + "\n")
        logger.info(f"Failures logged to {FAILED_LOG}")

    # Atomic replace
    if extracted > 0:
        os.rename(OUTPUT_PATH, JSONL_PATH)
        logger.info(f"Replaced {JSONL_PATH} — {extracted} entries now have full text")
    else:
        logger.warning("No extractions succeeded — original file unchanged")


if __name__ == "__main__":
    main()
