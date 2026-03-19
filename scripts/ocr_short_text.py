#!/usr/bin/env python3
"""OCR decisions with short extracted text.

Downloads PDFs for decisions where full_text < 500 chars,
runs Tesseract OCR, and updates the JSONL files with extracted text.

Usage:
    nice -n 19 python3 scripts/ocr_short_text.py \
        --db /mnt/HC_Volume_104655575/output/decisions.db \
        --jsonl-dir /mnt/HC_Volume_104655575/output/decisions \
        --max 1000 \
        -v

Requires: tesseract-ocr, tesseract-ocr-deu, tesseract-ocr-fra, tesseract-ocr-ita, PyMuPDF
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path

import fitz  # PyMuPDF
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

LANG_MAP = {"de": "deu", "fr": "fra", "it": "ita"}
MIN_OCR_CHARS = 200  # only update if OCR produces more than this
TIMEOUT = 30  # seconds per PDF download
MAX_RETRIES = 2


def get_short_text_decisions(db_path: str, max_decisions: int) -> list[dict]:
    """Get decisions with short text and a PDF URL."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT decision_id, court, language, pdf_url, source_url,
               LENGTH(COALESCE(full_text, '')) as text_len
        FROM decisions
        WHERE LENGTH(COALESCE(full_text, '')) < 500
          AND (pdf_url IS NOT NULL AND pdf_url != ''
               OR source_url IS NOT NULL AND source_url != '')
        ORDER BY text_len ASC
        LIMIT ?
    """, (max_decisions,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def download_pdf(url: str) -> bytes | None:
    """Download PDF, return bytes or None on failure."""
    # Extract domain for Referer header (some courts block direct access)
    from urllib.parse import urlparse
    parsed = urlparse(url)
    referer = f"{parsed.scheme}://{parsed.netloc}/"

    for attempt in range(MAX_RETRIES):
        try:
            with httpx.Client(timeout=TIMEOUT, follow_redirects=True) as client:
                resp = client.get(url, headers={
                    "User-Agent": "OpenCaseLaw-OCR/1.0 (+https://opencaselaw.ch)",
                    "Referer": referer,
                })
                if resp.status_code == 200 and len(resp.content) > 100:
                    return resp.content
                logger.debug(f"  HTTP {resp.status_code} for {url[:80]}")
        except Exception as e:
            logger.debug(f"  Download error (attempt {attempt+1}): {e}")
            time.sleep(1)
    return None


def pdf_to_images_and_ocr(pdf_bytes: bytes, lang: str) -> str:
    """Render PDF pages to images and OCR with Tesseract."""
    tess_lang = LANG_MAP.get(lang, "deu+fra+ita")
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    full_text_parts = []

    for page_num in range(min(len(doc), 50)):  # cap at 50 pages
        page = doc[page_num]
        # Render at 300 DPI for good OCR quality
        mat = fitz.Matrix(300 / 72, 300 / 72)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")

        # Run Tesseract on the image
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp.write(img_bytes)
            tmp_path = tmp.name

        try:
            result = subprocess.run(
                ["tesseract", tmp_path, "stdout", "-l", tess_lang, "--psm", "1"],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode == 0 and result.stdout.strip():
                full_text_parts.append(result.stdout.strip())
        except subprocess.TimeoutExpired:
            logger.debug(f"  Tesseract timeout on page {page_num}")
        finally:
            os.unlink(tmp_path)

    doc.close()
    return "\n\n".join(full_text_parts)


def update_jsonl(jsonl_dir: str, court: str, decision_id: str, new_text: str) -> bool:
    """Update the full_text field in the court's JSONL file."""
    jsonl_path = Path(jsonl_dir) / f"{court}.jsonl"
    if not jsonl_path.exists():
        logger.warning(f"  JSONL not found: {jsonl_path}")
        return False

    # Read all lines, find and update the matching decision
    lines = jsonl_path.read_bytes().split(b"\n")
    updated = False
    new_lines = []

    for line in lines:
        if not line.strip():
            new_lines.append(line)
            continue
        try:
            row = json.loads(line)
            if row.get("decision_id") == decision_id:
                row["full_text"] = new_text
                row["ocr_applied"] = True
                new_lines.append(json.dumps(row, ensure_ascii=False).encode("utf-8"))
                updated = True
            else:
                new_lines.append(line)
        except json.JSONDecodeError:
            new_lines.append(line)

    if updated:
        jsonl_path.write_bytes(b"\n".join(new_lines))
    return updated


def main():
    parser = argparse.ArgumentParser(description="OCR decisions with short text")
    parser.add_argument("--db", type=str, required=True, help="Path to decisions.db")
    parser.add_argument("--jsonl-dir", type=str, required=True, help="JSONL directory")
    parser.add_argument("--max", type=int, default=100, help="Max decisions to process")
    parser.add_argument("--dry-run", action="store_true", help="Download and OCR but don't update JSONL")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    decisions = get_short_text_decisions(args.db, args.max)
    logger.info(f"Found {len(decisions)} decisions with short text and PDF URL")

    ocr_success = 0
    ocr_fail = 0
    download_fail = 0
    skipped = 0
    t0 = time.time()

    for i, dec in enumerate(decisions):
        did = dec["decision_id"]
        url = dec["pdf_url"] or ""
        source_url = dec.get("source_url") or ""
        lang = dec["language"] or "de"
        court = dec["court"]

        if (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed * 3600
            logger.info(
                f"Progress: {i+1}/{len(decisions)} "
                f"(success={ocr_success}, fail={ocr_fail}, dl_fail={download_fail}, "
                f"rate={rate:.0f}/hr)"
            )

        # Download PDF
        pdf_bytes = download_pdf(url) if url else None
        if not pdf_bytes and source_url:
            # Fallback: try source_url (HTML page) for inline text
            try:
                with httpx.Client(timeout=TIMEOUT, follow_redirects=True) as client:
                    resp = client.get(source_url, headers={
                        "User-Agent": "OpenCaseLaw-OCR/1.0 (+https://opencaselaw.ch)"
                    })
                    if resp.status_code == 200 and len(resp.text) > MIN_OCR_CHARS:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(resp.text, "html.parser")
                        # Remove script/style
                        for tag in soup(["script", "style", "nav", "header", "footer"]):
                            tag.decompose()
                        html_text = soup.get_text(separator="\n", strip=True)
                        if len(html_text) >= MIN_OCR_CHARS:
                            if not args.dry_run:
                                if update_jsonl(args.jsonl_dir, court, did, html_text):
                                    ocr_success += 1
                                    logger.debug(f"  {did}: HTML fallback extracted {len(html_text)} chars")
                                    continue
                            else:
                                ocr_success += 1
                                logger.debug(f"  {did}: [dry-run] HTML would extract {len(html_text)} chars")
                                continue
            except Exception as e:
                logger.debug(f"  {did}: HTML fallback failed: {e}")

        if not pdf_bytes:
            download_fail += 1
            continue

        # Check if it's actually a PDF
        if not pdf_bytes[:5] == b"%PDF-":
            logger.debug(f"  {did}: not a PDF (starts with {pdf_bytes[:20]})")
            skipped += 1
            continue

        # First try text extraction without OCR (maybe fitz can extract text)
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            direct_text = ""
            for page in doc:
                direct_text += page.get_text()
            doc.close()
            if len(direct_text.strip()) >= MIN_OCR_CHARS:
                # fitz extracted enough text — no OCR needed
                if not args.dry_run:
                    if update_jsonl(args.jsonl_dir, court, did, direct_text.strip()):
                        ocr_success += 1
                        logger.debug(f"  {did}: fitz extracted {len(direct_text)} chars")
                    else:
                        ocr_fail += 1
                else:
                    ocr_success += 1
                    logger.debug(f"  {did}: [dry-run] fitz would extract {len(direct_text)} chars")
                continue
        except Exception:
            pass

        # Fall back to OCR
        try:
            ocr_text = pdf_to_images_and_ocr(pdf_bytes, lang)
            if len(ocr_text.strip()) >= MIN_OCR_CHARS:
                if not args.dry_run:
                    if update_jsonl(args.jsonl_dir, court, did, ocr_text.strip()):
                        ocr_success += 1
                        logger.debug(f"  {did}: OCR extracted {len(ocr_text)} chars")
                    else:
                        ocr_fail += 1
                else:
                    ocr_success += 1
                    logger.debug(f"  {did}: [dry-run] OCR would extract {len(ocr_text)} chars")
            else:
                logger.debug(f"  {did}: OCR too short ({len(ocr_text)} chars)")
                ocr_fail += 1
        except Exception as e:
            logger.debug(f"  {did}: OCR error: {e}")
            ocr_fail += 1

    elapsed = time.time() - t0
    logger.info(f"\nDone in {elapsed/60:.1f} min")
    logger.info(f"  Success: {ocr_success}")
    logger.info(f"  OCR fail: {ocr_fail}")
    logger.info(f"  Download fail: {download_fail}")
    logger.info(f"  Skipped (not PDF): {skipped}")
    logger.info(f"  Total processed: {i+1}")


if __name__ == "__main__":
    main()
