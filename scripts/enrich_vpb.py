#!/usr/bin/env python3
"""
VPB PDF Enrichment Script
==========================

Enriches es_ch_vb.jsonl (Verwaltungspraxis der Bundesbehörden) with full text
extracted from PDFs at amtsdruckschriften.bar.admin.ch.

The existing JSONL has 23,032 decisions but ~23,031 have < 500 chars of text
(metadata-only). This script downloads the original PDFs and extracts full text.

Usage:
    python3 scripts/enrich_vpb.py                   # full run
    python3 scripts/enrich_vpb.py --max 10 -v       # test with 10 decisions
    python3 scripts/enrich_vpb.py --dry-run          # count eligible, no downloads

Run on VPS:
    nohup python3 scripts/enrich_vpb.py >> logs/enrich_vpb.log 2>&1 &
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("enrich_vpb")

# Rate limit: 0.5s between requests (bar.admin.ch is generous)
REQUEST_DELAY = 0.5
TIMEOUT = 60
MIN_TEXT_LEN = 500

# Docket patterns in PDF headers
JAAC_PATTERN = re.compile(r"JAAC\s+(\d+\.\d+)")
VPB_PATTERN = re.compile(r"VPB\s+(\d{4}\.\d+)")

# Language detection (duplicated from models.py for standalone use)
_LANG_WORDS = {
    "de": re.compile(
        r"\b(?:der|die|das|ein|eine|einer|er|sie|ihn|hat|hatte|hätte|ist|war|sind)\b",
        re.IGNORECASE,
    ),
    "fr": re.compile(
        r"\b(?:le|lui|elle|je|on|vous|nous|leur|qui|quand|parce|que|faire|sont|vont)\b",
        re.IGNORECASE,
    ),
    "it": re.compile(
        r"\b(?:della|del|di|casi|una|al|questa|più|primo|grado|che|diritto|leggi|corte)\b",
        re.IGNORECASE,
    ),
}


def detect_language(text: str) -> str:
    scores = {
        lang: len(pattern.findall(text[:5000]))
        for lang, pattern in _LANG_WORDS.items()
    }
    return max(scores, key=scores.get)  # type: ignore


def extract_pdf_text(data: bytes) -> str:
    """Extract text from PDF bytes using fitz (PyMuPDF) with pdfplumber fallback."""
    try:
        import fitz

        doc = fitz.open(stream=data, filetype="pdf")
        return "\n\n".join(p.get_text() for p in doc)
    except ImportError:
        pass
    try:
        import pdfplumber
        import io

        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n\n".join(p.extract_text() or "" for p in pdf.pages)
    except ImportError:
        pass
    return ""


def parse_vpb_docket(text: str) -> str | None:
    """Extract JAAC/VPB docket number from PDF text header."""
    header = text[:1000]
    m = JAAC_PATTERN.search(header)
    if m:
        return f"JAAC {m.group(1)}"
    m = VPB_PATTERN.search(header)
    if m:
        return f"VPB {m.group(1)}"
    return None


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "SwissCaselawBot/1.0 (https://github.com/jonashertner/caselaw-repo; "
            "legal research; respects rate limits)"
        ),
    })
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def enrich(
    jsonl_path: Path,
    max_decisions: int | None = None,
    dry_run: bool = False,
) -> None:
    """Enrich VPB decisions with PDF full text."""
    if not jsonl_path.exists():
        logger.error(f"JSONL not found: {jsonl_path}")
        sys.exit(1)

    # Load all decisions
    logger.info(f"Loading {jsonl_path} ...")
    decisions = []
    with open(jsonl_path) as f:
        for line in f:
            line = line.strip()
            if line:
                decisions.append(json.loads(line))
    logger.info(f"Loaded {len(decisions)} decisions")

    # Count eligible (short text + valid pdf_url)
    eligible = [
        d for d in decisions
        if len(d.get("full_text", "")) < MIN_TEXT_LEN
        and d.get("pdf_url")
    ]
    logger.info(
        f"Eligible for enrichment: {len(eligible)} / {len(decisions)} "
        f"(< {MIN_TEXT_LEN} chars with pdf_url)"
    )

    if dry_run:
        logger.info("Dry run — exiting")
        return

    session = build_session()
    enriched = 0
    download_errors = 0
    extract_errors = 0
    last_request = 0.0

    # Build index for fast lookup
    id_to_idx = {d["decision_id"]: i for i, d in enumerate(decisions)}

    if max_decisions:
        eligible = eligible[:max_decisions]
        logger.info(f"Limited to {max_decisions} decisions")

    for i, d in enumerate(eligible):
        pdf_url = d["pdf_url"]
        did = d["decision_id"]

        # Rate limit
        elapsed = time.time() - last_request
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)

        try:
            last_request = time.time()
            resp = session.get(pdf_url, timeout=TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            download_errors += 1
            logger.warning(f"[{i+1}/{len(eligible)}] Download failed {did}: {e}")
            continue

        # Extract text
        text = extract_pdf_text(resp.content)
        if not text or len(text.strip()) < 50:
            extract_errors += 1
            logger.warning(
                f"[{i+1}/{len(eligible)}] No text extracted from {did} "
                f"({len(resp.content)} bytes PDF)"
            )
            continue

        # Update the decision in-place
        idx = id_to_idx[did]
        decisions[idx]["full_text"] = text.strip()

        # Try to parse docket from PDF
        vpb_docket = parse_vpb_docket(text)
        if vpb_docket and (not d.get("docket_number") or d["docket_number"] == did):
            decisions[idx]["docket_number"] = vpb_docket

        # Detect language from new text
        decisions[idx]["language"] = detect_language(text)

        enriched += 1
        if enriched % 100 == 0:
            logger.info(
                f"[{i+1}/{len(eligible)}] Enriched {enriched} so far "
                f"(errors: {download_errors} download, {extract_errors} extract)"
            )

    # Write back atomically
    logger.info(f"Writing enriched JSONL ({len(decisions)} decisions) ...")
    tmp_path = jsonl_path.with_suffix(".jsonl.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        for d in decisions:
            f.write(json.dumps(d, ensure_ascii=False, default=str) + "\n")
    os.replace(tmp_path, jsonl_path)

    logger.info(
        f"Done. Enriched: {enriched}, "
        f"Download errors: {download_errors}, "
        f"Extract errors: {extract_errors}, "
        f"Total decisions: {len(decisions)}"
    )


def main():
    parser = argparse.ArgumentParser(description="Enrich VPB decisions with PDF text")
    parser.add_argument(
        "--input",
        type=str,
        default="output/decisions/es_ch_vb.jsonl",
        help="Input JSONL path",
    )
    parser.add_argument("--max", type=int, help="Max decisions to enrich")
    parser.add_argument("--dry-run", action="store_true", help="Count eligible only")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    for noisy in ("pdfminer", "pdfplumber", "urllib3", "chardet", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    enrich(
        jsonl_path=Path(args.input),
        max_decisions=args.max,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
