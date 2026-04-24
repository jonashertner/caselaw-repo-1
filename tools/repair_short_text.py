"""
One-shot repair for short-text decisions.

Walks output/decisions/<court>.jsonl, finds entries whose `full_text` is
below THRESHOLD chars, refetches the stored pdf_url, re-extracts via
fitz → pdfplumber → pdfminer, and rewrites the JSONL atomically.

URLs that no longer work (HTTP 500 / non-PDF response — Tribuna session
expiry on older entries) are kept as-is and counted as `expired`.

Why a separate tool rather than a scraper change:
  - sz_gerichte already has the forward-fix shipped (335dd81); this
    repair is purely about backfilling content on existing rows.
  - gr/be/fr/zg short entries pre-date even the current scraper code —
    they appear to come from an older federation ingest. Their stored
    URLs sometimes still work (recent ones do), so we can refresh
    content without re-running discovery.

Safety:
  - Output JSONL is rewritten via .tmp + os.replace() — atomic swap.
  - Original file is preserved as .bak alongside.
  - decision_id, court, canton, docket_number, date are NEVER changed —
    only full_text, language (re-detected), cited_decisions, and
    full_text-derived flags. This means the snapshot mechanism does NOT
    flag these as new (correct: id-set unchanged).
  - Designed to be safe to run in parallel with the daily scrape: the
    scraper appends to the live JSONL during 01:00-02:30 UTC. Run this
    tool outside that window (recommended: 11:00-23:00 UTC).

Usage:
  python3 tools/repair_short_text.py sz_gerichte --threshold 2000 --max 5
  python3 tools/repair_short_text.py sz_gerichte --threshold 2000
  python3 tools/repair_short_text.py gr_gerichte --threshold 2000
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


def extract_pdf(data: bytes) -> str:
    """Try fitz → pdfplumber → pdfminer. Return whichever yields the
    longest text. Empty string if all fail."""
    candidates: list[str] = []
    try:
        import fitz  # PyMuPDF
        with fitz.open(stream=data, filetype="pdf") as doc:
            text = "\n\n".join(p.get_text() for p in doc)
            if text:
                candidates.append(text)
    except Exception as e:
        logger.debug(f"fitz failed: {e}")
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            text = "\n\n".join(p.extract_text() or "" for p in pdf.pages)
            if text:
                candidates.append(text)
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")
    try:
        from pdfminer.high_level import extract_text
        text = extract_text(io.BytesIO(data))
        if text:
            candidates.append(text)
    except Exception as e:
        logger.debug(f"pdfminer failed: {e}")
    if not candidates:
        return ""
    return max(candidates, key=len)


def detect_language_safe(text: str) -> str:
    """detect_language requires Swiss-language matchable text. Default to
    'de' for empty / very short input."""
    if len(text) < 50:
        return "de"
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from models import detect_language
        return detect_language(text)
    except Exception:
        return "de"


def extract_citations_safe(text: str) -> list[str]:
    if len(text) < 200:
        return []
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from models import extract_citations
        return extract_citations(text)
    except Exception:
        return []


def repair_court(court: str, threshold: int, max_count: int | None, delay: float):
    repo = Path(__file__).parent.parent
    src = repo / "output" / "decisions" / f"{court}.jsonl"
    if not src.exists():
        logger.error(f"JSONL not found: {src}")
        return 1

    tmp = src.with_suffix(".jsonl.tmp")
    bak = src.with_suffix(".jsonl.bak")

    sess = requests.Session()
    sess.headers["User-Agent"] = "SwissCaselawRepair/1.0 (research)"

    total = repaired = expired = no_url = unchanged = errors = 0
    last_log = time.time()
    repair_cap_reached = False

    with src.open("r", encoding="utf-8") as fin, tmp.open("w", encoding="utf-8") as fout:
        for line_no, line in enumerate(fin, 1):
            total += 1
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                # Preserve unreadable line as-is (defensive: never lose data)
                fout.write(line)
                errors += 1
                continue

            ft = d.get("full_text") or ""

            # Done collecting — pass everything else through unchanged
            if repair_cap_reached or len(ft) >= threshold:
                fout.write(line)
                unchanged += 1
                continue

            url = d.get("pdf_url") or ""
            if not url:
                # Some scrapers store the PDF URL in source_url (sz_gerichte
                # uses tx-style HTML URL in source_url, PDF in pdf_url)
                src_url = d.get("source_url") or ""
                if "ServletDownload" in src_url or src_url.lower().endswith(".pdf"):
                    url = src_url
            if not url:
                fout.write(line)
                no_url += 1
                continue

            try:
                time.sleep(delay)
                r = sess.get(url, timeout=60)
                if r.status_code != 200 or r.content[:4] != b"%PDF":
                    fout.write(line)
                    expired += 1
                    continue
                text = extract_pdf(r.content)
                # Tighten — only accept if substantially better than current.
                if len(text) <= len(ft) + 100:
                    fout.write(line)
                    expired += 1
                    continue
                d["full_text"] = text
                d["language"] = detect_language_safe(text)
                d["cited_decisions"] = extract_citations_safe(text)
                fout.write(json.dumps(d, ensure_ascii=False) + "\n")
                repaired += 1
                if max_count is not None and repaired >= max_count:
                    repair_cap_reached = True
            except Exception as e:
                logger.warning(f"[{court}] {d.get('decision_id','?')} fetch err: {e}")
                fout.write(line)
                errors += 1

            now = time.time()
            if now - last_log > 30:
                logger.info(
                    f"[{court}] processed={total} repaired={repaired} "
                    f"expired={expired} no_url={no_url} unchanged={unchanged}"
                )
                last_log = now

    # Atomic swap with backup
    if bak.exists():
        bak.unlink()
    os.replace(src, bak)
    os.replace(tmp, src)

    logger.info(
        f"[{court}] DONE total={total} repaired={repaired} "
        f"expired={expired} no_url={no_url} unchanged={unchanged} errors={errors}"
    )
    logger.info(f"[{court}] backup retained at {bak}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("court", help="Court code, e.g. sz_gerichte, gr_gerichte")
    ap.add_argument("--threshold", type=int, default=2000,
                    help="Repair entries with full_text < THRESHOLD chars")
    ap.add_argument("--max", type=int, default=None,
                    help="Stop after repairing N entries (for test runs)")
    ap.add_argument("--delay", type=float, default=2.0,
                    help="Seconds between portal requests")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    return repair_court(args.court, args.threshold, args.max, args.delay)


if __name__ == "__main__":
    sys.exit(main())
