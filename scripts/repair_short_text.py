#!/usr/bin/env python3
"""
Short-Text Recovery: PDF Re-extraction for decisions with < 500 chars.

Re-downloads PDFs and extracts text using a 3-stage pipeline:
  1. fitz (PyMuPDF) — fast native-text extraction
  2. pdfplumber — layout-aware fallback
  3. Tesseract OCR — for scanned/image PDFs (optional, --no-ocr to skip)

Streams JSONL line-by-line, writes to temp file, atomic os.replace().
Resume support via output/.repair_progress.json.

Usage:
    python3 scripts/repair_short_text.py --dry-run
    python3 scripts/repair_short_text.py --no-ocr
    python3 scripts/repair_short_text.py --court gr_gerichte --max 100 -v
    python3 scripts/repair_short_text.py --clean-cache

Run on VPS:
    nohup python3 scripts/repair_short_text.py --no-ocr >> logs/repair_short_text.log 2>&1 &
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("repair_short_text")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SHORT_TEXT_THRESHOLD = 500       # Characters — decisions below this are candidates
MIN_EXTRACTED_LENGTH = 200       # New text must be at least this long to replace
MAX_PDF_SIZE = 20 * 1024 * 1024  # 20 MB — skip compilations
MAX_OCR_PAGES = 50               # OCR timeout protection
OCR_TIMEOUT_PER_PAGE = 30        # Seconds per page for Tesseract
OCR_TIMEOUT_TOTAL = 300          # 5 min total per document
REQUEST_DELAY = 1.5              # Seconds between requests to same domain

JSONL_DIR = Path("output/decisions")
PDF_CACHE_DIR = Path("output/.pdf_cache")
PROGRESS_FILE = Path("output/.repair_progress.json")

# ---------------------------------------------------------------------------
# Cross-court dedup (mirrors build_fts5.py overlap groups)
# ---------------------------------------------------------------------------
_COURT_OVERLAP_GROUPS: list[set[str]] = [
    # ZH: entscheidsuche → zh_gerichte, direct scraper → sub-courts
    {"zh_gerichte", "zh_obergericht", "zh_kassationsgericht", "zh_handelsgericht",
     "zh_bezirksgericht_zuerich", "zh_bezirksgericht_winterthur",
     "zh_bezirksgericht_horgen", "zh_bezirksgericht_dietikon",
     "zh_bezirksgericht_buelach", "zh_bezirksgericht_dielsdorf",
     "zh_bezirksgericht_uster", "zh_bezirksgericht_pfaeffikon",
     "zh_bezirksgericht_hinwil", "zh_bezirksgericht_meilen",
     "zh_bezirksgericht_affoltern", "zh_mietgericht", "zh_arbeitsgericht"},
    # SG
    {"sg_gerichte", "sg_publikationen", "sg_versicherungsgericht",
     "sg_verwaltungsgericht", "sg_verwaltungsrekurskommission",
     "sg_kantonsgericht", "sg_handelsgericht"},
    # AG
    {"ag_gerichte", "ag_obergericht", "ag_versicherungsgericht",
     "ag_handelsgericht", "ag_spezialverwaltungsgericht",
     "ag_strafgericht", "ag_zivilgericht", "ag_verwaltungsgericht",
     "ag_anwaltskommission", "ag_aufsichtskommission", "ag_regierungsrat",
     "ag_departement_bks", "ag_departement_bvu", "ag_departement_gs",
     "ag_departement_vi", "ag_justizgericht", "ag_justizleitung", "ag_weitere"},
    # SH
    {"sh_gerichte", "sh_obergericht"},
    # TG
    {"tg_gerichte", "tg_obergericht"},
    # VD
    {"vd_findinfo", "vd_gerichte", "vd_omni"},
    # BS
    {"bs_gerichte", "bs_appellationsgericht", "bs_sozialversicherungsgericht"},
    # BE
    {"be_steuerrekurs", "be_verwaltungsgericht"},
]

_COURT_TO_GROUP: dict[str, str] = {}
for _group in _COURT_OVERLAP_GROUPS:
    _representative = sorted(_group)[0]  # deterministic group ID
    for _code in _group:
        _COURT_TO_GROUP[_code] = _representative


def _make_dedup_key(court: str, docket: str, decision_date: str | None) -> str:
    """Build a dedup key that accounts for cross-court overlap groups."""
    group_court = _COURT_TO_GROUP.get(court, court)
    docket_norm = re.sub(r"[^A-Z0-9]", "", (docket or "").upper())
    # TG: strip NR noise word (matches build_fts5.py)
    if court in ("tg_gerichte", "tg_obergericht"):
        docket_norm = re.sub(r"NR(?=\d)", "", docket_norm)
    # BGE: strip BGE/ATF/DTF prefixes
    if court == "bge":
        docket_norm = re.sub(r"^(?:CH)?(?:BGE|ATF|DTF)", "", docket_norm)
    date_compact = (decision_date or "").replace("-", "")[:8]
    return f"{group_court}|{docket_norm}|{date_compact}"


def build_dedup_index(threshold: int) -> set[str]:
    """
    Pre-scan ALL JSONL files and return the set of dedup keys that already
    have at least one entry with text >= threshold. Entries matching such a
    key in other files can be skipped (they'd lose dedup at publish time).
    """
    logger.info("Building dedup index across all JSONL files...")
    covered: set[str] = set()
    total_entries = 0

    for jsonl_path in sorted(JSONL_DIR.glob("*.jsonl")):
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total_entries += 1
                full_text = obj.get("full_text", "")
                if len(full_text) >= threshold:
                    court = obj.get("court", "")
                    docket = obj.get("docket_number", "")
                    date_str = str(obj.get("decision_date", "") or "")
                    key = _make_dedup_key(court, docket, date_str)
                    covered.add(key)

    logger.info(
        f"Dedup index built: {total_entries} entries scanned, "
        f"{len(covered)} unique decisions already have good text"
    )
    return covered


# Language detection (from models.py)
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


# ---------------------------------------------------------------------------
# Session & rate limiting
# ---------------------------------------------------------------------------
class DomainRateLimiter:
    """Per-domain rate limiter to be polite to different hosts."""

    def __init__(self, delay: float = REQUEST_DELAY):
        self.delay = delay
        self._last_request: dict[str, float] = defaultdict(float)

    def wait(self, url: str) -> None:
        domain = urlparse(url).netloc
        elapsed = time.time() - self._last_request[domain]
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request[domain] = time.time()


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "SwissCaselawBot/1.0 (https://opencaselaw.ch; "
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


# ---------------------------------------------------------------------------
# PDF download with caching
# ---------------------------------------------------------------------------
def download_pdf(
    session: requests.Session,
    rate_limiter: DomainRateLimiter,
    pdf_url: str,
    decision_id: str,
) -> bytes | None:
    """Download PDF, using cache if available. Returns bytes or None."""
    cache_path = PDF_CACHE_DIR / f"{decision_id}.pdf"

    # Check cache first
    if cache_path.exists():
        data = cache_path.read_bytes()
        if len(data) > 0:
            logger.debug(f"Cache hit: {decision_id}")
            return data

    rate_limiter.wait(pdf_url)

    try:
        resp = session.get(pdf_url, timeout=60, stream=True)
        resp.raise_for_status()

        # Check Content-Length before downloading full body
        content_length = resp.headers.get("Content-Length")
        if content_length and int(content_length) > MAX_PDF_SIZE:
            logger.debug(f"Skipping oversized PDF ({content_length} bytes): {decision_id}")
            resp.close()
            return None

        data = resp.content
        if len(data) > MAX_PDF_SIZE:
            logger.debug(f"Skipping oversized PDF ({len(data)} bytes): {decision_id}")
            return None

        # Cache the download
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(data)
        return data

    except Exception as e:
        logger.debug(f"Download failed for {decision_id}: {e}")
        return None


# ---------------------------------------------------------------------------
# 3-stage extraction pipeline
# ---------------------------------------------------------------------------
def extract_with_fitz(pdf_bytes: bytes) -> str:
    """Stage 1: PyMuPDF native text extraction."""
    import fitz

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = []
    for page in doc:
        text = page.get_text()
        if text.strip():
            pages.append(text)
    doc.close()
    return "\n\n".join(pages)


def extract_with_pdfplumber(pdf_bytes: bytes) -> str:
    """Stage 2: pdfplumber layout-aware extraction."""
    import pdfplumber

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        pages = []
        for page in pdf.pages:
            text = page.extract_text()
            if text and text.strip():
                pages.append(text)
    return "\n\n".join(pages)


def _lang_to_tesseract(lang: str) -> str:
    """Map language code to Tesseract language pack."""
    return {"de": "deu", "fr": "fra", "it": "ita", "rm": "deu"}.get(lang, "deu")


def extract_with_ocr(pdf_bytes: bytes, lang_hint: str = "de") -> str:
    """Stage 3: Tesseract OCR via fitz pixmap rendering at 300 DPI."""
    import fitz

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    num_pages = min(len(doc), MAX_OCR_PAGES)
    tess_lang = _lang_to_tesseract(lang_hint)
    pages = []
    total_start = time.time()

    for i in range(num_pages):
        if time.time() - total_start > OCR_TIMEOUT_TOTAL:
            logger.debug(f"OCR total timeout after {i} pages")
            break

        page = doc[i]
        # Render at 300 DPI
        mat = fitz.Matrix(300 / 72, 300 / 72)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")

        try:
            result = subprocess.run(
                ["tesseract", "stdin", "stdout", "-l", tess_lang, "--psm", "1"],
                input=img_bytes,
                capture_output=True,
                timeout=OCR_TIMEOUT_PER_PAGE,
            )
            text = result.stdout.decode("utf-8", errors="replace").strip()
            if text:
                pages.append(text)
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.debug(f"OCR page {i} failed: {e}")
            if isinstance(e, FileNotFoundError):
                # Tesseract not installed — abort all OCR
                doc.close()
                return ""

    doc.close()
    return "\n\n".join(pages)


def extract_text(pdf_bytes: bytes, lang_hint: str = "de", use_ocr: bool = True) -> str:
    """Run 3-stage extraction pipeline. Early exit when text >= MIN_EXTRACTED_LENGTH."""
    # Stage 1: fitz
    try:
        text = extract_with_fitz(pdf_bytes)
        if len(text.strip()) >= MIN_EXTRACTED_LENGTH:
            return text.strip()
    except Exception as e:
        logger.debug(f"fitz extraction failed: {e}")

    # Stage 2: pdfplumber
    try:
        text = extract_with_pdfplumber(pdf_bytes)
        if len(text.strip()) >= MIN_EXTRACTED_LENGTH:
            return text.strip()
    except Exception as e:
        logger.debug(f"pdfplumber extraction failed: {e}")

    # Stage 3: Tesseract OCR (optional)
    if use_ocr:
        try:
            text = extract_with_ocr(pdf_bytes, lang_hint=lang_hint)
            if len(text.strip()) >= MIN_EXTRACTED_LENGTH:
                return text.strip()
        except Exception as e:
            logger.debug(f"OCR extraction failed: {e}")

    return ""


# ---------------------------------------------------------------------------
# Progress / resume
# ---------------------------------------------------------------------------
def load_progress() -> set[str]:
    """Load set of already-processed decision_ids."""
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text())
            return set(data.get("processed", []))
        except (json.JSONDecodeError, KeyError):
            pass
    return set()


def save_progress(processed: set[str], stats: dict) -> None:
    """Save progress atomically."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    payload = {
        "processed": sorted(processed),
        "stats": stats,
    }
    tmp.write_text(json.dumps(payload, indent=2))
    os.replace(tmp, PROGRESS_FILE)


# ---------------------------------------------------------------------------
# JSONL streaming repair
# ---------------------------------------------------------------------------
def repair_file(
    jsonl_path: Path,
    session: requests.Session,
    rate_limiter: DomainRateLimiter,
    processed: set[str],
    covered_keys: set[str],
    attempted_keys: set[str],
    threshold: int,
    use_ocr: bool,
    dry_run: bool,
    max_count: int | None,
) -> tuple[int, int, int, int, int]:
    """
    Repair a single JSONL file by streaming.
    Returns (total_lines, candidates, fixed, skipped_already_done, skipped_dedup).
    """
    total = 0
    candidates = 0
    fixed = 0
    skipped = 0
    skipped_dedup = 0
    file_processed_ids: list[str] = []

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

                try:
                    obj = json.loads(stripped)
                except json.JSONDecodeError as e:
                    log.warning("Skipping malformed JSON line in %s: %s", jsonl_path.name, e)
                    fout.write(line)
                    continue
                full_text = obj.get("full_text", "")
                pdf_url = obj.get("pdf_url", "")
                decision_id = obj.get("decision_id", "")

                # Check if this is a candidate
                if len(full_text) >= threshold or not pdf_url:
                    fout.write(line)
                    continue

                candidates += 1

                # Skip if another entry already has good text for this decision
                court = obj.get("court", "")
                docket = obj.get("docket_number", "")
                date_str = str(obj.get("decision_date", "") or "")
                dedup_key = _make_dedup_key(court, docket, date_str)

                if dedup_key in covered_keys:
                    skipped_dedup += 1
                    fout.write(line)
                    continue

                # Skip if we already attempted this dedup key in this run
                # (avoids downloading same PDF for duplicate entries)
                if dedup_key in attempted_keys:
                    skipped_dedup += 1
                    fout.write(line)
                    continue

                # Already processed in a previous run?
                if decision_id in processed:
                    skipped += 1
                    fout.write(line)
                    continue

                # Respect --max across the whole run
                if max_count is not None and fixed >= max_count:
                    fout.write(line)
                    continue

                if dry_run:
                    fout.write(line)
                    continue

                # Mark this dedup key as attempted
                attempted_keys.add(dedup_key)

                # Download PDF
                pdf_bytes = download_pdf(session, rate_limiter, pdf_url, decision_id)
                if pdf_bytes is None:
                    file_processed_ids.append(decision_id)
                    fout.write(line)
                    continue

                # Extract text
                lang_hint = obj.get("language", "de")
                new_text = extract_text(pdf_bytes, lang_hint=lang_hint, use_ocr=use_ocr)

                # Only replace if strictly longer
                if len(new_text) > len(full_text):
                    obj["full_text"] = new_text
                    obj["text_length"] = len(new_text)
                    obj["has_full_text"] = True
                    if len(new_text) > 100:
                        obj["language"] = detect_language(new_text)
                    fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    fixed += 1
                    file_processed_ids.append(decision_id)

                    if fixed % 50 == 0:
                        logger.info(
                            f"  [{jsonl_path.name}] Fixed {fixed}/{candidates} "
                            f"candidates so far..."
                        )
                else:
                    file_processed_ids.append(decision_id)
                    fout.write(line)

        # Replace original with repaired file
        if not dry_run and fixed > 0:
            os.replace(tmp_path, str(jsonl_path))
            logger.info(f"  Replaced {jsonl_path.name} ({fixed} entries repaired)")
        else:
            os.unlink(tmp_path)

    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    # Update processed set
    processed.update(file_processed_ids)

    return total, candidates, fixed, skipped, skipped_dedup


# ---------------------------------------------------------------------------
# Scanning for candidates
# ---------------------------------------------------------------------------
def scan_candidates(
    court_filter: str | None,
    threshold: int,
    covered_keys: set[str],
) -> list[tuple[Path, int]]:
    """Scan JSONL files and return list of (path, candidate_count).

    Excludes entries whose dedup key is already covered (another entry
    already has good text for the same decision).
    """
    target_files = []
    seen_keys: set[str] = set()  # track within-scan to count unique only
    total_raw = 0
    total_dedup_skipped = 0

    for jsonl_path in sorted(JSONL_DIR.glob("*.jsonl")):
        # Filter by court if specified
        if court_filter:
            court_name = jsonl_path.stem
            if court_name != court_filter:
                continue

        count = 0
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                full_text = obj.get("full_text", "")
                pdf_url = obj.get("pdf_url", "")
                if len(full_text) < threshold and pdf_url:
                    total_raw += 1
                    court = obj.get("court", "")
                    docket = obj.get("docket_number", "")
                    date_str = str(obj.get("decision_date", "") or "")
                    key = _make_dedup_key(court, docket, date_str)
                    if key in covered_keys:
                        total_dedup_skipped += 1
                        continue
                    if key in seen_keys:
                        total_dedup_skipped += 1
                        continue
                    seen_keys.add(key)
                    count += 1

        if count > 0:
            target_files.append((jsonl_path, count))
            logger.info(f"  {jsonl_path.name}: {count} unique candidates")

    if total_dedup_skipped > 0:
        logger.info(
            f"  (Skipped {total_dedup_skipped} duplicates of {total_raw} "
            f"raw candidates)"
        )

    return target_files


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-extract text from PDFs for short-text decisions"
    )
    parser.add_argument(
        "--court", type=str, default=None,
        help="Process only this court (e.g., gr_gerichte)",
    )
    parser.add_argument(
        "--threshold", type=int, default=SHORT_TEXT_THRESHOLD,
        help=f"Short text threshold in chars (default: {SHORT_TEXT_THRESHOLD})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count candidates, no downloads",
    )
    parser.add_argument(
        "--max", type=int, default=None,
        help="Process at most N entries (for testing)",
    )
    parser.add_argument(
        "--no-ocr", action="store_true",
        help="Skip Tesseract OCR stage",
    )
    parser.add_argument(
        "--clean-cache", action="store_true",
        help="Delete PDF cache after run",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Debug logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    for noisy in ("pdfminer", "pdfplumber", "urllib3", "chardet", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    if not JSONL_DIR.exists():
        logger.error(f"JSONL directory not found: {JSONL_DIR}")
        sys.exit(1)

    # Build dedup index to skip entries that are duplicates of good-text entries
    covered_keys = build_dedup_index(args.threshold)

    # Scan for candidates (dedup-aware)
    logger.info(f"Scanning for decisions with < {args.threshold} chars of text...")
    target_files = scan_candidates(args.court, args.threshold, covered_keys)

    total_candidates = sum(c for _, c in target_files)
    logger.info(
        f"Found {total_candidates} unique candidates across {len(target_files)} files"
    )

    if not target_files:
        logger.info("No candidates found.")
        return

    if args.dry_run:
        logger.info("Dry run — exiting.")
        return

    # Load resume progress
    processed = load_progress()
    if processed:
        logger.info(f"Resuming: {len(processed)} decisions already processed")

    session = build_session()
    rate_limiter = DomainRateLimiter(REQUEST_DELAY)
    use_ocr = not args.no_ocr
    attempted_keys: set[str] = set()  # dedup keys attempted this run

    if not use_ocr:
        logger.info("OCR disabled (--no-ocr)")

    # Process each file
    grand_total = 0
    grand_candidates = 0
    grand_fixed = 0
    grand_skipped = 0
    grand_skipped_dedup = 0

    for jsonl_path, candidate_count in target_files:
        logger.info(
            f"\nProcessing {jsonl_path.name} ({candidate_count} unique candidates)..."
        )

        total, candidates, fixed, skipped, skipped_dedup = repair_file(
            jsonl_path=jsonl_path,
            session=session,
            rate_limiter=rate_limiter,
            processed=processed,
            covered_keys=covered_keys,
            attempted_keys=attempted_keys,
            threshold=args.threshold,
            use_ocr=use_ocr,
            dry_run=False,
            max_count=args.max,
        )

        grand_total += total
        grand_candidates += candidates
        grand_fixed += fixed
        grand_skipped += skipped
        grand_skipped_dedup += skipped_dedup

        logger.info(
            f"  {jsonl_path.name}: {total} total, {candidates} candidates, "
            f"{fixed} fixed, {skipped} resumed, {skipped_dedup} dedup-skipped"
        )

        # Save progress after each file
        save_progress(processed, {
            "total_lines": grand_total,
            "total_candidates": grand_candidates,
            "total_fixed": grand_fixed,
            "total_skipped": grand_skipped,
            "total_skipped_dedup": grand_skipped_dedup,
        })

    # Summary
    logger.info("\n=== Summary ===")
    logger.info(f"  Files processed: {len(target_files)}")
    logger.info(f"  Total lines scanned: {grand_total}")
    logger.info(f"  Candidates found: {grand_candidates}")
    logger.info(f"  Successfully fixed: {grand_fixed}")
    logger.info(f"  Skipped (already done): {grand_skipped}")
    logger.info(f"  Skipped (dedup): {grand_skipped_dedup}")
    remaining = grand_candidates - grand_fixed - grand_skipped - grand_skipped_dedup
    logger.info(f"  Remaining unfixed: {remaining}")
    if grand_candidates > 0:
        pct = grand_fixed / grand_candidates * 100
        logger.info(f"  Fix rate: {pct:.1f}%")

    # Clean cache if requested
    if args.clean_cache and PDF_CACHE_DIR.exists():
        import shutil
        shutil.rmtree(PDF_CACHE_DIR)
        logger.info("PDF cache cleaned.")


if __name__ == "__main__":
    main()
