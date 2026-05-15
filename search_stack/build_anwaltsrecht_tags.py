#!/usr/bin/env python3
"""
Build Anwaltsrecht tags SQLite database from SAV BGFA and Bund PDFs.

Downloads BGFA article PDFs and Bundesrechtsprechung period PDFs from the SAV
website, extracts BGer docket numbers and BGE references, resolves them against
the FTS5 decisions DB, and writes a tags SQLite DB.

Output: output/anwaltsrecht_tags.db

Usage:
    python -m search_stack.build_anwaltsrecht_tags
    python -m search_stack.build_anwaltsrecht_tags --fts5-db output/decisions.db --output output/anwaltsrecht_tags.db
"""

import argparse
import io
import logging
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_anwaltsrecht_tags")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BGFA_ARTICLES = [2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 25, 27, 28, 29, 34, 36]

BUND_PERIODS = [
    "2001",
    "2003-2004",
    "2005-2006",
    "2007-2008",
    "2009-2010",
    "2012-2013",
    "2014-2015",
    "2016-2018",
    "2019-2021",
    "2022-2023",
    "2024-2025",
]

# Period → explicit URL override. Used when the period's PDF lives at
# a URL the `_BUND_BASE` template can't reconstruct (post-2026 CMS
# migration on the SAV-FSA side). Periods absent from this map fall
# back to the template. Audited 2026-05-15 against
# https://www.sav-fsa.ch/bund-weiteres-anwaltsrecht.
_BUND_URL_OVERRIDES = {
    "2016-2018": (
        "https://www.sav-fsa.ch/documents/672183/2096091/"
        "Rechtsprechung_Bund_und_weiteres_Anwaltsrecht_2016%20-%202018.pdf"
        "/8808f703-11e9-9805-d4d8-104296a5365f?t=1675690468317"
    ),
    "2019-2021": (
        "https://www.sav-fsa.ch/documents/672183/2096091/"
        "Rechtsprechung_Bund_und_weiteres_Anwaltsrecht_2019%20-%202021.pdf"
        "/9cc210cc-44de-9456-c379-ecd2054c0091?t=1675689763760"
    ),
    "2022-2023": (
        "https://www.sav-fsa.ch/documents/672183/2096091/"
        "Rechtsprechung_Bund_und_weiteres_Anwaltsrecht_2022_2023.pdf"
        "/d8647e1f-50bc-f8d5-a5f7-d9546c9b69d6?t=1720690966618"
    ),
    "2024-2025": (
        "https://www.sav-fsa.ch/documents/672183/2096091/"
        "Rechtsprechung_Bund_und_weiteres_Anwaltsrecht_2024_2025%20%281%29.pdf"
        "/2416f15b-5105-c3d8-4199-77808a7bae51?t=1765808820729"
    ),
}

# Base URLs for SAV PDFs
# BGFA article PDFs — two variants: plain and with " (1)" suffix for updated versions
_BGFA_BASE = "https://www.sav-fsa.ch/documents/672183/2059208/Art{n}.pdf"
_BGFA_BASE_ALT = "https://www.sav-fsa.ch/documents/672183/2059208/Art{n}%20(1).pdf"

# Bundesrechtsprechung period PDFs.
#
# SAV-FSA migrated their CMS in 2026 — periods up to 2014-2015 still
# resolve via the simple `_BUND_BASE.format(period=...)` template, but
# 2016 onwards now require URL-encoded separators (" - " → "%20-%20" or
# "_" instead of "-") and a content-addressed UUID + timestamp suffix
# that we can't reconstruct from period alone. The override map below
# pins the current SAV-FSA CMS URLs for the affected periods. Source:
# https://www.sav-fsa.ch/bund-weiteres-anwaltsrecht (audited 2026-05-15).
# Re-audit when a new period gets added or the index page restructures.
_BUND_BASE = (
    "https://www.sav-fsa.ch/documents/672183/2096091/"
    "Rechtsprechung_Bund_und_weiteres_Anwaltsrecht_{period}.pdf"
)

# ---------------------------------------------------------------------------
# Regex patterns (module-level constants, exported for tests)
# ---------------------------------------------------------------------------

# BGer docket numbers: e.g. 2C_345/2023, 2P.100/2005, 5A_123/2019
# Pattern: digit + uppercase letter(s) + underscore or dot + digits/year
DOCKET_PATTERNS = [
    re.compile(r"\b\d[A-Z][_\.]\d+/\d{4}\b"),
    re.compile(r"\b\d[A-Z]{2}[_\.]\d+/\d{4}\b"),
]

# BGE / ATF / DTF references: e.g. BGE 130 II 270, ATF 140 II 102
BGE_PATTERN = re.compile(
    r"\b(?:BGE|ATF|DTF)\s+\d{2,3}\s+[A-Z]+\s+\d+"
)

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _extract_bgfa_article(filename: str) -> Optional[str]:
    """Extract BGFA article label from filename like 'Art12.pdf' → 'Art. 12'."""
    m = re.search(r"[Aa]rt(\d+)", filename)
    if m:
        return f"Art. {m.group(1)}"
    return None


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber with fitz fallback."""
    text = ""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            parts = []
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    parts.append(page_text)
            text = "\n".join(parts)
        if text.strip():
            return text
    except Exception as e:
        log.debug("pdfplumber failed: %s", e)

    # Fallback to fitz (PyMuPDF)
    try:
        import fitz
        doc = fitz.open(stream=content, filetype="pdf")
        parts = []
        for page in doc:
            parts.append(page.get_text())
        doc.close()
        text = "\n".join(parts)
    except Exception as e:
        log.debug("fitz fallback failed: %s", e)

    return text


def _extract_dockets(text: str) -> set:
    """Extract all BGer docket numbers and BGE references from text."""
    dockets = set()

    # BGer docket numbers
    for pattern in DOCKET_PATTERNS:
        for m in pattern.finditer(text):
            dockets.add(m.group(0))

    # BGE/ATF/DTF references
    for m in BGE_PATTERN.finditer(text):
        dockets.add(m.group(0))

    return dockets


def _download_pdf(url: str, session) -> Optional[bytes]:
    """Download a PDF with up to 3 retries. Returns bytes or None on failure."""
    for attempt in range(1, 4):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                ct = resp.headers.get("Content-Type", "")
                if "pdf" in ct.lower() or resp.content[:4] == b"%PDF":
                    return resp.content
                log.debug("Non-PDF response for %s (Content-Type: %s)", url, ct)
                return None
            elif resp.status_code == 404:
                return None
            else:
                log.warning("HTTP %d for %s (attempt %d/3)", resp.status_code, url, attempt)
        except Exception as e:
            log.warning("Download error for %s (attempt %d/3): %s", url, attempt, e)
        if attempt < 3:
            time.sleep(2 ** attempt)
    return None


def _resolve_dockets(dockets: set, fts5_db: str) -> dict:
    """
    Resolve docket strings → decision_id using the FTS5 DB.

    For BGer docket numbers: exact match on docket_number column.
    For BGE refs (e.g. "BGE 130 II 270"): normalize and match via LIKE.

    Returns dict mapping docket_str → decision_id.
    """
    resolved = {}
    if not dockets:
        return resolved

    try:
        conn = sqlite3.connect(f"file:{fts5_db}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        log.error("Cannot open FTS5 DB %s: %s", fts5_db, e)
        return resolved

    try:
        for docket in dockets:
            # BGE/ATF/DTF reference
            if re.match(r"^(?:BGE|ATF|DTF)\s+", docket):
                # Normalize: "BGE 130 II 270" → "bge_130_II_270"
                normed = re.sub(r"^(?:BGE|ATF|DTF)\s+", "", docket)
                normed = re.sub(r"\s+", "_", normed.strip())
                like_pat = f"bge_{normed}%"
                try:
                    row = conn.execute(
                        "SELECT decision_id FROM decisions WHERE decision_id LIKE ? LIMIT 1",
                        (like_pat,),
                    ).fetchone()
                    if row:
                        resolved[docket] = row["decision_id"]
                except Exception as e:
                    log.debug("BGE resolve error for %s: %s", docket, e)
            else:
                # BGer docket number — exact match
                try:
                    row = conn.execute(
                        "SELECT decision_id FROM decisions WHERE docket_number = ? LIMIT 1",
                        (docket,),
                    ).fetchone()
                    if row:
                        resolved[docket] = row["decision_id"]
                except Exception as e:
                    log.debug("Docket resolve error for %s: %s", docket, e)
    finally:
        conn.close()

    return resolved


def _create_schema(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS anwaltsrecht_tags (
            decision_id TEXT NOT NULL,
            bgfa_article TEXT,
            source TEXT NOT NULL,
            docket_number TEXT,
            PRIMARY KEY (decision_id, bgfa_article, source)
        );
        CREATE INDEX IF NOT EXISTS idx_article ON anwaltsrecht_tags(bgfa_article);
        CREATE INDEX IF NOT EXISTS idx_decision ON anwaltsrecht_tags(decision_id);
    """)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def build_tags_db(fts5_db: str, output_db: str):
    """Main pipeline: download PDFs, extract dockets, resolve, write DB."""
    import requests

    session = requests.Session()
    session.headers.update({
        "User-Agent": "SwissCaselawBot/1.0 (https://opencaselaw.ch; research)",
    })

    # Accumulate (docket_str, bgfa_article_or_None, source) tuples
    raw_tags: list[tuple] = []

    # --- BGFA article PDFs ---
    log.info("Downloading BGFA article PDFs (%d articles)...", len(BGFA_ARTICLES))
    for n in BGFA_ARTICLES:
        article_label = f"Art. {n}"
        filename = f"Art{n}.pdf"
        url_primary = _BGFA_BASE.format(n=n)
        url_alt = _BGFA_BASE_ALT.format(n=n)

        content = _download_pdf(url_primary, session)
        if content is None:
            log.debug("Primary URL failed for %s, trying alt...", filename)
            content = _download_pdf(url_alt, session)

        if content is None:
            log.warning("Could not download %s (tried both URLs)", filename)
            time.sleep(1)
            continue

        text = _extract_pdf_text(content)
        dockets = _extract_dockets(text)
        log.info("Art. %d: %d dockets extracted from %d chars", n, len(dockets), len(text))

        for d in dockets:
            raw_tags.append((d, article_label, "bgfa"))

        time.sleep(1)

    # --- Bund period PDFs ---
    log.info("Downloading Bund period PDFs (%d periods)...", len(BUND_PERIODS))
    for period in BUND_PERIODS:
        url = _BUND_URL_OVERRIDES.get(period) or _BUND_BASE.format(period=period)
        content = _download_pdf(url, session)

        if content is None:
            log.warning("Could not download Bund PDF for period %s", period)
            time.sleep(1)
            continue

        text = _extract_pdf_text(content)
        dockets = _extract_dockets(text)
        log.info("Bund %s: %d dockets extracted from %d chars", period, len(dockets), len(text))

        for d in dockets:
            raw_tags.append((d, None, "bund"))

        time.sleep(1)

    # --- Resolve all unique dockets ---
    all_dockets = {d for d, _, _ in raw_tags}
    log.info("Resolving %d unique dockets against FTS5 DB...", len(all_dockets))
    resolved = _resolve_dockets(all_dockets, fts5_db)
    log.info("Resolved %d / %d dockets", len(resolved), len(all_dockets))

    # --- Write to SQLite ---
    output_path = Path(output_db)
    tmp_path = output_path.with_suffix(".db.tmp")

    conn = sqlite3.connect(str(tmp_path))
    _create_schema(conn)

    inserted = 0
    skipped = 0
    for docket_str, bgfa_article, source in raw_tags:
        decision_id = resolved.get(docket_str)
        if decision_id is None:
            skipped += 1
            continue
        try:
            conn.execute(
                "INSERT OR IGNORE INTO anwaltsrecht_tags (decision_id, bgfa_article, source, docket_number) VALUES (?, ?, ?, ?)",
                (decision_id, bgfa_article, source, docket_str),
            )
            inserted += 1
        except Exception as e:
            log.debug("Insert error: %s", e)

    conn.commit()
    conn.close()

    # Atomic replace
    import os
    os.replace(str(tmp_path), str(output_path))
    log.info(
        "Done. %d tags inserted, %d unresolved dockets skipped → %s",
        inserted,
        skipped,
        output_path,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Build Anwaltsrecht tags DB from SAV BGFA + Bund PDFs"
    )
    parser.add_argument(
        "--fts5-db",
        default=os.environ.get("SWISS_CASELAW_FTS5_DB", "output/decisions.db"),
        help="Path to FTS5 decisions DB (default: output/decisions.db)",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("ANWALTSRECHT_TAGS_DB", "output/anwaltsrecht_tags.db"),
        help="Path to output SQLite DB (default: output/anwaltsrecht_tags.db)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("FTS5 DB: %s", args.fts5_db)
    log.info("Output DB: %s", args.output)

    build_tags_db(fts5_db=args.fts5_db, output_db=args.output)


if __name__ == "__main__":
    main()
