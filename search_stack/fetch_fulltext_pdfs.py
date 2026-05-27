#!/usr/bin/env python3
"""Option B: bulk-download + extract PDFs for permissively-licensed records.

Walks legal_scholarship.db looking for publications that:
  - Have no full_text stored yet
  - Carry a permissive license (CC-BY*, CC-BY-SA*, CC-BY-NC*, CC-BY-NC-SA*,
    OA-Swiss-federal, OA-author-permitted-reuse)
  - Have a resolvable PDF URL (per-source heuristic in
    search_stack/fulltext_extractor.py)

For each, fetches the PDF, extracts text via pymupdf, stores back into
publications.full_text + has_full_text=1, optionally caches the PDF on
disk for re-extraction.

Designed to run safely against the live MCP-served DB:
  - SQLite write happens through a SHORT transaction per record
  - WAL mode kept (default for live writes)
  - Each FTS5 row updated via DELETE+reinsert (no AFTER UPDATE trigger
    exists on publications)

Usage:
    python -m search_stack.fetch_fulltext_pdfs
    python -m search_stack.fetch_fulltext_pdfs --max-records 100
    python -m search_stack.fetch_fulltext_pdfs --source e_periodica_law
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

from search_stack.fulltext_extractor import (
    fetch_and_extract,
    is_permissive_license,
    resolve_pdf_url,
)

log = logging.getLogger("fetch_fulltext_pdfs")

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "output" / "legal_scholarship.db"
DEFAULT_PDF_CACHE = REPO_ROOT / "output" / "scholarship_pdfs"


def fetch_pending(
    db_path: Path = DEFAULT_DB,
    *,
    max_records: int | None = None,
    source_filter: str | None = None,
    rate_limit: float = 1.0,
    pdf_cache_dir: Path | None = DEFAULT_PDF_CACHE,
) -> dict:
    if not db_path.exists():
        log.error("DB not found: %s", db_path)
        return {"error": "db_missing"}

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    where = (
        "(full_text IS NULL OR LENGTH(full_text) < 200) "
        "AND ("
        "     license LIKE 'CC-BY%' "
        "  OR license = 'OA-Swiss-federal' "
        "  OR license = 'OA-author-permitted-reuse' "
        "  OR license = 'info:eu-repo/semantics/openAccess' "
        "  OR LOWER(license) IN ('openaccess','open access','open_access','free access') "
        "  OR (license IS NULL AND source IN ('edoc_unibas_law','libra_unine'))"
        ")"
    )
    params: list = []
    if source_filter:
        where += " AND source = ?"
        params.append(source_filter)
    sql = (
        f"SELECT id, pub_id, source, url, pdf_url, license, raw_metadata "
        f"FROM publications WHERE {where} ORDER BY id"
    )
    if max_records:
        sql += f" LIMIT {int(max_records)}"
    rows = [dict(r) for r in cur.execute(sql, params).fetchall()]
    log.info("permissive-license candidates lacking full_text: %d", len(rows))

    if not rows:
        return {"candidates": 0, "extracted": 0}

    # Pre-filter: those for which a PDF URL can actually be resolved
    resolvable = []
    for r in rows:
        if resolve_pdf_url(r):
            resolvable.append(r)
    log.info("of those, resolvable PDF URLs: %d", len(resolvable))

    n_ok = 0
    n_nopdf = 0
    n_nopermit = 0
    n_fetchfail = 0
    n_extractfail = 0
    total_bytes = 0
    started = time.time()

    for i, r in enumerate(resolvable):
        result = fetch_and_extract(
            r,
            rate_limit_secs=rate_limit,
            require_permissive=True,
            pdf_cache_dir=pdf_cache_dir,
        )
        if result.get("ok"):
            try:
                cur.execute(
                    "UPDATE publications SET full_text=?, has_full_text=1 "
                    "WHERE id=?",
                    (result["text"], r["id"]),
                )
                cur.execute(
                    "DELETE FROM publications_fts WHERE rowid=?",
                    (r["id"],),
                )
                cur.execute(
                    "INSERT INTO publications_fts(rowid, title, authors, "
                    "abstract, full_text, journal, keywords, subjects) "
                    "SELECT id, title, authors, abstract, full_text, "
                    "journal, keywords, subjects FROM publications "
                    "WHERE id=?",
                    (r["id"],),
                )
                conn.commit()
                n_ok += 1
                total_bytes += result.get("bytes") or 0
            except sqlite3.Error as e:
                log.warning("db write failed for %s: %s", r["pub_id"], e)
                n_extractfail += 1
        else:
            reason = result.get("reason", "?")
            if reason == "no_resolvable_pdf_url":
                n_nopdf += 1
            elif reason == "non_permissive_license":
                n_nopermit += 1
            elif reason in ("fetch_failed", "not_a_pdf"):
                n_fetchfail += 1
            else:
                n_extractfail += 1
        if (i + 1) % 50 == 0:
            elapsed = time.time() - started
            rate = (i + 1) / max(elapsed, 0.1)
            log.info(
                "progress: %d/%d processed (ok=%d, rate %.1f/s)",
                i + 1, len(resolvable), n_ok, rate,
            )

    conn.execute(
        "INSERT INTO publications_fts(publications_fts) VALUES ('optimize')"
    )
    conn.commit()
    conn.close()

    summary = {
        "candidates": len(rows),
        "resolvable_pdf_urls": len(resolvable),
        "extracted_ok": n_ok,
        "no_pdf_resolved": n_nopdf,
        "fetch_failed": n_fetchfail,
        "extract_failed": n_extractfail,
        "non_permissive": n_nopermit,
        "total_bytes_downloaded": total_bytes,
        "elapsed_seconds": round(time.time() - started, 1),
    }
    log.info("Done: %s", summary)
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--max-records", type=int, default=None)
    p.add_argument("--source", default=None)
    p.add_argument("--rate-limit", type=float, default=1.0,
                   help="Sleep between PDF fetches (sec)")
    p.add_argument("--pdf-cache-dir", type=Path, default=DEFAULT_PDF_CACHE)
    p.add_argument("--no-cache", action="store_true",
                   help="Don't store PDFs on disk")
    args = p.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    summary = fetch_pending(
        args.db,
        max_records=args.max_records,
        source_filter=args.source,
        rate_limit=args.rate_limit,
        pdf_cache_dir=None if args.no_cache else args.pdf_cache_dir,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
