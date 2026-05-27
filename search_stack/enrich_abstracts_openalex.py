#!/usr/bin/env python3
"""Option A: enrich missing abstracts from OpenAlex.

For every legal_scholarship.db publication that has a DOI but a NULL/short
abstract, query OpenAlex (free, no auth needed for polite pool) and store
the returned abstract back.

OpenAlex stores abstracts as an inverted index (`abstract_inverted_index`):
a dict mapping each word to the list of positions where it appears. We
reconstruct flat text by sorting word→positions into the right order.

Usage:
    python -m search_stack.enrich_abstracts_openalex
    python -m search_stack.enrich_abstracts_openalex --max-records 100
    python -m search_stack.enrich_abstracts_openalex --source unige_law
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("enrich_abstracts_openalex")

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "output" / "legal_scholarship.db"

OPENALEX = "https://api.openalex.org"
# Polite pool: include a contact email to be eligible for higher rate-limits.
# OpenAlex docs: https://docs.openalex.org/how-to-use-the-api/api-overview
# Best practice: ?mailto=you@example.com (~10 req/sec sustainable).
MAILTO = os.environ.get(
    "OPENALEX_MAILTO", "scholarship@opencaselaw.ch",
)
USER_AGENT = (
    f"OpenCaseLaw-scholarship/0.1 (mailto:{MAILTO}; +https://opencaselaw.ch)"
)


def _normalize_doi(doi: str | None) -> str | None:
    """Strip URL/prefix variants down to canonical '10.NNN/...' form."""
    if not doi:
        return None
    d = doi.strip()
    for prefix in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/",
                   "http://dx.doi.org/", "doi:", "info:doi/", "DOI:"):
        if d.startswith(prefix):
            d = d[len(prefix):]
            break
    if not d.startswith("10."):
        return None
    return d


def _reconstruct_abstract(inv_idx: dict | None) -> str | None:
    """Rebuild flat text from OpenAlex's abstract_inverted_index."""
    if not inv_idx or not isinstance(inv_idx, dict):
        return None
    # Each value is a list of positions where the word appears
    pos_to_word: list[tuple[int, str]] = []
    for word, positions in inv_idx.items():
        if isinstance(positions, list):
            for p in positions:
                if isinstance(p, int):
                    pos_to_word.append((p, word))
    pos_to_word.sort()
    return " ".join(w for _, w in pos_to_word) or None


def fetch_abstract(doi: str, timeout: int = 15) -> str | None:
    """Query OpenAlex for a DOI and return its abstract text or None."""
    url = f"{OPENALEX}/works/doi:{urllib.parse.quote(doi, safe='/.-')}"
    if MAILTO:
        url += f"?mailto={urllib.parse.quote(MAILTO)}"
    req = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        log.debug("OpenAlex %s: HTTP %d", doi, e.code)
        return None
    except Exception as e:
        log.debug("OpenAlex %s: %s", doi, e)
        return None
    return _reconstruct_abstract(data.get("abstract_inverted_index"))


def enrich(
    db_path: Path = DEFAULT_DB,
    *,
    max_records: int | None = None,
    source_filter: str | None = None,
    rate_limit: float = 0.1,
) -> dict:
    """Walk publications needing abstract enrichment; query OpenAlex; update."""
    if not db_path.exists():
        log.error("DB not found: %s", db_path)
        return {"error": "db_missing"}
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    # Candidates: have a DOI, abstract is missing/short
    where = (
        "doi IS NOT NULL AND TRIM(doi) != '' "
        "AND (abstract IS NULL OR LENGTH(abstract) < 50)"
    )
    params: list = []
    if source_filter:
        where += " AND source = ?"
        params.append(source_filter)
    sql = (
        f"SELECT id, pub_id, doi FROM publications "
        f"WHERE {where} ORDER BY id"
    )
    if max_records:
        sql += f" LIMIT {int(max_records)}"
    rows = cur.execute(sql, params).fetchall()
    log.info("candidates needing abstract enrichment: %d", len(rows))
    n_enriched = 0
    n_no_data = 0
    n_failed = 0
    started = time.time()
    for i, (rowid, pub_id, doi) in enumerate(rows):
        d = _normalize_doi(doi)
        if not d:
            n_failed += 1
            continue
        abstract = fetch_abstract(d)
        if abstract:
            try:
                # Need to use UPDATE through the FTS5-content backed insert;
                # easiest: direct UPDATE on publications + manual FTS5 refresh
                # (since we have CREATE TRIGGER AFTER INSERT but no AFTER
                # UPDATE). Workaround: delete from fts + reinsert.
                cur.execute(
                    "UPDATE publications SET abstract=? WHERE id=?",
                    (abstract, rowid),
                )
                cur.execute(
                    "DELETE FROM publications_fts WHERE rowid=?", (rowid,),
                )
                cur.execute(
                    "INSERT INTO publications_fts(rowid, title, authors, "
                    "abstract, full_text, journal, keywords, subjects) "
                    "SELECT id, title, authors, abstract, full_text, "
                    "journal, keywords, subjects FROM publications "
                    "WHERE id=?",
                    (rowid,),
                )
                n_enriched += 1
                if n_enriched % 200 == 0:
                    conn.commit()
                    elapsed = time.time() - started
                    rate = n_enriched / max(elapsed, 0.1)
                    log.info(
                        "progress: %d/%d enriched (rate %.1f/s, elapsed %.0fs)",
                        n_enriched, len(rows), rate, elapsed,
                    )
            except sqlite3.Error as e:
                log.warning("DB update failed for %s: %s", pub_id, e)
                n_failed += 1
        else:
            n_no_data += 1
        time.sleep(rate_limit)
    conn.commit()
    conn.execute(
        "INSERT INTO publications_fts(publications_fts) VALUES ('optimize')"
    )
    conn.commit()
    conn.close()
    summary = {
        "candidates": len(rows),
        "enriched": n_enriched,
        "no_openalex_match": n_no_data,
        "failed": n_failed,
        "elapsed_seconds": round(time.time() - started, 1),
    }
    log.info("Done: %s", summary)
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--max-records", type=int, default=None)
    p.add_argument("--source", default=None,
                   help="Only enrich records from this source")
    p.add_argument("--rate-limit", type=float, default=0.1,
                   help="Sleep between OpenAlex calls (sec). OpenAlex polite "
                        "pool sustains ~10/s.")
    args = p.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    summary = enrich(
        args.db,
        max_records=args.max_records,
        source_filter=args.source,
        rate_limit=args.rate_limit,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
