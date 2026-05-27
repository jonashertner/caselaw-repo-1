#!/usr/bin/env python3
"""Build the Fedlex first-pages index used by /api/amendment-ref to
resolve inner-page citations to their containing document.

Background. Swiss legal footnote citations like "BBl 2019 6697" refer
to a specific page INSIDE a multi-page publication. Fedlex's ELI URIs
only address first-pages (`/eli/fga/{year}/{first_page}`); the SPA
serves a page-not-found for inner-page paths.

This script enumerates every first-page work in Fedlex's RDF graph
per (family, year) and stores a sorted list in a small SQLite DB.
The /amendment-ref endpoint then does a `max(first_page <= cited_page)`
lookup to find the containing document.

Coverage:
  - family 'fga' = Federal Gazette (Bundesblatt / BBl / FF)
  - family 'oc'  = Official Compilation (Amtliche Sammlung / AS / RO / RU)
  - years 1940 → current year (older years return 0 hits from SPARQL)

Storage: ~150 KB SQLite, ~100k-150k rows total. Negligible.

Idempotent: re-running rebuilds the index from scratch (atomic swap).
Run weekly or on demand.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
import json as _json
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("build_fedlex_first_pages_index")

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "output" / "fedlex_first_pages.db"
SPARQL_ENDPOINT = "https://fedlex.data.admin.ch/sparqlendpoint"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fedlex_first_pages (
    -- ref_type as it appears in the Fedlex historicalId predicate
    -- (BBl, FF, AS, RO, RU). NOT the URI family code.
    ref_type TEXT NOT NULL,
    year INTEGER NOT NULL,
    -- The actual BBl/AS/etc. page number where this publication
    -- starts, as recorded in historicalId. Multiple Fedlex works
    -- (one per language) may share the same page; the URI is the
    -- abstract Work, which serves as base for the language-specific
    -- expressions Fedlex's web UI navigates to.
    page INTEGER NOT NULL,
    uri TEXT NOT NULL,
    PRIMARY KEY (ref_type, year, page)
);
CREATE INDEX IF NOT EXISTS idx_ffp_lookup ON fedlex_first_pages(ref_type, year, page);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

# Parse Fedlex historicalId values like "BBl 2019 6645", "FF 2014 3988",
# "AS 2009 3067". The space-separated 3-part form is universal.
_HISTORICAL_ID_RE = re.compile(
    r"^\s*(BBl|FF|AS|RO|RU)\s+(\d{4})\s+(\d+)\s*$"
)


def fetch_first_pages(
    family: str, year: int, timeout: int = 30,
) -> list[tuple[str, int, int, str]]:
    """SPARQL-enumerate every Fedlex work in (family, year) AND parse its
    historicalId to recover the actual BBl/AS page number.

    Returns list of (ref_type, year, page, uri) tuples. Multiple works
    may share the same page across languages — we keep just one URI
    per (ref_type, year, page) at the SQL INSERT OR IGNORE level.
    """
    # NOTE: historicalId is in the Cogni-internal namespace, NOT jolux.
    # Discovered 2026-05-27 via predicate dump on eli/fga/2014/1001:
    #   http://cogni.internal.system/model#historicalId -> "FF 2014 3988"
    # This is the only way to map a Fedlex Work URI to its actual
    # BBl/FF/AS page reference; the jolux:sequenceInTheYearOfPublication
    # is a doc-index that does NOT match the BBl page.
    q = f"""
    PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>
    PREFIX cogni: <http://cogni.internal.system/model#>
    SELECT DISTINCT ?work ?historicalId WHERE {{
      ?work jolux:legalResourceFamilyType
            <https://fedlex.data.admin.ch/vocabulary/resource-family/{family}> .
      ?work cogni:historicalId ?historicalId .
      FILTER(STRSTARTS(STR(?work),
             "https://fedlex.data.admin.ch/eli/{family}/{year}/"))
    }}
    """
    req = urllib.request.Request(
        SPARQL_ENDPOINT,
        data=urllib.parse.urlencode({"query": q}).encode(),
        headers={"Accept": "application/sparql-results+json"},
    )
    out: list[tuple[str, int, int, str]] = []
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = _json.loads(r.read())
        for b in data.get("results", {}).get("bindings", []):
            uri = b.get("work", {}).get("value", "")
            hist = b.get("historicalId", {}).get("value", "")
            m = _HISTORICAL_ID_RE.match(hist)
            if not m:
                continue
            rt, yr_str, pg_str = m.groups()
            try:
                out.append((rt, int(yr_str), int(pg_str), uri))
            except ValueError:
                continue
    except Exception as e:
        log.warning("SPARQL failed for %s/%d: %s", family, year, e)
    return out


def build_index(
    db_path: Path,
    *,
    year_min: int = 1940,
    year_max: int | None = None,
    rate_limit: float = 0.5,
) -> dict:
    """Build the fedlex_first_pages index, atomic-swap on completion."""
    year_max = year_max or datetime.now(timezone.utc).year
    tmp_path = db_path.with_suffix(".db.tmp")
    # Also remove leftover WAL/SHM auxiliary files from a prior failed run.
    for aux in (
        tmp_path,
        tmp_path.with_name(tmp_path.name + "-wal"),
        tmp_path.with_name(tmp_path.name + "-shm"),
        tmp_path.with_name(tmp_path.name + "-journal"),
    ):
        if aux.exists():
            aux.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # NOTE: keep DELETE journal mode (default) — the index is small
    # and the build is single-writer one-shot. Using WAL here previously
    # left the main .db.tmp file in an undefined state after the
    # WAL→DELETE switch, causing os.replace to fail with FileNotFound.
    conn = sqlite3.connect(str(tmp_path))
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA_SQL)

    summary = {"fga_total": 0, "oc_total": 0, "years_with_data": 0}
    for family in ("fga", "oc"):
        for year in range(year_min, year_max + 1):
            rows = fetch_first_pages(family, year)
            if not rows:
                continue
            summary["years_with_data"] += 1
            with conn:
                conn.executemany(
                    "INSERT OR IGNORE INTO fedlex_first_pages"
                    "(ref_type, year, page, uri) VALUES (?, ?, ?, ?)",
                    rows,
                )
            summary[f"{family}_total"] += len(rows)
            log.info("%s %d: %d historicalId rows", family, year, len(rows))
            time.sleep(rate_limit)

    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('built_at', ?)",
        (datetime.now(timezone.utc).isoformat(),),
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('year_min', ?)",
        (str(year_min),),
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('year_max', ?)",
        (str(year_max),),
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('summary', ?)",
        (_json.dumps(summary),),
    )
    conn.commit()
    conn.close()

    os.replace(str(tmp_path), str(db_path))
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db", type=Path, default=DEFAULT_DB,
                   help=f"Output DB path (default: {DEFAULT_DB})")
    p.add_argument("--year-min", type=int, default=1940)
    p.add_argument("--year-max", type=int, default=None)
    p.add_argument("--rate-limit", type=float, default=0.5,
                   help="Sleep between SPARQL queries (s)")
    args = p.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    log.info("Building Fedlex first-pages index at %s", args.db)
    summary = build_index(
        args.db,
        year_min=args.year_min,
        year_max=args.year_max,
        rate_limit=args.rate_limit,
    )
    log.info("Done: %s", summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
