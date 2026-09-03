#!/usr/bin/env python3
"""
DEPRECATED (2026-09): build amendment reference table mapping AS/BBl page
numbers to ELI URIs.

Nothing reads the amendment_refs table in statutes.db any more: every
consumer resolves AS/BBl references through materialien.db, and
search_stack.build_statutes_db no longer creates the table. This script also
writes in place, in WAL mode, into the live statutes.db that MCP workers open
with immutable=1, which is exactly the write path CLAUDE.md invariant 1
forbids. It is kept only for forensic re-runs against a copy and refuses to
run without --force.

Original purpose:

Queries the Fedlex SPARQL endpoint to resolve page-based references
(e.g. "AS 2016 1249", "BBl 2012 4721") to their correct ELI URIs
(e.g. "eli/oc/2016/249", "eli/fga/2012/670").

The mapping is non-trivial because AS/BBl page numbers do NOT correspond
to the sequence numbers in ELI URIs. Fedlex stores this mapping in its
RDF graph via jolux:memorialPage on language-specific expressions.

Output: amendment_refs table added to output/statutes.db

Schema:
    amendment_refs (
        ref_type TEXT,      -- 'AS', 'RO', 'RU', 'BBl', 'FF'
        year INT,
        page_num INT,
        eli_uri TEXT,       -- e.g. 'eli/oc/2016/249'
        PRIMARY KEY(ref_type, year, page_num)
    )

Usage:
    python -m search_stack.build_amendment_refs
    python -m search_stack.build_amendment_refs --db output/statutes.db
"""

import argparse
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_amendment_refs")

SPARQL_ENDPOINT = "https://fedlex.data.admin.ch/sparqlendpoint"
OUTPUT_DB = Path(os.environ.get("STATUTES_DB", "output/statutes.db"))
REQUEST_DELAY = 0.3

# Memorial name variants per collection type
# AS (DE) / RO (FR) / RU (IT) → Official Collection (eli/oc/...)
# BBl (DE) / FF (FR+IT) → Federal Gazette (eli/fga/...)
MEMORIAL_NAMES = {
    "oc": ["AS", "RO", "RU"],
    "fga": ["BBl", "FF"],
}

session = requests.Session()
session.headers["User-Agent"] = "OpenCaseLaw/1.0 (amendment-refs; +https://opencaselaw.ch)"
session.headers["Accept"] = "application/sparql-results+json"


def sparql_query(query: str, timeout: int = 300) -> list[dict]:
    """Execute a SPARQL query and return results as list of dicts."""
    resp = session.get(
        SPARQL_ENDPOINT,
        params={"query": query, "format": "application/sparql-results+json"},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    results = []
    for binding in data["results"]["bindings"]:
        row = {}
        for key, val in binding.items():
            row[key] = val["value"]
        results.append(row)
    return results


def _sparql_with_retry(query: str, timeout: int = 120) -> list[dict]:
    """Execute SPARQL query with one retry on failure."""
    try:
        return sparql_query(query, timeout=timeout)
    except Exception as e:
        log.warning("SPARQL query failed: %s — retrying...", e)
        time.sleep(2)
        return sparql_query(query, timeout=timeout)


def _fetch_year_paginated(memorial_name: str, year: int, page_size: int = 10000) -> list[dict]:
    """Fetch all mappings for one memorial name + year, paginating if needed.

    The Fedlex SPARQL endpoint has a default result limit (~500-1000).
    We use OFFSET/LIMIT to page through all results.
    """
    results = []
    offset = 0

    while True:
        query = f"""
        PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>

        SELECT DISTINCT ?act ?page WHERE {{
          ?expr jolux:memorialName "{memorial_name}" .
          ?expr jolux:memorialYear {year} .
          ?expr jolux:memorialPage ?page .
          ?act jolux:isRealizedBy ?expr .
        }}
        ORDER BY ?page
        LIMIT {page_size}
        OFFSET {offset}
        """
        try:
            rows = _sparql_with_retry(query, timeout=120)
        except Exception as e:
            log.error("Failed to fetch %s %d offset %d: %s — skipping", memorial_name, year, offset, e)
            break

        results.extend(rows)
        time.sleep(REQUEST_DELAY)

        # If we got fewer than page_size results, we've reached the end
        if len(rows) < page_size:
            break
        offset += page_size

    return results


def fetch_memorial_mappings(memorial_name: str, year_start: int, year_end: int) -> list[dict]:
    """Fetch page->ELI mappings for a given memorial name, year by year.

    Returns list of dicts with keys: ref_type, year, page_num, eli_uri.

    Queries one year at a time with pagination to handle the SPARQL
    endpoint's result limit (~500 default). Most years have 200-3500
    entries per memorial name.
    """
    all_rows = []

    for year in range(year_start, year_end + 1):
        rows = _fetch_year_paginated(memorial_name, year)

        year_count = 0
        for row in rows:
            act_uri = row["act"]
            page = row["page"]

            # Extract relative ELI path from full URI
            # e.g. "https://fedlex.data.admin.ch/eli/oc/2016/249" -> "eli/oc/2016/249"
            eli_uri = act_uri.replace("https://fedlex.data.admin.ch/", "")

            try:
                page_num = int(page)
            except ValueError:
                log.debug("Non-numeric page for %s %d: %s", memorial_name, year, page)
                continue

            all_rows.append({
                "ref_type": memorial_name,
                "year": year,
                "page_num": page_num,
                "eli_uri": eli_uri,
            })
            year_count += 1

        if year_count > 0:
            log.debug("%s %d: %d mappings", memorial_name, year, year_count)

    log.info("Fetched %d mappings for %s (%d-%d)", len(all_rows), memorial_name, year_start, year_end)
    return all_rows


def detect_year_range(memorial_name: str) -> tuple[int, int]:
    """Detect the min and max year for a memorial name via SPARQL."""
    query = f"""
    PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>

    SELECT (MIN(?year) AS ?minYear) (MAX(?year) AS ?maxYear) WHERE {{
      ?expr jolux:memorialName "{memorial_name}" .
      ?expr jolux:memorialYear ?year .
      FILTER(?year > 0)
    }}
    """
    rows = sparql_query(query, timeout=60)
    if rows and "minYear" in rows[0] and "maxYear" in rows[0]:
        return int(rows[0]["minYear"]), int(rows[0]["maxYear"])
    return 1848, 2026  # fallback


def create_schema(conn: sqlite3.Connection):
    """Create the amendment_refs table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS amendment_refs (
            ref_type TEXT NOT NULL,
            year INTEGER NOT NULL,
            page_num INTEGER NOT NULL,
            eli_uri TEXT NOT NULL,
            PRIMARY KEY (ref_type, year, page_num)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_amendment_refs_eli
            ON amendment_refs(eli_uri)
    """)
    conn.commit()


def build(db_path: Path, year_start: int | None = None, year_end: int | None = None):
    """Main build pipeline."""
    if not db_path.exists():
        log.error("Statutes DB not found at %s — run build_statutes_db.py first", db_path)
        return

    # Resolve symlinks for atomic operations on same filesystem
    resolved_db = db_path.resolve()
    conn = sqlite3.connect(str(resolved_db))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    create_schema(conn)

    # Clear existing data (full rebuild)
    conn.execute("DELETE FROM amendment_refs")
    conn.commit()

    total = 0

    for collection, names in MEMORIAL_NAMES.items():
        for memorial_name in names:
            # Detect year range if not specified
            if year_start is None or year_end is None:
                min_y, max_y = detect_year_range(memorial_name)
                ys = year_start if year_start is not None else min_y
                ye = year_end if year_end is not None else max_y
            else:
                ys, ye = year_start, year_end

            log.info("Fetching %s mappings for years %d-%d...", memorial_name, ys, ye)
            mappings = fetch_memorial_mappings(memorial_name, ys, ye)

            # Insert in batches
            batch_size = 5000
            inserted = 0
            for i in range(0, len(mappings), batch_size):
                batch = mappings[i : i + batch_size]
                conn.executemany(
                    """INSERT OR IGNORE INTO amendment_refs (ref_type, year, page_num, eli_uri)
                       VALUES (:ref_type, :year, :page_num, :eli_uri)""",
                    batch,
                )
                conn.commit()
                inserted += len(batch)

            log.info("Inserted %d %s mappings (of %d fetched)", inserted, memorial_name, len(mappings))
            total += inserted

    # Summary stats
    stats = conn.execute("""
        SELECT ref_type, COUNT(*), MIN(year), MAX(year)
        FROM amendment_refs
        GROUP BY ref_type
        ORDER BY ref_type
    """).fetchall()

    log.info("Amendment refs table complete: %d total entries", total)
    for ref_type, count, min_y, max_y in stats:
        log.info("  %s: %d entries (%d-%d)", ref_type, count, min_y, max_y)

    # Verify a known mapping: AS 2016 1249 → eli/oc/2016/249
    test = conn.execute(
        "SELECT eli_uri FROM amendment_refs WHERE ref_type='AS' AND year=2016 AND page_num=1249"
    ).fetchone()
    if test:
        log.info("Verification: AS 2016 1249 → %s", test[0])
    else:
        log.warning("Verification FAILED: AS 2016 1249 not found in amendment_refs")

    conn.close()
    log.info("Done. Table added to %s", resolved_db)


def main():
    parser = argparse.ArgumentParser(
        description="Build AS/BBl → ELI amendment reference mappings"
    )
    parser.add_argument(
        "--db", type=Path, default=OUTPUT_DB,
        help="Path to statutes.db (default: output/statutes.db)",
    )
    parser.add_argument(
        "--year-start", type=int, default=None,
        help="Start year (default: auto-detect from SPARQL)",
    )
    parser.add_argument(
        "--year-end", type=int, default=None,
        help="End year (default: auto-detect from SPARQL)",
    )
    parser.add_argument(
        "--delay", type=float, default=0.3,
        help="Delay between SPARQL requests (seconds)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Actually run. Deprecated: writes WAL into the target DB in place; "
             "never point it at the live statutes.db",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if not args.force:
        log.error(
            "build_amendment_refs is deprecated: the amendment_refs table has no readers "
            "and this script writes into the target DB in place (WAL). Rerun with --force "
            "against a copy if you really need it."
        )
        sys.exit(2)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    global REQUEST_DELAY
    REQUEST_DELAY = args.delay

    t0 = time.time()
    build(args.db, args.year_start, args.year_end)
    log.info("Total time: %.1f seconds", time.time() - t0)


if __name__ == "__main__":
    main()
