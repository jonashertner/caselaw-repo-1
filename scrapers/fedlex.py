#!/usr/bin/env python3
"""
Fedlex scraper — downloads Swiss federal law texts from fedlex.data.admin.ch.

Uses SPARQL to discover all laws in the Classified Compilation (SR/RS),
then downloads Akoma Ntoso XML for the latest in-force consolidation
in all available languages (DE/FR/IT).

Output: output/fedlex/xml/{sr_number}/{lang}.xml
        output/fedlex/laws.json (metadata index)

Usage:
    python -m scrapers.fedlex                  # Full download
    python -m scrapers.fedlex --sr 220 210     # Specific SR numbers only
    python -m scrapers.fedlex --top 100        # Only laws cited in reference graph
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fedlex")

SPARQL_ENDPOINT = "https://fedlex.data.admin.ch/sparqlendpoint"
FILESTORE_BASE = "https://fedlex.data.admin.ch/filestore/fedlex.data.admin.ch"
OUTPUT_DIR = Path(os.environ.get("FEDLEX_OUTPUT", "output/fedlex"))
GRAPH_DB = Path(os.environ.get("SWISS_CASELAW_GRAPH_DB", "output/reference_graph.db"))
LANGUAGES = ["de", "fr", "it"]
REQUEST_DELAY = 0.3  # seconds between downloads

LANG_URIS = {
    "de": "http://publications.europa.eu/resource/authority/language/DEU",
    "fr": "http://publications.europa.eu/resource/authority/language/FRA",
    "it": "http://publications.europa.eu/resource/authority/language/ITA",
}

session = requests.Session()
session.headers["User-Agent"] = "OpenCaseLaw/1.0 (fedlex scraper; +https://opencaselaw.ch)"
session.headers["Accept"] = "application/sparql-results+json"


def sparql_query(query: str, timeout: int = 120) -> list[dict]:
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


def discover_laws() -> list[dict]:
    """Get all laws in the Classified Compilation with SR numbers and latest consolidation dates."""
    log.info("Discovering laws via SPARQL...")
    query = """
    PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>

    SELECT ?work ?srNumber (MAX(?date) AS ?latestDate) WHERE {
      ?work a jolux:ConsolidationAbstract .
      ?work jolux:historicalLegalId ?srNumber .
      ?consolidation jolux:isMemberOf ?work .
      ?consolidation jolux:dateApplicability ?date .
      FILTER(?date <= NOW())
    }
    GROUP BY ?work ?srNumber
    ORDER BY ?srNumber
    """
    rows = sparql_query(query, timeout=300)
    log.info("Found %d law entries with SR numbers", len(rows))
    return rows


def resolve_consolidation_uris(entries: list[dict]) -> dict[str, str]:
    """Resolve actual consolidation URIs for work+date pairs via SPARQL.

    Returns dict of work_uri -> consolidation_uri.
    """
    log.info("Resolving consolidation URIs for %d entries...", len(entries))
    result = {}

    for i in range(0, len(entries), 50):
        batch = entries[i : i + 50]
        # Query each work for its latest consolidation individually,
        # since the date type in SPARQL can vary (xsd:date, string, etc.)
        work_uris = " ".join(f"<{e['work']}>" for e in batch)
        query = f"""
        PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>

        SELECT ?work ?consolidation ?date WHERE {{
          VALUES ?work {{ {work_uris} }}
          ?consolidation jolux:isMemberOf ?work .
          ?consolidation jolux:dateApplicability ?date .
          FILTER(?date <= NOW())
        }}
        ORDER BY ?work DESC(?date)
        """
        rows = sparql_query(query, timeout=120)
        # Keep the first (latest date) consolidation per work
        for row in rows:
            work = row["work"]
            if work not in result:
                result[work] = row["consolidation"]
        time.sleep(REQUEST_DELAY)

    log.info("Resolved %d consolidation URIs", len(result))
    return result


def get_law_metadata(work_uris: list[str]) -> dict[str, dict]:
    """Get titles and abbreviations for laws (batched by language)."""
    log.info("Fetching law metadata (titles, abbreviations)...")
    metadata = {}

    for lang, lang_uri in LANG_URIS.items():
        # Batch in groups of 80 to avoid URL length limits on SPARQL GET
        for i in range(0, len(work_uris), 80):
            batch = work_uris[i : i + 80]
            values = " ".join(f"<{uri}>" for uri in batch)
            query = f"""
            PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>

            SELECT ?work ?title ?abbreviation WHERE {{
              VALUES ?work {{ {values} }}
              ?work jolux:isRealizedBy ?expr .
              ?expr jolux:language <{lang_uri}> .
              OPTIONAL {{ ?expr jolux:titleShort ?abbreviation }}
              OPTIONAL {{ ?expr jolux:title ?title }}
            }}
            """
            try:
                rows = sparql_query(query, timeout=120)
            except Exception as e:
                log.warning("Metadata query failed (batch %d-%d): %s", i, i + len(batch), e)
                continue
            for row in rows:
                work = row["work"]
                if work not in metadata:
                    metadata[work] = {}
                if "title" in row:
                    metadata[work][f"title_{lang}"] = row["title"]
                if "abbreviation" in row:
                    metadata[work][f"abbr_{lang}"] = row["abbreviation"]
            time.sleep(REQUEST_DELAY)

    return metadata


def get_xml_urls(consolidation_uris: list[str]) -> dict[str, dict[str, str]]:
    """Get XML download URLs for consolidations, keyed by consolidation URI and language."""
    log.info("Resolving XML download URLs via SPARQL...")
    url_map = {}  # consolidation_uri -> {lang: url}

    for i in range(0, len(consolidation_uris), 100):
        batch = consolidation_uris[i : i + 100]
        values = " ".join(f"<{uri}>" for uri in batch)
        query = f"""
        PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>

        SELECT ?consolidation ?lang ?url WHERE {{
          VALUES ?consolidation {{ {values} }}
          ?consolidation jolux:isRealizedBy ?expr .
          ?expr jolux:language ?lang .
          ?expr jolux:isEmbodiedBy ?manif .
          ?manif jolux:userFormat <https://fedlex.data.admin.ch/vocabulary/user-format/xml> .
          ?manif jolux:isExemplifiedBy ?url .
        }}
        """
        rows = sparql_query(query, timeout=120)
        for row in rows:
            cons = row["consolidation"]
            lang_uri = row["lang"]
            url = row["url"]
            lang = next(
                (k for k, v in LANG_URIS.items() if v == lang_uri), None
            )
            if lang:
                url_map.setdefault(cons, {})[lang] = url
        time.sleep(REQUEST_DELAY)

    return url_map


def download_xml(url: str, dest: Path, retries: int = 3) -> bool:
    """Download an XML file with retries."""
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=60)
            if resp.status_code == 200:
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(resp.content)
                return True
            elif resp.status_code == 404:
                log.debug("404: %s", url)
                return False
            else:
                log.warning("HTTP %d for %s (attempt %d)", resp.status_code, url, attempt + 1)
        except requests.RequestException as e:
            log.warning("Error downloading %s: %s (attempt %d)", url, e, attempt + 1)
        time.sleep(2 ** attempt)
    return False


def get_top_cited_sr_numbers(limit: int = 100) -> set[str]:
    """Get the most-cited law codes from the reference graph, mapped to SR numbers."""
    if not GRAPH_DB.exists():
        log.warning("Reference graph not found at %s, cannot prioritize", GRAPH_DB)
        return set()

    conn = sqlite3.connect(str(GRAPH_DB))
    try:
        rows = conn.execute("""
            SELECT s.law_code, COUNT(*) as cite_count
            FROM decision_statutes ds
            JOIN statutes s ON ds.statute_id = s.statute_id
            GROUP BY s.law_code
            ORDER BY cite_count DESC
            LIMIT ?
        """, (limit,)).fetchall()
    finally:
        conn.close()

    law_codes = {row[0] for row in rows}
    log.info("Top %d cited law codes: %s", len(law_codes), ", ".join(sorted(law_codes)[:20]))
    return law_codes


def _eli_year(work_uri: str) -> int:
    """Extract the enactment year from an ELI work URI.

    Old format: eli/cc/54/... → 1954 (add 1900 if <=99)
    New format: eli/cc/1999/... → 1999
    Roman:      eli/cc/IV/... → 4
    """
    m = re.search(r"/eli/cc/([^/]+)/", work_uri)
    if not m:
        return 0
    part = m.group(1)
    try:
        year = int(part)
    except ValueError:
        # Roman numerals (very old laws) — treat as year 0
        return 0
    if year <= 99:
        year += 1900
    return year


def normalize_sr(sr: str) -> str:
    """Normalize SR number for use as directory name (replace dots with underscores)."""
    return sr.replace(".", "_").replace("/", "_")


def run(sr_filter: set[str] | None = None, top_cited: int = 0):
    """Main scraper pipeline."""
    # Step 1: Discover all laws
    laws = discover_laws()

    # Step 2: Deduplicate — multiple works can share one SR number
    # (e.g. old 1874 BV and current 1999 BV both have SR 101).
    # Group by SR, then for each group try to resolve consolidation URIs.
    # Keep the work whose consolidation resolves; if multiple resolve, prefer latest.
    from collections import defaultdict
    sr_candidates: dict[str, list[dict]] = defaultdict(list)
    for row in laws:
        sr_candidates[row["srNumber"]].append(row)

    # For SR numbers with only one work, keep it directly
    # For SR numbers with multiple works, prefer the newer enactment
    single_laws = []
    multi_sr = {}
    for sr, candidates in sr_candidates.items():
        if len(candidates) == 1:
            single_laws.append(candidates[0])
        else:
            multi_sr[sr] = candidates
    log.info("Found %d unique SR numbers (%d with multiple works)", len(sr_candidates), len(multi_sr))

    # For multi-work SR numbers, prefer the work with the higher year
    # (newer enactment replaces old one, e.g. BV 1999 replaces BV 1874)
    if multi_sr:
        for sr, candidates in multi_sr.items():
            candidates.sort(key=lambda c: _eli_year(c["work"]), reverse=True)
            single_laws.append(candidates[0])
            if len(candidates) > 1:
                log.debug("SR %s: picked %s (year %d) over %s",
                          sr, candidates[0]["work"][-30:], _eli_year(candidates[0]["work"]),
                          candidates[1]["work"][-30:])

    laws = single_laws

    # Step 3: Early filter
    if sr_filter:
        laws = [r for r in laws if r["srNumber"] in sr_filter]
        log.info("Filtered to %d laws by SR number", len(laws))
    elif top_cited > 0:
        # For --top mode: fetch metadata first (lightweight), then filter by cited codes
        cited_codes = get_top_cited_sr_numbers(top_cited)
        if cited_codes:
            work_uris_all = list({row["work"] for row in laws})
            metadata_all = get_law_metadata(work_uris_all)
            matched = []
            cited_upper = {c.upper() for c in cited_codes}
            for row in laws:
                meta = metadata_all.get(row["work"], {})
                abbrs = {
                    meta.get("abbr_de", "").upper(),
                    meta.get("abbr_fr", "").upper(),
                    meta.get("abbr_it", "").upper(),
                }
                if abbrs & cited_upper:
                    matched.append(row)
            if matched:
                laws = matched
                log.info("Filtered to %d laws matching cited law codes", len(laws))

    # Step 4: Get metadata (skip if already fetched for --top)
    work_uris = list({row["work"] for row in laws})
    if top_cited > 0 and 'metadata_all' in dir():
        metadata = {w: metadata_all[w] for w in work_uris if w in metadata_all}
    else:
        metadata = get_law_metadata(work_uris)

    # Step 5: Resolve consolidation URIs (only for matched laws)
    cons_map = resolve_consolidation_uris(laws)

    # Step 6: Build law index
    law_index = []
    for row in laws:
        work = row["work"]
        sr = row["srNumber"]
        date_str = row["latestDate"]
        consolidation_uri = cons_map.get(work)
        if not consolidation_uri:
            log.warning("No consolidation URI for SR %s (work %s, date %s)", sr, work, date_str)
            continue

        # Extract consolidation date from URI (more accurate than discover query)
        cons_date_match = re.search(r"/(\d{8})$", consolidation_uri)
        if cons_date_match:
            d = cons_date_match.group(1)
            cons_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        else:
            cons_date = date_str

        entry = {
            "sr_number": sr,
            "work_uri": work,
            "consolidation_uri": consolidation_uri,
            "consolidation_date": cons_date,
        }
        # Add metadata
        meta = metadata.get(work, {})
        for key in ["title_de", "title_fr", "title_it", "abbr_de", "abbr_fr", "abbr_it"]:
            if key in meta:
                entry[key] = meta[key]

        law_index.append(entry)

    log.info("Built index of %d laws", len(law_index))

    # Step 5: Resolve XML download URLs
    consolidation_uris = [e["consolidation_uri"] for e in law_index]
    xml_urls = get_xml_urls(consolidation_uris)

    # Step 6: Download XMLs
    xml_dir = OUTPUT_DIR / "xml"
    downloaded = 0
    skipped = 0
    failed = 0

    for entry in law_index:
        sr = entry["sr_number"]
        sr_dir = normalize_sr(sr)
        cons_uri = entry["consolidation_uri"]
        urls = xml_urls.get(cons_uri, {})

        if not urls:
            log.debug("No XML URLs for SR %s", sr)
            failed += 1
            continue

        for lang in LANGUAGES:
            url = urls.get(lang)
            if not url:
                continue

            dest = xml_dir / sr_dir / f"{lang}.xml"
            if dest.exists():
                skipped += 1
                continue

            if download_xml(url, dest):
                downloaded += 1
                # Store the URL in the entry for the index
                entry[f"xml_url_{lang}"] = url
            else:
                failed += 1

            time.sleep(REQUEST_DELAY)

        if downloaded % 50 == 0 and downloaded > 0:
            log.info("Progress: %d downloaded, %d skipped, %d failed", downloaded, skipped, failed)

    log.info(
        "Done: %d downloaded, %d skipped (existing), %d failed",
        downloaded, skipped, failed,
    )

    # Step 7: Save law index
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    index_path = OUTPUT_DIR / "laws.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(law_index, f, ensure_ascii=False, indent=2)
    log.info("Saved law index to %s (%d entries)", index_path, len(law_index))


def main():
    global REQUEST_DELAY

    parser = argparse.ArgumentParser(description="Download Swiss federal law texts from Fedlex")
    parser.add_argument("--sr", nargs="+", help="Specific SR numbers to download")
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        help="Only download laws matching the top N cited law codes from reference graph",
    )
    parser.add_argument("--delay", type=float, default=0.3, help="Delay between requests")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    REQUEST_DELAY = args.delay

    sr_filter = set(args.sr) if args.sr else None
    run(sr_filter=sr_filter, top_cited=args.top)


if __name__ == "__main__":
    main()
