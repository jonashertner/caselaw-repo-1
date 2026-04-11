#!/usr/bin/env python3
"""
generate_stats.py — Generate statistics JSON from FTS5 database
=================================================================

Queries the SQLite FTS5 database and outputs docs/stats.json
for the public dashboard.

Usage:
    python3 generate_stats.py
    python3 generate_stats.py --db output/decisions.db --output docs/stats.json
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("generate_stats")

# Canton names for display
CANTON_NAMES = {
    "CH": "Schweiz / Suisse",
    "AG": "Aargau", "AI": "Appenzell Innerrhoden", "AR": "Appenzell Ausserrhoden",
    "BE": "Bern", "BL": "Basel-Landschaft", "BS": "Basel-Stadt",
    "FR": "Fribourg", "GE": "Genève", "GL": "Glarus", "GR": "Graubünden",
    "JU": "Jura", "LU": "Luzern", "NE": "Neuchâtel", "NW": "Nidwalden",
    "OW": "Obwalden", "SG": "St. Gallen", "SH": "Schaffhausen",
    "SO": "Solothurn", "SZ": "Schwyz", "TG": "Thurgau", "TI": "Ticino",
    "UR": "Uri", "VD": "Vaud", "VS": "Valais", "ZG": "Zug", "ZH": "Zürich",
}


def generate_stats(db_path: Path) -> dict:
    """Query the FTS5 database and return comprehensive statistics."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    stats: dict = {}
    now_utc = datetime.now(timezone.utc)
    current_year = now_utc.year
    today_iso = now_utc.date().isoformat()

    # Total decisions
    stats["total"] = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    # By court (with date ranges and languages)
    courts = conn.execute("""
        SELECT
            court,
            canton,
            COUNT(*) as count,
            MIN(CASE WHEN decision_date IS NOT NULL AND decision_date != 'None'
                     AND decision_date > '1800-01-01' AND decision_date <= ? THEN decision_date END) as earliest,
            MAX(CASE WHEN decision_date IS NOT NULL AND decision_date != 'None'
                     AND decision_date > '1800-01-01' AND decision_date <= ? THEN decision_date END) as latest,
            MAX(scraped_at) as last_scraped,
            GROUP_CONCAT(DISTINCT language) as languages
        FROM decisions
        GROUP BY court, canton
        ORDER BY count DESC
    """, (today_iso, today_iso)).fetchall()
    stats["by_court"] = [
        {
            "court": r["court"],
            "canton": r["canton"],
            "count": r["count"],
            "earliest": r["earliest"],
            "latest": r["latest"],
            "last_scraped": r["last_scraped"],
            "languages": r["languages"].split(",") if r["languages"] else [],
        }
        for r in courts
    ]

    # By canton (exclude CH — federal courts are not a canton)
    cantons = conn.execute("""
        SELECT canton, COUNT(*) as count
        FROM decisions
        WHERE canton != 'CH'
        GROUP BY canton
        ORDER BY count DESC
    """).fetchall()
    stats["by_canton"] = [
        {
            "canton": r["canton"],
            "name": CANTON_NAMES.get(r["canton"], r["canton"]),
            "count": r["count"],
        }
        for r in cantons
    ]

    # By language
    languages = conn.execute("""
        SELECT language, COUNT(*) as count
        FROM decisions
        GROUP BY language
        ORDER BY count DESC
    """).fetchall()
    stats["by_language"] = {r["language"]: r["count"] for r in languages}

    # By year (all years, filter invalid dates)
    years = conn.execute("""
        SELECT substr(decision_date, 1, 4) as year, COUNT(*) as count
        FROM decisions
        WHERE decision_date IS NOT NULL
          AND decision_date != 'None'
          AND length(decision_date) >= 4
          AND substr(decision_date, 1, 4) BETWEEN '1800' AND '2100'
        GROUP BY year
        ORDER BY year ASC
    """).fetchall()
    stats["by_year"] = {r["year"]: r["count"] for r in years}

    # Recent daily additions (last 30 days by decision_date)
    recent = conn.execute("""
        SELECT decision_date as day, COUNT(*) as count
        FROM decisions
        WHERE decision_date >= date('now', '-30 days')
          AND decision_date <= date('now')
        GROUP BY day
        ORDER BY day ASC
    """).fetchall()
    stats["recent_daily"] = {r["day"]: r["count"] for r in recent}

    # Date range (filter out invalid dates)
    date_range = conn.execute("""
        SELECT MIN(decision_date) as earliest, MAX(decision_date) as latest
        FROM decisions
        WHERE decision_date IS NOT NULL
          AND decision_date != 'None'
          AND decision_date > '1800-01-01'
          AND decision_date <= ?
    """, (today_iso,)).fetchone()
    stats["date_range"] = {
        "earliest": date_range["earliest"],
        "latest": date_range["latest"],
    }

    # Counts
    stats["court_count"] = len(stats["by_court"])

    # ── Derived fields (no new SQL) ──

    # Top 10 courts (pre-sorted for chart)
    stats["top_courts"] = [
        {"court": c["court"], "canton": c["canton"], "count": c["count"]}
        for c in stats["by_court"][:10]
    ]

    # Year-over-year growth %
    year_items = sorted(stats["by_year"].items(), key=lambda x: x[0])
    yoy = {}
    for i in range(1, len(year_items)):
        yr, cnt = year_items[i]
        prev_cnt = year_items[i - 1][1]
        if prev_cnt > 0:
            yoy[yr] = round((cnt - prev_cnt) / prev_cnt * 100, 1)
    stats["yoy_growth"] = yoy

    # Federal vs cantonal split
    fed_total = sum(c["count"] for c in stats["by_court"] if c["canton"] == "CH")
    can_total = sum(c["count"] for c in stats["by_court"] if c["canton"] != "CH")
    stats["federal_vs_cantonal"] = {"federal": fed_total, "cantonal": can_total}

    # ── New SQL queries ──

    # Enrich by_canton with earliest, latest, court_count, languages
    canton_details = conn.execute("""
        SELECT
            canton,
            COUNT(DISTINCT court) as court_count,
            MIN(CASE WHEN decision_date IS NOT NULL AND decision_date != 'None'
                     AND decision_date > '1800-01-01' AND decision_date <= ? THEN decision_date END) as earliest,
            MAX(CASE WHEN decision_date IS NOT NULL AND decision_date != 'None'
                     AND decision_date <= ? THEN decision_date END) as latest,
            GROUP_CONCAT(DISTINCT language) as languages
        FROM decisions
        WHERE canton != 'CH'
        GROUP BY canton
    """, (today_iso, today_iso)).fetchall()
    canton_detail_map = {
        r["canton"]: {
            "court_count": r["court_count"],
            "earliest": r["earliest"],
            "latest": r["latest"],
            "languages": r["languages"].split(",") if r["languages"] else [],
        }
        for r in canton_details
    }
    for entry in stats["by_canton"]:
        detail = canton_detail_map.get(entry["canton"], {})
        entry["court_count"] = detail.get("court_count", 0)
        entry["earliest"] = detail.get("earliest")
        entry["latest"] = detail.get("latest")
        entry["languages"] = detail.get("languages", [])

    # Language by year (2005-current year for stacked area chart)
    lang_by_year = conn.execute("""
        SELECT
            substr(decision_date, 1, 4) as year,
            language,
            COUNT(*) as count
        FROM decisions
        WHERE decision_date IS NOT NULL
          AND decision_date != 'None'
          AND length(decision_date) >= 4
          AND substr(decision_date, 1, 4) BETWEEN ? AND ?
        GROUP BY year, language
        ORDER BY year ASC, language ASC
    """, ("2005", str(current_year))).fetchall()
    lby = {}
    for r in lang_by_year:
        yr = r["year"]
        if yr not in lby:
            lby[yr] = {}
        lby[yr][r["language"]] = r["count"]
    stats["language_by_year"] = lby

    # Monthly counts for last 3 years
    by_month = conn.execute("""
        SELECT
            substr(decision_date, 1, 7) as month,
            COUNT(*) as count
        FROM decisions
        WHERE decision_date IS NOT NULL
          AND decision_date != 'None'
          AND length(decision_date) >= 7
          AND decision_date >= date('now', '-3 years')
          AND decision_date < date('now', '+1 day')
        GROUP BY month
        ORDER BY month ASC
    """).fetchall()
    stats["by_month"] = {r["month"]: r["count"] for r in by_month}

    # Generated timestamp
    stats["generated_at"] = datetime.now(timezone.utc).isoformat()

    conn.close()
    return stats


def collect_corpus_snapshot(repo_dir: Path) -> dict:
    """Count federal laws, cantonal laws, commentaries, citation graph size.

    Each sub-key is optional: if a DB file is missing or corrupt, we skip
    that field rather than failing the whole snapshot. This lets us ship
    the corpus panel ahead of scrapers that are still backfilling.
    """
    out: dict = {}

    def _open_ro(path: Path):
        if not path.exists():
            return None
        try:
            c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            c.row_factory = sqlite3.Row
            return c
        except sqlite3.Error:
            return None

    def _count(conn, sql: str) -> int | None:
        try:
            return conn.execute(sql).fetchone()[0]
        except sqlite3.Error:
            return None

    # Federal laws — statutes.db (Fedlex mirror)
    statutes_path = repo_dir / "output" / "statutes.db"
    conn = _open_ro(statutes_path)
    if conn is not None:
        try:
            n_laws = _count(conn, "SELECT COUNT(*) FROM laws")
            n_articles = _count(
                conn, "SELECT COUNT(*) FROM articles WHERE lang='de'"
            )
            if n_laws is not None:
                out["federal_laws"] = n_laws
            if n_articles is not None:
                out["federal_law_articles"] = n_articles
        finally:
            conn.close()

    # Cantonal laws — cantonal_laws.db (LexFind mirror)
    cantonal_path = repo_dir / "output" / "cantonal_laws.db"
    conn = _open_ro(cantonal_path)
    if conn is not None:
        try:
            n_laws = _count(conn, "SELECT COUNT(DISTINCT lexfind_id) FROM laws")
            n_articles = _count(conn, "SELECT COUNT(*) FROM articles")
            if n_laws is not None:
                out["cantonal_laws"] = n_laws
            if n_articles is not None:
                out["cantonal_law_articles"] = n_articles
            # Per-canton counts
            try:
                by_canton = {
                    r[0]: r[1]
                    for r in conn.execute(
                        "SELECT canton, COUNT(DISTINCT lexfind_id) FROM laws "
                        "GROUP BY canton ORDER BY canton"
                    )
                }
                if by_canton:
                    out["cantonal_laws_by_canton"] = by_canton
            except sqlite3.Error:
                pass
        finally:
            conn.close()

    # Commentaries — ok_commentaries.db (OnlineKommentar + OpenLegalCommentary)
    ok_path = repo_dir / "output" / "ok_commentaries.db"
    conn = _open_ro(ok_path)
    if conn is not None:
        try:
            n = _count(conn, "SELECT COUNT(*) FROM commentaries")
            if n is not None:
                out["commentaries"] = n
        finally:
            conn.close()

    # Citation graph — reference_graph.db
    graph_path = repo_dir / "output" / "reference_graph.db"
    conn = _open_ro(graph_path)
    if conn is not None:
        try:
            # Try known table names (schema has evolved a bit over time).
            for sql in (
                "SELECT COUNT(*) FROM decision_citations",
                "SELECT COUNT(*) FROM citation_targets",
            ):
                n = _count(conn, sql)
                if n is not None:
                    out["citation_edges"] = n
                    break
            for sql in (
                "SELECT COUNT(*) FROM decision_statutes",
                "SELECT COUNT(*) FROM statute_links",
                "SELECT COUNT(*) FROM statute_refs",
            ):
                n = _count(conn, sql)
                if n is not None:
                    out["statute_edges"] = n
                    break
        finally:
            conn.close()

    return out


def _read_health_file(path: Path) -> dict | None:
    """Read a single scraper_health*.json file, swallowing any read errors."""
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Could not read {path.name}: {e}")
        return None


def collect_scraper_health(repo_dir: Path) -> dict | None:
    """Read scraper health JSON, merging the daily full-scrape file with
    the hourly federal poller file when both exist. The daily file is the
    base; per-court entries from the federal file override the daily ones
    (federal data is always fresher for the courts it polls).
    """
    logs_dir = repo_dir / "logs"
    daily = _read_health_file(logs_dir / "scraper_health.json")
    federal = _read_health_file(logs_dir / "scraper_health_federal.json")

    if not daily and not federal:
        logger.info("No scraper_health*.json found, skipping health data")
        return None

    # Start with whichever base has the most scrapers — usually daily.
    if daily and federal:
        health = dict(daily)
        merged_scrapers = dict(daily.get("scrapers", {}))
        merged_scrapers.update(federal.get("scrapers", {}))
        health["scrapers"] = merged_scrapers
        # Record both run timestamps so the dashboard can show freshness.
        health["run_at_daily"] = daily.get("run_at")
        health["run_at_federal"] = federal.get("run_at")
    else:
        health = daily or federal

    scrapers = health.get("scrapers", {})
    state_dir = repo_dir / "state"
    output_dir = repo_dir / "output" / "decisions"

    for court, info in scrapers.items():
        # State file line count = total known decisions
        state_file = state_dir / f"{court}.jsonl"
        if state_file.exists():
            try:
                with open(state_file, "rb") as f:
                    info["state_count"] = sum(1 for _ in f)
            except Exception:
                info["state_count"] = None
        else:
            info["state_count"] = None

        # JSONL output file size and mtime
        jsonl_file = output_dir / f"{court}.jsonl"
        if jsonl_file.exists():
            try:
                st = jsonl_file.stat()
                info["jsonl_size_mb"] = round(st.st_size / (1024 * 1024), 1)
                info["jsonl_mtime"] = datetime.fromtimestamp(
                    st.st_mtime, tz=timezone.utc
                ).isoformat()
            except Exception:
                info["jsonl_size_mb"] = None
                info["jsonl_mtime"] = None
        else:
            info["jsonl_size_mb"] = None
            info["jsonl_mtime"] = None

    result = {
        "run_at": health.get("run_at"),
        "run_duration_s": health.get("run_duration_s"),
        "scrapers": scrapers,
    }
    # Preserve merged run-timestamps from the daily/federal split when present
    # so the dashboard can show freshness for both runs.
    if "run_at_daily" in health:
        result["run_at_daily"] = health["run_at_daily"]
    if "run_at_federal" in health:
        result["run_at_federal"] = health["run_at_federal"]
    if "disk" in health:
        result["disk"] = health["disk"]
    return result


def main():
    parser = argparse.ArgumentParser(description="Generate stats.json from FTS5 database")
    parser.add_argument(
        "--db", type=str, default="output/decisions.db",
        help="Path to SQLite database (default: output/decisions.db)",
    )
    parser.add_argument(
        "--output", type=str, default="docs/stats.json",
        help="Output path for stats.json (default: docs/stats.json)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    stats = generate_stats(db_path)

    # ── Scraper health ──
    repo_dir = Path(__file__).parent.resolve()
    scraper_health = collect_scraper_health(repo_dir)
    if scraper_health:
        stats["scraper_health"] = scraper_health

    # ── Corpus snapshot: laws, commentaries, citation graph ──
    corpus = collect_corpus_snapshot(repo_dir)
    if corpus:
        stats["corpus"] = corpus

    # ── Compute deltas vs previous stats.json ──
    prev = {}
    if output_path.exists():
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                prev = json.load(f)
            logger.info("Loaded previous stats.json for delta computation")
        except Exception as e:
            logger.warning(f"Could not load previous stats.json: {e}")

    if prev:
        prev_total = prev.get("total", 0)
        delta_total = stats["total"] - prev_total

        # by_court delta (only where delta > 0)
        prev_court_counts = {}
        for c in prev.get("by_court", []):
            prev_court_counts[c["court"]] = c["count"]
        delta_by_court = {}
        for c in stats["by_court"]:
            d = c["count"] - prev_court_counts.get(c["court"], 0)
            if d > 0:
                delta_by_court[c["court"]] = d

        # by_canton delta (only where delta > 0)
        prev_canton_counts = {}
        for c in prev.get("by_canton", []):
            prev_canton_counts[c["canton"]] = c["count"]
        delta_by_canton = {}
        for c in stats["by_canton"]:
            d = c["count"] - prev_canton_counts.get(c["canton"], 0)
            if d > 0:
                delta_by_canton[c["canton"]] = d

        # Corpus delta (federal laws, cantonal laws, commentaries, citations)
        prev_corpus = prev.get("corpus", {}) or {}
        now_corpus = stats.get("corpus", {}) or {}
        delta_corpus = {}
        for key in (
            "federal_laws", "federal_law_articles",
            "cantonal_laws", "cantonal_law_articles",
            "commentaries", "citation_edges", "statute_edges",
        ):
            if key in now_corpus and key in prev_corpus:
                d = now_corpus[key] - prev_corpus[key]
                if d != 0:
                    delta_corpus[key] = d

        stats["delta"] = {
            "total": delta_total,
            "by_court": delta_by_court,
            "by_canton": delta_by_canton,
            "corpus": delta_corpus,
            "previous_generated_at": prev.get("generated_at"),
        }
    else:
        stats["delta"] = {
            "total": 0, "by_court": {}, "by_canton": {},
            "corpus": {}, "previous_generated_at": None,
        }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    delta_total = stats["delta"]["total"]
    delta_str = f" (+{delta_total} new)" if delta_total > 0 else ""
    logger.info(f"Stats written to {output_path}")
    print(f"Total: {stats['total']} decisions, {stats['court_count']} courts{delta_str}")


if __name__ == "__main__":
    main()
