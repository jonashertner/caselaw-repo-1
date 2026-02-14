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
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    stats: dict = {}

    # Total decisions
    stats["total"] = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    # By court (with date ranges and languages)
    courts = conn.execute("""
        SELECT
            court,
            canton,
            COUNT(*) as count,
            MIN(decision_date) as earliest,
            MAX(decision_date) as latest,
            GROUP_CONCAT(DISTINCT language) as languages
        FROM decisions
        GROUP BY court, canton
        ORDER BY count DESC
    """).fetchall()
    stats["by_court"] = [
        {
            "court": r["court"],
            "canton": r["canton"],
            "count": r["count"],
            "earliest": r["earliest"],
            "latest": r["latest"],
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
          AND decision_date < '2100-01-01'
    """).fetchone()
    stats["date_range"] = {
        "earliest": date_range["earliest"],
        "latest": date_range["latest"],
    }

    # Counts
    stats["court_count"] = len(stats["by_court"])

    # Generated timestamp
    stats["generated_at"] = datetime.now(timezone.utc).isoformat()

    conn.close()
    return stats


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
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    logger.info(f"Stats written to {output_path}")
    print(f"Total: {stats['total']} decisions, {stats['court_count']} courts")


if __name__ == "__main__":
    main()
