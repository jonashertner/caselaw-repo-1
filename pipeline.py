"""
Daily Scraping Pipeline
========================

Orchestrates all court scrapers, produces Parquet shards, uploads to HuggingFace,
and optionally imports into SQLite FTS5 for full-text search.

Architecture:
- Each scraper produces a list of Decision objects
- Decisions are written as daily Parquet shards: data/daily/YYYY-MM-DD_{court}.parquet
- HuggingFace upload: incremental (only new shards)
- Monthly consolidation: merge daily shards into monthly files
- Optional: SQLite FTS5 import for local/VPS search

Usage:
    python pipeline.py --scrape --upload --fts5
    python pipeline.py --scrape --courts bger,bge,bvger
    python pipeline.py --consolidate  # monthly merge
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ============================================================
# Scraper Registry
# ============================================================


def get_scraper_registry() -> dict:
    """
    Return a dict mapping court_code -> scraper class.

    Uses the canonical registry from run_scraper.py to avoid drift between
    entrypoints. Scraper imports remain lazy and fault-tolerant per scraper.
    """
    registry: dict[str, type] = {}
    from run_scraper import SCRAPERS

    for court_code, (module_name, class_name) in sorted(SCRAPERS.items()):
        try:
            module = importlib.import_module(module_name)
            registry[court_code] = getattr(module, class_name)
        except Exception as e:
            logger.warning(f"{court_code} scraper not available: {e}")
    return registry


# ============================================================
# Parquet output
# ============================================================


def write_parquet_shard(
    decisions: list,
    output_dir: Path,
    court_code: str,
    scrape_date: date | None = None,
) -> Path | None:
    """
    Write decisions to a daily Parquet shard.

    File: {output_dir}/data/daily/{YYYY-MM-DD}_{court_code}.parquet

    Returns the path to the written file, or None if no decisions.
    """
    if not decisions:
        return None

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.error("pyarrow not installed. Run: pip install pyarrow")
        return None

    scrape_date = scrape_date or date.today()
    daily_dir = output_dir / "data" / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{scrape_date.isoformat()}_{court_code}.parquet"
    filepath = daily_dir / filename

    # Convert to dicts
    rows = []
    for d in decisions:
        row = d.model_dump()
        # Convert date/datetime to strings for Parquet compatibility
        for key, val in row.items():
            if isinstance(val, (date, datetime)):
                row[key] = val.isoformat()
            elif isinstance(val, list):
                row[key] = json.dumps(val)
        rows.append(row)

    table = pa.Table.from_pylist(rows)
    pq.write_table(table, filepath, compression="zstd")

    logger.info(f"Wrote {len(decisions)} decisions to {filepath}")
    return filepath


# ============================================================
# HuggingFace upload
# ============================================================


def upload_to_huggingface(
    output_dir: Path,
    repo_id: str,
    token: str | None = None,
) -> None:
    """
    Upload new Parquet shards to HuggingFace.

    Uses the HuggingFace Hub API for incremental uploads.
    Only uploads files that don't already exist in the repo.
    """
    try:
        from huggingface_hub import HfApi
    except ImportError:
        logger.error("huggingface_hub not installed. Run: pip install huggingface_hub")
        return

    token = token or os.environ.get("HF_TOKEN")
    if not token:
        logger.error("No HuggingFace token. Set HF_TOKEN env var.")
        return

    api = HfApi()

    daily_dir = output_dir / "data" / "daily"
    if not daily_dir.exists():
        logger.info("No daily shards to upload.")
        return

    # List existing files in repo
    try:
        existing = set()
        for f in api.list_repo_files(repo_id, repo_type="dataset", token=token):
            existing.add(f)
    except Exception:
        # Repo might not exist yet
        existing = set()
        try:
            api.create_repo(repo_id, repo_type="dataset", token=token, private=False)
            logger.info(f"Created HuggingFace dataset repo: {repo_id}")
        except Exception as e:
            logger.error(f"Failed to create repo: {e}")
            return

    # Upload new shards
    uploaded = 0
    for parquet_file in sorted(daily_dir.glob("*.parquet")):
        remote_path = f"data/daily/{parquet_file.name}"
        if remote_path not in existing:
            try:
                api.upload_file(
                    path_or_fileobj=str(parquet_file),
                    path_in_repo=remote_path,
                    repo_id=repo_id,
                    repo_type="dataset",
                    token=token,
                )
                uploaded += 1
                logger.info(f"Uploaded: {remote_path}")
            except Exception as e:
                logger.error(f"Failed to upload {parquet_file.name}: {e}")

    logger.info(f"HuggingFace upload complete. {uploaded} new files.")


# ============================================================
# Monthly consolidation
# ============================================================


def consolidate_monthly(output_dir: Path) -> None:
    """
    Merge daily Parquet shards into monthly files.

    Output: data/monthly/{YYYY-MM}.parquet (one per month).
    Daily shards are kept for reference but can be cleaned up.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        logger.error("pyarrow not installed.")
        return

    daily_dir = output_dir / "data" / "daily"
    monthly_dir = output_dir / "data" / "monthly"
    monthly_dir.mkdir(parents=True, exist_ok=True)

    if not daily_dir.exists():
        return

    # Group shards by month
    from collections import defaultdict
    month_groups: dict[str, list[Path]] = defaultdict(list)

    for f in sorted(daily_dir.glob("*.parquet")):
        # Filename: YYYY-MM-DD_court.parquet
        month_key = f.name[:7]  # YYYY-MM
        month_groups[month_key].append(f)

    for month, files in sorted(month_groups.items()):
        monthly_file = monthly_dir / f"{month}.parquet"

        tables = []
        for f in files:
            try:
                t = pq.read_table(f)
                tables.append(t)
            except Exception as e:
                logger.warning(f"Skipping corrupt shard {f}: {e}")

        if tables:
            import pyarrow as pa
            merged = pa.concat_tables(tables)
            pq.write_table(merged, monthly_file, compression="zstd")
            logger.info(f"Consolidated {month}: {len(files)} shards → {merged.num_rows} rows")


# ============================================================
# SQLite FTS5 import
# ============================================================


def import_to_fts5(output_dir: Path, db_path: Path | None = None) -> None:
    """
    Import Parquet data into SQLite FTS5 for full-text search.

    Creates/updates a decisions table and FTS5 virtual table.
    """
    import sqlite3

    try:
        import pyarrow.parquet as pq
    except ImportError:
        logger.error("pyarrow not installed.")
        return

    db_path = db_path or output_dir / "decisions.db"

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")

    # Create tables
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS decisions (
            decision_id TEXT PRIMARY KEY,
            court TEXT NOT NULL,
            canton TEXT NOT NULL,
            chamber TEXT,
            docket_number TEXT NOT NULL,
            decision_date TEXT NOT NULL,
            publication_date TEXT,
            language TEXT NOT NULL,
            title TEXT,
            legal_area TEXT,
            regeste TEXT,
            full_text TEXT,
            decision_type TEXT,
            outcome TEXT,
            source_url TEXT,
            pdf_url TEXT,
            cited_decisions TEXT,
            scraped_at TEXT,
            json_data TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_decisions_court ON decisions(court);
        CREATE INDEX IF NOT EXISTS idx_decisions_canton ON decisions(canton);
        CREATE INDEX IF NOT EXISTS idx_decisions_date ON decisions(decision_date);
        CREATE INDEX IF NOT EXISTS idx_decisions_language ON decisions(language);
        CREATE INDEX IF NOT EXISTS idx_decisions_docket ON decisions(docket_number);

        CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
            decision_id UNINDEXED,
            court,
            canton,
            docket_number,
            language,
            title,
            regeste,
            full_text,
            content=decisions,
            content_rowid=rowid,
            tokenize='unicode61 remove_diacritics 2'
        );

        -- Triggers to keep FTS in sync with main table
        CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
            INSERT INTO decisions_fts(rowid, decision_id, court, canton,
                docket_number, language, title, regeste, full_text)
            VALUES (new.rowid, new.decision_id, new.court, new.canton,
                new.docket_number, new.language, new.title, new.regeste,
                new.full_text);
        END;

        CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
            INSERT INTO decisions_fts(decisions_fts, rowid, decision_id, court,
                canton, docket_number, language, title, regeste, full_text)
            VALUES ('delete', old.rowid, old.decision_id, old.court, old.canton,
                old.docket_number, old.language, old.title, old.regeste,
                old.full_text);
        END;

        CREATE TRIGGER IF NOT EXISTS decisions_au AFTER UPDATE ON decisions BEGIN
            INSERT INTO decisions_fts(decisions_fts, rowid, decision_id, court,
                canton, docket_number, language, title, regeste, full_text)
            VALUES ('delete', old.rowid, old.decision_id, old.court, old.canton,
                old.docket_number, old.language, old.title, old.regeste,
                old.full_text);
            INSERT INTO decisions_fts(rowid, decision_id, court, canton,
                docket_number, language, title, regeste, full_text)
            VALUES (new.rowid, new.decision_id, new.court, new.canton,
                new.docket_number, new.language, new.title, new.regeste,
                new.full_text);
        END;
    """)

    # Import from daily shards
    daily_dir = output_dir / "data" / "daily"
    if not daily_dir.exists():
        conn.close()
        return

    imported = 0
    skipped = 0
    for parquet_file in sorted(daily_dir.glob("*.parquet")):
        try:
            table = pq.read_table(parquet_file)
            for batch in table.to_batches():
                for row in batch.to_pylist():
                    try:
                        # Skip if already exists
                        exists = conn.execute(
                            "SELECT 1 FROM decisions WHERE decision_id = ?",
                            (row["decision_id"],),
                        ).fetchone()
                        if exists:
                            skipped += 1
                            continue

                        conn.execute(
                            """INSERT INTO decisions
                            (decision_id, court, canton, chamber, docket_number,
                             decision_date, publication_date, language, title,
                             legal_area, regeste, full_text, decision_type,
                             outcome, source_url, pdf_url, cited_decisions,
                             scraped_at, json_data)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                row["decision_id"],
                                row["court"],
                                row["canton"],
                                row.get("chamber"),
                                row["docket_number"],
                                row["decision_date"],
                                row.get("publication_date"),
                                row["language"],
                                row.get("title"),
                                row.get("legal_area"),
                                row.get("regeste"),
                                row.get("full_text"),
                                row.get("decision_type"),
                                row.get("outcome"),
                                row.get("source_url"),
                                row.get("pdf_url"),
                                row.get("cited_decisions"),
                                row.get("scraped_at"),
                                json.dumps(row, default=str),
                            ),
                        )
                        imported += 1
                    except Exception as e:
                        logger.warning(f"Failed to import {row.get('decision_id', '?')}: {e}")
            conn.commit()
        except Exception as e:
            logger.warning(f"Failed to read {parquet_file}: {e}")

    # Optimize FTS index
    conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('optimize')")
    conn.commit()
    conn.close()

    logger.info(
        f"FTS5 import complete. {imported} new, {skipped} skipped → {db_path}"
    )


# ============================================================
# Main pipeline
# ============================================================


def run_pipeline(
    courts: list[str] | None = None,
    since_date: str | None = None,
    max_per_court: int | None = None,
    output_dir: Path = Path("output"),
    state_dir: Path = Path("state"),
    do_upload: bool = False,
    hf_repo: str = "voilaj/swiss-caselaw",
    do_fts5: bool = False,
    do_consolidate: bool = False,
    fail_on_any_error: bool = True,
) -> dict:
    """
    Run the full scraping pipeline.

    Returns dict of court_code -> count of new decisions.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    registry = get_scraper_registry()
    results = {}
    failed_courts: list[str] = []

    # Determine which courts to scrape
    if courts:
        active = {k: v for k, v in registry.items() if k in courts}
    else:
        active = registry

    if not active:
        logger.warning("No active scrapers. Available: " + ", ".join(registry.keys()))
        return results

    logger.info(f"Pipeline starting. Courts: {list(active.keys())}")

    # Scrape each court
    for court_code, scraper_class in active.items():
        try:
            scraper = scraper_class(state_dir=state_dir)

            since = None
            if since_date:
                from models import parse_date
                since = parse_date(since_date)

            decisions = scraper.run(since_date=since, max_decisions=max_per_court)
            results[court_code] = len(decisions)

            if decisions:
                write_parquet_shard(decisions, output_dir, court_code)

        except Exception as e:
            logger.error(f"Pipeline error for {court_code}: {e}", exc_info=True)
            results[court_code] = -1
            failed_courts.append(court_code)

    if failed_courts:
        logger.error(f"Scraper failures detected: {failed_courts}")
        if fail_on_any_error:
            raise RuntimeError(
                "Aborting pipeline because scraper failures prevent completeness: "
                + ", ".join(failed_courts)
            )

    # Upload to HuggingFace
    if do_upload:
        upload_to_huggingface(output_dir, hf_repo)

    # Consolidate monthly
    if do_consolidate:
        consolidate_monthly(output_dir)

    # FTS5 import
    if do_fts5:
        import_to_fts5(output_dir)

    # Summary
    total = sum(v for v in results.values() if v > 0)
    logger.info(f"Pipeline complete. Total new decisions: {total}")
    for court, count in results.items():
        logger.info(f"  {court}: {count}")

    return results


# ============================================================
# CLI
# ============================================================


def main():
    parser = argparse.ArgumentParser(
        description="Swiss Case Law Scraping Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Scrape all federal courts, upload to HuggingFace
    python pipeline.py --scrape --upload --hf-repo your-org/swiss-caselaw

    # Scrape only BGE Leitentscheide
    python pipeline.py --scrape --courts bge --max 50

    # Monthly consolidation
    python pipeline.py --consolidate

    # Full pipeline: scrape, upload, build FTS5
    python pipeline.py --scrape --upload --fts5

Available courts: see run_scraper.py SCRAPERS registry
        """,
    )

    parser.add_argument("--scrape", action="store_true", help="Run scrapers")
    parser.add_argument(
        "--courts", type=str, help="Comma-separated court codes (default: all)"
    )
    parser.add_argument("--since", type=str, help="Only scrape since this date")
    parser.add_argument("--max", type=int, help="Max decisions per court")
    parser.add_argument("--upload", action="store_true", help="Upload to HuggingFace")
    parser.add_argument(
        "--hf-repo",
        type=str,
        default="voilaj/swiss-caselaw",
        help="HuggingFace dataset repo",
    )
    parser.add_argument("--fts5", action="store_true", help="Import to SQLite FTS5")
    parser.add_argument(
        "--consolidate", action="store_true", help="Run monthly consolidation"
    )
    parser.add_argument(
        "--allow-partial-failures",
        action="store_true",
        help="Continue and publish even if one or more scrapers fail",
    )
    parser.add_argument("--output", type=str, default="output", help="Output directory")
    parser.add_argument(
        "--state",
        type=str,
        default="state",
        help="State directory shared with run_scraper.py (default: state)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if not any([args.scrape, args.upload, args.fts5, args.consolidate]):
        parser.print_help()
        return

    courts = args.courts.split(",") if args.courts else None
    output_dir = Path(args.output)
    state_dir = Path(args.state)

    if args.scrape:
        try:
            results = run_pipeline(
                courts=courts,
                since_date=args.since,
                max_per_court=args.max,
                output_dir=output_dir,
                state_dir=state_dir,
                do_upload=args.upload,
                hf_repo=args.hf_repo,
                do_fts5=args.fts5,
                do_consolidate=args.consolidate,
                fail_on_any_error=not args.allow_partial_failures,
            )
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            sys.exit(1)
    else:
        if args.upload:
            upload_to_huggingface(output_dir, args.hf_repo)
        if args.consolidate:
            consolidate_monthly(output_dir)
        if args.fts5:
            import_to_fts5(output_dir)


if __name__ == "__main__":
    main()
