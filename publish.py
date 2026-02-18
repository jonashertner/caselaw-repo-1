#!/usr/bin/env python3
"""
publish.py — Daily publishing pipeline for Swiss Case Law
==========================================================

Orchestration script for VPS cron job. Runs the full pipeline:
  1.  Ingest new entscheidsuche.ch downloads (if entscheidsuche_ingest.py exists)
  2.  Build/update FTS5 database
  2b. Quality report (optional)
  2c. Build reference graph (citations + statutes, ~78 min)
  2d. Quality enrichment (titles, regeste, dates, hashes, dedup)
  3.  Export JSONL → Parquet
  4.  Upload Parquet + dataset card to HuggingFace
  5.  Generate stats.json
  6.  Git commit + push docs/stats.json

Each step is wrapped in try/except — failures are logged but don't block
subsequent steps.

Cron:
    15 3 * * * cd /opt/caselaw/repo && python3 publish.py >> logs/publish.log 2>&1

Usage:
    python3 publish.py              # run full pipeline
    python3 publish.py --step 3     # run only step 3 (export)
    python3 publish.py --dry-run    # log what would happen
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("publish")

REPO_DIR = Path(__file__).parent.resolve()
OUTPUT_DIR = REPO_DIR / "output"
DATASET_DIR = OUTPUT_DIR / "dataset"
DOCS_DIR = REPO_DIR / "docs"
DB_PATH = OUTPUT_DIR / "decisions.db"

HF_REPO_ID = "voilaj/swiss-caselaw"


def run_cmd(cmd: list[str], description: str, dry_run: bool = False, timeout: int = 3600) -> bool:
    """Run a command, return True on success."""
    logger.info(f"  $ {' '.join(cmd)}")
    if dry_run:
        logger.info("  [dry-run] skipped")
        return True
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, cwd=str(REPO_DIR),
        )
        if result.stdout.strip():
            for line in result.stdout.strip().split("\n"):
                logger.info(f"  stdout: {line}")
        if result.returncode != 0:
            logger.error(f"  exit code {result.returncode}")
            if result.stderr.strip():
                for line in result.stderr.strip().split("\n"):
                    logger.error(f"  stderr: {line}")
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"  timed out after {timeout}s")
        return False
    except Exception as e:
        logger.error(f"  failed: {e}")
        return False


def step_1_ingest(dry_run: bool = False) -> bool:
    """Step 1: Ingest new entscheidsuche.ch downloads."""
    logger.info("Step 1: Ingest entscheidsuche downloads")

    ingest_script = REPO_DIR / "entscheidsuche_ingest.py"
    if not ingest_script.exists():
        # Try scrapers directory
        ingest_script = REPO_DIR / "scrapers" / "entscheidsuche_ingest.py"
    if not ingest_script.exists():
        logger.info("  No ingest script found, skipping")
        return True

    return run_cmd(
        [sys.executable, str(ingest_script)],
        "Ingest entscheidsuche downloads",
        dry_run,
    )


def step_2_build_fts5(dry_run: bool = False, full_rebuild: bool = False) -> bool:
    """Step 2: Build/update FTS5 search database.

    Sunday or --full-rebuild: full rebuild with optimize (~3h).
    Mon–Sat: incremental mode, no optimize (< 1 min).
    """
    script = REPO_DIR / "build_fts5.py"
    if not script.exists():
        logger.error("  build_fts5.py not found")
        return False

    # Sunday (weekday 6) = full rebuild, other days = incremental
    is_rebuild_day = full_rebuild or datetime.now(timezone.utc).weekday() == 6

    cmd = [sys.executable, str(script), "--output", str(OUTPUT_DIR)]

    if is_rebuild_day:
        cmd.append("--full-rebuild")
        logger.info("Step 2: Full FTS5 rebuild (weekly)")
        timeout = 18000  # ~3h40m for 1M decisions + optimize
    else:
        cmd.extend(["--incremental", "--no-optimize"])
        logger.info("Step 2: Incremental FTS5 update")
        timeout = 3600

    return run_cmd(cmd, "Build FTS5 database", dry_run, timeout=timeout)


def step_2b_quality_report(dry_run: bool = False, full_rebuild: bool = False) -> bool:
    """Step 2b: Generate quality report and check gates (weekly)."""
    is_rebuild_day = full_rebuild or datetime.now(timezone.utc).weekday() == 6

    if not is_rebuild_day:
        logger.info("Step 2b: Quality report — skipped (runs on Sundays)")
        return True

    logger.info("Step 2b: Quality report (weekly)")

    script = REPO_DIR / "quality_report.py"
    if not script.exists():
        logger.info("  quality_report.py not found, skipping")
        return True

    if not DB_PATH.exists():
        logger.info("  Database not found, skipping quality report")
        return True

    return run_cmd(
        [sys.executable, str(script),
         "--db", str(DB_PATH),
         "--output", str(OUTPUT_DIR / "quality_report.json"),
         "--gate"],
        "Quality report",
        dry_run,
        timeout=7200,
    )


def step_2c_build_reference_graph(dry_run: bool = False, full_rebuild: bool = False) -> bool:
    """Step 2c: Build reference graph (citations + statutes, weekly)."""
    is_rebuild_day = full_rebuild or datetime.now(timezone.utc).weekday() == 6

    if not is_rebuild_day:
        logger.info("Step 2c: Reference graph — skipped (runs on Sundays)")
        return True

    logger.info("Step 2c: Build reference graph (weekly)")

    script = REPO_DIR / "search_stack" / "build_reference_graph.py"
    if not script.exists():
        logger.info("  build_reference_graph.py not found, skipping")
        return True

    if not DB_PATH.exists():
        logger.info("  FTS5 database not found, skipping reference graph")
        return True

    graph_db = OUTPUT_DIR / "reference_graph.db"
    return run_cmd(
        [sys.executable, str(script),
         "--source-db", str(DB_PATH),
         "--db", str(graph_db)],
        "Build reference graph",
        dry_run,
        timeout=7200,  # ~78 min for 1M decisions
    )


def step_2d_enrich_quality(dry_run: bool = False, full_rebuild: bool = False) -> bool:
    """Step 2d: Enrich data quality (titles, regeste, dates, hashes, dedup).

    Only runs on Sunday (or --full-rebuild). Uses checkpoint internally so
    even a full run is fast when no new decisions exist.
    """
    is_enrichment_day = full_rebuild or datetime.now(timezone.utc).weekday() == 6

    if not is_enrichment_day:
        logger.info("Step 2d: Quality enrichment — skipped (runs weekly on Sunday)")
        return True

    logger.info("Step 2d: Quality enrichment (weekly)")

    script = REPO_DIR / "scripts" / "enrich_quality.py"
    if not script.exists():
        logger.info("  enrich_quality.py not found, skipping")
        return True

    if not DB_PATH.exists():
        logger.info("  FTS5 database not found, skipping enrichment")
        return True

    cmd = [
        sys.executable, str(script),
        "--db", str(DB_PATH),
        "--output", str(OUTPUT_DIR),
    ]
    if dry_run:
        cmd.append("--dry-run")

    return run_cmd(cmd, "Quality enrichment", dry_run, timeout=7200)


def step_3_export_parquet(dry_run: bool = False) -> bool:
    """Step 3: Export JSONL → Parquet."""
    logger.info("Step 3: Export Parquet")

    script = REPO_DIR / "export_parquet.py"
    if not script.exists():
        logger.error("  export_parquet.py not found")
        return False

    return run_cmd(
        [sys.executable, str(script),
         "--input", str(OUTPUT_DIR / "decisions"),
         "--output", str(DATASET_DIR)],
        "Export Parquet",
        dry_run,
    )


def step_4_upload_hf(dry_run: bool = False) -> bool:
    """Step 4: Upload Parquet + dataset card to HuggingFace."""
    logger.info("Step 4: Upload to HuggingFace")

    if dry_run:
        logger.info("  [dry-run] would upload to HuggingFace")
        return True

    try:
        from huggingface_hub import HfApi
    except ImportError:
        logger.error("  huggingface_hub not installed. Run: pip install huggingface_hub")
        return False

    if not DATASET_DIR.exists():
        logger.error(f"  Dataset directory not found: {DATASET_DIR}")
        return False

    parquet_files = list(DATASET_DIR.glob("*.parquet"))
    if not parquet_files:
        logger.error("  No Parquet files to upload")
        return False

    try:
        api = HfApi()

        # Upload dataset card
        card_path = REPO_DIR / "dataset_card.md"
        if card_path.exists():
            api.upload_file(
                path_or_fileobj=str(card_path),
                path_in_repo="README.md",
                repo_id=HF_REPO_ID,
                repo_type="dataset",
            )
            logger.info("  Uploaded dataset card")

        # Upload Parquet files to data/ directory (batch upload)
        logger.info(f"  Uploading {len(parquet_files)} Parquet files to data/...")
        api.upload_folder(
            folder_path=str(DATASET_DIR),
            path_in_repo="data",
            repo_id=HF_REPO_ID,
            repo_type="dataset",
            allow_patterns="*.parquet",
        )

        logger.info(f"  Uploaded {len(parquet_files)} files to {HF_REPO_ID}")
        return True

    except Exception as e:
        logger.error(f"  HuggingFace upload failed: {e}")
        return False


def step_5_generate_stats(dry_run: bool = False) -> bool:
    """Step 5: Generate stats.json from database."""
    logger.info("Step 5: Generate stats.json")

    script = REPO_DIR / "generate_stats.py"
    if not script.exists():
        logger.error("  generate_stats.py not found")
        return False

    return run_cmd(
        [sys.executable, str(script),
         "--db", str(DB_PATH),
         "--output", str(DOCS_DIR / "stats.json")],
        "Generate stats",
        dry_run,
    )


def step_6_git_push(dry_run: bool = False) -> bool:
    """Step 6: Git commit + push docs/stats.json."""
    logger.info("Step 6: Git commit + push stats.json")

    stats_file = DOCS_DIR / "stats.json"
    if not stats_file.exists():
        logger.warning("  docs/stats.json does not exist, skipping")
        return True

    # Check if there are changes
    result = subprocess.run(
        ["git", "diff", "--quiet", "docs/stats.json"],
        capture_output=True, cwd=str(REPO_DIR),
    )
    if result.returncode == 0:
        logger.info("  No changes to stats.json, skipping")
        return True

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if dry_run:
        logger.info(f"  [dry-run] would commit and push stats.json ({today})")
        return True

    ok = run_cmd(["git", "add", "docs/stats.json"], "git add", dry_run)
    if not ok:
        return False

    ok = run_cmd(
        ["git", "commit", "-m", f"Update stats.json ({today})"],
        "git commit",
        dry_run,
    )
    if not ok:
        return False

    return run_cmd(["git", "push"], "git push", dry_run)


STEPS = [
    (1, "Ingest", step_1_ingest),
    (2, "Build FTS5", step_2_build_fts5),
    ("2b", "Quality Report", step_2b_quality_report),
    ("2c", "Reference Graph", step_2c_build_reference_graph),
    ("2d", "Quality Enrichment", step_2d_enrich_quality),
    (3, "Export Parquet", step_3_export_parquet),
    (4, "Upload HuggingFace", step_4_upload_hf),
    (5, "Generate Stats", step_5_generate_stats),
    (6, "Git Push", step_6_git_push),
]


def main():
    parser = argparse.ArgumentParser(description="Swiss Case Law publishing pipeline")
    parser.add_argument(
        "--step", type=str, default=None,
        help="Run only a specific step (1, 2, 2b, 2c, 2d, 3, 4, 5, 6)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log actions without executing")
    parser.add_argument(
        "--full-rebuild", action="store_true",
        help="Force full FTS5 rebuild regardless of day of week"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    logger.info(f"=== Swiss Case Law publish pipeline — {datetime.now(timezone.utc).isoformat()} ===")

    if args.dry_run:
        logger.info("DRY RUN — no changes will be made")

    results = {}
    start = time.time()
    manual_step_mode = args.step is not None

    for num, name, func in STEPS:
        if args.step is not None and str(args.step) != str(num):
            continue
        step_start = time.time()
        try:
            if num == 2:
                ok = func(dry_run=args.dry_run, full_rebuild=args.full_rebuild)
            elif num in ("2b", "2c", "2d"):
                ok = func(
                    dry_run=args.dry_run,
                    full_rebuild=(args.full_rebuild or manual_step_mode),
                )
            else:
                ok = func(dry_run=args.dry_run)
            results[num] = ok
            elapsed = time.time() - step_start
            status = "OK" if ok else "FAILED"
            logger.info(f"  → {status} ({elapsed:.1f}s)\n")
        except Exception as e:
            results[num] = False
            logger.error(f"  → EXCEPTION: {e}\n", exc_info=True)

    # Summary
    total_elapsed = time.time() - start
    logger.info("=== Summary ===")
    for num, name, _ in STEPS:
        if num in results:
            status = "OK" if results[num] else "FAILED"
            logger.info(f"  Step {num} ({name}): {status}")
    logger.info(f"  Total time: {total_elapsed:.1f}s")

    # Exit with error if any step failed
    if any(not v for v in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
