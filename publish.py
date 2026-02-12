#!/usr/bin/env python3
"""
publish.py — Daily publishing pipeline for Swiss Caselaw
==========================================================

Orchestration script for VPS cron job. Runs the full pipeline:
  1. Ingest new entscheidsuche.ch downloads (if entscheidsuche_ingest.py exists)
  2. Build/update FTS5 database
  3. Export JSONL → Parquet
  4. Upload Parquet + dataset card to HuggingFace
  5. Generate stats.json
  6. Git commit + push docs/stats.json

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
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("publish")

REPO_DIR = Path(__file__).parent.resolve()
OUTPUT_DIR = REPO_DIR / "output"
DATASET_DIR = OUTPUT_DIR / "dataset"
DOCS_DIR = REPO_DIR / "docs"
DB_PATH = OUTPUT_DIR / "decisions.db"

HF_REPO_ID = "voilaj/swiss-caselaw"


def run_cmd(cmd: list[str], description: str, dry_run: bool = False) -> bool:
    """Run a command, return True on success."""
    logger.info(f"  $ {' '.join(cmd)}")
    if dry_run:
        logger.info("  [dry-run] skipped")
        return True
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=3600, cwd=str(REPO_DIR),
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
        logger.error(f"  timed out after 3600s")
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


def step_2_build_fts5(dry_run: bool = False) -> bool:
    """Step 2: Build/update FTS5 search database."""
    logger.info("Step 2: Build FTS5 database")

    script = REPO_DIR / "build_fts5.py"
    if not script.exists():
        logger.error("  build_fts5.py not found")
        return False

    return run_cmd(
        [sys.executable, str(script), "--output", str(OUTPUT_DIR)],
        "Build FTS5 database",
        dry_run,
    )


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

        # Upload Parquet files
        for pf in parquet_files:
            api.upload_file(
                path_or_fileobj=str(pf),
                path_in_repo=pf.name,
                repo_id=HF_REPO_ID,
                repo_type="dataset",
            )
            logger.info(f"  Uploaded {pf.name}")

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

    today = datetime.utcnow().strftime("%Y-%m-%d")

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
    (3, "Export Parquet", step_3_export_parquet),
    (4, "Upload HuggingFace", step_4_upload_hf),
    (5, "Generate Stats", step_5_generate_stats),
    (6, "Git Push", step_6_git_push),
]


def main():
    parser = argparse.ArgumentParser(description="Swiss Caselaw publishing pipeline")
    parser.add_argument(
        "--step", type=int, default=None,
        help="Run only a specific step (1-6)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log actions without executing")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    logger.info(f"=== Swiss Caselaw publish pipeline — {datetime.utcnow().isoformat()} ===")

    if args.dry_run:
        logger.info("DRY RUN — no changes will be made")

    results = {}
    start = time.time()

    for num, name, func in STEPS:
        if args.step is not None and args.step != num:
            continue
        step_start = time.time()
        try:
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
