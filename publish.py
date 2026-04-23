#!/usr/bin/env python3
"""
publish.py — Daily publishing pipeline for Swiss Case Law
==========================================================

Orchestration script for VPS cron job. Runs the full pipeline:
  1.  Ingest new entscheidsuche.ch downloads (if entscheidsuche_ingest.py exists)
  2.  Build/update FTS5 database
  2d. Quality enrichment (titles, regeste, dates, hashes, dedup)
  2b. Quality report (optional)
  2c. Build reference graph (citations + statutes, ~78 min)
  3.  Export database/JSONL → Parquet
  4.  Upload Parquet + dataset card to HuggingFace
  5.  Generate stats.json
  6.  Git commit + push docs/stats.json

Most steps are wrapped in try/except — failures are logged. Critical steps
(FTS5, Parquet) will skip subsequent guarded steps (HF upload, git push) to
avoid publishing an incomplete dataset.

Cron:
    15 3 * * * cd /opt/caselaw/repo && python3 publish.py >> logs/publish.log 2>&1

Usage:
    python3 publish.py              # run full pipeline
    python3 publish.py --step 3     # run only step 3 (export)
    python3 publish.py --dry-run    # log what would happen
"""
from __future__ import annotations

import argparse
import fcntl
import json
import urllib.request
import logging
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

LOCK_FILE_PATH = "/tmp/opencaselaw-publish.lock"
CHECKPOINT_PATH = Path("/tmp/opencaselaw-publish-checkpoint.json")
NTFY_TOPIC = "opencaselaw-publish"  # https://ntfy.sh/opencaselaw-publish


def _notify(title: str, message: str, *, priority: str = "default"):
    """Send push notification via ntfy.sh (best-effort, never fails the pipeline)."""
    try:
        req = urllib.request.Request(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode(),
            headers={"Title": title, "Priority": priority},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # notification failure must never break the pipeline


def _save_checkpoint(step_num, results: dict):
    """Save completed step so pipeline can resume after crash."""
    CHECKPOINT_PATH.write_text(json.dumps({
        "last_completed_step": str(step_num),
        "results": {str(k): v for k, v in results.items()},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }))


def _load_checkpoint() -> dict | None:
    """Load checkpoint from prior crashed run (if any)."""
    if CHECKPOINT_PATH.exists():
        try:
            data = json.loads(CHECKPOINT_PATH.read_text())
            age_hours = (datetime.now(timezone.utc) - datetime.fromisoformat(data["timestamp"])).total_seconds() / 3600
            if age_hours < 12:  # only resume if < 12h old
                return data
        except Exception:
            pass
    return None


def _clear_checkpoint():
    """Remove checkpoint after successful completion."""
    CHECKPOINT_PATH.unlink(missing_ok=True)

logger = logging.getLogger("publish")

REPO_DIR = Path(__file__).parent.resolve()
OUTPUT_DIR = REPO_DIR / "output"
DATASET_DIR = OUTPUT_DIR / "dataset"
DOCS_DIR = REPO_DIR / "docs"
DB_PATH = OUTPUT_DIR / "decisions.db"

HF_REPO_ID = "voilaj/swiss-caselaw"




def run_cmd(cmd: list[str], description: str, dry_run: bool = False, timeout: int = 3600) -> bool:
    """Run a command, return True on success.

    Streams stdout/stderr line-by-line to the logger instead of buffering
    the full output in memory (avoids OOM on long-running steps like
    build_fts5 or graph build that can produce hundreds of MB of output).
    """
    logger.info(f"  $ {' '.join(cmd)}")
    if dry_run:
        logger.info("  [dry-run] skipped")
        return True
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # merge stderr into stdout to avoid pipe deadlock
            text=True,
            cwd=str(REPO_DIR),
        )
        # Watchdog timer: kill the process if it exceeds the timeout.
        # We can't rely on proc.wait(timeout=) because the for-loop
        # over proc.stdout blocks until EOF (i.e. process exit).
        timed_out = threading.Event()

        def _kill_on_timeout():
            timed_out.set()
            proc.kill()

        timer = threading.Timer(timeout, _kill_on_timeout)
        timer.start()
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line:
                    logger.info(f"  | {line}")
            proc.wait()
        finally:
            timer.cancel()
        if timed_out.is_set():
            logger.error(f"  timed out after {timeout}s")
            return False
        if proc.returncode != 0:
            logger.error(f"  exit code {proc.returncode}")
            return False
        return True
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

    Always uses full rebuild: builds to .db.tmp then atomic os.replace().
    This avoids DB locks with live MCP workers (immutable=1 connections).
    """
    script = REPO_DIR / "build_fts5.py"
    if not script.exists():
        logger.error("  build_fts5.py not found")
        return False

    # Use ionice/nice to prevent I/O starvation of live MCP workers.
    cmd = ["ionice", "-c3", "nice", "-n", "19",
           sys.executable, str(script), "--output", str(OUTPUT_DIR),
           "--full-rebuild"]

    logger.info("Step 2: Full FTS5 rebuild (low I/O priority, zero-downtime swap)")
    timeout = 18000  # ~3h40m for 1M decisions + optimize

    return run_cmd(cmd, "Build FTS5 database", dry_run, timeout=timeout)


def step_2b_quality_report(dry_run: bool = False, full_rebuild: bool = False) -> bool:
    """Step 2b: Generate quality report and check gates."""
    logger.info("Step 2b: Quality report")

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
    """Step 2c: Build reference graph (citations + statutes)."""
    logger.info("Step 2c: Build reference graph")

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
    """Step 2d: Enrich data quality (titles, regeste, dates, hashes, dedup)."""
    logger.info("Step 2d: Quality enrichment")

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


def step_2e_build_anwaltsrecht_tags(dry_run: bool = False, full_rebuild: bool = False) -> bool:
    """Step 2e: Build Anwaltsrecht tags DB from SAV PDFs."""
    logger.info("Step 2e: Build Anwaltsrecht tags")

    script = REPO_DIR / "search_stack" / "build_anwaltsrecht_tags.py"
    if not script.exists():
        logger.info("  build_anwaltsrecht_tags.py not found, skipping")
        return True

    if not DB_PATH.exists():
        logger.info("  FTS5 database not found, skipping Anwaltsrecht tags")
        return True

    tags_db = OUTPUT_DIR / "anwaltsrecht_tags.db"
    return run_cmd(
        [sys.executable, str(script),
         "--fts5-db", str(DB_PATH),
         "--output", str(tags_db)],
        "Build Anwaltsrecht tags",
        dry_run,
        timeout=600,
    )


def step_2g_build_decision_structure(dry_run: bool = False, full_rebuild: bool = False) -> bool:
    """Step 2g: Rebuild decision_structure.db sidecar (Sachverhalt / Erwägungen-Paragraphs / Dispositiv / Regeste).

    Federal courts only (BGer/BVGer/BStGer/BGE/BPatGer/EGMR-CH/BGE-historical/MKG).
    Cantonal courts: separate iteration. Reads JSONL shards, writes sidecar
    SQLite with atomic swap. Used by get_decision_structure / get_erwaegung /
    get_regeste MCP tools and to enrich get_case_brief responses.
    """
    logger.info("Step 2g: Build decision_structure sidecar")

    script = REPO_DIR / "search_stack" / "extract_decision_structure.py"
    if not script.exists():
        logger.info("  extract_decision_structure.py not found, skipping")
        return True

    return run_cmd(
        [sys.executable, str(script), "--build",
         "--shards", "bger,bvger,bstger,bge,bpatger,bge_egmr,bge_historical,mkg",
         "--decisions-dir", str(OUTPUT_DIR / "decisions"),
         "--output", str(OUTPUT_DIR / "decision_structure.db")],
        "Build decision_structure sidecar",
        dry_run,
        timeout=1800,  # ~6min typical, generous bound
    )


def step_3_export_parquet(dry_run: bool = False) -> bool:
    """Step 3: Export SQLite/JSONL corpus to Parquet."""
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
            delete_patterns="*.parquet",  # prune remote parquet not in local folder
        )

        logger.info(f"  Uploaded {len(parquet_files)} files to {HF_REPO_ID}")
        return True

    except Exception as e:
        logger.error(f"  HuggingFace upload failed: {e}")
        return False


def step_2f_build_materialien(dry_run: bool = False, full_rebuild: bool = False) -> bool:
    """Step 2f: Rebuild materialien.db (Botschaft refs + digests + debates)."""
    logger.info("Step 2f: Build materialien.db")

    script = REPO_DIR / "search_stack" / "build_materialien_db.py"
    if not script.exists():
        logger.info("  build_materialien_db.py not found, skipping")
        return True

    materialien_dir = REPO_DIR / "data" / "materialien"
    if not materialien_dir.exists():
        logger.info("  data/materialien/ not found, skipping")
        return True

    return run_cmd(
        [sys.executable, "-m", "search_stack.build_materialien_db",
         "--input-dir", str(materialien_dir)],
        "Build materialien.db",
        dry_run,
        timeout=600,
    )


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


def step_7_publish_delta(dry_run: bool = False) -> bool:
    """Step 7: Publish daily delta (SQLite + parquet + manifest) to HuggingFace.

    Env-gated by `OCL_PUBLISH_DELTA=1` — off by default until the new
    pipeline has been validated end-to-end. When OFF, the step logs
    "skipped" and returns True (non-fatal).

    Requires a seeded snapshot at state/hf_delta_snapshot.json. Seed once
    with `python3 -m search_stack.publish_delta --seed` BEFORE enabling.
    Without a seed, the first run would treat the entire corpus as "new".

    Replaces the broken private-repo pipeline
    (`jonashertner/caselaw-repo/daily_update.yml`) that's been publishing
    federal-less deltas for 30+ days. Keep that workflow disabled while
    this is on — two pipelines racing will corrupt artifacts/manifest.json.
    """
    if os.environ.get("OCL_PUBLISH_DELTA") != "1":
        logger.info("Step 7: Publish Delta — DISABLED (set OCL_PUBLISH_DELTA=1 to enable)")
        return True

    logger.info("Step 7: Publish Delta to HuggingFace")
    snapshot_path = REPO_DIR / "state" / "hf_delta_snapshot.json"
    if not snapshot_path.exists():
        logger.error("  state/hf_delta_snapshot.json missing — run with --seed first")
        return False

    return run_cmd(
        [sys.executable, "-m", "search_stack.publish_delta",
         "--db", str(DB_PATH),
         "--snapshot", str(snapshot_path),
         "--build-dir", "/tmp/caselaw_delta_build"]
        + (["--dry-run"] if dry_run else []),
        "Publish Delta",
        dry_run,
        timeout=1800,
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

    # Pull --rebase to avoid conflicts when local commits were pushed
    # from development machines between cron runs.  Stash with --include-untracked
    # to handle temp scripts and other untracked files that block rebase.
    run_cmd(["git", "stash", "--include-untracked"], "git stash --include-untracked", dry_run)
    rebase_ok = run_cmd(["git", "pull", "--rebase", "origin", "main"], "git pull --rebase", dry_run)
    run_cmd(["git", "stash", "pop"], "git stash pop", dry_run)
    if not rebase_ok:
        return False
    return run_cmd(["git", "push"], "git push", dry_run)


# Execution order intentionally differs from the step IDs to preserve the
# existing CLI surface (`--step 2b`, `--step 2c`, `--step 2d`) while ensuring
# weekly enrichment happens before quality gating and graph construction.
# Two-tier pipeline:
# - FAST tier (Steps 1→2→5→6): runs daily, completes in ~3.5h.
#   After FTS5 build + atomic swap, immediately regenerate stats.json
#   and push to GitHub so the site shows today's date.
# - SLOW tier (Steps 2d→2e→2b→2c→3→4→5→6): enrichment, graph,
#   Parquet export, HuggingFace upload, final stats refresh + push.
#   Runs after the fast tier. Can be skipped on --fast-only.
#
# The key insight: stats.json + git push happen TWICE — once after
# FTS5 (fast, site-visible) and again after everything else finishes
# (includes updated graph counts, quality report, etc.).

STEPS = [
    (1, "Ingest", step_1_ingest),
    (2, "Build FTS5", step_2_build_fts5),
    # ── Fast publish: site shows today's date immediately ──
    ("5a", "Generate Stats (early)", step_5_generate_stats),
    ("6a", "Git Push (early)", step_6_git_push),
    # ── Slow tier: enrichment, graph, materialien, export ──
    ("2d", "Quality Enrichment", step_2d_enrich_quality),
    ("2e", "Anwaltsrecht Tags", step_2e_build_anwaltsrecht_tags),
    ("2b", "Quality Report", step_2b_quality_report),
    ("2c", "Reference Graph", step_2c_build_reference_graph),
    ("2f", "Materialien", step_2f_build_materialien),
    ("2g", "Decision Structure", step_2g_build_decision_structure),
    (3, "Export Parquet", step_3_export_parquet),
    (4, "Upload HuggingFace", step_4_upload_hf),
    # ── Delta publish (env-gated; empty no-op until OCL_PUBLISH_DELTA=1) ──
    (7, "Publish Delta", step_7_publish_delta),
    # ── Final stats refresh (includes graph counts) ──
    (5, "Generate Stats (final)", step_5_generate_stats),
    (6, "Git Push (final)", step_6_git_push),
]


def main():
    parser = argparse.ArgumentParser(description="Swiss Case Law publishing pipeline")
    parser.add_argument(
        "--step", type=str, default=None,
        help="Run only a specific step (1, 2, 2b, 2c, 2d, 2e, 3, 4, 5, 6)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log actions without executing")
    parser.add_argument(
        "--full-rebuild", action="store_true",
        help="Force full FTS5 rebuild regardless of day of week"
    )
    parser.add_argument(
        "--ingest", action="store_true",
        help="Run entscheidsuche ingest (step 1); skipped by default"
    )
    parser.add_argument(
        "--fast-only", action="store_true",
        help="Stop after FTS5 build + early stats push (skip graph, Parquet, HF upload). "
             "The site shows today's date in ~3.5h instead of ~6h."
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    logger.info(f"=== Swiss Case Law publish pipeline — {datetime.now(timezone.utc).isoformat()} ===")

    # Prevent concurrent publish runs (cron + manual overlap)
    lock_file = open(LOCK_FILE_PATH, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        logger.error("Another publish process is already running. Exiting.")
        return
    logger.info("Acquired publish lock")

    if args.dry_run:
        logger.info("DRY RUN — no changes will be made")

    results = {}
    start = time.time()
    manual_step_mode = args.step is not None

    # Steps 4 (HF upload) and 6/6a (git push) must not run if critical steps failed,
    # because step 4 prunes remote parquet based on local state.
    CRITICAL_STEPS = {2, 3}
    GUARDED_STEPS = {4, 6}
    # Non-fatal steps: still logged as FAILED in summary, but don't trigger
    # systemd exit-code=1. These depend on flaky external sources (e.g. sav-fsa.ch
    # PDFs) and their failure doesn't degrade the published dataset.
    NON_FATAL_STEPS = {"2e"}
    # Steps after the fast tier — skipped with --fast-only
    SLOW_STEPS = {"2d", "2e", "2b", "2c", "2f", "2g", 3, 4, 5, 6}

    # Resume from checkpoint if prior run crashed
    checkpoint = _load_checkpoint() if not manual_step_mode else None
    if checkpoint:
        logger.info(f"  Resuming from checkpoint (last completed: step {checkpoint['last_completed_step']})")
        for k, v in checkpoint["results"].items():
            # Restore prior results; step keys can be int or str
            for snum, _, _ in STEPS:
                if str(snum) == k:
                    results[snum] = v
                    break

    for num, name, func in STEPS:
        if args.step is not None and str(args.step) != str(num):
            continue
        # Skip steps already completed in a prior checkpoint
        if checkpoint and str(num) in checkpoint["results"] and checkpoint["results"][str(num)]:
            logger.info(f"  Step {num} ({name}): SKIPPED (completed in prior run)")
            results[num] = True
            continue
        # Step 1 (ingest) is opt-in: skip unless --ingest or --step 1
        if num == 1 and not args.ingest and not manual_step_mode:
            logger.info(f"  Step {num} ({name}): SKIPPED (use --ingest to enable)")
            results[num] = True
            continue
        # --fast-only: stop after the early stats push (5a + 6a)
        if args.fast_only and num in SLOW_STEPS:
            logger.info(f"  Step {num} ({name}): SKIPPED (--fast-only)")
            results[num] = True
            continue
        # Skip guarded steps if a critical step failed (unless running single step)
        if not manual_step_mode and num in GUARDED_STEPS:
            critical_failed = any(
                results.get(s) is False for s in CRITICAL_STEPS
            )
            if critical_failed:
                results[num] = False
                logger.warning(
                    f"  Step {num} ({name}): SKIPPED — critical earlier step failed\n"
                )
                continue
        step_start = time.time()
        try:
            if num == 2:
                ok = func(dry_run=args.dry_run, full_rebuild=args.full_rebuild)
            elif num in ("2b", "2c", "2d", "2e", "2f", "2g"):
                ok = func(
                    dry_run=args.dry_run,
                    full_rebuild=(args.full_rebuild or manual_step_mode),
                )
            else:
                # Steps 5, 5a, 6, 6a and all others take only dry_run
                ok = func(dry_run=args.dry_run)
            results[num] = ok
            elapsed = time.time() - step_start
            status = "OK" if ok else "FAILED"
            logger.info(f"  → {status} ({elapsed:.1f}s)\n")
            if ok:
                _save_checkpoint(num, results)
        except Exception as e:
            results[num] = False
            logger.error(f"  → EXCEPTION: {e}\n", exc_info=True)

    # Summary
    total_elapsed = time.time() - start
    failed_steps = []
    non_fatal_failures = []
    logger.info("=== Summary ===")
    for num, name, _ in STEPS:
        if num in results:
            status = "OK" if results[num] else "FAILED"
            logger.info(f"  Step {num} ({name}): {status}")
            if not results[num]:
                if num in NON_FATAL_STEPS:
                    non_fatal_failures.append(f"{num} ({name})")
                else:
                    failed_steps.append(f"{num} ({name})")
    logger.info(f"  Total time: {total_elapsed:.1f}s")

    # Notify on completion
    if failed_steps:
        msg = f"Failed steps: {', '.join(failed_steps)}"
        if non_fatal_failures:
            msg += f"\nNon-fatal: {', '.join(non_fatal_failures)}"
        msg += f"\nTotal: {total_elapsed/60:.0f} min"
        _notify("Publish FAILED", msg, priority="high")
        sys.exit(1)
    else:
        _clear_checkpoint()
        if non_fatal_failures:
            logger.warning(
                f"Publish OK (non-fatal failures: {', '.join(non_fatal_failures)})"
            )
        # Count decisions from stats if available
        try:
            stats = json.loads((DOCS_DIR / "stats.json").read_text())
            n = stats.get("total_decisions", "?")
        except Exception:
            n = "?"
        _notify(
            "Publish OK",
            f"{n} decisions, {total_elapsed/60:.0f} min",
        )


if __name__ == "__main__":
    main()
