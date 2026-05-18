#!/usr/bin/env python3
"""Incremental nightly publish orchestrator (Workstream A v0.1, 2026-05-18).

Composes the three incremental builders that have shipped over the past
two weeks into a single nightly run that replaces the 7h+ full rebuild
for weekday nights:

  Step 1  quick_publish               — ingest new JSONL, bump db_generation,
                                        atomic swap (~30s typical, ~5 min on
                                        first-of-day delta)
  Step 2  reference_graph_incremental — hash-track processed decisions in
                                        a state table, re-extract only the
                                        delta (~3-5 min)
  Step 3  decision_structure_incremental — same shape as Step 2 (~3-5 min)
  Step 4  generate_stats              — refresh dashboard JSON (~30s)

Modes
-----
* ``--shadow`` (default): incremental builders write to SIBLING .db files
  next to the live ones. Live data untouched. Pairs with
  ``scripts/publish_drift_check.py`` for nightly validation.
* ``--in-place``: incremental builders swap live DBs. Only flip once
  shadow has been green for at least one week.

Sequencing
----------
Designed to run AFTER ``opencaselaw-scrape.timer`` finishes (01:00 UTC).
A new ``opencaselaw-publish-incremental.timer`` should fire at 03:30 UTC
Mon-Sat; the legacy ``opencaselaw-publish.timer`` keeps Sun 03:30 UTC for
the weekly safety-net full rebuild (which still does wayback_queue
provisioning, FTS5 optimize, full parquet export).

Failure semantics
-----------------
Each step runs in a subprocess with its own log capture. A failure in
any step short-circuits the run with non-zero exit (systemd notices,
on-failure alerting fires). The atomic swap pattern in each underlying
builder guarantees that a mid-run failure never corrupts the live DB.

A summary record is appended to ``logs/incremental_nightly.jsonl`` on
every run (success or failure) — gives the drift-check tool a stable
audit trail.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_FILE = REPO_ROOT / "logs" / "incremental_nightly.jsonl"

# DB paths follow the SWISS_CASELAW_DIR convention used everywhere in the
# codebase (mcp_server.py, build_fts5.py, the legacy publish steps). On
# the VPS this resolves to /opt/caselaw/repo/output. The actual files
# may be symlinks to /mnt/HC_Volume_104655575/... — both builders open
# them via _resolve_real_path so that's transparent.
DATA_DIR = Path(os.environ.get("SWISS_CASELAW_DIR", str(REPO_ROOT / "output")))
DECISIONS_DB = DATA_DIR / "decisions.db"
REFERENCE_GRAPH_DB = DATA_DIR / "reference_graph.db"
DECISION_STRUCTURE_DB = DATA_DIR / "decision_structure.db"

logger = logging.getLogger("incremental_nightly")


def _run_step(name: str, argv: list[str], dry_run: bool) -> dict:
    """Run one builder step, capture timing + exit code, stream output to
    the orchestrator log so journalctl shows real-time progress.

    Returns a step record. On non-zero exit code, the orchestrator
    short-circuits in main() — but we still write the partial record so
    the drift check can see which step blew up.
    """
    record = {
        "step": name,
        "argv": argv,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
    }
    if dry_run:
        logger.info("[%s] DRY RUN — would exec: %s", name, " ".join(argv))
        record["exit_code"] = 0
        record["duration_s"] = 0.0
        record["ended_at"] = record["started_at"]
        return record

    logger.info("[%s] starting: %s", name, " ".join(argv))
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            argv,
            cwd=str(REPO_ROOT),
            check=False,
            text=True,
            capture_output=False,  # stream to orchestrator stdout/stderr
        )
        record["exit_code"] = proc.returncode
    except FileNotFoundError as e:
        logger.error("[%s] binary not found: %s", name, e)
        record["exit_code"] = 127
        record["error"] = str(e)
    except Exception as e:  # noqa: BLE001
        logger.exception("[%s] unexpected error", name)
        record["exit_code"] = 1
        record["error"] = repr(e)
    record["duration_s"] = round(time.monotonic() - t0, 2)
    record["ended_at"] = datetime.now(timezone.utc).isoformat()
    logger.info(
        "[%s] done in %ss → exit=%d",
        name, record["duration_s"], record["exit_code"],
    )
    return record


def _append_summary(run_record: dict) -> None:
    """Append the run summary to logs/incremental_nightly.jsonl.

    JSONL append is the same pattern used by logs/llm_usage.jsonl —
    durable, grep-friendly, drift-check ingestible.
    """
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(run_record, ensure_ascii=False) + "\n")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--in-place",
        action="store_true",
        help="Swap incremental outputs into the live DBs. Default is "
             "shadow mode (sibling files only).",
    )
    p.add_argument(
        "--skip-quick-publish",
        action="store_true",
        help="Skip Step 1 (quick_publish). Useful when scrapers haven't "
             "completed yet or for testing the graph/structure steps "
             "in isolation.",
    )
    p.add_argument(
        "--skip-graph",
        action="store_true",
        help="Skip Step 2 (reference_graph_incremental).",
    )
    p.add_argument(
        "--skip-structure",
        action="store_true",
        help="Skip Step 3 (decision_structure_incremental).",
    )
    p.add_argument(
        "--skip-stats",
        action="store_true",
        help="Skip Step 4 (generate_stats).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would run without executing.",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    mode = "in-place" if args.in_place else "shadow"
    logger.info("=== incremental_nightly start (mode=%s) ===", mode)

    run = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "dry_run": args.dry_run,
        "steps": [],
        "ok": True,
    }
    t_start = time.monotonic()

    # ── Step 1: quick_publish (always in-place — this IS the ingestion
    # path; quick_publish itself uses the atomic-swap pattern with
    # db_generation bump that the Monday gate validated today)
    if not args.skip_quick_publish:
        rec = _run_step(
            "quick_publish",
            [sys.executable, "scripts/quick_publish.py", "-v"],
            args.dry_run,
        )
        run["steps"].append(rec)
        if rec["exit_code"] != 0:
            run["ok"] = False
            run["failed_at"] = "quick_publish"
            run["total_duration_s"] = round(time.monotonic() - t_start, 2)
            run["ended_at"] = datetime.now(timezone.utc).isoformat()
            _append_summary(run)
            return rec["exit_code"]
    else:
        logger.info("[quick_publish] SKIPPED (--skip-quick-publish)")

    # ── Step 2: reference_graph_incremental
    if not args.skip_graph:
        graph_argv = [
            sys.executable,
            "search_stack/build_reference_graph_incremental.py",
            "--decisions-db", str(DECISIONS_DB),
            "--graph-db", str(REFERENCE_GRAPH_DB),
        ]
        if args.in_place:
            graph_argv.append("--in-place")
        rec = _run_step("reference_graph", graph_argv, args.dry_run)
        run["steps"].append(rec)
        if rec["exit_code"] != 0:
            run["ok"] = False
            run["failed_at"] = "reference_graph"
            run["total_duration_s"] = round(time.monotonic() - t_start, 2)
            run["ended_at"] = datetime.now(timezone.utc).isoformat()
            _append_summary(run)
            return rec["exit_code"]
    else:
        logger.info("[reference_graph] SKIPPED (--skip-graph)")

    # ── Step 3: decision_structure_incremental
    if not args.skip_structure:
        struct_argv = [
            sys.executable,
            "search_stack/extract_decision_structure_incremental.py",
            "--decisions-db", str(DECISIONS_DB),
            "--structure-db", str(DECISION_STRUCTURE_DB),
        ]
        if args.in_place:
            struct_argv.append("--in-place")
        rec = _run_step("decision_structure", struct_argv, args.dry_run)
        run["steps"].append(rec)
        if rec["exit_code"] != 0:
            run["ok"] = False
            run["failed_at"] = "decision_structure"
            run["total_duration_s"] = round(time.monotonic() - t_start, 2)
            run["ended_at"] = datetime.now(timezone.utc).isoformat()
            _append_summary(run)
            return rec["exit_code"]
    else:
        logger.info("[decision_structure] SKIPPED (--skip-structure)")

    # ── Step 4: generate_stats (refreshes docs/stats.json)
    if not args.skip_stats:
        stats_script = REPO_ROOT / "generate_stats.py"
        if stats_script.exists():
            rec = _run_step(
                "generate_stats",
                [sys.executable, "generate_stats.py"],
                args.dry_run,
            )
            run["steps"].append(rec)
            # generate_stats failure is non-fatal — dashboard staleness
            # is annoying but doesn't break search/MCP
            if rec["exit_code"] != 0:
                logger.warning(
                    "generate_stats failed (exit=%d) — continuing",
                    rec["exit_code"],
                )
        else:
            logger.info("[generate_stats] script missing — skipping")
    else:
        logger.info("[generate_stats] SKIPPED (--skip-stats)")

    run["total_duration_s"] = round(time.monotonic() - t_start, 2)
    run["ended_at"] = datetime.now(timezone.utc).isoformat()
    _append_summary(run)

    logger.info(
        "=== incremental_nightly done (mode=%s, total=%ss, ok=%s) ===",
        mode, run["total_duration_s"], run["ok"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
