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
the weekly safety-net full rebuild (which still does FTS5 optimize
and full parquet export).

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
        help="Swap BOTH incremental outputs into the live DBs. Shorthand for "
             "--in-place-graph --in-place-structure. Default is shadow mode "
             "(sibling files only).",
    )
    # The two pairs are independent and have never been equally ready: the
    # reference_graph pair went green 2026-08-24, while decision_structure has
    # never passed once. A single flag forced the healthy half to wait for the
    # sick one — worth ~1h50m per night. Splitting them lets each cut over on
    # its own evidence, which is also why publish_drift_check.py now counts a
    # streak per pair rather than one run-level boolean.
    p.add_argument(
        "--in-place-graph",
        action="store_true",
        help="Swap only the reference_graph output into the live DB.",
    )
    p.add_argument(
        "--in-place-structure",
        action="store_true",
        help="Swap only the decision_structure output into the live DB.",
    )
    # ── Stage 2 of the cutover (docs/incremental_nightly_runbook.md) ──
    # Both default OFF, so deploying this file changes nothing until the
    # unit's ExecStart asks for them.
    p.add_argument(
        "--structure-from-shards",
        action="store_true",
        help="Build decision_structure with `publish.py --step 2g` (from the "
             "pristine JSONL shards) instead of the incremental builder. "
             "Costs ~2h but is byte-for-byte the behaviour the full build has "
             "always had, so it sidesteps the decision_structure drift "
             "question entirely — that pair has never passed its gate.",
    )
    p.add_argument(
        "--with-distribution",
        action="store_true",
        help="After stats, run the cheap distribution steps that would "
             "otherwise only happen on Sunday: RSS feeds, the QC gate, the "
             "release manifest, the HuggingFace delta and the git push. "
             "The push is NOT optional in practice — check_output_freshness "
             "deadmans on docs/stats.json commit age at 36h and would page "
             "every Tuesday without it.",
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
    # --in-place stays the both-pairs shorthand so existing invocations and
    # the runbook keep working unchanged.
    in_place_graph = args.in_place or args.in_place_graph
    in_place_structure = args.in_place or args.in_place_structure
    any_in_place = in_place_graph or in_place_structure

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if in_place_graph and in_place_structure:
        mode = "in-place"
    elif not any_in_place:
        mode = "shadow"
    else:
        # Partial cutover — name the pairs, so a summary line is never
        # ambiguous about which half was writing live that night.
        mode = "in-place:" + ",".join(
            (["graph"] if in_place_graph else [])
            + (["structure"] if in_place_structure else []))
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
        if in_place_graph:
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
        if args.structure_from_shards:
            # publish.py step 2g reads output/decisions/*.jsonl, not
            # decisions.db, and writes the live sidecar with its own atomic
            # swap. Identical to what the full build does today, which is
            # why it needs no shadow pair and no drift verdict.
            struct_argv = [sys.executable, "publish.py", "--step", "2g"]
        else:
            struct_argv = [
                sys.executable,
                "search_stack/extract_decision_structure_incremental.py",
                "--decisions-db", str(DECISIONS_DB),
                "--structure-db", str(DECISION_STRUCTURE_DB),
            ]
            if in_place_structure:
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

    # ── Step 3b: drift check (shadow mode only) — compares the sibling
    # incremental DBs against the live full-rebuilt ones. This is the
    # 7-green-nights cutover gate from docs/incremental_nightly_runbook.md
    # (row delta < 0.5%, top-30 cited identical). Drift is a VERDICT on
    # the night, not a crash: the builders already succeeded, so the run
    # keeps ok=true and the gate reads drift_ok from the summary jsonl.
    # A pair built from shards writes the live DB directly and has no
    # sibling, exactly like an --in-place pair.
    _structure_is_shadow = not in_place_structure and not args.structure_from_shards
    shadow_pairs = ([] if in_place_graph else ["reference_graph"]) + \
                   ([] if not _structure_is_shadow else ["decision_structure"])
    if shadow_pairs:
        drift_script = REPO_ROOT / "scripts" / "publish_drift_check.py"
        if drift_script.exists():
            # Check ONLY the pairs still in shadow. A pair that has cut over
            # writes the live DB directly, so its sibling is stale or absent
            # and comparing it would report a permanent false failure that
            # buries the pair still being evaluated.
            drift_argv = [sys.executable, "scripts/publish_drift_check.py",
                          "--tolerance-pct", "0.5"]
            if any_in_place or args.structure_from_shards:
                drift_argv += ["--pairs", ",".join(shadow_pairs)]
            rec = _run_step("drift_check", drift_argv, args.dry_run)
            run["steps"].append(rec)
            run["drift_ok"] = rec["exit_code"] == 0
            if rec["exit_code"] != 0:
                logger.warning(
                    "drift_check FAILED (exit=%d) — this shadow night does "
                    "NOT count toward the 7-green-nights cutover gate",
                    rec["exit_code"],
                )
        else:
            logger.info("[drift_check] script missing — skipping")

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

    # ── Step 5: distribution (opt-in; Sunday-only without it)
    #
    # Order mirrors publish.py's own STEPS list: feeds, then the QC gate,
    # then the manifest (so it captures the gate's verdict), then the push.
    # The gate is treated the way publish.py treats it — a CRITICAL
    # regression must not reach users, so a failing gate skips the push and
    # the delta while leaving the DB work that already happened in place.
    #
    # Measured 2026-08-28: feeds 223s, gate 1,434s, manifest 3s, delta 17s,
    # push 4s, health 25s — under 30 min in total, against the ~3,029s of
    # full Parquet + HuggingFace upload that stay on Sunday.
    if args.with_distribution:
        gate_ok = True
        for step, argv_step, fatal in (
            ("rss_feeds",        "5b", False),
            ("qc_gate",          "5c", True),
            ("release_manifest", "5d", False),
        ):
            rec = _run_step(step, [sys.executable, "publish.py", "--step", argv_step],
                            args.dry_run)
            run["steps"].append(rec)
            if rec["exit_code"] != 0:
                if fatal:
                    gate_ok = False
                    logger.error(
                        "%s FAILED (exit=%d) — skipping git push and delta "
                        "publish so a regression does not reach users",
                        step, rec["exit_code"])
                else:
                    logger.warning("%s failed (exit=%d) — continuing",
                                   step, rec["exit_code"])
        if gate_ok:
            for step, argv_step in (("publish_delta", "7"),
                                    ("git_push", "6"),
                                    ("health_check", "6b")):
                rec = _run_step(step, [sys.executable, "publish.py", "--step", argv_step],
                                args.dry_run)
                run["steps"].append(rec)
                if rec["exit_code"] != 0:
                    # Distribution failures are loud but not fatal: the data
                    # is already correct on the box, only its publication
                    # lagged. check_output_freshness catches a persistent one.
                    logger.warning("%s failed (exit=%d) — continuing",
                                   step, rec["exit_code"])
        run["distribution_ok"] = gate_ok
    else:
        logger.info("[distribution] SKIPPED (--with-distribution not set)")

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
