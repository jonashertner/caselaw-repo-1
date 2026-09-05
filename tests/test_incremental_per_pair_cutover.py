"""Per-pair cutover: the two shadow pairs must be able to flip independently.

Before this, `--in-place` was a single flag driving both incremental builders
(incremental_nightly.py) and `drift_ok` was a single run-level boolean written
to logs/incremental_nightly.jsonl and read by no code at all. Two consequences:

  * the reference_graph pair, green since 2026-08-24, was held behind
    decision_structure, which has never passed once — worth ~1h50m a night; and
  * the runbook's "7 consecutive green nights" gate was uncountable, because a
    green pair was invisible behind a red one in the same boolean.

A pair that has cut over writes the live DB directly, so its shadow sibling is
stale or absent — it must be EXCLUDED from the drift check rather than left to
report a permanent false failure that buries the pair still under evaluation.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent


def _dry_run(*flags: str) -> str:
    # These assertions cover the planned command sequence, not the wall clock.
    # Late-start behavior has dedicated tests using explicit times.
    r = subprocess.run(
        [sys.executable, "scripts/incremental_nightly.py", "--dry-run",
         "--skip-quick-publish", "--now-utc", "20:00", *flags],
        cwd=REPO, text=True, capture_output=True, check=False,
    )
    return r.stdout + r.stderr


def _line_for(out: str, step: str) -> str:
    for line in out.splitlines():
        if f"[{step}]" in line:
            return line
    return ""


def test_default_is_shadow_for_both_pairs():
    out = _dry_run()
    assert "mode=shadow" in out
    assert "--in-place" not in _line_for(out, "reference_graph")
    assert "--in-place" not in _line_for(out, "decision_structure")
    # Both still shadow => drift check covers everything, no --pairs narrowing.
    assert "--pairs" not in _line_for(out, "drift_check")


def test_graph_only_cutover_leaves_structure_in_shadow():
    out = _dry_run("--in-place-graph")
    assert "mode=in-place:graph" in out
    assert "--in-place" in _line_for(out, "reference_graph")
    assert "--in-place" not in _line_for(out, "decision_structure")


def test_graph_only_cutover_narrows_the_drift_check():
    """The whole point: keep evaluating the pair that has NOT cut over."""
    drift = _line_for(_dry_run("--in-place-graph"), "drift_check")
    assert "--pairs decision_structure" in drift
    assert "reference_graph" not in drift.split("--pairs")[1]


def test_structure_only_cutover_is_symmetric():
    out = _dry_run("--in-place-structure")
    assert "mode=in-place:structure" in out
    assert "--in-place" in _line_for(out, "decision_structure")
    assert "--in-place" not in _line_for(out, "reference_graph")
    assert "--pairs reference_graph" in _line_for(out, "drift_check")


def test_legacy_in_place_still_means_both():
    """Existing invocations and the runbook must keep working unchanged."""
    out = _dry_run("--in-place")
    assert "mode=in-place" in out
    assert "--in-place" in _line_for(out, "reference_graph")
    assert "--in-place" in _line_for(out, "decision_structure")
    # Nothing left in shadow => no drift check at all.
    assert _line_for(out, "drift_check") == ""


# ── green-streak counting ──────────────────────────────────────────────

def _pdc():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "pdc", REPO / "scripts" / "publish_drift_check.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _hist(tmp_path: Path, runs: list[list[tuple[str, bool]]]) -> Path:
    p = tmp_path / "drift.jsonl"
    with p.open("w") as fh:
        for run in runs:
            fh.write(json.dumps(
                {"pairs": [{"pair": n, "ok": ok} for n, ok in run]}) + "\n")
    return p


def test_streak_counts_consecutive_greens_including_this_run(tmp_path):
    m = _pdc()
    hist = _hist(tmp_path, [[("g", True)], [("g", True)]])
    st = m._green_streaks(hist, {"pairs": [{"pair": "g", "ok": True}]})
    assert st["g"] == 3


def test_streak_breaks_on_a_red_night(tmp_path):
    m = _pdc()
    hist = _hist(tmp_path, [[("g", True)], [("g", False)], [("g", True)]])
    st = m._green_streaks(hist, {"pairs": [{"pair": "g", "ok": True}]})
    assert st["g"] == 2


def test_a_red_run_has_streak_zero(tmp_path):
    m = _pdc()
    hist = _hist(tmp_path, [[("g", True)] for _ in range(9)])
    st = m._green_streaks(hist, {"pairs": [{"pair": "g", "ok": False}]})
    assert st["g"] == 0


def test_pairs_are_counted_independently(tmp_path):
    """The bug this exists to prevent: a green pair hidden behind a red one."""
    m = _pdc()
    hist = _hist(tmp_path, [
        [("graph", True), ("structure", False)],
        [("graph", True), ("structure", False)],
    ])
    st = m._green_streaks(hist, {"pairs": [
        {"pair": "graph", "ok": True}, {"pair": "structure", "ok": False}]})
    assert st == {"graph": 3, "structure": 0}


def test_nights_that_did_not_check_a_pair_are_transparent(tmp_path):
    """Once graph cuts over it stops being checked; that must not break
    structure's streak, nor silently extend it."""
    m = _pdc()
    hist = _hist(tmp_path, [
        [("graph", True), ("structure", True)],
        [("structure", True)],          # graph has cut over, not checked
        [("structure", True)],
    ])
    st = m._green_streaks(hist, {"pairs": [{"pair": "structure", "ok": True}]})
    assert st["structure"] == 4


def test_torn_history_line_does_not_break_the_gate(tmp_path):
    m = _pdc()
    hist = _hist(tmp_path, [[("g", True)]])
    with hist.open("a") as fh:
        fh.write('{"pairs": [{"pair": "g", "ok": tr\n')   # truncated write
    st = m._green_streaks(hist, {"pairs": [{"pair": "g", "ok": True}]})
    assert st["g"] == 2


def test_missing_history_is_not_fatal(tmp_path):
    m = _pdc()
    st = m._green_streaks(tmp_path / "nope.jsonl",
                          {"pairs": [{"pair": "g", "ok": True}]})
    assert st["g"] == 1


def test_gate_threshold_matches_the_runbook():
    assert _pdc().GATE_GREEN_NIGHTS == 7


def test_unknown_pair_name_is_rejected():
    r = subprocess.run(
        [sys.executable, "scripts/publish_drift_check.py", "--pairs", "nonsense"],
        cwd=REPO, text=True, capture_output=True, check=False)
    assert r.returncode == 2
    assert "unknown pair" in (r.stdout + r.stderr).lower()


# ── Stage 2: the weekday night that replaces the full build ───────────

def test_stage2_flags_are_off_by_default():
    """Deploying the orchestrator must change nothing until the unit asks."""
    out = _dry_run()
    assert "publish.py --step 2g" not in out
    assert "--step 5c" not in out and "--step 6" not in out


def test_structure_from_shards_uses_publish_step_2g():
    """The full builder reads output/decisions/*.jsonl, not decisions.db.

    That is the point: it is byte-for-byte today's behaviour, so it needs no
    shadow pair and no drift verdict — which matters because the
    decision_structure pair has never passed its gate.
    """
    out = _dry_run("--structure-from-shards")
    line = _line_for(out, "decision_structure")
    assert "publish.py --step 2g" in line
    assert "extract_decision_structure_incremental" not in line


def test_structure_from_shards_is_excluded_from_the_drift_check():
    """It writes the live DB directly, so it has no sibling to compare."""
    drift = _line_for(_dry_run("--structure-from-shards"), "drift_check")
    assert "--pairs reference_graph" in drift
    assert "decision_structure" not in drift.split("--pairs")[1]


def test_full_stage2_night_runs_no_drift_check_at_all():
    out = _dry_run("--in-place-graph", "--structure-from-shards", "--with-distribution")
    assert _line_for(out, "drift_check") == ""


def test_distribution_runs_the_cheap_steps_in_publish_order():
    """Order mirrors publish.py: feeds, gate, manifest, then push.

    The manifest must follow the gate so it captures the verdict, and the
    push must follow both.
    """
    out = _dry_run("--with-distribution")
    order = [s for s in ("rss_feeds", "qc_gate", "release_manifest",
                         "publish_delta", "git_push", "health_check")
             if _line_for(out, s)]
    assert order == ["rss_feeds", "qc_gate", "release_manifest",
                     "publish_delta", "git_push", "health_check"]


def test_distribution_includes_the_git_push():
    """Not optional in practice: check_output_freshness deadmans on
    docs/stats.json commit age at 36h and would page every Tuesday."""
    assert "--step 6" in _line_for(_dry_run("--with-distribution"), "git_push")


def test_distribution_excludes_the_full_parquet_and_hf_upload():
    """Those are ~3,029s of full-corpus work and belong to Sunday.
    The 17s delta publish keeps the mirror daily instead."""
    out = _dry_run("--with-distribution")
    assert _line_for(out, "export_parquet") == ""
    assert _line_for(out, "upload_hf") == ""
    assert "--step 7" in _line_for(out, "publish_delta")


def test_a_failing_qc_gate_blocks_publication_not_the_db_work():
    """publish.py puts 5c in CRITICAL_STEPS so a regression never reaches
    users. The orchestrator has to honour the same contract."""
    src = (REPO / "scripts" / "incremental_nightly.py").read_text(encoding="utf-8")
    i = src.index('("qc_gate",')
    assert "True" in src[i:i + 80], "the gate is no longer marked fatal"
    assert "skipping git push" in src, "a failing gate no longer blocks the push"


# ── Stage A prerequisites (2026-09-04 safety review) ──────────────────────

import scripts.incremental_nightly as inc  # noqa: E402


def test_late_start_guard_is_inert_without_stage2_flags():
    out = _dry_run("--in-place-graph", "--now-utc", "23:30")
    assert "late start" not in out
    assert _line_for(out, "decision_structure")            # shadow builder still runs


def test_late_start_skips_structure_and_distribution():
    """A run queued behind a late full build must not still be driving
    `publish.py --step` children (which hold the publish flock) at 03:30,
    or the daily full build exits 'already running' and the safety net is
    lost. quick_publish, the graph and stats still run."""
    out = _dry_run("--in-place-graph", "--structure-from-shards",
                   "--with-distribution", "--now-utc", "23:10")
    assert "late start" in out
    assert "SKIPPED (late start" in _line_for(out, "decision_structure")
    assert "SKIPPED (late start" in _line_for(out, "distribution")
    assert "--step 2g" not in out
    assert not _line_for(out, "qc_gate")
    assert _line_for(out, "reference_graph")
    assert _line_for(out, "generate_stats")


def test_small_hours_count_as_late_too():
    out = _dry_run("--structure-from-shards", "--now-utc", "01:15")
    assert "late start" in out


def test_on_time_start_runs_everything():
    out = _dry_run("--in-place-graph", "--structure-from-shards",
                   "--with-distribution", "--now-utc", "20:05")
    assert "late start" not in out
    assert "--step 2g" in _line_for(out, "decision_structure")
    assert _line_for(out, "qc_gate") and _line_for(out, "git_push")


def test_cutoff_is_configurable():
    out = _dry_run("--with-distribution", "--now-utc", "21:00",
                   "--latest-start-utc", "20:30")
    assert "late start" in out


def test_is_late_start_boundaries():
    assert inc._is_late_start("22:30", "22:30")
    assert inc._is_late_start("22:30", "23:59")
    assert inc._is_late_start("22:30", "00:00")
    assert inc._is_late_start("22:30", "03:29")
    assert not inc._is_late_start("22:30", "03:30")      # the full build's own slot
    assert not inc._is_late_start("22:30", "05:48")      # Step B: after the scrape
    assert not inc._is_late_start("22:30", "20:00")
    assert not inc._is_late_start("22:30", "22:29")


def test_distribution_children_never_get_the_sqlite_snapshot(monkeypatch):
    """publish.py gates the ~60 GB snapshot on the weekday; a Saturday night
    run crossing midnight would otherwise produce Sunday's artefact twice."""
    monkeypatch.setenv("OCL_PUBLISH_SQLITE_SNAPSHOT", "1")
    monkeypatch.setenv("HF_TOKEN", "x")
    env = inc._distribution_env()
    assert env["OCL_PUBLISH_SQLITE_SNAPSHOT"] == "0"
    assert env["HF_TOKEN"] == "x"                      # the rest is inherited


def test_file_identity_changes_when_a_builder_swaps_a_new_file_in(tmp_path):
    db = tmp_path / "decision_structure.db"
    db.write_bytes(b"old")
    before = inc._file_identity(db)
    assert before is not None and inc._file_identity(db) == before   # stable
    tmp = tmp_path / "decision_structure.db.tmp"
    tmp.write_bytes(b"new-and-longer")
    import os
    os.replace(tmp, db)                                # the builders' swap
    assert inc._file_identity(db) != before
    assert inc._file_identity(tmp_path / "missing.db") is None


def test_structure_failure_detection_is_in_the_from_shards_path():
    """publish.py marks 2g NON_FATAL, so `--step 2g` exits 0 on failure; the
    orchestrator must judge by the live file instead."""
    src = (REPO / "scripts" / "incremental_nightly.py").read_text(encoding="utf-8")
    i = src.index('"--step", "2g"')
    tail = src[i:i + 2500]
    assert "_file_identity(DECISION_STRUCTURE_DB) == before" in tail
    assert 'rec["exit_code"] = 1' in tail


def test_exit_codes_are_documented_in_the_unit():
    unit = (REPO / "systemd" / "opencaselaw-publish-incremental.service").read_text()
    assert "OnFailure=ntfy-alert@%n.service" in unit
    assert "ReadWritePaths=/opt/caselaw/repo /mnt /tmp" in unit
    assert "EnvironmentFile=-/opt/caselaw/repo/.env.publish" in unit
    assert "MemoryHigh=32G" in unit
    # Not flipped yet: Stage A is documented, the live ExecStart is unchanged.
    assert "ExecStart=/usr/bin/python3 /opt/caselaw/repo/scripts/incremental_nightly.py --skip-quick-publish --in-place-graph" in unit
