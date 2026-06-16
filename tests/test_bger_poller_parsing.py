"""Regression tests for the 2026-06-11 poller findings.

Three production defects, one investigation (docket 2C_252/2026 missing
while live on BGer's Neuheiten page):

1. The poller parsed only the STDOUT of run_scraper / quick_publish, but
   both log via bare ``logging.StreamHandler`` → STDERR. Consequences:
   "BGer scraper completed: 0 new decisions" on a +30 run, and
   ``inserted_count`` always 0 — so ``_maybe_update_stats`` never fired
   once in repo history (zero "Update stats.json — BGer poller" commits).

2. quick_publish was run with ``subprocess.run(timeout=900)``. The 64 GB
   decisions.db copy+insert took 619 s on 2026-06-11 under publish-tail
   I/O — no headroom, and on expiry subprocess.run SIGKILLs the child,
   bypassing quick_publish's SIGTERM cleanup (orphaned .quick copy) and
   crashing the poller with an uncaught TimeoutExpired.

3. Persistent doc-service failures (same docket failing poll after poll,
   e.g. 6B_1014/2025 + 7B_461/2025 on 2026-06-10) had only WARNING-level
   visibility; no escalation, nothing in scraper health.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import scripts.bger_poller as bp  # noqa: E402


# ── 1. stream parsing ────────────────────────────────────────────────────

REAL_DONE_LINE = (
    "2026-06-11 11:05:42,151 run_scraper INFO [bger] Done. +30 new, 96837, "
    "Errors: 0, NoneReturns: 0, Time: 5.3 min, "
    "File: output/decisions/bger.jsonl (1459.7 MB)"
)

REAL_INSERTED_LINE = (
    "2026-06-11 11:16:01,000 quick_publish INFO Inserted 30/30 new "
    "decisions (990197 total, 614.2s)"
)

REAL_LOCK_LINE = (
    "2026-06-11 05:01:00,000 quick_publish INFO Full publish.py is running "
    "(holds /tmp/opencaselaw-publish.lock); skipping quick_publish."
)


def test_scraper_count_parses_from_stderr_position():
    """The Done line arrives on stderr; the caller passes out+err
    combined — count must parse regardless of which stream held it."""
    combined = "some stdout noise\n" + REAL_DONE_LINE
    assert bp._parse_scraper_new_count(combined) == 30


def test_scraper_count_zero_when_absent():
    assert bp._parse_scraper_new_count("") == 0
    assert bp._parse_scraper_new_count("unrelated output\n") == 0


def test_quick_publish_inserted_count():
    inserted, skipped = bp._parse_quick_publish_output(
        "noise\n" + REAL_INSERTED_LINE)
    assert inserted == 30
    assert skipped is False


def test_quick_publish_lock_skip_detected():
    inserted, skipped = bp._parse_quick_publish_output(
        "noise\n" + REAL_LOCK_LINE)
    assert inserted == 0
    assert skipped is True


def test_quick_publish_empty_output():
    assert bp._parse_quick_publish_output("") == (0, False)


# ── 2. graceful timeout ──────────────────────────────────────────────────

def test_communicate_graceful_normal_completion():
    proc = subprocess.Popen(
        [sys.executable, "-c", "print('ok')"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    out, err, rc, timed_out = bp._communicate_graceful(proc, timeout_s=30)
    assert timed_out is False
    assert rc == 0
    assert "ok" in out


def test_communicate_graceful_sigterm_lets_cleanup_run():
    """On timeout the child gets SIGTERM (not SIGKILL) so its cleanup
    handler runs — exit code must be the handler's, not -9."""
    child = (
        "import signal, sys, time\n"
        "signal.signal(signal.SIGTERM, lambda *a: sys.exit(7))\n"
        "time.sleep(30)\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", child],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    time.sleep(0.3)  # let the child install its handler
    out, err, rc, timed_out = bp._communicate_graceful(
        proc, timeout_s=0.2, grace_s=10)
    assert timed_out is True
    assert rc == 7, f"expected graceful exit via SIGTERM handler, got {rc}"


def test_communicate_graceful_sigkill_after_grace():
    """A child that ignores SIGTERM is SIGKILLed after the grace period."""
    child = (
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "time.sleep(30)\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", child],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    time.sleep(0.3)
    out, err, rc, timed_out = bp._communicate_graceful(
        proc, timeout_s=0.2, grace_s=0.5)
    assert timed_out is True
    assert rc == -9


# ── 3. failure streaks ───────────────────────────────────────────────────

def test_failing_streaks_increment_and_recover():
    prev = {"A_1/2026": 2, "B_2/2026": 1}
    # A still failing, B recovered, C new failure
    nxt = bp._update_failing_streaks(prev, {"A_1/2026", "C_3/2026"})
    assert nxt == {"A_1/2026": 3, "C_3/2026": 1}


def test_failing_streaks_empty_when_all_recover():
    assert bp._update_failing_streaks({"A_1/2026": 5}, set()) == {}


def test_save_state_persists_failing(tmp_path, monkeypatch):
    import json
    monkeypatch.setattr(bp, "STATE_FILE", tmp_path / "state.json")
    bp._save_state("2026-06-11", {"X_1/2026"}, failing={"Y_2/2026": 3})
    saved = json.loads((tmp_path / "state.json").read_text())
    assert saved["failing"] == {"Y_2/2026": 3}
    assert saved["dockets"] == ["X_1/2026"]


def test_save_state_failing_defaults_empty(tmp_path, monkeypatch):
    """Existing two-arg callers (and older states) stay valid."""
    import json
    monkeypatch.setattr(bp, "STATE_FILE", tmp_path / "state.json")
    bp._save_state("2026-06-11", set())
    saved = json.loads((tmp_path / "state.json").read_text())
    assert saved["failing"] == {}


# ── G5: poller publish-window guard (build/serve I/O-coupling stall) ──────


class _FakeProc:
    def __init__(self, stdout):
        self.stdout = stdout


def _patch_systemctl(monkeypatch, state):
    monkeypatch.setattr(
        bp.subprocess, "run",
        lambda *a, **k: _FakeProc(state + "\n"))


def test_publish_running_true_when_activating(monkeypatch):
    # the publish is a oneshot unit → ActiveState=activating for its whole run;
    # `is-active --quiet` would wrongly report not-active here (the bug we avoid)
    _patch_systemctl(monkeypatch, "activating")
    assert bp._full_publish_running() is True


def test_publish_running_true_when_active(monkeypatch):
    _patch_systemctl(monkeypatch, "active")
    assert bp._full_publish_running() is True


def test_publish_running_false_when_inactive(monkeypatch):
    _patch_systemctl(monkeypatch, "inactive")
    assert bp._full_publish_running() is False


def test_publish_running_false_when_failed(monkeypatch):
    _patch_systemctl(monkeypatch, "failed")
    assert bp._full_publish_running() is False


def test_publish_running_fail_open_on_probe_error(monkeypatch):
    def boom(*a, **k):
        raise FileNotFoundError("no systemctl")
    monkeypatch.setattr(bp.subprocess, "run", boom)
    assert bp._full_publish_running() is False  # fail-open, never blocks refresh


def test_maybe_update_stats_skips_during_publish(monkeypatch):
    # publish running → return BEFORE the flock / generate_stats / git spawn
    monkeypatch.setattr(bp, "_full_publish_running", lambda: True)
    called = {"run": False}

    def fake_run(*a, **k):
        called["run"] = True
        return _FakeProc("")

    monkeypatch.setattr(bp.subprocess, "run", fake_run)
    bp._maybe_update_stats(5)          # 5 new rows, but publish active → skip
    assert called["run"] is False       # nothing spawned


def test_maybe_update_stats_zero_new_never_probes(monkeypatch):
    # the pre-existing guard fires first: 0 new rows → no publish probe at all
    def must_not_probe():
        raise AssertionError("should not probe publish state when 0 new rows")

    monkeypatch.setattr(bp, "_full_publish_running", must_not_probe)
    bp._maybe_update_stats(0)           # returns at the new_decisions<=0 check
