"""A QC-gate timeout must degrade distribution, never veto it.

The gate runs AFTER the atomic swap: it guards the HF upload and git
pushes, not what users are served. Yet on 08-18 and 08-21 the gate was
killed at exactly its 3600s wall-clock cap — no verdict either night —
and both runs cascade-skipped Step 4 and Step 6, losing two full
distribution cycles to slow scans that found nothing.

The contract pinned here: timeout/stall → a truthy "WARN (…)" outcome
(no cascade, alerted, recorded in non_fatal_failures); a genuine
CRITICAL verdict (exit 1) → False, which still blocks everything it
blocked before. Offline; subprocesses are stubs or tiny sleeps.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import publish  # noqa: E402


# ── run_cmd tells its caller HOW a command failed ────────────────────────────

def test_sink_on_success():
    sink: dict = {}
    ok = publish.run_cmd([sys.executable, "-c", "pass"], "t",
                         outcome_sink=sink, stall_timeout=None)
    assert ok is True
    assert sink == {"timed_out": False, "stalled": False, "returncode": 0}


def test_sink_on_nonzero_exit():
    sink: dict = {}
    ok = publish.run_cmd([sys.executable, "-c", "raise SystemExit(3)"], "t",
                         outcome_sink=sink, stall_timeout=None)
    assert ok is False
    assert sink["timed_out"] is False and sink["returncode"] == 3


def test_sink_on_wall_clock_timeout():
    sink: dict = {}
    ok = publish.run_cmd(
        [sys.executable, "-c", "import time; time.sleep(30)"], "t",
        timeout=1, stall_timeout=None, outcome_sink=sink)
    assert ok is False
    assert sink["timed_out"] is True


def test_no_sink_still_works():
    assert publish.run_cmd([sys.executable, "-c", "pass"], "t",
                           stall_timeout=None) is True


# ── the gate's three-way outcome ─────────────────────────────────────────────

def _run_gate(monkeypatch, *, ok: bool, timed_out=False, stalled=False):
    notes: list[tuple] = []

    def fake_run_cmd(cmd, desc, dry_run=False, outcome_sink=None, **kw):
        if outcome_sink is not None:
            outcome_sink.update(
                {"timed_out": timed_out, "stalled": stalled,
                 "returncode": 0 if ok else 1})
        return ok

    monkeypatch.setattr(publish, "run_cmd", fake_run_cmd)
    monkeypatch.setattr(publish, "_notify",
                        lambda title, body, **kw: notes.append((title, body)))
    return publish.step_5c_quality_gate(dry_run=False), notes


def test_pass_is_unchanged(monkeypatch):
    out, notes = _run_gate(monkeypatch, ok=True)
    assert out is True and notes == []


def test_timeout_degrades_instead_of_blocking(monkeypatch):
    out, notes = _run_gate(monkeypatch, ok=False, timed_out=True)
    assert isinstance(out, str) and out.startswith("WARN")
    assert "timed out" in out
    assert notes and "DEGRADED" in notes[0][0]


def test_stall_degrades_too(monkeypatch):
    out, _ = _run_gate(monkeypatch, ok=False, stalled=True)
    assert isinstance(out, str) and "stalled" in out


def test_a_real_critical_verdict_still_blocks(monkeypatch):
    """Exit 1 without a timeout is the gate SAYING NO — that must stay a
    hard False, or the gate stops being a gate."""
    out, notes = _run_gate(monkeypatch, ok=False)
    assert out is False
    assert notes and "BLOCKED" in notes[0][0]


def test_warn_outcome_does_not_trip_the_cascade():
    """The cascade condition is `results.get(s) is False`; the WARN string
    must be invisible to it and visible to the summary as itself."""
    results = {"5c": "WARN (gate timed out — distribution proceeded, verdict unknown)"}
    assert not any(results.get(s) is False for s in {2, 3, "5c"})
    assert results["5c"]                      # truthy — guarded steps proceed


# ── homepage sync is constitutionally incapable of failing the publish ───────

def test_sync_helper_survives_a_crashing_subprocess(monkeypatch):
    def boom(*a, **kw):
        raise OSError("no python today")
    monkeypatch.setattr(publish.subprocess, "run", boom)
    assert publish._sync_homepage_fallbacks(dry_run=False) is None


def test_sync_helper_survives_a_missing_script(monkeypatch, tmp_path):
    monkeypatch.setattr(publish, "REPO_DIR", tmp_path)
    assert publish._sync_homepage_fallbacks(dry_run=False) is None


def test_sync_helper_dry_run_spawns_nothing(monkeypatch):
    def boom(*a, **kw):
        raise AssertionError("dry-run must not spawn a subprocess")
    monkeypatch.setattr(publish.subprocess, "run", boom)
    assert publish._sync_homepage_fallbacks(dry_run=True) is None


def test_step_6_pushes_the_homepage(monkeypatch):
    """docs/index.html must be in the push set, or the nightly sync would
    rewrite a file that never ships."""
    src = Path(publish.__file__).read_text(encoding="utf-8")
    import re
    m = re.search(r'paths = \[(.*?)\]', src, re.S)
    assert m and '"docs/index.html"' in m.group(1)
