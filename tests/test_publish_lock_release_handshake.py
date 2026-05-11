"""Regression tests for the build_fts5 → publish.py swap-done handshake.

Background — 2026-05-11 incident:
  The publish lock was held for the full duration of Step 2 (build_fts5),
  including the 1–3 h post-swap PRAGMA integrity_check on the 60 GB DB.
  During that window, the BGer poller's quick_publish call observed the
  lock as held and exited silently. 13 fresh BGer dockets sat in
  bger.jsonl all day — the "Neueste Bundesgerichtsentscheide" feed went
  stale even though the scraper had succeeded.

The fix:
  build_fts5.py prints an exact-match sentinel ``OCL_SWAP_DONE`` to
  stdout immediately after os.replace() succeeds and stale-sidecar
  cleanup completes. publish.py's run_cmd accepts an ``on_line``
  callback. Step 2's dispatch passes a callback that detects the
  sentinel and releases the publish lock via fcntl.LOCK_UN — without
  waiting for build_fts5 to exit.

These tests defend the contract:
  1. The exact sentinel literal is present in build_fts5.py.
  2. publish.run_cmd accepts the on_line keyword argument and
     invokes it per stdout line.
  3. step_2_build_fts5 accepts and forwards on_line.
  4. The Step 2 dispatch site wires a lock-release callback that
     fires on the sentinel.

Each test catches a different class of regression — e.g. an over-eager
"clean up unused parameter" refactor that drops on_line, or a rename of
the sentinel that breaks the cross-process handshake.
"""
from __future__ import annotations

import inspect
import io
import re
import subprocess
import sys
import textwrap
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import publish  # noqa: E402

REPO = Path(__file__).resolve().parents[1]


def _src(path: str) -> str:
    return (REPO / path).read_text()


# ── 1. Sentinel literal lives where the parent expects it ────────────────


def test_build_fts5_emits_swap_done_sentinel():
    src = _src("build_fts5.py")
    # The literal token must appear at least once. Both sides hard-code
    # this string; a rename in one without the other silently breaks the
    # handshake (the lock release just never fires, returning the system
    # to pre-fix behaviour).
    assert "OCL_SWAP_DONE" in src, (
        "build_fts5.py must emit the 'OCL_SWAP_DONE' sentinel "
        "after os.replace() so the parent can release the publish lock"
    )
    # And it must be emitted via logger.info so it actually reaches stdout
    # (logger output is captured by publish.py's Popen pipe).
    assert re.search(r"logger\.info\([^)]*OCL_SWAP_DONE", src), (
        "OCL_SWAP_DONE must be logged via logger.info (not bare print) "
        "so publish.py's stdout-capture pipeline catches it"
    )


def test_swap_done_sentinel_is_after_os_replace():
    """The sentinel must NOT be printed before os.replace() succeeds —
    otherwise we'd release the lock with no DB to serve."""
    src = _src("build_fts5.py")
    swap_idx = src.find("os.replace(str(db_path), str(final_db_path))")
    sentinel_idx = src.find("OCL_SWAP_DONE")
    assert swap_idx != -1, "os.replace call must remain in build_fts5"
    assert sentinel_idx != -1, "sentinel literal must remain in build_fts5"
    assert sentinel_idx > swap_idx, (
        "OCL_SWAP_DONE must appear AFTER the os.replace() call — "
        "releasing the lock before the swap commits would expose a "
        "DB that hasn't been published yet"
    )


# ── 2. run_cmd grew an on_line hook and actually uses it ─────────────────


def test_run_cmd_accepts_on_line_kwarg():
    sig = inspect.signature(publish.run_cmd)
    assert "on_line" in sig.parameters, (
        "publish.run_cmd must expose on_line= callback so Step 2 can "
        "wire the lock-release hook"
    )
    # Default None preserves backward compat for every other caller.
    assert sig.parameters["on_line"].default is None


def test_run_cmd_invokes_on_line_per_stdout_line():
    """End-to-end: run_cmd streams a child's stdout line-by-line through
    on_line. We use Python's -c with a small print loop to keep the test
    fast and OS-agnostic."""
    seen: list[str] = []
    cmd = [sys.executable, "-c",
           "import sys\n"
           "print('line one'); print('OCL_SWAP_DONE marker'); "
           "print('line three')\n"
           "sys.stdout.flush()"]
    # stall_timeout=None to skip the watchdog thread for this 200 ms run.
    ok = publish.run_cmd(
        cmd, "on_line smoke", timeout=30, stall_timeout=None,
        on_line=lambda line: seen.append(line),
    )
    assert ok, "smoke command must exit 0"
    assert "line one" in seen
    assert any("OCL_SWAP_DONE" in s for s in seen), (
        "the sentinel line must reach the on_line callback verbatim"
    )
    assert "line three" in seen


def test_run_cmd_swallows_on_line_exceptions():
    """A faulty hook must NOT crash the publish — log and continue."""
    cmd = [sys.executable, "-c", "print('hello')"]
    crashed_hook = lambda line: (_ for _ in ()).throw(RuntimeError("boom"))
    ok = publish.run_cmd(cmd, "hook-fault smoke",
                        timeout=30, stall_timeout=None,
                        on_line=crashed_hook)
    assert ok, (
        "an exception inside the on_line callback must not be "
        "propagated to the caller — otherwise a buggy hook could "
        "abort the publish entirely"
    )


# ── 3. step_2_build_fts5 forwards on_line ────────────────────────────────


def test_step_2_build_fts5_accepts_on_line():
    sig = inspect.signature(publish.step_2_build_fts5)
    assert "on_line" in sig.parameters, (
        "step_2_build_fts5 must accept on_line so the dispatch site "
        "can plumb the lock-release callback all the way to run_cmd"
    )


def test_step_2_build_fts5_forwards_on_line_to_run_cmd():
    """Mock run_cmd and verify step_2_build_fts5 passes our hook through."""
    callback = lambda _: None
    with mock.patch.object(publish, "run_cmd",
                           return_value=True) as mock_run:
        # dry_run=True short-circuits disk preflight; we only care about
        # the kwargs forwarded to run_cmd.
        publish.step_2_build_fts5(dry_run=True, on_line=callback)
    # dry_run=True returns "[dry-run] skipped" via the run_cmd internals;
    # but the outer step still calls run_cmd. If the dry_run guard inside
    # step_2_build_fts5 returns before run_cmd, this test would skip —
    # exercise the non-dry path with disk preflight mocked instead.
    if not mock_run.called:
        with mock.patch.object(publish, "_preflight_disk_check",
                               return_value=True), \
             mock.patch.object(publish, "_cleanup_stale_build_artifacts"), \
             mock.patch.object(publish, "run_cmd",
                               return_value=True) as mock_run:
            publish.step_2_build_fts5(dry_run=False, on_line=callback)
    assert mock_run.called, (
        "step_2_build_fts5 must call run_cmd at least once"
    )
    kwargs = mock_run.call_args.kwargs
    assert kwargs.get("on_line") is callback, (
        f"step_2_build_fts5 must forward on_line= verbatim to run_cmd; "
        f"got {kwargs}"
    )


# ── 4. Step 2 dispatch site installs the lock-release callback ───────────


def test_publish_step_2_dispatch_wires_lock_release_callback():
    """Verify the dispatch loop at the `if num == 2:` site installs a
    callback that:
      - watches for the OCL_SWAP_DONE token
      - releases the publish lock via fcntl.LOCK_UN

    We pattern-match the source rather than executing the loop because
    the loop also runs scrapers, builds, and pushes — it's not a unit-
    testable function in isolation. A source-level check catches the
    most likely regression: an unrelated refactor of the dispatch site
    that drops the on_line wiring without realising it's load-bearing.
    """
    src = _src("publish.py")
    # 1. The dispatch site must reference OCL_SWAP_DONE.
    assert "OCL_SWAP_DONE" in src, (
        "publish.py's Step 2 dispatch must mention OCL_SWAP_DONE — "
        "otherwise it can't react to the build_fts5 handshake"
    )
    # 2. Must call fcntl.flock(..., fcntl.LOCK_UN) inside a callback that
    #    reads the sentinel.
    callback_pattern = re.compile(
        r"def\s+_release_on_swap_done.*?fcntl\.flock\([^)]*LOCK_UN\)",
        re.DOTALL,
    )
    assert callback_pattern.search(src), (
        "the Step 2 dispatch must define a _release_on_swap_done "
        "callback that calls fcntl.flock(..., LOCK_UN)"
    )
    # 3. The callback must be passed to step_2_build_fts5 via on_line.
    assert "on_line=_release_on_swap_done" in src, (
        "the Step 2 dispatch must pass _release_on_swap_done as on_line "
        "to step_2_build_fts5"
    )
