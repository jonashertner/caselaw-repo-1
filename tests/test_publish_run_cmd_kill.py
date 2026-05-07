"""Tests for publish.run_cmd's wall-clock + stall watchdog kill behavior.

Regression test for the 2026-05-07 incident: Step 2's wall-clock timer
fired at 25200s but the build_fts5 subprocess kept running for another
3h 20m. The old ``proc.kill()`` only signalled the immediate child;
this suite verifies that the rewrite (process-group SIGTERM → 5s grace →
SIGKILL via ``os.killpg``) actually kills the whole subprocess tree.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import publish  # noqa: E402


def test_wall_clock_timeout_actually_kills_immediate_child(tmp_path: Path) -> None:
    """A subprocess that would otherwise sleep 30s must be killed
    within ~5s of the wall-clock timer firing. If the kill misfires,
    this test hangs (and pytest's per-test cap catches it)."""
    cmd = [sys.executable, "-c", "import time; time.sleep(30); print('SHOULD NOT RUN')"]
    t0 = time.monotonic()
    ok = publish.run_cmd(cmd, "sleep30", timeout=2, stall_timeout=None)
    elapsed = time.monotonic() - t0
    # The wall_timer fires at t=2s; SIGTERM sent immediately; 5s grace;
    # SIGKILL after that. Python sleep is interruptible, so SIGTERM kills
    # within ~0.1s. Total ≤ ~3s in practice.
    assert ok is False, "wall-clock timeout must mark the run as failed"
    assert elapsed < 8, f"kill took too long: {elapsed:.1f}s (sleep was 30s)"


def test_wall_clock_timeout_kills_the_whole_process_group(tmp_path: Path) -> None:
    """Spawn a parent that forks a child sleeping 60s. The parent
    exits immediately. The OLD code (``proc.kill()`` on the parent)
    would NOT kill the child. The NEW code uses ``os.killpg`` so the
    child also dies. We verify by making the child write to a file
    after sleeping; if the child survives, the file gets written.
    """
    marker = tmp_path / "child_was_alive.txt"
    # Parent forks a long-sleeping grandchild that writes the marker.
    # If we successfully kill the whole process group, the grandchild
    # never gets to write the marker.
    parent_script = (
        "import os, sys, time\n"
        "pid = os.fork()\n"
        "if pid == 0:\n"
        # grandchild
        f"    time.sleep(20)\n"
        f"    open({str(marker)!r}, 'w').write('alive')\n"
        "    sys.exit(0)\n"
        "else:\n"
        # parent: signal we're set up, then loop
        "    print('parent_alive', flush=True)\n"
        "    while True: time.sleep(60)\n"
    )
    cmd = [sys.executable, "-c", parent_script]

    t0 = time.monotonic()
    ok = publish.run_cmd(cmd, "fork-test", timeout=2, stall_timeout=None)
    elapsed = time.monotonic() - t0
    assert ok is False
    assert elapsed < 10, f"kill took too long: {elapsed:.1f}s"

    # Wait past the grandchild's 20s sleep — if the kill was correct,
    # the marker file should NEVER exist.
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        time.sleep(1)
    assert not marker.exists(), (
        "grandchild survived parent kill — process-group kill is not working"
    )


def test_stall_watchdog_kills_the_process(tmp_path: Path) -> None:
    """A subprocess that blocks silently must be killed by the stall
    watchdog (the post-mortem class). Same group-kill semantics."""
    cmd = [sys.executable, "-c", "import time; time.sleep(30)"]
    t0 = time.monotonic()
    ok = publish.run_cmd(
        cmd, "stall-test",
        timeout=120,        # well past sleep
        stall_timeout=2,    # but stall fires at 2s of idle
    )
    elapsed = time.monotonic() - t0
    assert ok is False
    # Stall watchdog polls every 5s minimum; allow up to ~10s.
    assert elapsed < 15, f"stall kill too slow: {elapsed:.1f}s"


def test_clean_exit_returns_true(tmp_path: Path) -> None:
    """A normally-exiting subprocess returns True — the new kill
    machinery must not interfere with non-timeout paths."""
    cmd = [sys.executable, "-c", "print('hello'); import sys; sys.exit(0)"]
    ok = publish.run_cmd(cmd, "clean-exit", timeout=10, stall_timeout=None)
    assert ok is True


def test_nonzero_exit_returns_false(tmp_path: Path) -> None:
    """Non-zero exit code surfaces as False (not as timeout)."""
    cmd = [sys.executable, "-c", "import sys; sys.exit(7)"]
    ok = publish.run_cmd(cmd, "fail-exit", timeout=10, stall_timeout=None)
    assert ok is False


def test_subprocess_started_in_new_session() -> None:
    """The Popen flag start_new_session=True is what enables os.killpg
    semantics. Pin it via a module-level smoke check so a future
    refactor can't quietly drop it.
    """
    import inspect
    src = inspect.getsource(publish.run_cmd)
    assert "start_new_session=True" in src, (
        "run_cmd must launch the child in a new process group "
        "for os.killpg to target it"
    )
    assert "os.killpg" in src, (
        "run_cmd must use os.killpg, not proc.kill, to kill the whole tree"
    )
