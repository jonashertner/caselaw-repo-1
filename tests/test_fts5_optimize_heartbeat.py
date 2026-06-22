"""Test the FTS5-optimize heartbeat wrapper (build_fts5._fts5_optimize_with_heartbeat).

The real optimize is one blocking SQLite call that runs ~4h silently and was false-killed by
the publish stall-watchdog. The wrapper must emit periodic stdout heartbeats so the watchdog's
no-output timer keeps resetting — validated here with a stubbed, fast 'optimize'.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from build_fts5 import _fts5_optimize_with_heartbeat  # noqa: E402


class _FakeConn:
    def __init__(self):
        self.executed = []
        self.committed = 0

    def execute(self, sql):
        self.executed.append(sql)
        time.sleep(0.45)  # simulate a long, silent optimize

    def commit(self):
        self.committed += 1


def test_heartbeat_emits_during_optimize(capsys):
    conn = _FakeConn()
    _fts5_optimize_with_heartbeat(conn, interval=0.1)
    out = capsys.readouterr().out

    # the real optimize ran exactly once + was committed
    assert len(conn.executed) == 1 and "optimize" in conn.executed[0].lower()
    assert conn.committed == 1
    # and the watchdog-feeding heartbeat fired repeatedly during the 0.45s run
    assert out.count("heartbeat") >= 2


def test_heartbeat_stops_after_optimize(capsys):
    # a fast optimize should produce no heartbeat (it finishes before the first interval)
    conn = _FakeConn()
    conn.execute = lambda sql: None  # instant
    _fts5_optimize_with_heartbeat(conn, interval=0.2)
    time.sleep(0.3)  # past one interval — the daemon must have stopped, not still printing
    assert "heartbeat" not in capsys.readouterr().out
