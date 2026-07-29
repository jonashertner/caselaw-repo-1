"""Reconstruction of per-day usage from cumulative flush records.

The bug this guards against is subtle enough that it fooled an ops review:
flush records carry counters that are cumulative PER WORKER BOOT, so neither
summing all records (massive overcount) nor reading the last one (per-boot
undercount) yields daily traffic. The delta-per-boot reconstruction in
scripts/metrics_report.py is the correct reading; these tests pin it with a
hand-computed fixture.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.metrics_report import reconstruct  # noqa: E402


def _rec(boot, flushed_at, calls, sessions=0, clients=None, errors=0):
    return json.dumps({
        "uptime_since": boot, "flushed_at": flushed_at, "sessions": sessions,
        "tools": {"get_decision": {"calls": calls, "errors": errors}},
        "clients": clients or {},
    })


def test_cumulative_counters_become_daily_deltas():
    lines = [
        # boot A: day1 grows 10 -> 25, day2 grows to 40
        _rec("bootA", "2026-07-01T08:00:00", 10, sessions=3),
        _rec("bootA", "2026-07-01T20:00:00", 25, sessions=7),
        _rec("bootA", "2026-07-02T10:00:00", 40, sessions=9),
        # boot B: restarted on day2, counters reset, grow to 30
        _rec("bootB", "2026-07-02T12:00:00", 12, sessions=2),
        _rec("bootB", "2026-07-02T23:50:00", 30, sessions=5),
    ]
    daily, dsess, dcli, dtools, derr = reconstruct(lines)
    assert daily["2026-07-01"] == 25          # last flush of the day, not the sum
    assert daily["2026-07-02"] == (40 - 25) + 30   # boot A delta + boot B total
    assert dsess["2026-07-01"] == 7
    assert dsess["2026-07-02"] == 2 + 5
    assert dtools["2026-07-02"]["get_decision"] == 45


def test_summing_records_would_overcount():
    """The naive reading (sum every record) is 4.7x the truth on this fixture."""
    lines = [
        _rec("bootA", "2026-07-01T08:00:00", 10),
        _rec("bootA", "2026-07-01T12:00:00", 20),
        _rec("bootA", "2026-07-01T23:00:00", 30),
    ]
    daily, *_ = reconstruct(lines)
    assert daily["2026-07-01"] == 30
    naive = 10 + 20 + 30
    assert naive == 60 and daily["2026-07-01"] != naive


def test_client_mix_deltas_per_boot():
    lines = [
        _rec("bootA", "2026-07-01T08:00:00", 5, clients={"claude.ai": 4}),
        _rec("bootA", "2026-07-01T20:00:00", 9, clients={"claude.ai": 6, "chatgpt": 2}),
        _rec("bootB", "2026-07-01T21:00:00", 3, clients={"claude.ai": 3}),
    ]
    _, _, dcli, _, _ = reconstruct(lines)
    assert dcli["2026-07-01"]["claude.ai"] == 6 + 3
    assert dcli["2026-07-01"]["chatgpt"] == 2


def test_counter_reset_within_a_boot_never_goes_negative():
    """Clock skew or a partial write must clamp at 0, not subtract."""
    lines = [
        _rec("bootA", "2026-07-01T08:00:00", 50),
        _rec("bootA", "2026-07-02T08:00:00", 10),   # shrank: impossible, clamp
    ]
    daily, *_ = reconstruct(lines)
    assert daily["2026-07-01"] == 50
    assert daily["2026-07-02"] == 0


def test_garbage_lines_are_skipped():
    lines = ["not json", '{"flushed_at": null}', '[]',
             _rec("bootA", "2026-07-01T08:00:00", 7)]
    daily, *_ = reconstruct(lines)
    assert daily["2026-07-01"] == 7 and len(daily) == 1


def test_errors_aggregate_like_calls():
    lines = [
        _rec("bootA", "2026-07-01T08:00:00", 10, errors=1),
        _rec("bootA", "2026-07-01T20:00:00", 30, errors=4),
    ]
    *_, derr = reconstruct(lines)
    assert derr["2026-07-01"]["get_decision"] == 4
