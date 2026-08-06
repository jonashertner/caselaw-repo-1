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


def _rec(boot, flushed_at, calls, sessions=0, clients=None, errors=0,
         substantive=None, empty=None):
    tool = {"calls": calls, "errors": errors}
    # Records written before outcome labelling shipped carry neither key.
    if substantive is not None:
        tool["substantive"] = substantive
    if empty is not None:
        tool["empty"] = empty
    return json.dumps({
        "uptime_since": boot, "flushed_at": flushed_at, "sessions": sessions,
        "tools": {"get_decision": tool},
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
    daily, dsess, dcli, dtools, derr, dsub, dempty = reconstruct(lines)
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
    dcli = reconstruct(lines)[2]
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
    # Positional, not *_: reconstruct now returns outcome counters after
    # derr, and a trailing-unpack would have silently bound the wrong one.
    derr = reconstruct(lines)[4]
    assert derr["2026-07-01"]["get_decision"] == 4


def test_outcome_counters_delta_like_calls():
    lines = [
        _rec("bootA", "2026-07-01T08:00:00", 10, substantive=8, empty=2),
        _rec("bootA", "2026-07-01T20:00:00", 30, substantive=25, empty=5),
        _rec("bootB", "2026-07-01T21:00:00", 6, substantive=4, empty=2),
    ]
    _, _, _, _, _, dsub, dempty = reconstruct(lines)
    assert dsub["2026-07-01"]["get_decision"] == 25 + 4
    assert dempty["2026-07-01"]["get_decision"] == 5 + 2


def test_records_without_outcome_keys_report_zero_labelled():
    """Days before the feature shipped must read as unlabelled, never as
    'everything answered'."""
    lines = [
        _rec("bootA", "2026-07-01T08:00:00", 10),
        _rec("bootA", "2026-07-01T20:00:00", 30),
    ]
    daily, _, _, _, _, dsub, dempty = reconstruct(lines)
    assert daily["2026-07-01"] == 30
    assert dsub["2026-07-01"]["get_decision"] == 0
    assert dempty["2026-07-01"]["get_decision"] == 0


def test_outcome_counters_clamp_on_worker_restart():
    lines = [
        _rec("bootA", "2026-07-01T08:00:00", 50, substantive=40, empty=10),
        _rec("bootA", "2026-07-02T08:00:00", 10, substantive=8, empty=2),
    ]
    _, _, _, _, _, dsub, dempty = reconstruct(lines)
    assert dsub["2026-07-02"]["get_decision"] == 0
    assert dempty["2026-07-02"]["get_decision"] == 0
