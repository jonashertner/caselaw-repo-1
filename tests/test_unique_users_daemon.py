"""ocl-uniq daemon: sketch accuracy, UA classing, window rotation.

The privacy contract under test: nothing member-identifying is ever
persisted (registers only), salts are per-window files that rotate, and a
finalized day record carries per-class uniques whose error stays within
HLL tolerance (~1% at p=14).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts import unique_users_daemon as uq  # noqa: E402


@pytest.fixture()
def state(tmp_path, monkeypatch):
    monkeypatch.setattr(uq, "STATE_DIR", tmp_path / "state")
    monkeypatch.setattr(uq, "OUT_PATH", tmp_path / "out" / "unique_users.jsonl")
    return tmp_path


def test_hll_estimate_within_tolerance(state):
    h = uq.HLL()
    salt = b"s" * 32
    n = 10_000
    for i in range(n):
        h.add_hash(uq.salted_h64(salt, f"10.{i >> 16}.{(i >> 8) & 255}.{i & 255}"))
    est = h.estimate()
    assert abs(est - n) / n < 0.03, est


def test_duplicates_do_not_inflate(state):
    h = uq.HLL()
    salt = b"s" * 32
    for _ in range(50):
        for i in range(100):
            h.add_hash(uq.salted_h64(salt, f"192.168.1.{i}"))
    assert abs(h.estimate() - 100) <= 5


def test_classify_ua():
    assert uq.classify_ua("Mozilla/5.0 (Macintosh…)") == "browser"
    assert uq.classify_ua("python-httpx/0.27") == "script"
    assert uq.classify_ua("claude-code/2.1") == "claude-code"
    assert uq.classify_ua("Claude-User/1.0 (+claude.ai)") == "anthropic-egress"
    assert uq.classify_ua("ChatGPT-User/2.0") == "openai-egress"
    assert uq.classify_ua("") == "other"


def test_parse_line_strips_syslog_prefix():
    raw = "<190>Aug  2 18:00:00 host ocluniq: 203.0.113.9|python-requests/2.32"
    assert uq.parse_line(raw) == ("203.0.113.9", "python-requests/2.32")
    assert uq.parse_line("garbage without pipe") is None
    assert uq.parse_line("ocluniq: 2001:db8::1|curl/8.0") == ("2001:db8::1", "curl/8.0")


def test_day_rollover_finalizes_and_resets(state):
    t = [1754130000.0]                      # 2026-08-02 ~10:20 UTC
    win = uq.Windows(now=lambda: t[0])
    for i in range(50):
        assert win.observe(f"10.0.0.{i}", "curl/8") is None
    assert abs(win.snapshot()["uniques"]["total"] - 50) <= 2   # sketch tolerance
    t[0] += 86_400                          # next UTC day
    rec = win.observe("10.9.9.9", "curl/8")
    assert rec is not None and rec["final"] is True
    assert abs(rec["uniques"]["total"] - 50) <= 2
    assert abs(rec["uniques"]["script"] - 50) <= 2
    # new day counts fresh; month-to-date keeps accumulating
    assert win.snapshot()["uniques"]["total"] == 1
    assert win.snapshot()["month_to_date"]["total"] >= 48


def test_checkpoint_roundtrip_preserves_counts(state):
    t = [1754130000.0]
    win = uq.Windows(now=lambda: t[0])
    for i in range(200):
        win.observe(f"10.1.{i >> 8}.{i & 255}", "Mozilla/5.0")
    win.save_state()
    win2 = uq.Windows(now=lambda: t[0])
    assert win2.load_state() is True
    assert win2.snapshot()["uniques"]["total"] == win.snapshot()["uniques"]["total"]
    assert "resumed-from-checkpoint" in win2.snapshot()["flags"]


def test_state_files_contain_no_membership_data(state):
    win = uq.Windows()
    win.observe("198.51.100.77", "curl/8")
    win.save_state()
    blob = (uq.STATE_DIR / "state.json").read_text()
    assert "198.51.100.77" not in blob
    rec = json.dumps(win.snapshot())
    assert "198.51.100.77" not in rec
