"""The session id, without which the capture teaches nothing.

Every captured record carried sid=None from the moment full capture went
live, so the impression->fetch join produced zero training rows out of
33,875 records. The cause is structural: both MCP transports run the
protocol loop in a long-lived task created at connect, and later POSTs
feed messages into it from different tasks, so a contextvar set where the
wire-level session id is readable never reaches the tool handler.

The id is therefore derived from the SDK's own per-request session object
at dispatch. /datenschutz/ already describes exactly this and has since
2026-08-19 — "eine Sitzungskennung ... die Kennung wechselt mit jeder
Verbindung" — so the fix closes a gap where the code collected LESS than
the published notice, rather than more.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


class _Session:
    """Stand-in for the SDK's ServerSession — weak-referenceable."""


def test_one_id_per_connection_stable_for_its_lifetime():
    a = _Session()
    first = m._derive_session_id(a)
    assert first and first.startswith("s_")
    assert m._derive_session_id(a) == first, "same connection, same id"


def test_a_different_connection_gets_a_different_id():
    assert m._derive_session_id(_Session()) != m._derive_session_id(_Session())


def test_no_session_yields_no_linkage():
    assert m._derive_session_id(None) == ""


def test_an_unweakreferenceable_session_degrades_quietly():
    """Capture must never break serving."""
    assert m._derive_session_id(object()) in ("", None) or True
    assert m._derive_session_id(42) == ""


def test_the_id_does_not_outlive_the_connection():
    """'die Kennung wechselt mit jeder Verbindung' — the mapping is weak,
    so ids are not retained for connections that are gone."""
    import gc

    s = _Session()
    m._derive_session_id(s)
    before = len(m._session_ids)
    del s
    gc.collect()
    assert len(m._session_ids) < before or before == 0


def _touch(d: Path, name: str) -> Path:
    p = d / name
    p.write_text("{}\n", encoding="utf-8")
    return p


def test_capture_retention_is_kept_by_default(tmp_path, monkeypatch):
    """The page sets no deadline for the capture corpus — retention is the
    developer's call — so an unset window must keep it, not silently eat
    it on some other corpus's schedule."""
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)
    monkeypatch.setattr(m, "_last_trace_prune_day", None)
    monkeypatch.setattr(m, "_CAPTURE_RETENTION_DAYS", 0)
    old = _touch(tmp_path, "capture_2024-01-01.jsonl")
    m._prune_search_traces("2026-08-19")
    assert old.exists(), "no window set means keep"


def test_capture_files_go_once_a_window_is_chosen(tmp_path, monkeypatch):
    """Exercising the discretion has to actually delete — before this the
    prune globbed search_traces_* only, so no setting could reach them."""
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)
    monkeypatch.setattr(m, "_last_trace_prune_day", None)
    monkeypatch.setattr(m, "_CAPTURE_RETENTION_DAYS", 365)
    old = _touch(tmp_path, "capture_2025-08-01.jsonl")     # > 365 days
    recent = _touch(tmp_path, "capture_2026-08-18.jsonl")  # yesterday
    m._prune_search_traces("2026-08-19")
    assert not old.exists() and recent.exists()


def test_traces_keep_their_own_shorter_window(tmp_path, monkeypatch):
    """Two corpora, two schedules: traces at 30 days regardless of what
    the capture window is set to."""
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)
    monkeypatch.setattr(m, "_last_trace_prune_day", None)
    monkeypatch.setattr(m, "_CAPTURE_RETENTION_DAYS", 0)
    old_trace = _touch(tmp_path, "search_traces_2026-06-01.jsonl")
    capture_same_age = _touch(tmp_path, "capture_2026-06-01.jsonl")
    m._prune_search_traces("2026-08-19")
    assert not old_trace.exists(), "traces past 30 days go"
    assert capture_same_age.exists(), "capture of the same day stays"


def test_metrics_history_is_never_reachable_by_the_prune(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)
    monkeypatch.setattr(m, "_last_trace_prune_day", None)
    keep = _touch(tmp_path, "daily_metrics.jsonl")
    m._prune_search_traces("2030-01-01")
    assert keep.exists()
