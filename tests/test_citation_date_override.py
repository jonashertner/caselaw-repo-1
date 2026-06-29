"""Audit v5: find_citations dates come from the citation graph's own (stale)
decisions copy. _override_citation_dates replaces them with the corrected FTS
decisions.db dates; degrades gracefully if the lookup fails.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _fts(path):
    c = sqlite3.connect(path); c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE decisions(decision_id TEXT PRIMARY KEY, decision_date TEXT)")
    c.execute("INSERT INTO decisions VALUES('bge_152 II 1','2025-09-27')")  # corrected
    c.execute("INSERT INTO decisions VALUES('bger_2C_597_2022','2023-04-11')")
    c.commit()
    return c


def test_override_replaces_stale_graph_date(tmp_path, monkeypatch):
    conn = _fts(tmp_path / "d.db")
    monkeypatch.setattr(m, "get_db", lambda: conn)
    items = [
        {"source_decision_id": "bge_152 II 1", "decision_date": "2026-01-01"},   # stale graph date
        {"source_decision_id": "bger_2C_597_2022", "decision_date": "2023-01-01"},
    ]
    out = m._override_citation_dates(items, "source_decision_id")
    assert out[0]["decision_date"] == "2025-09-27"   # overridden with corrected FTS date
    assert out[1]["decision_date"] == "2023-04-11"


def test_unknown_id_keeps_graph_date(tmp_path, monkeypatch):
    conn = _fts(tmp_path / "d.db")
    monkeypatch.setattr(m, "get_db", lambda: conn)
    items = [{"target_decision_id": "bge_not_in_fts", "decision_date": "2026-01-01"}]
    out = m._override_citation_dates(items, "target_decision_id")
    assert out[0]["decision_date"] == "2026-01-01"   # untouched (not in FTS)


def test_graceful_when_db_unavailable(monkeypatch):
    def boom():
        raise sqlite3.Error("no db")
    monkeypatch.setattr(m, "get_db", boom)
    items = [{"source_decision_id": "x", "decision_date": "2026-01-01"}]
    assert m._override_citation_dates(items, "source_decision_id")[0]["decision_date"] == "2026-01-01"


def test_empty_items():
    assert m._override_citation_dates([], "target_decision_id") == []
    assert m._override_citation_dates(None, "target_decision_id") is None
