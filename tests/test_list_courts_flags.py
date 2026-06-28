"""L-2 / L-4 (2026-06-28 audit): list_courts must flag near-empty collections
(<15 decisions — a null result there proves nothing) and an unvalidated future
`Latest` (the M-1 cantonal date-parse bug surfacing as a headline figure).
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _conn(path):
    future = (datetime.now() + timedelta(days=40)).date().isoformat()
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE decisions(decision_id TEXT, court TEXT, canton TEXT, "
              "decision_date TEXT, language TEXT)")
    # a big, healthy court
    for i in range(20):
        c.execute("INSERT INTO decisions VALUES(?,?,?,?,?)", (f"big_{i}", "bger", "CH", "2024-01-15", "de"))
    # a sparse court (1 decision)
    c.execute("INSERT INTO decisions VALUES('z1','zh_mietgericht','ZH','2023-06-01','de')")
    # a court with a future Latest
    c.execute("INSERT INTO decisions VALUES('f1','fr_gerichte','FR',?,'fr')", (future,))
    c.commit()
    c.row_factory = sqlite3.Row
    return c


def test_list_courts_flags(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "get_db", lambda: _conn(tmp_path / "d.db"))
    m._cache_clear()  # the function memoises; clear so the fixture is used
    try:
        rows = {r["court"]: r for r in m.list_courts()}
    finally:
        m._cache_clear()
    assert rows["bger"]["sparse"] is False
    assert rows["zh_mietgericht"]["sparse"] is True
    assert rows["fr_gerichte"].get("latest_unvalidated") is True
    assert rows["bger"].get("latest_unvalidated") is None
