"""Regression test for B1 — _search_practice must sanitize FTS5 input (invariant #3).

Before the fix, _search_practice bound the raw query into `practice_fts MATCH ?`, so a
query containing an apostrophe / hyphen / trailing `Art.` / unbalanced quote raised
sqlite3.OperationalError and returned {"error": "fts5_query_error"} instead of results —
the exact failure mode every sibling FTS handler (search_commentaries, search_botschaft,
search_scholarship, …) guards against by routing the query through _sanitize_fts5 first.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402


def _fixture_conn():
    """A minimal in-memory practice DB with the columns _search_practice reads."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(
        """
        CREATE TABLE practice (
            doc_id TEXT, source TEXT, issuing_authority TEXT, doc_type TEXT,
            doc_number TEXT, title TEXT, date TEXT, language TEXT,
            url TEXT, pdf_url TEXT, body_text TEXT, topics_json TEXT, scraped_at TEXT
        );
        CREATE VIRTUAL TABLE practice_fts USING fts5(title, doc_number, body_text);
        """
    )
    c.execute(
        "INSERT INTO practice (rowid, doc_id, source, issuing_authority, doc_type, "
        "doc_number, title, date, language, url, pdf_url, body_text) VALUES "
        "(1,'d1','bazg','BAZG','weisung','30','Praxis von Mueller','2026-01-01','de',"
        "'http://x','http://x.pdf','Text ueber Art. 30 und die Praxis von Mueller')"
    )
    c.execute(
        "INSERT INTO practice_fts (rowid, title, doc_number, body_text) VALUES "
        "(1,'Praxis von Mueller','30','Text ueber Art. 30 und die Praxis von Mueller')"
    )
    c.commit()
    return c


def test_special_chars_do_not_error(monkeypatch):
    # unbalanced quote + trailing 'Art.' dot — raises OperationalError on the raw query
    monkeypatch.setattr(mcp_server, "_open_practice_db", _fixture_conn)
    res = mcp_server._search_practice(query='Praxis Mueller "Art. 30')
    assert res.get("error") != "fts5_query_error", res
    assert "error" not in res, res
    assert res["total"] >= 1, res  # sanitized terms still match the row


def test_all_special_returns_empty_not_error(monkeypatch):
    monkeypatch.setattr(mcp_server, "_open_practice_db", _fixture_conn)
    res = mcp_server._search_practice(query="'\"-:")
    assert "error" not in res, res
    assert res["total"] == 0, res


def test_normal_query_still_works(monkeypatch):
    monkeypatch.setattr(mcp_server, "_open_practice_db", _fixture_conn)
    res = mcp_server._search_practice(query="Praxis")
    assert "error" not in res, res
    assert res["total"] >= 1, res
