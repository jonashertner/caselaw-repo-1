"""C2 — bibliographic scholarship search: author= filter, sort=year, browse-without-query.

Before, search_scholarship required a topic query and ranked only by BM25, so "all
works by Müller" or "newest scholarship on X" weren't expressible. This verifies the
new author filter, the year sort, and the filter-only browse path.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402

_ROWS = [
    # id, pub_id, source, pub_type, title, authors, lang, year, journal, doi, url, pdf, license, body
    (1, "p1", "sui_generis", "article", "Verjaehrung im OR", "Mueller, Hans", "de", 2020, "sui generis", None, "http://a", None, "CC-BY-4.0", "Text ueber Verjaehrung"),
    (2, "p2", "sui_generis", "article", "Verjaehrung neu", "Mueller, Hans", "de", 2024, "sui generis", None, "http://b", None, "CC-BY-4.0", "Neuer Text ueber Verjaehrung"),
    (3, "p3", "zora_law", "dissertation", "Haftung", "Weber, Anna", "de", 2022, None, None, "http://c", None, "CC-BY-4.0", "Text ueber Haftung"),
]


def _fixture_conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(
        """
        CREATE TABLE publications (
            id INTEGER PRIMARY KEY, pub_id TEXT, source TEXT, pub_type TEXT,
            title TEXT, authors TEXT, language TEXT, year INTEGER, journal TEXT,
            doi TEXT, url TEXT, pdf_url TEXT, license TEXT
        );
        CREATE VIRTUAL TABLE publications_fts USING fts5(title, authors, body);
        """
    )
    for r in _ROWS:
        c.execute(
            "INSERT INTO publications (id,pub_id,source,pub_type,title,authors,language,"
            "year,journal,doi,url,pdf_url,license) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            r[:13],
        )
        c.execute(
            "INSERT INTO publications_fts (rowid,title,authors,body) VALUES (?,?,?,?)",
            (r[0], r[4], r[5], r[13]),
        )
    c.commit()
    return c


def test_author_browse_without_query(monkeypatch):
    monkeypatch.setattr(mcp_server, "_get_scholarship_conn", _fixture_conn)
    res = mcp_server.search_scholarship(query="", author="Mueller")
    assert "error" not in res, res
    ids = [r["pub_id"] for r in res["results"]]
    assert ids == ["p2", "p1"], res        # both Mueller works, newest (2024) first
    assert "p3" not in ids                  # Weber excluded


def test_sort_year_on_topic_query(monkeypatch):
    monkeypatch.setattr(mcp_server, "_get_scholarship_conn", _fixture_conn)
    res = mcp_server.search_scholarship(query="Verjaehrung", sort="year")
    ids = [r["pub_id"] for r in res["results"]]
    assert ids == ["p2", "p1"], res        # both match, newest first (not BM25 order)


def test_author_filter_narrows_topic(monkeypatch):
    monkeypatch.setattr(mcp_server, "_get_scholarship_conn", _fixture_conn)
    res = mcp_server.search_scholarship(query="Verjaehrung", author="Weber")
    assert res["count"] == 0, res          # Weber has no Verjaehrung work


def test_empty_query_no_filter_returns_empty(monkeypatch):
    monkeypatch.setattr(mcp_server, "_get_scholarship_conn", _fixture_conn)
    res = mcp_server.search_scholarship(query="")
    assert res["count"] == 0, res
