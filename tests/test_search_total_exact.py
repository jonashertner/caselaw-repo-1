"""`total` on a text search is the EXACT number of matching decisions, not the
reranked candidate-pool size, and it does not depend on `limit`.

Pre-fix, total was len(candidate_pool): it under-reported broad queries and
scaled with `limit` (an integrator hit this 2026-06-18, getting the full corpus
back as `total` regardless of the term). Regression guard for the exact-count
fix in search_fts5 (`_exact_fts_total` + the `max(exact, pool)` return).
"""
import sqlite3

import pytest

import mcp_server


@pytest.fixture(autouse=True)
def _clear_count_cache():
    # In-memory test DBs all report user_version=0, so the (generation-keyed)
    # count cache would collide across tests; clear it. (In production each
    # nightly build stamps a fresh user_version, so the cache self-invalidates.)
    mcp_server._FTS_TOTAL_CACHE.clear()
    yield
    mcp_server._FTS_TOTAL_CACHE.clear()


def _conn_with_n_matches(n):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        "CREATE VIRTUAL TABLE decisions_fts USING fts5("
        "decision_id, court, canton, docket_number, language, title, regeste, full_text);"
        "CREATE TABLE decisions (decision_id TEXT, court TEXT, canton TEXT, chamber TEXT,"
        " docket_number TEXT, decision_date TEXT, language TEXT, title TEXT, regeste TEXT,"
        " full_text TEXT, source_url TEXT, pdf_url TEXT);"
    )
    for i in range(1, n + 1):
        conn.execute(
            "INSERT INTO decisions_fts (rowid, decision_id, court, canton, docket_number,"
            " language, title, regeste, full_text)"
            " VALUES (?,?,'bger','CH',?,'de','Notwehr','r','Notwehr Putativnotwehr')",
            (i, f"bger_{i}", f"1A_{i}/2020"),
        )
        conn.execute(
            "INSERT INTO decisions (rowid, decision_id, court, canton, chamber, docket_number,"
            " decision_date, language, title, regeste, full_text, source_url, pdf_url)"
            " VALUES (?,?,'bger','CH','',?,'2020-01-01','de','Notwehr','r','Notwehr Putativnotwehr',"
            "'http://x','http://x.pdf')",
            (i, f"bger_{i}", f"1A_{i}/2020"),
        )
    conn.commit()
    return conn


def _wire(monkeypatch):
    monkeypatch.setattr(mcp_server, "_analyze_query",
                        lambda q, d: ([{"query": "Notwehr", "name": "nl_and", "weight": 1.0}], [], {}))
    monkeypatch.setattr(mcp_server, "_load_graph_signal_map", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_search_vectors_chunks", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_search_statute_graph", lambda *a, **k: [])
    monkeypatch.setattr(mcp_server, "_past_deadline", lambda d: False)
    monkeypatch.setattr(mcp_server, "_search_vectors", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_search_sparse", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_rerank_rows", lambda rows, *a, **k: [dict(r) for r in rows])


def test_exact_fts_total_matches_direct_count():
    conn = _conn_with_n_matches(7)
    assert mcp_server._exact_fts_total(conn, "Notwehr", "", []) == 7
    assert mcp_server._exact_fts_total(conn, "Notwehr", " AND d.court = ?", ["bger"]) == 7
    assert mcp_server._exact_fts_total(conn, "Notwehr", " AND d.court = ?", ["bvger"]) == 0
    assert mcp_server._exact_fts_total(conn, "Nichttreffer", "", []) == 0


def test_text_total_is_exact_not_pool_and_limit_independent(monkeypatch):
    conn = _conn_with_n_matches(4)
    _wire(monkeypatch)
    # Force a tiny candidate pool so its size (the pre-fix total) is strictly
    # smaller than the true match count (4).
    monkeypatch.setattr(mcp_server, "MIN_CANDIDATE_POOL", 1)
    monkeypatch.setattr(mcp_server, "MAX_RERANK_CANDIDATES", 1)
    _, t1 = mcp_server._search_fts5_inner(
        conn, "Notwehr", None, None, None, None, None, None, None, None, 1)
    _, t9 = mcp_server._search_fts5_inner(
        conn, "Notwehr", None, None, None, None, None, None, None, None, 9)
    assert t1 == 4, f"total should be the exact match count 4, got {t1}"
    assert t1 == t9, f"total must not depend on limit ({t1} vs {t9})"
