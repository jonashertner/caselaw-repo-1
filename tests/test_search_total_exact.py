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
                        lambda q, d, **kw: ([{"query": "Notwehr", "name": "nl_and", "weight": 1.0}], [], {}))
    monkeypatch.setattr(mcp_server, "_load_graph_signal_map", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_search_vectors_chunks", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_search_statute_graph", lambda *a, **k: [])
    monkeypatch.setattr(mcp_server, "_past_deadline", lambda d: False)
    monkeypatch.setattr(mcp_server, "_search_vectors", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_search_sparse", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_rerank_rows", lambda rows, *a, **k: [dict(r) for r in rows])


def test_exact_fts_total_matches_direct_count():
    # Returns (total, is_capped); under-cap counts are exact -> is_capped False (#53).
    conn = _conn_with_n_matches(7)
    assert mcp_server._exact_fts_total(conn, "Notwehr", "", []) == (7, False)
    assert mcp_server._exact_fts_total(conn, "Notwehr", " AND d.court = ?", ["bger"]) == (7, False)
    assert mcp_server._exact_fts_total(conn, "Notwehr", " AND d.court = ?", ["bvger"]) == (0, False)
    assert mcp_server._exact_fts_total(conn, "Nichttreffer", "", []) == (0, False)


def test_total_is_bounded_by_cap(monkeypatch):
    """Beyond _FTS_TOTAL_CAP the count is reported as the cap (a floor) and the
    underlying FTS scan is LIMIT-ed, so a broad term cannot walk the whole
    doclist."""
    conn = _conn_with_n_matches(7)
    monkeypatch.setattr(mcp_server, "_FTS_TOTAL_CAP", 3)
    # Capped -> total is the cap AND is_capped True (the #53 lower-bound signal).
    assert mcp_server._exact_fts_total(conn, "Notwehr", "", []) == (3, True)
    # cap is a constant in production (not part of the cache key); clear the
    # cache before re-counting under a different cap in this test.
    mcp_server._FTS_TOTAL_CACHE.clear()
    monkeypatch.setattr(mcp_server, "_FTS_TOTAL_CAP", 100)
    assert mcp_server._exact_fts_total(conn, "Notwehr", "", []) == (7, False)


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


def test_meta_flags_lower_bound_only_when_capped(monkeypatch):
    """#53: meta['total_is_lower_bound'] is True only when the FTS count hit the
    cap; an exact under-cap count is not a lower bound."""
    conn = _conn_with_n_matches(7)
    _wire(monkeypatch)
    monkeypatch.setattr(mcp_server, "MIN_CANDIDATE_POOL", 1)
    monkeypatch.setattr(mcp_server, "MAX_RERANK_CANDIDATES", 1)

    # Exact (under cap) -> not a lower bound.
    meta = {}
    _, total = mcp_server._search_fts5_inner(
        conn, "Notwehr", None, None, None, None, None, None, None, None, 5, meta=meta)
    assert total == 7 and meta["total_is_lower_bound"] is False

    # Capped -> total is the cap AND the flag is set.
    mcp_server._FTS_TOTAL_CACHE.clear()
    monkeypatch.setattr(mcp_server, "_FTS_TOTAL_CAP", 3)
    meta2 = {}
    _, total2 = mcp_server._search_fts5_inner(
        conn, "Notwehr", None, None, None, None, None, None, None, None, 5, meta=meta2)
    assert total2 == 3 and meta2["total_is_lower_bound"] is True

    # Filter-only path is always exact -> never a lower bound.
    mcp_server._FTS_TOTAL_CACHE.clear()
    meta3 = {}
    mcp_server._search_fts5_inner(
        conn, "", "bger", None, None, None, None, None, None, None, 5, meta=meta3)
    assert meta3["total_is_lower_bound"] is False


def test_pool_winning_the_max_is_flagged_as_lower_bound(monkeypatch):
    """#56: when the candidate pool is larger than the exact count, `total`
    is the pool size — and the pool scales with offset+limit, so it is a
    lower bound on the expanded result set, never an exact count.

    Pre-fix it was rendered unmarked, which made `total` a function of the
    caller's page size (one live query reported 60 / 60 / 400 / 2000 for
    limit 5 / 10 / 100 / 500, all with total_is_lower_bound=false).
    """
    conn = _conn_with_n_matches(10)
    _wire(monkeypatch)
    # Exact count for the *counted* query is 3; query expansion puts 10 rows
    # in the pool. That is the production shape: the pool spans strategies
    # the single counted fts_query does not cover.
    monkeypatch.setattr(mcp_server, "_exact_fts_total", lambda *a, **k: (3, False))

    meta = {}
    _, total = mcp_server._search_fts5_inner(
        conn, "Notwehr", None, None, None, None, None, None, None, None, 5, meta=meta)
    assert total == 10, f"total must not drop below what the pool holds, got {total}"
    assert meta["total_is_lower_bound"] is True, (
        "a pool-derived total is a lower bound and must be marked")


def test_exact_count_beating_the_pool_stays_unmarked(monkeypatch):
    """The converse guard: when the exact count wins the max(), nothing has
    been estimated, so the #53 contract ('no marker means exact') holds."""
    conn = _conn_with_n_matches(6)
    _wire(monkeypatch)
    monkeypatch.setattr(mcp_server, "MIN_CANDIDATE_POOL", 1)
    monkeypatch.setattr(mcp_server, "MAX_RERANK_CANDIDATES", 1)

    meta = {}
    _, total = mcp_server._search_fts5_inner(
        conn, "Notwehr", None, None, None, None, None, None, None, None, 5, meta=meta)
    assert total == 6 and meta["total_is_lower_bound"] is False
