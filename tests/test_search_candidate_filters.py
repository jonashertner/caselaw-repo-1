"""Vector- and sparse-only candidates must honour the user's explicit search
filters (court / canton / date / chamber / decision_type / language). The FTS
pass applies the {where} clause; the semantic-retrieval candidate pools must
apply the SAME clause, or a filtered search (court=bger, date_from=…) silently
returns decisions from other courts/years whenever vector/sparse retrieval adds
candidates the lexical pass didn't. Regression test for that leak.
"""
import sqlite3

import mcp_server


def _two_court_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        "CREATE VIRTUAL TABLE decisions_fts USING fts5("
        "decision_id, court, canton, docket_number, language, title, regeste, full_text);"
        "CREATE TABLE decisions (decision_id TEXT, court TEXT, canton TEXT, chamber TEXT,"
        " docket_number TEXT, decision_date TEXT, language TEXT, title TEXT, regeste TEXT,"
        " full_text TEXT, source_url TEXT, pdf_url TEXT);"
    )
    # bger_x: matches the query lexically (court=bger).
    conn.execute(
        "INSERT INTO decisions_fts (rowid, decision_id, court, canton, docket_number, language, title, regeste, full_text)"
        " VALUES (1,'bger_x','bger','CH','1A_1/2020','de','Notwehr','r','Notwehr Putativnotwehr')"
    )
    conn.execute(
        "INSERT INTO decisions (rowid, decision_id, court, canton, chamber, docket_number, decision_date,"
        " language, title, regeste, full_text, source_url, pdf_url)"
        " VALUES (1,'bger_x','bger','CH','','1A_1/2020','2020-01-01','de','Notwehr','r','Notwehr Putativnotwehr','http://x','http://x.pdf')"
    )
    # bvger_y: a DIFFERENT court, does NOT match the query lexically — it can only
    # enter the pool via the (stubbed) vector/sparse retrievers.
    conn.execute(
        "INSERT INTO decisions (rowid, decision_id, court, canton, chamber, docket_number, decision_date,"
        " language, title, regeste, full_text, source_url, pdf_url)"
        " VALUES (2,'bvger_y','bvger','CH','','D-2/2019','2019-05-05','de','Asylrecht','r','Asyl Wegweisung','http://y','http://y.pdf')"
    )
    conn.commit()
    return conn


def _wire_common(monkeypatch):
    monkeypatch.setattr(mcp_server, "_analyze_query",
                        lambda q, d, **kw: ([{"query": "Notwehr", "name": "nl_and", "weight": 1.0}], [], {}, "empty"))
    monkeypatch.setattr(mcp_server, "_load_graph_signal_map", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_search_vectors_chunks", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_search_statute_graph", lambda *a, **k: [])
    monkeypatch.setattr(mcp_server, "_past_deadline", lambda d: False)
    monkeypatch.setattr(mcp_server, "VECTOR_WEIGHT", 1.0)
    # Echo the candidate pool back so the test can inspect which rows survived.
    monkeypatch.setattr(mcp_server, "_rerank_rows",
                        lambda rows, *a, **k: [dict(r) for r in rows])


def test_vector_only_candidate_respects_court_filter(monkeypatch):
    conn = _two_court_conn()
    _wire_common(monkeypatch)
    monkeypatch.setattr(mcp_server, "_search_vectors", lambda *a, **k: {"bvger_y": 0.1})
    monkeypatch.setattr(mcp_server, "_search_sparse", lambda *a, **k: {})
    results, _ = mcp_server._search_fts5_inner(
        conn, "Notwehr", "bger", None, None, None, None, None, None, None, 10)
    ids = {r["decision_id"] for r in results}
    assert "bvger_y" not in ids, "vector-only candidate leaked past court=bger filter"
    assert "bger_x" in ids


def test_sparse_only_candidate_respects_court_filter(monkeypatch):
    conn = _two_court_conn()
    _wire_common(monkeypatch)
    monkeypatch.setattr(mcp_server, "_search_vectors", lambda *a, **k: {})
    monkeypatch.setattr(mcp_server, "_search_sparse", lambda *a, **k: {"bvger_y": 5.0})
    results, _ = mcp_server._search_fts5_inner(
        conn, "Notwehr", "bger", None, None, None, None, None, None, None, 10)
    ids = {r["decision_id"] for r in results}
    assert "bvger_y" not in ids, "sparse-only candidate leaked past court=bger filter"


def test_no_filter_keeps_vector_candidate(monkeypatch):
    # Control: with NO court filter, the vector-only candidate is admitted —
    # proving the exclusion above is the filter, not a blanket drop. (This is
    # also why the fix is MRR-neutral: benchmark queries set no filters.)
    conn = _two_court_conn()
    _wire_common(monkeypatch)
    monkeypatch.setattr(mcp_server, "_search_vectors", lambda *a, **k: {"bvger_y": 0.1})
    monkeypatch.setattr(mcp_server, "_search_sparse", lambda *a, **k: {})
    results, _ = mcp_server._search_fts5_inner(
        conn, "Notwehr", None, None, None, None, None, None, None, None, 10)
    ids = {r["decision_id"] for r in results}
    assert "bvger_y" in ids
