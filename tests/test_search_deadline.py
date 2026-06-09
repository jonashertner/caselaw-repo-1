"""Search soft-deadline: when a query's elapsed time crosses the budget, skip the
expensive augmentation (cross-encoder + Haiku LLM-rerank) and stop launching more
FTS strategies, degrading to fast BM25/RRF results instead of a >50s hard timeout
that breaks token-limited connectors. The deadline only fires for pathological/
contended queries (default budget is generous), so normal searches are unaffected."""
import time

import mcp_server


def test_past_deadline():
    assert mcp_server._past_deadline(None) is False
    assert mcp_server._past_deadline(time.monotonic() - 1.0) is True
    assert mcp_server._past_deadline(time.monotonic() + 100.0) is False


def test_cross_encoder_skipped_when_past_deadline(monkeypatch):
    calls = []
    monkeypatch.setattr(mcp_server, "CROSS_ENCODER_ENABLED", True)
    monkeypatch.setattr(mcp_server, "_get_cross_encoder", lambda: (calls.append(1), None)[1])
    scored = [(1.0, 0.5, 0, {"decision_id": "x", "full_text": "t", "regeste": "r"})]
    # past deadline -> early return, cross-encoder NEVER consulted
    out = mcp_server._apply_cross_encoder_boosts(scored, "q", deadline=time.monotonic() - 1.0)
    assert out == scored
    assert calls == [], "cross-encoder consulted despite a passed deadline"
    # no deadline -> cross-encoder IS consulted (control, proves the gate is what skips it)
    mcp_server._apply_cross_encoder_boosts(scored, "q", deadline=None)
    assert calls == [1], "cross-encoder not consulted without a deadline"


def test_llm_rerank_skipped_when_past_deadline():
    scored = [(1.0, 0.5, 0, {"decision_id": "x"})]
    out = mcp_server._apply_llm_rerank(scored, "q", deadline=time.monotonic() - 1.0)
    assert out == scored  # early return, no Haiku call


def _fixture_conn():
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        "CREATE VIRTUAL TABLE decisions_fts USING fts5("
        "decision_id, court, canton, docket_number, language, title, regeste, full_text);"
        "CREATE TABLE decisions (decision_id TEXT, court TEXT, canton TEXT, chamber TEXT,"
        " docket_number TEXT, decision_date TEXT, language TEXT, title TEXT, regeste TEXT,"
        " full_text TEXT, source_url TEXT, pdf_url TEXT);"
    )
    conn.execute(
        "INSERT INTO decisions_fts (rowid, decision_id, court, canton, docket_number, language, title, regeste, full_text)"
        " VALUES (1,'bger_x','bger','CH','1A_1/2020','de','Notwehr','r','Notwehr Putativnotwehr text')"
    )
    conn.execute(
        "INSERT INTO decisions (rowid, decision_id, court, canton, chamber, docket_number, decision_date,"
        " language, title, regeste, full_text, source_url, pdf_url)"
        " VALUES (1,'bger_x','bger','CH','','1A_1/2020','2020-01-01','de','Notwehr','r','Notwehr Putativnotwehr text','http://x','http://x.pdf')"
    )
    conn.commit()
    return conn


def test_search_inner_skips_expensive_sources_past_deadline(monkeypatch):
    conn = _fixture_conn()
    monkeypatch.setattr(mcp_server, "_analyze_query",
                        lambda q, d: ([{"query": "Notwehr", "name": "nl_and", "weight": 1.0}], [], {}))
    monkeypatch.setattr(mcp_server, "_rerank_rows", lambda *a, **k: [])
    monkeypatch.setattr(mcp_server, "_load_graph_signal_map", lambda *a, **k: {})
    calls = []
    for nm in ("_search_vectors", "_search_vectors_chunks", "_search_sparse", "_search_statute_graph"):
        monkeypatch.setattr(mcp_server, nm, (lambda name: (lambda *a, **k: (calls.append(name), {})[1]))(nm))

    # Force the deadline DECISION deterministically (its time-math is covered by
    # test_past_deadline); here we assert the post-FTS sources actually respect it.
    monkeypatch.setattr(mcp_server, "_past_deadline", lambda d: True)
    mcp_server._search_fts5_inner(conn, "Notwehr", None, None, None, None, None, None, None, None, 5)
    assert calls == [], f"expensive sources ran past the deadline: {calls}"

    # control: never past the deadline -> they DO run (proves the wiring + the gate)
    calls.clear()
    monkeypatch.setattr(mcp_server, "_past_deadline", lambda d: False)
    mcp_server._search_fts5_inner(conn, "Notwehr", None, None, None, None, None, None, None, None, 5)
    assert calls, "expensive sources were not consulted with no deadline (wiring broken)"
