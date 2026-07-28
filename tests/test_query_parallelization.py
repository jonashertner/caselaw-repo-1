"""The expand-Haiku (inside _build_query_strategies) and the structured-parse
Haiku are independent calls on the same query; _analyze_query runs them
concurrently so the two ~2s round-trips overlap (identical results, ~2s saved).
"""
import threading

import mcp_server


def test_analyze_query_runs_the_two_haiku_calls_concurrently(monkeypatch):
    # A Barrier(2) only releases when BOTH threads reach it simultaneously.
    # If _analyze_query ran them sequentially, the first wait() would block alone
    # and time out -> BrokenBarrierError -> the call would raise and fail here.
    barrier = threading.Barrier(2, timeout=4)

    def fake_build_strategies(q, **kw):
        barrier.wait()
        return (["strat"], ["term"])

    def fake_parse(q):
        barrier.wait()
        return {"doctrine": "Kündigung"}

    monkeypatch.setattr(mcp_server, "_build_query_strategies", fake_build_strategies)
    monkeypatch.setattr(mcp_server, "_parse_query_structured", fake_parse)

    strategies, llm_terms, parse = mcp_server._analyze_query("q", is_docket_query=False)
    assert strategies == ["strat"]
    assert llm_terms == ["term"]
    assert parse == {"doctrine": "Kündigung"}


def test_analyze_query_skips_parse_for_docket_queries(monkeypatch):
    monkeypatch.setattr(mcp_server, "_build_query_strategies", lambda q, **kw: (["s"], []))
    parse_calls = []
    monkeypatch.setattr(mcp_server, "_parse_query_structured",
                        lambda q: (parse_calls.append(q), {})[1])
    strategies, llm_terms, parse = mcp_server._analyze_query("4A_1/2020", is_docket_query=True)
    assert strategies == ["s"]
    assert parse == {}
    assert parse_calls == []   # parse must NOT run for docket queries


def test_analyze_query_propagates_strategies_result(monkeypatch):
    monkeypatch.setattr(mcp_server, "_build_query_strategies", lambda q, **kw: (["a", "b"], ["x"]))
    monkeypatch.setattr(mcp_server, "_parse_query_structured", lambda q: {"domain": "civil"})
    s, t, p = mcp_server._analyze_query("Verjährung", is_docket_query=False)
    assert s == ["a", "b"] and t == ["x"] and p == {"domain": "civil"}
