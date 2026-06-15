"""ChatGPT Deep Research search/fetch shim (mcp_server).

Deep-research models only call MCP servers exposing `search` and `fetch`
in OpenAI's exact schema. These tests pin the shim's output shape and the
R1/R6 contract (citations + URLs come from _build_citation_strings, never
constructed), and guard the regressions that motivated the shim:
- the old "search" alias (search -> search_decisions wrong schema, 87% err)
  must be gone;
- the dispatch must return a (content, structuredContent) tuple so the
  open-access-note appender can't corrupt the JSON.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402


# ── search ────────────────────────────────────────────────────────────────

def test_search_shape_and_citation_from_builder(monkeypatch):
    rows = [{
        "decision_id": "bge_BGE_140_III_86", "court": "bge",
        "docket_number": "140 III 86", "decision_date": "2014-01-01",
        "language": "de", "title": "X gegen Y", "snippet": "Mietrecht <mark>Kündigung</mark>",
    }]
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **k: (rows, 1))
    out = mcp_server._deep_research_search("Mietrecht Kündigung", 10)
    assert list(out.keys()) == ["results"]
    r = out["results"][0]
    assert {"id", "title", "url"}.issubset(r)
    assert r["id"] == "bge_BGE_140_III_86"
    # R1/R6: citation + url derive from the builder, not constructed here
    assert "140 III 86" in r["title"]
    assert r["url"].endswith("/entscheid/bge_BGE_140_III_86")
    # snippet stripped of <mark>
    assert "<mark>" not in r["snippet"]


def test_search_limit_clamped(monkeypatch):
    captured = {}
    def fake(**k):
        captured.update(k)
        return ([], 0)
    monkeypatch.setattr(mcp_server, "search_fts5", fake)
    mcp_server._deep_research_search("x", 999)
    assert captured["limit"] <= 50
    mcp_server._deep_research_search("x", 0)
    assert captured["limit"] >= 1


def test_search_empty_results(monkeypatch):
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **k: ([], 0))
    out = mcp_server._deep_research_search("nothing", 10)
    assert out == {"results": []}


# ── fetch ───────────────────────────────────────────────────────────────────

def test_fetch_shape_and_verbatim_text(monkeypatch):
    dec = {
        "decision_id": "bge_BGE_140_III_86", "court": "bge",
        "docket_number": "140 III 86", "decision_date": "2014-01-01",
        "language": "de", "title": "X gegen Y",
        "full_text": "Sachverhalt: ... Considerando in diritto: ...",
    }
    monkeypatch.setattr(mcp_server, "_resolve_decision_id", lambda x: "bge_BGE_140_III_86")
    monkeypatch.setattr(mcp_server, "get_decision_by_id", lambda x: dec)
    out = mcp_server._deep_research_fetch("BGE 140 III 86")
    assert {"id", "title", "text", "url", "metadata"}.issubset(out)
    assert out["id"] == "bge_BGE_140_III_86"
    assert out["text"] == dec["full_text"]  # verbatim
    assert out["url"].endswith("/entscheid/bge_BGE_140_III_86")
    # R1: citation strings in metadata come from the builder
    assert "140 III 86" in (out["metadata"]["citation_string_de"] or "")


def test_fetch_text_capped(monkeypatch):
    dec = {"decision_id": "d1", "court": "bger", "docket_number": "6B_1/2025",
           "decision_date": "2025-01-01", "language": "de",
           "full_text": "A" * (mcp_server._FETCH_TEXT_CAP + 5000)}
    monkeypatch.setattr(mcp_server, "_resolve_decision_id", lambda x: "d1")
    monkeypatch.setattr(mcp_server, "get_decision_by_id", lambda x: dec)
    out = mcp_server._deep_research_fetch("d1")
    assert len(out["text"]) == mcp_server._FETCH_TEXT_CAP


def test_fetch_not_found(monkeypatch):
    monkeypatch.setattr(mcp_server, "_resolve_decision_id", lambda x: x)
    monkeypatch.setattr(mcp_server, "get_decision_by_id", lambda x: None)
    out = mcp_server._deep_research_fetch("nonexistent")
    assert out["text"] == ""
    assert out["metadata"].get("error") == "not_found"


def test_fetch_missing_id():
    out = mcp_server._deep_research_fetch("")
    assert out["metadata"].get("error") == "missing_id"


# ── dispatch + registration regressions ─────────────────────────────────────

def test_search_alias_removed():
    # The old "search" -> search_decisions alias caused the 87% error rate.
    assert "search" not in mcp_server._TOOL_NAME_ALIASES


def test_search_and_fetch_registered():
    names = {t.name for t in mcp_server._list_tools()}
    assert "search" in names and "fetch" in names
    # the rich tools remain for general clients
    assert "search_decisions" in names and "get_decision" in names


def test_dispatch_returns_structured_tuple(monkeypatch):
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **k: ([], 0))
    monkeypatch.setattr(mcp_server, "_record_query", lambda q: None)
    result = asyncio.run(mcp_server._handle_call_tool_inner("search", {"query": "x"}))
    assert isinstance(result, tuple) and len(result) == 2
    content, structured = result
    assert structured == {"results": []}
    # content[0].text must be valid JSON equal to structured
    assert json.loads(content[0].text) == structured
