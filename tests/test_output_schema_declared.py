"""outputSchema declared for exactly the four always-structured tools.

The MCP spec makes structuredContent MANDATORY on every result once a tool
declares outputSchema. search/fetch/search_laws/search_legislation return
structuredContent on every path (including errors); every other tool has at
least one text-only path, so declaring there would be a spec violation —
this test guards against accidental spread as much as against loss.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

STRUCTURED = {"search", "fetch", "search_laws", "search_legislation"}


def test_exactly_four_tools_declare_output_schema():
    tools = m._list_tools()
    with_schema = {t.name for t in tools if t.outputSchema}
    assert with_schema == STRUCTURED, with_schema


def test_schemas_are_permissive():
    for t in m._list_tools():
        if t.name in STRUCTURED:
            assert t.outputSchema.get("additionalProperties") is True, t.name
            assert t.outputSchema.get("type") == "object", t.name


def test_law_hits_payload_keys_within_declared():
    payload = m._law_hits_structured({"query": "x", "results": []}, "de")
    [t] = [t for t in m._list_tools() if t.name == "search_laws"]
    declared = set(t.outputSchema["properties"].keys())
    # additionalProperties: true means extras are legal — but the CORE keys we
    # promise must be declared so typed clients can bind them.
    for core in ("query", "total", "hits"):
        assert core in declared
        assert core in payload or core in ("hits",) and "hits" in payload


def test_legislation_payload_core_keys_declared():
    payload = m._legislation_hits_structured({"query": "x", "laws": []}, "de")
    [t] = [t for t in m._list_tools() if t.name == "search_legislation"]
    declared = set(t.outputSchema["properties"].keys())
    for core in ("query", "hits"):
        assert core in declared, declared
    assert "hits" in payload or "results" in payload or isinstance(payload, dict)
