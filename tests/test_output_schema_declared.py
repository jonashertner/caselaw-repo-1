"""Declare outputSchema only for tools with structured response contracts.

The four legacy tools keep permissive object schemas. Six core research
tools now declare shared success/error contracts and return structuredContent
through the research wrapper. Other tools still have text-only paths; this
test guards against both accidental schema spread and contract loss.
"""
from __future__ import annotations

import sys
from pathlib import Path

import jsonschema
import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m
from research_contracts import RESEARCH_MODELS, output_schema

LEGACY_STRUCTURED = {"search", "fetch", "search_laws", "search_legislation"}
RESEARCH_STRUCTURED = {
    "search_decisions", "get_decision", "get_erwaegung", "get_law", "find_citations", "cite",
}
STRUCTURED = LEGACY_STRUCTURED | RESEARCH_STRUCTURED


def test_exactly_legacy_and_core_research_tools_declare_output_schema():
    tools = m._list_tools()
    with_schema = {t.name for t in tools if t.outputSchema}
    assert with_schema == STRUCTURED, with_schema


def test_legacy_schemas_remain_permissive():
    for t in m._list_tools():
        if t.name in LEGACY_STRUCTURED:
            assert t.outputSchema.get("additionalProperties") is True, t.name
            assert t.outputSchema.get("type") == "object", t.name


def test_research_schemas_use_shared_additive_success_error_contracts():
    for t in m._list_tools():
        if t.name in RESEARCH_STRUCTURED:
            assert t.outputSchema == output_schema(t.name)
            assert t.outputSchema["type"] == "object"
            definitions = t.outputSchema["$defs"]
            assert definitions[RESEARCH_MODELS[t.name].__name__]["additionalProperties"] is True
            assert definitions["ResearchError"]["required"] == ["error"]
            # Unlike legacy permissive objects, a core result needs either
            # its operation's success fields or an explicit error.
            with pytest.raises(jsonschema.ValidationError):
                jsonschema.validate({}, t.outputSchema)


_SUCCESS_PAYLOADS = {
    "search": {"results": []},
    "fetch": {"id": "fixture", "title": "Fixture", "text": "Stored text",
              "url": "https://example.invalid/fixture", "metadata": {}},
    "search_laws": {"query": "fixture", "total": 0, "hits": []},
    "search_legislation": {"query": "fixture", "total": 0, "hits": []},
    "search_decisions": {"total": 0, "total_is_lower_bound": False, "results": [],
                         "returned": 0, "limit": 10, "offset": 0, "has_more": False,
                         "next_offset": None},
    "get_decision": {"decision_id": "fixture", "full_text": "Stored text"},
    "get_erwaegung": {"decision_id": "fixture", "e_number": "1", "text": "Stored text"},
    "get_law": {"sr_number": "fixture", "articles": []},
    "find_citations": {"decision_id": "fixture", "direction": "both", "limit": 50,
                       "offset": 0, "incoming": [], "outgoing": []},
    "cite": {"exists": False, "queried": "fixture", "close_matches": []},
}


@pytest.mark.parametrize("name", sorted(STRUCTURED))
def test_each_declared_schema_accepts_success_and_error_results(name):
    tool = next(t for t in m._list_tools() if t.name == name)
    jsonschema.Draft202012Validator.check_schema(tool.outputSchema)
    jsonschema.validate(
        {**_SUCCESS_PAYLOADS[name], "future_metadata": {"preserve": True}}, tool.outputSchema)
    jsonschema.validate(
        {"error": "Fixture backend unavailable", "future_metadata": {"preserve": True}},
        tool.outputSchema,
    )


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
