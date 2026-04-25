"""Regression tests for the local web-chat MCP tool surface.

The local web UI exposes its tool surface through `web_api/providers/base.py::MCP_TOOLS`.
These tests guard against silent regressions in two dimensions:

1.  **Coverage** — the documented core tools are present.  Without these, the
    web chat looks substantially less capable than the remote MCP server even
    though the underlying mcp_server.py exposes them.

2.  **Schema validity** — every tool entry conforms to the minimal shape that
    OpenAI / Anthropic / Google tool-calling adapters require: a string name,
    a string description, and a JSON-Schema-shaped parameters object.  A
    malformed entry can break tool registration silently for one provider
    while another is fine, which is hard to catch in production.
"""
from __future__ import annotations

import pytest

from web_api.providers.base import MCP_TOOLS


# Tools that must always be present in the local web tool surface — keeps
# parity with what the remote MCP server documents.
DOCUMENTED_CORE_TOOLS = {
    # Original 5 (search / retrieval / corpus stats / drafting):
    "search_decisions",
    "get_decision",
    "list_courts",
    "get_statistics",
    "draft_mock_decision",
    # Citation graph & jurisprudence:
    "find_citations",
    "find_appeal_chain",
    "find_leading_cases",
    "analyze_legal_trend",
    "get_case_brief",
    "get_doctrine",
    "generate_exam_question",
    # Statute lookup (federal Fedlex + cantonal LexFind mirrors):
    "get_law",
    "search_laws",
    "search_legislation",
    "get_legislation",
    # Scholarly commentary:
    "get_commentary",
    "search_commentaries",
    # Materialien (legislative history):
    "get_materialien",
    "search_materialien",
}


def _names() -> set[str]:
    return {tool["name"] for tool in MCP_TOOLS}


def test_mcp_tools_includes_all_documented_core_tools():
    """Every documented core tool must be present in MCP_TOOLS."""
    present = _names()
    missing = DOCUMENTED_CORE_TOOLS - present
    assert not missing, (
        f"web_api/providers/base.py::MCP_TOOLS is missing documented core tools: "
        f"{sorted(missing)}. Either add them or update DOCUMENTED_CORE_TOOLS in "
        f"this test file with a justification."
    )


def test_mcp_tools_have_unique_names():
    names = [tool["name"] for tool in MCP_TOOLS]
    assert len(names) == len(set(names)), (
        f"Duplicate tool names in MCP_TOOLS: "
        f"{[n for n in names if names.count(n) > 1]}"
    )


@pytest.mark.parametrize("tool", MCP_TOOLS, ids=[t["name"] for t in MCP_TOOLS])
def test_mcp_tool_schema_valid(tool):
    """Each tool entry must have name + description + parameters in the
    minimal shape every provider tool-calling adapter expects."""
    # Required top-level keys
    assert isinstance(tool.get("name"), str) and tool["name"], "name must be a non-empty string"
    assert isinstance(tool.get("description"), str) and tool["description"], "description must be a non-empty string"
    assert isinstance(tool.get("parameters"), dict), "parameters must be a dict"

    params = tool["parameters"]
    assert params.get("type") == "object", f"{tool['name']}: parameters.type must be 'object'"
    assert isinstance(params.get("properties", {}), dict), (
        f"{tool['name']}: parameters.properties must be a dict"
    )

    # If 'required' is present, it must be a list of property names that
    # actually appear in 'properties'.
    if "required" in params:
        required = params["required"]
        assert isinstance(required, list), f"{tool['name']}: required must be a list"
        properties = params.get("properties", {})
        unknown_required = set(required) - set(properties)
        assert not unknown_required, (
            f"{tool['name']}: required references unknown properties: "
            f"{sorted(unknown_required)}"
        )

    # Every property entry should at least declare a type or an enum.
    for prop_name, prop_schema in params.get("properties", {}).items():
        assert isinstance(prop_schema, dict), (
            f"{tool['name']}.{prop_name}: schema must be a dict"
        )
        has_shape = "type" in prop_schema or "enum" in prop_schema or "$ref" in prop_schema
        assert has_shape, (
            f"{tool['name']}.{prop_name}: must declare 'type', 'enum', or '$ref'"
        )
