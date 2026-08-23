"""`attest_response` declares every argument its handler reads.

The handler reads three: draft_text, audit_grounding and audit_quotes. The
input schema declared only the first two, so no MCP client could ever reach
the quote audit, even though the docstring of the underlying function tells
callers to "pass audit_quotes=True".

A schema that omits an argument the handler honours is invisible: the call
still succeeds, it just silently runs with the default.
"""
from __future__ import annotations

import mcp_server


def _schema(tool_name: str) -> dict:
    tool = next(t for t in mcp_server._list_tools() if t.name == tool_name)
    return tool.inputSchema


def test_attest_response_declares_audit_quotes():
    props = _schema("attest_response")["properties"]
    assert "audit_quotes" in props, (
        "the handler reads arguments.get('audit_quotes'); a client that cannot "
        "see it in the schema will never send it"
    )
    assert props["audit_quotes"]["type"] == "boolean"


def test_attest_response_required_is_unchanged():
    assert _schema("attest_response")["required"] == ["draft_text"]
