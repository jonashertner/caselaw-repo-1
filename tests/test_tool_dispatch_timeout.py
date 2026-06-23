"""Hardening: server-side tool-dispatch timeout (_dispatch_with_timeout).

An adopter observed 10-30min hangs on to_thread-dispatched tools (list_courts,
get_law) under thread-pool saturation, while list_tools (on the event loop) stayed
instant. A dispatch-level timeout returns a clean error payload instead of an
indefinite hang. These pin that contract.
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402


def test_slow_tool_times_out(monkeypatch):
    monkeypatch.setattr(mcp_server, "TOOL_DISPATCH_TIMEOUT_S", 0.2)

    async def _slow(name, arguments):
        await asyncio.sleep(5)
        return [mcp_server.TextContent(type="text", text="unreachable")]

    monkeypatch.setattr(mcp_server, "_handle_call_tool_inner", _slow)
    result = asyncio.run(mcp_server._dispatch_with_timeout("get_law", {"sr_number": "220"}))
    assert isinstance(result, list) and result
    payload = json.loads(result[0].text)
    assert payload["error"] == "server_timeout"
    assert payload["tool"] == "get_law"
    assert payload["timeout_seconds"] == 0.2
    assert "retry" in payload["message"].lower()


def test_fast_tool_passes_through(monkeypatch):
    monkeypatch.setattr(mcp_server, "TOOL_DISPATCH_TIMEOUT_S", 5)
    sentinel = [mcp_server.TextContent(type="text", text="real result")]

    async def _fast(name, arguments):
        return sentinel

    monkeypatch.setattr(mcp_server, "_handle_call_tool_inner", _fast)
    result = asyncio.run(mcp_server._dispatch_with_timeout("list_courts", {}))
    assert result is sentinel  # verbatim passthrough, no wrapping when fast


def test_timeout_aborts_quickly_not_after_full_duration(monkeypatch):
    monkeypatch.setattr(mcp_server, "TOOL_DISPATCH_TIMEOUT_S", 0.05)

    async def _slow(name, arguments):
        await asyncio.sleep(2)

    monkeypatch.setattr(mcp_server, "_handle_call_tool_inner", _slow)
    t0 = time.monotonic()
    result = asyncio.run(mcp_server._dispatch_with_timeout("x", {}))
    assert time.monotonic() - t0 < 1.0  # freed near the 0.05s cap, not the 2s sleep
    assert json.loads(result[0].text)["error"] == "server_timeout"
