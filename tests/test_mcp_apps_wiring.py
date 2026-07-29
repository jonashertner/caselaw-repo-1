"""MCP Apps (io.modelcontextprotocol/ui) server-side wiring.

MCP Apps is the one item in the 2026-07-28 revision that is adoptable on
the installed SDK (mcp 1.26) — it is an extension carried in tool _meta,
not a core-protocol change — and it is the only mechanism that actually
reduces model-visible tool calls: once a widget renders, its own tool
traffic goes iframe -> host -> server over postMessage and never becomes a
model turn.

Claude, Claude Desktop, VS Code Copilot and M365 Copilot all support it,
which is essentially our whole traffic mix.

These tests cover the SERVER side only. The widget's postMessage dialect
must be verified in a real Claude session — see the docstring in
law_widget.py.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import law_widget  # noqa: E402


def _tools_with_flag(monkeypatch, enabled: bool):
    """Toggle the flag WITHOUT importlib.reload.

    Reloading mcp_server re-executes module-level DB path resolution and
    left later tests raising FileNotFoundError — the reload poisoned shared
    state for the whole session. _list_tools() reads the module globals at
    call time, so patching them is both sufficient and side-effect free.
    """
    import mcp_server as m
    monkeypatch.setattr(m, "OCL_UI_WIDGETS", enabled)
    monkeypatch.setattr(m, "_LAW_TOOL_META",
                        law_widget.tool_ui_meta() if enabled else None)
    return m, m._list_tools()


def test_flag_off_advertises_no_ui(monkeypatch):
    _, tools = _tools_with_flag(monkeypatch, False)
    assert [t.name for t in tools if t.meta] == []


def test_flag_on_advertises_ui_on_the_law_tools(monkeypatch):
    _, tools = _tools_with_flag(monkeypatch, True)
    named = {t.name: t.meta for t in tools if t.meta}
    assert set(named) == {"search_laws", "search_legislation"}, named
    for name, meta in named.items():
        assert meta["ui"]["resourceUri"] == law_widget.WIDGET_URI, name


def test_meta_serialises_to_the_wire_alias(monkeypatch):
    """The SDK models it as `meta` with alias `_meta`; clients read `_meta`.
    (Reading t._meta instead of t.meta made correct wiring look broken.)"""
    _, tools = _tools_with_flag(monkeypatch, True)
    [t] = [t for t in tools if t.name == "search_laws"]
    wire = t.model_dump(by_alias=True, exclude_none=True)
    assert "_meta" in wire
    assert wire["_meta"]["ui"]["resourceUri"] == law_widget.WIDGET_URI
