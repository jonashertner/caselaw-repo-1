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

import importlib
import os
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import law_widget  # noqa: E402


def _server_with_flag(monkeypatch, value: str):
    monkeypatch.setenv("OCL_UI_WIDGETS", value)
    import mcp_server
    return importlib.reload(mcp_server)


def test_widget_constants_match_the_spec():
    assert law_widget.WIDGET_URI.startswith("ui://")
    assert law_widget.WIDGET_MIME == "text/html;profile=mcp-app"
    meta = law_widget.tool_ui_meta()
    assert meta["ui"]["resourceUri"] == law_widget.WIDGET_URI


def test_widget_html_is_fully_self_contained():
    """Apps run under a deny-by-default CSP in a sandboxed iframe; anything
    external needs an explicit _meta.ui.csp entry. Keeping it inlined avoids
    the whole question."""
    html = law_widget.LAW_SEARCH_WIDGET_HTML
    external = re.findall(r'(?:src|href)=["\']https?://', html)
    assert not external, external
    assert html.lstrip().lower().startswith("<!doctype html")


def test_flag_off_advertises_no_ui(monkeypatch):
    m = _server_with_flag(monkeypatch, "")
    try:
        assert m.OCL_UI_WIDGETS is False
        assert [t.name for t in m._list_tools() if t.meta] == []
    finally:
        monkeypatch.delenv("OCL_UI_WIDGETS", raising=False)
        importlib.reload(m)


def test_flag_on_advertises_ui_on_the_law_tools(monkeypatch):
    m = _server_with_flag(monkeypatch, "1")
    try:
        named = {t.name: t.meta for t in m._list_tools() if t.meta}
        assert set(named) == {"search_laws", "search_legislation"}, named
        for name, meta in named.items():
            assert meta["ui"]["resourceUri"] == law_widget.WIDGET_URI, name
    finally:
        monkeypatch.delenv("OCL_UI_WIDGETS", raising=False)
        importlib.reload(m)


def test_meta_serialises_to_the_wire_alias(monkeypatch):
    """The SDK models it as `meta` with alias `_meta`; clients read `_meta`.
    (Reading t._meta instead of t.meta made this look broken in testing.)"""
    m = _server_with_flag(monkeypatch, "1")
    try:
        [t] = [t for t in m._list_tools() if t.name == "search_laws"]
        wire = t.model_dump(by_alias=True, exclude_none=True)
        assert "_meta" in wire
        assert wire["_meta"]["ui"]["resourceUri"] == law_widget.WIDGET_URI
    finally:
        monkeypatch.delenv("OCL_UI_WIDGETS", raising=False)
        importlib.reload(m)
