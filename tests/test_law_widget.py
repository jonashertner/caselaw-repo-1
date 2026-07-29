"""Tier B: flag-gated in-client law-search widget (MCP Apps).

Safety contract: with OCL_UI_WIDGETS off (the default) the server advertises
no resources and the law tools carry no _meta, so the MCP surface is identical
to before. With it on, the widget resource is registered and the law tools
declare it via _meta. The widget HTML is self-contained and sandbox-safe.

Spec: docs/superpowers/specs/2026-06-24-cross-provider-law-search-ux-design.md
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import law_widget  # noqa: E402


def test_widget_module_contract():
    meta = law_widget.tool_ui_meta()
    assert meta["ui"]["resourceUri"] == law_widget.WIDGET_URI
    assert meta["openai/outputTemplate"] == law_widget.WIDGET_URI
    assert law_widget.WIDGET_MIME == "text/html;profile=mcp-app"
    html = law_widget.LAW_SEARCH_WIDGET_HTML
    assert html.lstrip().startswith("<!doctype html>")
    # data-source robustness + the host call paths + mark restoration
    assert "window.openai" in html and "postMessage" in html
    # Outbound calls route through one multi-dialect helper (2026-07-29)
    # instead of an inline window.openai.callTool per site: an official MCP
    # Apps host ignored the old {type:"tool"} shape, so the widget rendered
    # and every button was inert there. Same guarantee as before, expressed
    # against the current structure — both tools reachable, all dialects sent.
    assert 'callServerTool("get_law"' in html
    assert 'callServerTool("search_laws"' in html
    assert "window.openai.callTool" in html      # OpenAI / ChatGPT hosts
    assert 'method:"tools/call"' in html          # official MCP Apps hosts
    assert 'type:"tool"' in html                  # MCP-UI hosts
    assert "<mark>" in html
    # Fedlex/LexFind two-style cards + source links + DE/FR/IT/EN labels
    assert "federal" in html and "cantonal" in html
    assert "source_url" in html and "src-link" in html
    assert "Recherche de lois" in html and "Ricerca di leggi" in html
    # sandbox-safe: no external resource loads baked in (source_url is runtime)
    assert "http://" not in html and "https://" not in html
    assert "<script src" not in html and "<link" not in html


def test_flag_off_is_default_and_inert():
    import mcp_server as m
    assert m.OCL_UI_WIDGETS is False, "widget flag must default OFF"
    # The tool _meta is the real on/off switch: with the flag off the law tools
    # advertise no widget, so a fresh client sees plain (highlighted) results.
    assert m._LAW_TOOL_META is None
    # Resource handlers ARE registered (for graceful degradation), so a client
    # that cached the widget reference reads a benign empty panel instead of a
    # hard "failed to load". The empty-panel fallback is defined for off.
    assert m.law_widget is not None


def test_flag_on_registers_widget_and_meta():
    # Import mcp_server in a subprocess with the flag ON so the rest of the
    # suite keeps the default (off) module state.
    code = (
        "import os; os.environ['OCL_UI_WIDGETS']='1'\n"
        "import mcp_server as m, law_widget\n"
        "from mcp.types import ListResourcesRequest, ReadResourceRequest\n"
        "assert m.OCL_UI_WIDGETS is True\n"
        "assert m._LAW_TOOL_META == law_widget.tool_ui_meta()\n"
        "assert ListResourcesRequest in m.server.request_handlers\n"
        "assert ReadResourceRequest in m.server.request_handlers\n"
        "print('WIDGET_ON_OK')\n"
    )
    r = subprocess.run(
        [sys.executable, "-c", code], cwd=str(REPO),
        capture_output=True, text=True, timeout=180,
    )
    assert "WIDGET_ON_OK" in r.stdout, f"stdout={r.stdout!r}\nstderr={r.stderr[-1800:]}"
