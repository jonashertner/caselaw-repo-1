"""The MCP App widget must speak every host dialect (2026-07-29).

Verified empirically with a Playwright iframe harness
(scripts/widget_dialect_harness.py), not by reading the code:

  INBOUND  — window.openai.toolOutput, MCP-UI postMessage, and the official
             ui/ JSON-RPC notification all render results correctly.
  OUTBOUND — before this was fixed, a click against an official MCP Apps
             host emitted only {type:"tool"}, which such a host ignores.
             The widget rendered, showed results, and every button was
             inert — worse for the user than showing no widget at all.

The three outbound shapes are mutually unrecognisable (no `type` vs no
`jsonrpc`), so a host acts on exactly one and ignores the others.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import law_widget  # noqa: E402

HTML = law_widget.LAW_SEARCH_WIDGET_HTML


def test_outbound_speaks_all_three_dialects():
    assert "window.openai.callTool" in HTML, "OpenAI/ChatGPT hosts"
    assert '"tools/call"' in HTML or "method:\"tools/call\"" in HTML, \
        "official MCP Apps hosts (Claude, VS Code, M365)"
    assert 'type:"tool"' in HTML, "MCP-UI hosts"


def test_outbound_goes_through_one_helper():
    """Every call site must use callServerTool — a bespoke postMessage at a
    new call site is how one dialect silently gets dropped again."""
    assert "function callServerTool(" in HTML
    assert HTML.count("callServerTool(") >= 3          # definition + 2 uses
    # no call site may hand-roll the legacy shape any more
    assert HTML.count('postMessage({ type:"tool"') <= 1


def test_jsonrpc_envelope_is_well_formed():
    # anchor on the CODE form; the explanatory comment also says "tools/call"
    i = HTML.index('method:"tools/call"')
    frag = HTML[max(0, i - 200):i + 200]
    assert 'jsonrpc:"2.0"' in frag
    assert "params:{ name:name, arguments:args }" in frag
    assert "id:(++__rpcId)" in frag, "each request needs a distinct id"


def test_inbound_still_accepts_every_shape():
    assert "window.openai" in HTML                      # global / set_globals
    assert "openai:set_globals" in HTML
    assert "addEventListener" in HTML and "message" in HTML


def test_widget_stays_self_contained():
    """Apps run under a deny-by-default CSP; external refs would need an
    explicit _meta.ui.csp entry."""
    import re
    assert not re.findall(r'(?:src|href)=["\']https?://', HTML)
