"""Decision-search widget (Tier B, flag-gated) and its structured payload.

The safety property that matters most here is the first test: with
OCL_UI_WIDGETS off — production — search_decisions must return exactly what it
returned before, a plain list of TextContent. The widget must never be able to
change the shape of the busiest tool on the server.

The rest pins the payload contract the widget renders from, above all R1: the
citation strings come from _build_citation_strings, so no renderer ever has to
assemble one.
"""
from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import decision_widget  # noqa: E402
import mcp_server  # noqa: E402

def rows() -> list[dict]:
    """Fresh row dicts per call. The dispatcher mutates the rows it is handed
    (strips <mark> from snippet, stashes the marked form), so a module-level
    list would let one test rewrite another test's fixture."""
    return [
        {"decision_id": "bge_BGE_136_III_513", "court": "bge",
         "docket_number": "136 III 513", "decision_date": "2010-09-14",
         "language": "de", "title": "X. gegen Y.",
         "snippet": "Die <mark>Kündigung</mark> ist missbräuchlich"},
        {"decision_id": "ecthr_1", "court": "ecthr_chamber",
         "docket_number": "30696/09", "decision_date": "2011-01-21",
         "language": "de", "title": "M.S.S.", "snippet": "Art. 3 EMRK"},
    ]


# ── the invariant: flag off changes nothing ───────────────────────────────

def test_flag_off_search_decisions_returns_a_plain_list(monkeypatch):
    assert mcp_server.OCL_UI_WIDGETS is False
    assert mcp_server._DECISION_TOOL_META is None
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **k: (rows()[:1], 1))
    monkeypatch.setattr(mcp_server, "_pinpoint_enrich_results", lambda *a, **k: None)
    out = asyncio.run(mcp_server._handle_call_tool_inner(
        "search_decisions", {"query": "Kündigung"}))
    assert isinstance(out, list), "widget wiring changed the default return shape"
    assert out[0].text.startswith("Found 1 decisions")


def test_flag_off_tool_carries_no_widget_meta():
    sd = next(t for t in mcp_server._list_tools() if t.name == "search_decisions")
    assert sd.meta is None


# ── the payload the widget renders from ───────────────────────────────────

def test_payload_citations_come_from_the_builder():
    p = mcp_server._decision_hits_structured(rows(), "Kündigung", "de", total=42)
    assert p["total"] == 42 and len(p["decisions"]) == 2
    bge = p["decisions"][0]
    # R1: identical to what the citation builder produces for the same row
    expected = mcp_server._build_citation_strings(rows()[0])
    assert bge["citation_string_de"] == expected["citation_string_de"] == "BGE 136 III 513"
    assert bge["citation_string_fr"] and bge["citation_string_it"]
    assert bge["canonical_url"].endswith("/entscheid/bge_BGE_136_III_513")
    assert bge["level"] == "federal"        # renderer contract: 3 values
    assert bge["court_level"] == "federal_supreme"   # corpus taxonomy, kept


def test_ecthr_rows_flag_their_level_and_carry_attribution():
    p = mcp_server._decision_hits_structured(rows(), "EMRK", "de")
    # COURT_LEVELS has no ECtHR entry and defaults to "cantonal"; the widget
    # level must not inherit that, or Strasbourg renders as a cantonal court.
    assert mcp_server._get_court_level("ecthr_chamber") == "cantonal"
    assert p["decisions"][1]["level"] == "ecthr"
    assert p["attribution"] == mcp_server._ECHR_ATTRIBUTION


def test_no_ecthr_row_means_no_attribution():
    p = mcp_server._decision_hits_structured(rows()[:1], "Kündigung", "de")
    assert p["attribution"] is None


def test_marked_snippet_survives_the_llm_strip(monkeypatch):
    """The text path strips <mark> for LLM consumers; the widget payload keeps
    the highlights FTS5 actually matched."""
    r = rows()[:1]
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **k: (r, 1))
    monkeypatch.setattr(mcp_server, "_pinpoint_enrich_results", lambda *a, **k: None)
    monkeypatch.setattr(mcp_server, "_DECISION_TOOL_META", decision_widget.tool_ui_meta())
    out = asyncio.run(mcp_server._handle_call_tool_inner(
        "search_decisions", {"query": "Kündigung"}))
    assert isinstance(out, tuple), "flag on must add structuredContent"
    content, structured = out
    assert "<mark>" not in content[0].text          # text path unchanged
    assert "<mark>" in structured["decisions"][0]["snippet_html"]


def test_unbalanced_marks_are_stripped_not_emitted():
    r = [dict(rows()[0], snippet="Die <mark>Kündigung ist abgeschnitten")]
    p = mcp_server._decision_hits_structured(r, "Kündigung", "de")
    assert "<mark>" not in p["decisions"][0]["snippet_html"]


def test_zero_results_payload_is_well_formed():
    p = mcp_server._decision_hits_structured([], "nichts", "de", total=0)
    assert p["decisions"] == [] and p["total"] == 0 and p["attribution"] is None


# ── the widget document ───────────────────────────────────────────────────

def test_widget_document_contract():
    html = decision_widget.widget_html()
    assert html.lstrip().startswith("<!doctype html>")
    assert decision_widget.WIDGET_MIME == "text/html;profile=mcp-app"
    assert decision_widget.tool_ui_meta()["ui"]["resourceUri"] == decision_widget.WIDGET_URI
    # all three outbound host dialects present (see widget_runtime)
    assert "window.openai.callTool" in html
    assert 'method:"tools/call"' in html
    assert 'type:"tool"' in html
    # the three actions the card offers
    assert 'callServerTool("get_decision"' in html
    assert 'callServerTool("get_erwaegung"' in html
    assert 'callServerTool("search_decisions"' in html
    assert "copyText(citeOf(d)" in html
    # R1: the widget reads a citation field, never builds one
    assert 'd["citation_string_" + UI_LANG]' in html
    # localisation + attribution surface
    assert "Entscheidsuche" in html and "Recherche d'arrêts" in html
    assert "DATA.attribution" in html
    # sandbox-safe: no external resource loads baked in
    assert "http://" not in html and "https://" not in html
    assert "<script src" not in html and "<link" not in html


def test_highlight_restoration_survives_srcdoc_decoding():
    """A host may inject the widget via an iframe srcdoc attribute, which
    HTML-decodes the document first. Any &lt; entity in the JS would become a
    bare < and silently break highlight restoration, rendering '<mark>' as
    literal text. Guard: the executable JS carries no such entities."""
    import re
    for html in (decision_widget.widget_html(),
                 __import__("law_widget").widget_html()):
        js = html.split("<script>", 1)[1].split("</script>", 1)[0]
        code = "\n".join(ln for ln in js.splitlines()
                         if not ln.strip().startswith("//"))
        assert not re.search(r"&(lt|gt|amp|quot|#\d+);", code), \
            "HTML entity in widget JS breaks under srcdoc injection"


def test_flag_on_registers_both_widgets():
    code = (
        "import os; os.environ['OCL_UI_WIDGETS']='1'\n"
        "import asyncio, mcp_server as m, law_widget, decision_widget\n"
        "assert m._DECISION_TOOL_META == decision_widget.tool_ui_meta()\n"
        "res = asyncio.run(m._list_ui_resources())\n"
        "uris = {str(r.uri).rstrip('/') for r in res}\n"
        "assert uris == {law_widget.WIDGET_URI, decision_widget.WIDGET_URI}, uris\n"
        "got = asyncio.run(m._read_ui_resource(decision_widget.WIDGET_URI))\n"
        "assert 'Entscheidsuche' in got[0].content\n"
        "sd = next(t for t in m._list_tools() if t.name == 'search_decisions')\n"
        "assert sd.meta == decision_widget.tool_ui_meta()\n"
        "print('BOTH_WIDGETS_OK')\n"
    )
    r = subprocess.run([sys.executable, "-c", code], cwd=str(REPO),
                       capture_output=True, text=True, timeout=180)
    assert "BOTH_WIDGETS_OK" in r.stdout, f"stdout={r.stdout!r}\nstderr={r.stderr[-1800:]}"
