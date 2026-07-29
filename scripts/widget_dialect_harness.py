"""Manual harness: render each MCP App widget in a real iframe and observe it.

Run:  .venv/bin/python scripts/widget_dialect_harness.py
Needs: playwright + chromium (.venv/bin/python -m playwright install chromium)

Kept out of the pytest suite so `make test` stays fast; this is the tool for
verifying widget behaviour after any change to a widget or to
widget_runtime.py.

Outbound test: widget inside a REAL iframe, host captures what it sends.
An earlier version of this harness ran the widget as a top-level page, where
window.parent === window, so its postMessage fallback was unreachable — and
that is exactly the path an official MCP Apps host uses. A host renders the
widget in a sandboxed iframe; this reproduces that.

Two host shapes are exercised per widget:
  - a host that injects window.openai (ChatGPT / Apps SDK)
  - a host that injects NOTHING (Claude, VS Code, M365 Copilot) — the case
    where the widget must fall back to the JSON-RPC tools/call dialect.
"""
import json
import sys

sys.path.insert(0, "/Users/jonashertner/caselaw-repo-1")
from playwright.sync_api import sync_playwright  # noqa: E402

import decision_widget  # noqa: E402
import law_widget  # noqa: E402

LAW_PAYLOAD = {"query": "Kündigung", "total": 1, "hits": [
    {"level": "federal", "canton": "CH", "sr_number": "220", "abbreviation": "OR",
     "article_num": "336", "reference": "Art. 336 OR", "title": "Obligationenrecht",
     "heading": "Missbräuchliche Kündigung",
     "snippet_text": "Die Kündigung ist missbräuchlich, wenn...",
     "source_url": "https://www.fedlex.admin.ch/eli/x#art_336", "source_label": "Fedlex"}]}

DECISION_PAYLOAD = {
    "query": "missbräuchliche Kündigung", "query_lang": "de", "total": 342,
    "total_is_lower_bound": False, "attribution": None,
    "decisions": [
        {"decision_id": "bge_BGE_136_III_513", "court": "bge",
         "court_label": "Bundesgericht (BGE)", "level": "federal", "canton": None,
         "docket_number": "136 III 513", "decision_date": "2010-09-14",
         "language": "de", "title": "X. gegen Y. AG",
         "snippet_html": "Die <mark>Kündigung</mark> erweist sich als missbräuchlich",
         "citation_string_de": "BGE 136 III 513",
         "citation_string_fr": "ATF 136 III 513",
         "citation_string_it": "DTF 136 III 513",
         "canonical_url": "https://opencaselaw.ch/entscheid/bge_BGE_136_III_513",
         "pinpoint": {"e_number": "2.3", "confidence": "high",
                      "sentence": "Missbräuchlich ist eine Kündigung, die ...",
                      "url": "https://opencaselaw.ch/entscheid/bge_BGE_136_III_513#e-2-3"}},
        {"decision_id": "ecthr_001", "court": "ecthr_chamber",
         "court_label": "EGMR (Kammer)", "level": "ecthr", "canton": None,
         "docket_number": "30696/09", "decision_date": "2011-01-21",
         "language": "de", "title": "M.S.S. c. Belgique et Grèce",
         "snippet_html": "Art. 3 <mark>EMRK</mark>",
         "citation_string_de": "EGMR, M.S.S. gegen Belgien und Griechenland, Nr. 30696/09",
         "citation_string_fr": "CourEDH, M.S.S. c. Belgique et Grèce, no 30696/09",
         "citation_string_it": "CorteEDU, M.S.S. c. Belgio e Grecia, n. 30696/09",
         "canonical_url": "https://opencaselaw.ch/entscheid/ecthr_001",
         "pinpoint": None},
    ]}

HOST = """<!doctype html><html><body>
<script>
window.captured = [];
window.addEventListener('message', e => {
  if (e.source === document.getElementById('app')?.contentWindow)
    window.captured.push(e.data);
});
</script>
<iframe id="app" sandbox="allow-scripts" srcdoc="__DOC__" style="width:900px;height:600px"></iframe>
</body></html>"""

# Inbound delivery dialects a host may use.
DELIVERIES = {
    "jsonrpc ui/notifications/tool-result":
        "(d) => window.postMessage({jsonrpc:'2.0',"
        "method:'ui/notifications/tool-result',params:{structuredContent:d}}, '*')",
    "mcp-ui postMessage":
        "(d) => window.postMessage({type:'ui-lifecycle-iframe-render-data',"
        "payload:{renderData:d}}, '*')",
    "window.openai.toolOutput global":
        "(d) => { window.openai = window.openai || {}; window.openai.toolOutput = d;"
        "window.dispatchEvent(new Event('openai:set_globals')); }",
}


def run(name, html, payload, click_re, host_provides_openai_api, delivery):
    if host_provides_openai_api:
        shim = ("<script>window.openai={toolOutput:null,"
                "callTool:function(n,a){window.parent.postMessage("
                "{__via:'openai.callTool',name:n,args:a},'*');}};</script>")
        html = html.replace("<head>", "<head>" + shim, 1)
    host = HOST.replace("__DOC__", html.replace('"', "&quot;"))
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page()
        pg.set_content(host, wait_until="load")
        pg.wait_for_timeout(400)
        fr = pg.frames[-1]
        fr.evaluate(DELIVERIES[delivery], payload)
        pg.wait_for_timeout(600)
        txt = fr.evaluate("() => document.body.innerText.slice(0,120)")
        marks = fr.evaluate("() => document.querySelectorAll('mark').length")
        clicked = fr.evaluate("""(re) => {
            const els = [...document.querySelectorAll('button,a,[role=button],[data-act]')];
            const t = els.find(e => new RegExp(re, 'i').test(e.textContent||''));
            if (t) { t.click(); return t.textContent.trim().slice(0,40); }
            return null;
        }""", click_re)
        pg.wait_for_timeout(500)
        cap = pg.evaluate("() => window.captured || []")
        b.close()
    host_label = ("window.openai host (ChatGPT)" if host_provides_openai_api
                  else "NO globals (Claude / VS Code / M365)")
    print(f"\n--- {name} | {host_label} | inbound: {delivery} ---")
    print(f"  rendered : {txt!r}")
    print(f"  <mark> elements rendered: {marks}"
          + ("" if marks else "   <-- highlights are LITERAL TEXT" if "<mark>" in txt else ""))
    print(f"  clicked  : {clicked!r}")
    tool_msgs = [m for m in cap if isinstance(m, dict)
                 and (m.get("method") == "tools/call" or m.get("type") == "tool"
                      or m.get("__via") == "openai.callTool")]
    print(f"  host received {len(cap)} message(s), {len(tool_msgs)} tool request(s):")
    for m in tool_msgs[:4]:
        print(f"    {json.dumps(m, ensure_ascii=False)[:200]}")
    if not tool_msgs:
        print("    (NONE — the host would see no tool request: buttons are inert)")
    return bool(tool_msgs) and bool(clicked) and "<mark>" not in txt


def main():
    ok = True
    for openai_host in (True, False):
        for delivery in DELIVERIES:
            ok &= run("law-search", law_widget.widget_html(), LAW_PAYLOAD,
                      "Volltext|anzeigen|Texte", openai_host, delivery)
            ok &= run("decision-search", decision_widget.widget_html(),
                      DECISION_PAYLOAD, "^Volltext$", openai_host, delivery)
    print("\n==== every host/dialect combination produced a tool request:",
          "YES" if ok else "NO ****", "====")


if __name__ == "__main__":
    main()
