"""Manual harness: render the MCP App widget in a real iframe and observe it.

Run:  .venv/bin/python scripts/widget_dialect_harness.py
Needs: playwright + chromium (.venv/bin/python -m playwright install chromium)

Kept out of the pytest suite so `make test` stays fast; this is the tool
for verifying widget behaviour after any change to law_widget.py.

Outbound test: widget inside a REAL iframe, host captures what it sends.

The earlier harness ran the widget as a top-level page, where
window.parent === window, so its postMessage fallback was unreachable.
A host renders it in a sandboxed iframe — this reproduces that.
"""
import json, sys
sys.path.insert(0, "/Users/jonashertner/caselaw-repo-1")
from playwright.sync_api import sync_playwright
import law_widget

PAYLOAD = {"query": "Kündigung", "total": 1, "hits": [
    {"level": "federal", "canton": "CH", "sr_number": "220", "abbreviation": "OR",
     "article_num": "336", "reference": "Art. 336 OR", "title": "Obligationenrecht",
     "heading": "Missbräuchliche Kündigung",
     "snippet_text": "Die Kündigung ist missbräuchlich, wenn...",
     "source_url": "https://www.fedlex.admin.ch/eli/x#art_336", "source_label": "Fedlex"}]}

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


def run(host_provides_openai_api: bool):
    doc = law_widget.LAW_SEARCH_WIDGET_HTML
    if host_provides_openai_api:
        shim = ("<script>window.openai={toolOutput:null,"
                "callTool:function(n,a){window.parent.postMessage("
                "{__via:'openai.callTool',name:n,args:a},'*');}};</script>")
        doc = doc.replace("<head>", "<head>" + shim, 1)
    host = HOST.replace("__DOC__", doc.replace('"', "&quot;"))
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page()
        pg.set_content(host, wait_until="load")
        pg.wait_for_timeout(400)
        fr = pg.frame(name=None, url=lambda u: True) or pg.frames[-1]
        fr = pg.frames[-1]
        # deliver results the way a host would
        fr.evaluate("(d) => window.postMessage({jsonrpc:'2.0',"
                    "method:'ui/notifications/tool-result',"
                    "params:{structuredContent:d}}, '*')", PAYLOAD)
        pg.wait_for_timeout(600)
        txt = fr.evaluate("() => document.body.innerText.slice(0,80)")
        # now ACT: click the first in-widget action
        clicked = fr.evaluate("""() => {
            const els = [...document.querySelectorAll('button,a,[role=button],[data-act]')];
            const t = els.find(e => /Volltext|anzeigen|Suchen/i.test(e.textContent||''));
            if (t) { t.click(); return t.textContent.trim().slice(0,40); }
            return null;
        }""")
        pg.wait_for_timeout(500)
        cap = pg.evaluate("() => window.captured || []")
        b.close()
    label = "host provides window.openai.callTool" if host_provides_openai_api else "host provides NOTHING (official MCP Apps host)"
    print(f"\n--- {label} ---")
    print(f"  widget rendered: {txt!r}")
    print(f"  clicked: {clicked!r}")
    print(f"  messages the HOST received: {len(cap)}")
    for m in cap[:5]:
        print(f"    {json.dumps(m)[:190]}")
    if not cap:
        print("    (nothing — the host would see no tool request at all)")

run(host_provides_openai_api=True)
run(host_provides_openai_api=False)
