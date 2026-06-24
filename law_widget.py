"""Self-contained law-search widget for MCP-Apps capable clients (Claude;
best-effort for the ChatGPT Apps SDK). It is rendered from the
search_laws / search_legislation `structuredContent` (the LawHit list) that
A3 already returns. Flag-gated in mcp_server.py behind OCL_UI_WIDGETS (off by
default), so when off the server's MCP surface is unchanged.

No external resources (sandbox / CSP safe): all CSS and JS are inline, and the
snippet HTML is escaped before only <mark>...</mark> is re-allowed.

Spec: docs/superpowers/specs/2026-06-24-cross-provider-law-search-ux-design.md
"""

WIDGET_URI = "ui://opencaselaw/law-search"
WIDGET_NAME = "OpenCaseLaw law search"
WIDGET_MIME = "text/html;profile=mcp-app"


def tool_ui_meta() -> dict:
    """The `_meta` a law tool sets to declare its UI. Carries the MCP-Apps key
    (Claude) plus a best-effort OpenAI Apps SDK key (ChatGPT)."""
    return {
        "ui": {"resourceUri": WIDGET_URI},
        "openai/outputTemplate": WIDGET_URI,
    }


LAW_SEARCH_WIDGET_HTML = r"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { --bg:#fbfaf7; --card:#fff; --ink:#1a1a1a; --muted:#6b6b6b;
          --line:#e7e3da; --accent:#7a1f2b; --mark:#fff2a8; }
  * { box-sizing:border-box; }
  body { margin:0; padding:14px; color:var(--ink); background:var(--bg);
         font:15px/1.55 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; }
  .hd { display:flex; align-items:baseline; gap:10px; margin:0 0 12px; }
  .hd h1 { font-size:15px; font-weight:700; margin:0; }
  .hd .count { color:var(--muted); font-size:13px; }
  .langs { margin-left:auto; display:flex; gap:4px; }
  .langs button { border:1px solid var(--line); background:var(--card); color:var(--muted);
         border-radius:6px; padding:2px 8px; font-size:12px; cursor:pointer; }
  .langs button[aria-pressed="true"] { color:var(--accent); border-color:var(--accent); font-weight:600; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px;
          padding:12px 14px; margin:0 0 10px; }
  .ref { font-weight:700; color:var(--accent); font-size:14px; }
  .badge { display:inline-block; font-size:11px; color:var(--muted); border:1px solid var(--line);
           border-radius:5px; padding:0 6px; margin-left:8px; }
  .title { color:var(--muted); font-size:13px; margin:1px 0 7px; }
  .snip { margin:0 0 9px; }
  .snip mark { background:var(--mark); color:inherit; border-radius:2px; padding:0 1px; }
  .act { border:1px solid var(--line); background:var(--bg); color:var(--accent);
         border-radius:7px; padding:4px 10px; font-size:12px; cursor:pointer; }
  .act:hover { background:#f3efe6; }
  .empty { color:var(--muted); padding:20px; text-align:center; }
</style>
</head>
<body>
<div id="app"><div class="empty">Lade Resultate...</div></div>
<script>
(function () {
  var DATA = null, LANG = "de";

  function pickData() {
    try { if (window.openai && window.openai.toolOutput) return window.openai.toolOutput; } catch (e) {}
    if (window.__TOOL_OUTPUT__) return window.__TOOL_OUTPUT__;
    if (window.structuredContent) return window.structuredContent;
    return null;
  }
  function esc(s) { var d = document.createElement("div"); d.textContent = s == null ? "" : String(s); return d.innerHTML; }
  // snippet_html is server-produced and only ever wraps verbatim text in
  // <mark>; escape everything, then re-allow just the mark tags.
  function safeSnippet(html) {
    return esc(html == null ? "" : String(html))
      .replace(/&lt;mark&gt;/g, "<mark>").replace(/&lt;\/mark&gt;/g, "</mark>");
  }
  function callTool(name, args) {
    try { if (window.openai && window.openai.callTool) { window.openai.callTool(name, args); return; } } catch (e) {}
    try { window.parent.postMessage({ type: "tool", payload: { toolName: name, params: args } }, "*"); } catch (e) {}
  }
  function fullTextArgs(h) {
    var a = { language: LANG };
    if (h.level === "cantonal" && h.canton) a.canton = h.canton;
    if (h.sr_number) a.sr_number = h.sr_number;
    if (h.systematic_number) a.systematic_number = h.systematic_number;
    if (h.article_num) a.article = h.article_num;
    return a;
  }
  function reSearch(lang) {
    LANG = lang;
    callTool("search_laws", { query: (DATA && DATA.query) || "", language: lang, limit: 8 });
    render();
  }
  function render() {
    var app = document.getElementById("app");
    if (!DATA || !DATA.hits || !DATA.hits.length) {
      app.innerHTML = '<div class="empty">Keine Resultate.</div>'; return;
    }
    var hits = DATA.hits;
    var html = '<div class="hd"><h1>Gesetzessuche</h1>'
      + '<span class="count">' + esc(DATA.total != null ? DATA.total : hits.length) + ' Artikel</span>'
      + '<span class="langs">'
      + ["de", "fr", "it", "en"].map(function (l) {
          return '<button data-lang="' + l + '" aria-pressed="' + (l === LANG) + '">' + l.toUpperCase() + "</button>";
        }).join("")
      + "</span></div>";
    html += hits.map(function (h, i) {
      var badge = (h.canton && h.canton !== "CH") ? h.canton : (h.sr_number ? "SR " + esc(h.sr_number) : "CH");
      return '<div class="card">'
        + '<div><span class="ref">' + esc(h.reference || ("#" + (i + 1))) + "</span>"
        + '<span class="badge">' + esc(badge) + "</span></div>"
        + (h.title ? '<div class="title">' + esc(h.title) + "</div>" : "")
        + '<div class="snip">' + safeSnippet(h.snippet_html || h.snippet_text || "") + "</div>"
        + '<button class="act" data-i="' + i + '">Volltext anzeigen</button>'
        + "</div>";
    }).join("");
    app.innerHTML = html;
    Array.prototype.forEach.call(app.querySelectorAll(".langs button"), function (b) {
      b.onclick = function () { reSearch(b.getAttribute("data-lang")); };
    });
    Array.prototype.forEach.call(app.querySelectorAll(".act"), function (b) {
      b.onclick = function () { callTool("get_law", fullTextArgs(hits[+b.getAttribute("data-i")])); };
    });
  }
  function boot() {
    var d = pickData();
    if (d && d.hits) { DATA = d; render(); }
  }
  window.addEventListener("message", function (e) {
    var d = e && e.data; if (!d) return;
    var out = d.toolOutput || d.structuredContent || (d.type === "tool-output" ? d.payload : null);
    if (out && out.hits) { DATA = out; render(); }
  });
  window.addEventListener("openai:set_globals", boot);
  document.addEventListener("DOMContentLoaded", boot);
  boot();
})();
</script>
</body>
</html>
"""
