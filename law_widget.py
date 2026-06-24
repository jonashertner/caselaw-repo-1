"""Self-contained law-search widget for MCP-Apps capable clients (Claude;
best-effort for the ChatGPT Apps SDK). Rendered from the search_laws /
search_legislation `structuredContent` (the LawHit list).

Design: federal hits follow the Fedlex visual identity (white, Swiss-red
accent), cantonal hits follow LexFind (white, deep-teal accent). Each card
links to the original source (Fedlex for federal, LexFind for cantonal) and a
"full text" action that calls get_law in an MCP host or opens the source page
standalone. Labels are localised DE/FR/IT/EN and a language toggle re-searches.

Flag-gated in mcp_server.py behind OCL_UI_WIDGETS (off by default). No external
resources (sandbox / CSP safe); snippet HTML is escaped, then only <mark> is
re-allowed. Spec: docs/superpowers/specs/2026-06-24-cross-provider-law-search-ux-design.md
"""

WIDGET_URI = "ui://opencaselaw/law-search"
WIDGET_NAME = "OpenCaseLaw law search"
WIDGET_MIME = "text/html;profile=mcp-app"


def tool_ui_meta() -> dict:
    """The `_meta` a law tool sets to declare its UI (MCP Apps + best-effort OpenAI)."""
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
  :root {
    --bg:#f4f4f2; --card:#ffffff; --ink:#1d1d1b; --muted:#5f6368; --line:#e2e0da;
    --fed:#dc0018; --fed-soft:#fbe7e9; --can:#0e4d6b; --can-soft:#e3eef3; --mark:#fff3a3;
  }
  * { box-sizing:border-box; }
  body { margin:0; padding:14px; color:var(--ink); background:var(--bg);
         font:14.5px/1.55 "Helvetica Neue",-apple-system,Segoe UI,Roboto,Arial,sans-serif; }
  .hd { display:flex; flex-wrap:wrap; align-items:baseline; gap:6px 10px; margin:0 0 13px;
        padding-bottom:9px; border-bottom:2px solid var(--ink); }
  .hd h1 { font-size:16px; font-weight:700; margin:0; letter-spacing:.01em; }
  .hd .count { color:var(--muted); font-size:13px; }
  .langs { margin-left:auto; display:flex; gap:3px; }
  .langs button { border:1px solid var(--line); background:var(--card); color:var(--muted);
        border-radius:5px; padding:2px 8px; font-size:12px; cursor:pointer; }
  .langs button[aria-pressed="true"] { color:#fff; background:var(--ink); border-color:var(--ink); font-weight:600; }
  .card { background:var(--card); border:1px solid var(--line); border-left-width:4px;
          border-radius:7px; padding:11px 14px; margin:0 0 9px; }
  .card.federal  { border-left-color:var(--fed); }
  .card.cantonal { border-left-color:var(--can); }
  .top { display:flex; align-items:baseline; gap:8px; }
  .ref { font-weight:700; font-size:14.5px; }
  .card.federal  .ref { color:var(--fed); }
  .card.cantonal .ref { color:var(--can); }
  .src-badge { margin-left:auto; font-size:10.5px; font-weight:600; letter-spacing:.03em;
               text-transform:uppercase; padding:1px 7px; border-radius:10px; white-space:nowrap; }
  .card.federal  .src-badge { color:var(--fed); background:var(--fed-soft); }
  .card.cantonal .src-badge { color:var(--can); background:var(--can-soft); }
  .title { color:var(--muted); font-size:12.5px; margin:2px 0 7px; }
  .snip { margin:0 0 9px; }
  .snip mark { background:var(--mark); color:inherit; border-radius:2px; padding:0 1px; }
  .acts { display:flex; align-items:center; gap:14px; }
  .vt { border:1px solid; background:transparent; border-radius:6px; padding:4px 11px;
        font-size:12.5px; font-weight:600; cursor:pointer; }
  .card.federal  .vt { color:var(--fed); border-color:var(--fed); }
  .card.federal  .vt:hover { background:var(--fed); color:#fff; }
  .card.cantonal .vt { color:var(--can); border-color:var(--can); }
  .card.cantonal .vt:hover { background:var(--can); color:#fff; }
  .src-link { font-size:12.5px; text-decoration:none; }
  .card.federal  .src-link { color:var(--fed); }
  .card.cantonal .src-link { color:var(--can); }
  .src-link:hover { text-decoration:underline; }
  .empty { color:var(--muted); padding:22px; text-align:center; }
</style>
</head>
<body>
<div id="app"><div class="empty">Lade Resultate...</div></div>
<script>
(function () {
  var DATA = null, UI_LANG = "de";
  var LABELS = {
    de: { title:"Gesetzessuche",     art:"Artikel",  full:"Volltext anzeigen", src:"Quelle",  none:"Keine Resultate." },
    fr: { title:"Recherche de lois",  art:"articles", full:"Texte intégral", src:"Source",  none:"Aucun résultat." },
    it: { title:"Ricerca di leggi",   art:"articoli", full:"Testo integrale",   src:"Fonte",   none:"Nessun risultato." },
    en: { title:"Law search",         art:"articles", full:"Full text",         src:"Source",  none:"No results." }
  };
  function L() { return LABELS[UI_LANG] || LABELS.de; }

  // Locate the structuredContent (the object carrying .hits) anywhere inside a
  // global or a host message, regardless of nesting/wrapper shape.
  function findHits(o, depth) {
    if (!o || typeof o !== "object" || depth > 5) return null;
    if (Array.isArray(o.hits)) return o;
    for (var k in o) { try { var r = findHits(o[k], depth + 1); if (r) return r; } catch (e) {} }
    return null;
  }
  function applyData(out) {
    if (!out || !out.hits) return false;
    DATA = out; if (out.query_lang) UI_LANG = out.query_lang; render(); return true;
  }
  function fromGlobals() {
    var srcs = [];
    try { if (window.openai && window.openai.toolOutput) srcs.push(window.openai.toolOutput); } catch (e) {}
    srcs.push(window.__TOOL_OUTPUT__, window.structuredContent, window.toolOutput);
    for (var i = 0; i < srcs.length; i++) { var h = findHits(srcs[i], 0); if (h) return h; }
    return null;
  }
  function esc(s) { var d = document.createElement("div"); d.textContent = s == null ? "" : String(s); return d.innerHTML; }
  function safeSnippet(html) {
    return esc(html == null ? "" : String(html))
      .replace(/&lt;mark&gt;/g, "<mark>").replace(/&lt;\/mark&gt;/g, "</mark>");
  }
  function fullArgs(h) {
    var a = { language: UI_LANG };
    if (h.level === "cantonal" && h.canton) a.canton = h.canton;
    if (h.sr_number) a.sr_number = h.sr_number;
    if (h.systematic_number) a.systematic_number = h.systematic_number;
    if (h.article_num) a.article = h.article_num;
    return a;
  }
  function onFull(h) {
    var a = fullArgs(h);
    try { if (window.openai && window.openai.callTool) { window.openai.callTool("get_law", a); return; } } catch (e) {}
    try { if (window.parent && window.parent !== window) { window.parent.postMessage({ type:"tool", payload:{ toolName:"get_law", params:a } }, "*"); return; } } catch (e) {}
    if (h.source_url) window.open(h.source_url, "_blank", "noopener");
  }
  function reSearch(lang) {
    UI_LANG = lang;
    var q = (DATA && DATA.query) || "";
    try { if (window.openai && window.openai.callTool) window.openai.callTool("search_laws", { query:q, language:lang, limit:8 });
          else if (window.parent && window.parent !== window) window.parent.postMessage({ type:"tool", payload:{ toolName:"search_laws", params:{ query:q, language:lang, limit:8 } } }, "*"); } catch (e) {}
    render();
  }

  function render() {
    var app = document.getElementById("app");
    if (!DATA || !DATA.hits || !DATA.hits.length) { app.innerHTML = '<div class="empty">' + esc(L().none) + "</div>"; return; }
    var hits = DATA.hits, lab = L();
    var html = '<div class="hd"><h1>' + esc(lab.title) + "</h1>"
      + '<span class="count">' + esc(DATA.total != null ? DATA.total : hits.length) + " " + esc(lab.art) + "</span>"
      + '<span class="langs">'
      + ["de","fr","it","en"].map(function (l) { return '<button data-lang="' + l + '" aria-pressed="' + (l === UI_LANG) + '">' + l.toUpperCase() + "</button>"; }).join("")
      + "</span></div>";
    html += hits.map(function (h, i) {
      var cls = h.level === "cantonal" ? "cantonal" : "federal";
      var badge = h.source_label || (cls === "cantonal" ? "LexFind" : "Fedlex");
      var srcLink = h.source_url ? '<a class="src-link" href="' + esc(h.source_url) + '" target="_blank" rel="noopener">' + esc(lab.src) + ": " + esc(badge) + " ↗</a>" : "";
      return '<div class="card ' + cls + '">'
        + '<div class="top"><span class="ref">' + esc(h.reference || ("#" + (i + 1))) + "</span>"
        + '<span class="src-badge">' + esc(badge) + "</span></div>"
        + (h.title ? '<div class="title">' + esc(h.title) + "</div>" : "")
        + '<div class="snip">' + safeSnippet(h.snippet_html || h.snippet_text || "") + "</div>"
        + '<div class="acts"><button class="vt" data-i="' + i + '">' + esc(lab.full) + "</button>" + srcLink + "</div>"
        + "</div>";
    }).join("");
    app.innerHTML = html;
    Array.prototype.forEach.call(app.querySelectorAll(".langs button"), function (b) { b.onclick = function () { reSearch(b.getAttribute("data-lang")); }; });
    Array.prototype.forEach.call(app.querySelectorAll(".vt"), function (b) { b.onclick = function () { onFull(hits[+b.getAttribute("data-i")]); }; });
  }

  function boot() { return applyData(fromGlobals()); }

  // Inbound data arrives as: a global (window.openai.toolOutput), the
  // openai:set_globals event, OR a postMessage (MCP-UI / a JSON-RPC
  // ui/notifications/tool-result notification). findHits handles any shape.
  window.addEventListener("message", function (e) {
    var d = e && e.data; if (d == null) return;
    if (typeof d === "string") { try { d = JSON.parse(d); } catch (x) { return; } }
    applyData(findHits(d, 0));
  });
  window.addEventListener("openai:set_globals", boot);
  document.addEventListener("DOMContentLoaded", boot);

  // Some MCP-UI hosts only send the render data after the iframe announces it
  // is ready; post a few common ready signals (harmless if unused).
  ["ui-lifecycle-iframe-ready", "iframe-ready", "mcp-ui-ready", "ready"].forEach(function (t) {
    try { if (window.parent && window.parent !== window) window.parent.postMessage({ type: t }, "*"); } catch (e) {}
  });

  // Poll briefly for late-injected globals (host may set window.openai after load).
  var _tries = 0;
  (function poll() { if (boot()) return; if (_tries++ < 40) setTimeout(poll, 200); })();
})();
</script>
</body>
</html>
"""
