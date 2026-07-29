"""Self-contained decision-search widget for MCP-Apps capable clients.

Rendered from the `search_decisions` structuredContent. This is the widget over
the traffic that matters: get_decision is ~90% of all tool calls, and every one
of them starts as a search result the user then asks the model to open.

What it buys the user, none of which costs a model turn:
  - the citation, verbatim, one click into the clipboard (R1 made physical:
    the string is copied from citation_string_de/fr/it, never assembled here);
  - the pinpointed Erwägung, when search found one, as its own button;
  - the full text, on the decision the user actually wants, without a
    round-trip through the model to name a decision_id.

Design: federal courts carry the Swiss-red accent, cantonal courts deep teal,
ECtHR the Council-of-Europe blue with the © ECHR-CEDH attribution pinned to the
footer (a reuse condition, not decoration — it must ride with the text).

Flag-gated in mcp_server.py behind OCL_UI_WIDGETS (off by default). No external
resources (sandbox / CSP safe); snippet HTML is escaped, then only <mark> is
re-allowed. Host-facing plumbing is shared with the law widget in
widget_runtime.py.
"""

from widget_runtime import runtime_js

WIDGET_URI = "ui://opencaselaw/decision-search"
WIDGET_NAME = "OpenCaseLaw decision search"
WIDGET_MIME = "text/html;profile=mcp-app"
WIDGET_DESCRIPTION = (
    "Interactive decision-search results: verbatim citation to clipboard, "
    "pinpointed Erwaegung, full text on click.")


def widget_html() -> str:
    """The rendered widget document. Uniform accessor across widget modules so
    the server can register them from one list."""
    return DECISION_SEARCH_WIDGET_HTML


def tool_ui_meta() -> dict:
    """The `_meta` search_decisions sets to declare its UI (MCP Apps +
    best-effort OpenAI)."""
    return {
        "ui": {"resourceUri": WIDGET_URI},
        "openai/outputTemplate": WIDGET_URI,
    }


_DECISION_SEARCH_WIDGET_TEMPLATE = r"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root {
    --bg:#f4f4f2; --card:#ffffff; --ink:#1d1d1b; --muted:#5f6368; --line:#e2e0da;
    --fed:#dc0018; --fed-soft:#fbe7e9; --can:#0e4d6b; --can-soft:#e3eef3;
    --ecthr:#003399; --ecthr-soft:#e5eaf7; --mark:#fff3a3; --pin:#8a6d00; --pin-soft:#fdf6dd;
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
  .card.federal { border-left-color:var(--fed); }
  .card.cantonal { border-left-color:var(--can); }
  .card.ecthr   { border-left-color:var(--ecthr); }
  .top { display:flex; align-items:baseline; gap:8px; flex-wrap:wrap; }
  .cite { font-weight:700; font-size:14.5px; }
  .card.federal  .cite { color:var(--fed); }
  .card.cantonal .cite { color:var(--can); }
  .card.ecthr    .cite { color:var(--ecthr); }
  .src-badge { margin-left:auto; font-size:10.5px; font-weight:600; letter-spacing:.03em;
               text-transform:uppercase; padding:1px 7px; border-radius:10px; white-space:nowrap; }
  .card.federal  .src-badge { color:var(--fed); background:var(--fed-soft); }
  .card.cantonal .src-badge { color:var(--can); background:var(--can-soft); }
  .card.ecthr    .src-badge { color:var(--ecthr); background:var(--ecthr-soft); }
  .meta { color:var(--muted); font-size:12px; margin:2px 0 0; }
  .title { color:var(--muted); font-size:12.5px; margin:3px 0 7px; }
  .snip { margin:0 0 9px; }
  .snip mark { background:var(--mark); color:inherit; border-radius:2px; padding:0 1px; }
  .pin { background:var(--pin-soft); border-radius:5px; padding:6px 9px; margin:0 0 9px;
         font-size:13px; }
  .pin .pin-ref { color:var(--pin); font-weight:700; }
  .pin .pin-conf { color:var(--muted); font-size:11.5px; }
  .acts { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .btn { border:1px solid; background:transparent; border-radius:6px; padding:4px 11px;
         font-size:12.5px; font-weight:600; cursor:pointer; }
  .card.federal  .btn { color:var(--fed); border-color:var(--fed); }
  .card.federal  .btn:hover { background:var(--fed); color:#fff; }
  .card.cantonal .btn { color:var(--can); border-color:var(--can); }
  .card.cantonal .btn:hover { background:var(--can); color:#fff; }
  .card.ecthr    .btn { color:var(--ecthr); border-color:var(--ecthr); }
  .card.ecthr    .btn:hover { background:var(--ecthr); color:#fff; }
  .btn.copied { background:var(--ink) !important; color:#fff !important; border-color:var(--ink) !important; }
  .src-link { font-size:12.5px; text-decoration:none; margin-left:auto; }
  .card.federal  .src-link { color:var(--fed); }
  .card.cantonal .src-link { color:var(--can); }
  .card.ecthr    .src-link { color:var(--ecthr); }
  .src-link:hover { text-decoration:underline; }
  .attr { color:var(--muted); font-size:11.5px; margin:12px 0 0; padding-top:8px;
          border-top:1px solid var(--line); }
  .empty { color:var(--muted); padding:22px; text-align:center; }
</style>
</head>
<body>
<div id="app"><div class="empty">Lade Resultate...</div></div>
<script>
__RUNTIME__

(function () {
  var DATA = null, UI_LANG = "de";
  var LABELS = {
    de: { title:"Entscheidsuche", n:"Entscheide", full:"Volltext", erw:"Erwägung",
          copy:"Zitat kopieren", copied:"Kopiert", src:"Quelle", none:"Keine Resultate." },
    fr: { title:"Recherche d'arrêts", n:"arrêts", full:"Texte intégral", erw:"Considérant",
          copy:"Copier la référence", copied:"Copié", src:"Source", none:"Aucun résultat." },
    it: { title:"Ricerca di sentenze", n:"sentenze", full:"Testo integrale", erw:"Considerando",
          copy:"Copia la citazione", copied:"Copiato", src:"Fonte", none:"Nessun risultato." },
    en: { title:"Decision search", n:"decisions", full:"Full text", erw:"Consideration",
          copy:"Copy citation", copied:"Copied", src:"Source", none:"No results." }
  };
  function L() { return LABELS[UI_LANG] || LABELS.de; }

  // R1: the citation is whatever the server sent for this language. Never
  // assembled here, and never falls through to a docket number dressed up as
  // a citation — if the language variant is missing we use the German one,
  // which is always present.
  function citeOf(d) {
    return d["citation_string_" + UI_LANG] || d.citation_string_de || d.docket_number || "";
  }

  function applyData(out) {
    if (!out || !out.decisions) return false;
    DATA = out; if (out.query_lang) UI_LANG = out.query_lang; render(); return true;
  }

  function onFull(d) {
    if (d.decision_id && callServerTool("get_decision", { decision_id: d.decision_id })) return;
    if (d.canonical_url) window.open(d.canonical_url, "_blank", "noopener");
  }
  function onErw(d) {
    var pp = d.pinpoint || {};
    if (d.decision_id && pp.e_number
        && callServerTool("get_erwaegung", { decision_id: d.decision_id, e_number: String(pp.e_number) })) return;
    if (pp.url) window.open(pp.url, "_blank", "noopener");
  }
  function onCopy(d, btn) {
    var lab = L();
    copyText(citeOf(d), function (ok) {
      if (!ok) return;
      btn.textContent = lab.copied; btn.classList.add("copied");
      setTimeout(function () { btn.textContent = lab.copy; btn.classList.remove("copied"); }, 1600);
    });
  }
  function reSearch(lang) {
    UI_LANG = lang;
    var q = (DATA && DATA.query) || "";
    callServerTool("search_decisions", { query:q, language:lang, limit:10 });
    render();
  }

  function render() {
    var app = document.getElementById("app"), lab = L();
    if (!DATA || !DATA.decisions || !DATA.decisions.length) {
      app.innerHTML = '<div class="empty">' + esc(lab.none) + "</div>"; return;
    }
    var ds = DATA.decisions;
    var total = DATA.total != null ? DATA.total : ds.length;
    var html = '<div class="hd"><h1>' + esc(lab.title) + "</h1>"
      + '<span class="count">' + esc(total) + (DATA.total_is_lower_bound ? "+" : "")
      + " " + esc(lab.n) + "</span>"
      + '<span class="langs">'
      + ["de","fr","it","en"].map(function (l) { return '<button data-lang="' + l + '" aria-pressed="' + (l === UI_LANG) + '">' + l.toUpperCase() + "</button>"; }).join("")
      + "</span></div>";

    html += ds.map(function (d, i) {
      var cls = d.level === "cantonal" ? "cantonal" : (d.level === "ecthr" ? "ecthr" : "federal");
      var pp = d.pinpoint;
      var srcLink = d.canonical_url
        ? '<a class="src-link" href="' + esc(d.canonical_url) + '" target="_blank" rel="noopener">' + esc(lab.src) + " ↗</a>" : "";
      var pinBlock = "";
      if (pp && pp.e_number) {
        pinBlock = '<div class="pin"><span class="pin-ref">📍 E. ' + esc(pp.e_number) + "</span> "
          + (pp.confidence ? '<span class="pin-conf">(' + esc(pp.confidence) + ")</span> " : "")
          + safeSnippet(pp.sentence || "") + "</div>";
      }
      return '<div class="card ' + cls + '">'
        + '<div class="top"><span class="cite">' + esc(citeOf(d)) + "</span>"
        + '<span class="src-badge">' + esc(d.court_label || d.court || "") + "</span></div>"
        + '<div class="meta">' + esc(d.decision_date || "")
        + (d.language ? " · " + esc(String(d.language).toUpperCase()) : "") + "</div>"
        + (d.title ? '<div class="title">' + esc(d.title) + "</div>" : "")
        + '<div class="snip">' + safeSnippet(d.snippet_html || d.regeste || "") + "</div>"
        + pinBlock
        + '<div class="acts">'
        + '<button class="btn" data-act="full" data-i="' + i + '">' + esc(lab.full) + "</button>"
        + (pp && pp.e_number ? '<button class="btn" data-act="erw" data-i="' + i + '">' + esc(lab.erw) + " " + esc(pp.e_number) + "</button>" : "")
        + '<button class="btn" data-act="copy" data-i="' + i + '">' + esc(lab.copy) + "</button>"
        + srcLink
        + "</div></div>";
    }).join("");

    // Attribution is a reuse condition for the ECtHR material, so it renders
    // whenever an ECtHR hit is on screen — the server decides, not the widget.
    if (DATA.attribution) html += '<div class="attr">' + esc(DATA.attribution) + "</div>";

    app.innerHTML = html;
    Array.prototype.forEach.call(app.querySelectorAll(".langs button"), function (b) {
      b.onclick = function () { reSearch(b.getAttribute("data-lang")); };
    });
    Array.prototype.forEach.call(app.querySelectorAll(".btn"), function (b) {
      b.onclick = function () {
        var d = ds[+b.getAttribute("data-i")], act = b.getAttribute("data-act");
        if (act === "full") onFull(d);
        else if (act === "erw") onErw(d);
        else if (act === "copy") onCopy(d, b);
      };
    });
  }

  mountWidget(applyData);
})();
</script>
</body>
</html>
"""

DECISION_SEARCH_WIDGET_HTML = _DECISION_SEARCH_WIDGET_TEMPLATE.replace(
    "__RUNTIME__", runtime_js("decisions"))
