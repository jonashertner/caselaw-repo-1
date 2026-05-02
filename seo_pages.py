"""SEO-optimized HTML pages for individual court decisions.

Serves lightweight HTML pages with:
- Schema.org LegalCase markup
- Open Graph tags
- Regeste, metadata, statute references
- Links to PDF/source
- Canonical URLs

Mounted at /entscheid/{decision_id} on the Starlette app.
"""
from __future__ import annotations

import html
import logging
import os
import re
import sqlite3
from pathlib import Path

logger = logging.getLogger("swiss-caselaw-mcp")

DATA_DIR = Path(os.environ.get("SWISS_CASELAW_DIR", str(Path.home() / ".swiss-caselaw")))
BASE_URL = os.environ.get("SWISS_CASELAW_BASE_URL", "https://mcp.opencaselaw.ch")


# ── Text reflow ───────────────────────────────────────────────────────
#
# Source corpus text is hard-wrapped at PDF column width (60–80 chars)
# and additionally wraps inline citations like "BGE 129 III 209" or
# "Art. 27 ZGB" onto their own lines. Rendering those `\n`s with
# CSS `white-space: pre-wrap` produces the jagged "citation dangling
# mid-sentence" look the user reported.
#
# Fix: split on blank lines into logical paragraphs, then within each
# paragraph collapse single newlines + whitespace runs into single
# spaces. List items (1. / a) / – ) keep their hard break so dispositiv
# orders and enumerations stay legible.
#
# This mirrors the helpers in exports.py (used by DOCX / PDF) so HTML,
# DOCX and PDF outputs all flow identically.

_LIST_MARKER_RE = re.compile(r"^([0-9]+[.)]|[a-z][.)]|[-*\u2013\u2022])\s")


def _flow_paragraph(text: str) -> str:
    """Collapse a chunk of corpus text into a single re-flowable
    paragraph: newlines + runs of whitespace become single spaces."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\s*\n\s*", " ", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def _split_paragraphs(text: str) -> list[str]:
    """Split text into logical paragraphs.

    * Blank lines (``\\n\\s*\\n``) → paragraph boundary.
    * Within a paragraph, lines that begin with a list marker stay as
      their own paragraph (preserves dispositiv-style enumerations).
    * Other single newlines collapse into spaces (joins PDF column
      wraps and dangling citations).
    """
    if not text:
        return []
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    paragraphs: list[str] = []
    for block in re.split(r"\n\s*\n+", text.strip()):
        if not block.strip():
            continue
        items: list[str] = []
        for raw_line in block.split("\n"):
            line = raw_line.strip()
            if not line:
                continue
            if not items:
                items.append(line)
                continue
            if _LIST_MARKER_RE.match(line):
                items.append(line)
            else:
                items[-1] = re.sub(r" {2,}", " ", items[-1].rstrip() + " " + line)
        paragraphs.extend(items)
    return paragraphs


_REGESTE_SPLIT_RE = re.compile(r"(?=\b(?:Regeste|Regesto)\b)", re.IGNORECASE)


def _split_regeste(regeste: str) -> list[str]:
    """Split a multilingual regeste into per-language paragraphs.
    Falls back to a single flowed paragraph when no language headers
    are detectable (most cantonal decisions)."""
    if not regeste or not regeste.strip():
        return []
    parts = [p for p in _REGESTE_SPLIT_RE.split(regeste) if p and p.strip()]
    if len(parts) <= 1:
        return [_flow_paragraph(regeste)]
    return [_flow_paragraph(p) for p in parts if _flow_paragraph(p)]


def _paragraphs_html(text: str, *, classname: str | None = None) -> str:
    """Render `text` as a sequence of <p> elements after reflow.
    Returns the empty string for empty input."""
    paras = _split_paragraphs(text)
    if not paras:
        return ""
    cls = f' class="{classname}"' if classname else ""
    return "\n".join(f"<p{cls}>{_esc(p)}</p>" for p in paras)

# Court display names (subset — full list in mcp_server.py)
_COURT_NAMES = {
    "bger": "Bundesgericht", "bge": "Bundesgericht (BGE)",
    "bge_historical": "Bundesgericht (historisch)",
    "bvger": "Bundesverwaltungsgericht", "bstger": "Bundesstrafgericht",
    "bpatger": "Bundespatentgericht",
    "ag_gerichte": "Aargau", "ai_gerichte": "Appenzell I.Rh.",
    "ar_gerichte": "Appenzell A.Rh.", "be_verwaltungsgericht": "Bern VerwG",
    "be_zivilstraf": "Bern OG", "bl_gerichte": "Basel-Landschaft",
    "bs_appellationsgericht": "Basel-Stadt", "fr_gerichte": "Freiburg",
    "ge_gerichte": "Genf", "gl_gerichte": "Glarus", "gr_gerichte": "Graubünden",
    "ju_gerichte": "Jura", "lu_gerichte": "Luzern", "ne_gerichte": "Neuenburg",
    "nw_gerichte": "Nidwalden", "ow_gerichte": "Obwalden",
    "sg_gerichte": "St. Gallen", "sh_gerichte": "Schaffhausen",
    "so_gerichte": "Solothurn", "sz_gerichte": "Schwyz",
    "tg_gerichte": "Thurgau", "ti_gerichte": "Ticino",
    "ur_gerichte": "Uri", "vd_gerichte": "Waadt", "vd_findinfo": "Waadt",
    "vs_gerichte": "Wallis", "zg_obergericht": "Zug OG",
    "zh_obergericht": "Zürich OG", "zh_verwaltungsgericht": "Zürich VerwG",
    "zh_sozialversicherungsgericht": "Zürich SozVersG",
}


def _get_db():
    db_path = DATA_DIR / "decisions.db"
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _get_structure_db():
    """Open the decision-structure sidecar DB if available, else None."""
    db_path = Path(os.environ.get(
        "SWISS_CASELAW_STRUCTURE_DB", str(DATA_DIR / "decision_structure.db")
    ))
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def _e_number_sort_key(e_number: str) -> tuple:
    parts = e_number.split(".")
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except ValueError:
            out.append(p)
    return tuple(out)


def _fetch_structure(decision_id: str) -> dict | None:
    """Return {'row': structure_row, 'paragraphs': [..]} or None."""
    sconn = _get_structure_db()
    if sconn is None:
        return None
    try:
        row = sconn.execute(
            "SELECT * FROM structure WHERE decision_id = ?", (decision_id,)
        ).fetchone()
        if not row:
            return None
        paragraphs = sconn.execute(
            "SELECT e_number, depth, parent, text FROM erwaegungen_paragraph "
            "WHERE decision_id = ? ORDER BY depth, e_number",
            (decision_id,),
        ).fetchall()
        paragraphs = sorted(
            [dict(p) for p in paragraphs],
            key=lambda p: _e_number_sort_key(p["e_number"]),
        )
        return {"row": dict(row), "paragraphs": paragraphs}
    finally:
        sconn.close()


def _esc(text: str | None) -> str:
    return html.escape(text or "", quote=True)


def _truncate(text: str | None, max_len: int) -> str:
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_len:
        return text
    return text[:max_len].rsplit(" ", 1)[0] + "..."


def render_decision_page(decision_id: str) -> tuple[str, int]:
    """Render an HTML page for a single decision. Returns (html, status_code)."""
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT * FROM decisions WHERE decision_id = ?", (decision_id,)
        ).fetchone()

        if not row:
            # Try fuzzy match on docket
            row = conn.execute(
                "SELECT * FROM decisions WHERE docket_number LIKE ? LIMIT 1",
                (f"%{decision_id}%",),
            ).fetchone()

        if not row:
            return _render_404(decision_id), 404

        return _render_decision(row), 200
    finally:
        conn.close()


def _render_decision(row: sqlite3.Row) -> str:
    did = row["decision_id"]
    court = row["court"] or ""
    court_name = _COURT_NAMES.get(court, court.replace("_", " ").title())
    canton = row["canton"] or ""
    docket = row["docket_number"] or did
    date = row["decision_date"] or ""
    language = row["language"] or "de"
    title = row["title"] or ""
    regeste = row["regeste"] or ""
    full_text = row["full_text"] or ""
    source_url = row["source_url"] or ""
    pdf_url = row["pdf_url"] or ""

    # Clean regeste for meta description
    clean_regeste = re.sub(r"<[^>]+>", " ", regeste)  # strip HTML tags
    clean_regeste = re.sub(r"^Regeste\s*\n?\s*", "", clean_regeste)  # strip "Regeste" prefix
    clean_regeste = re.sub(r"^Regesto\s*\n?\s*", "", clean_regeste)
    meta_desc = _truncate(re.sub(r"\s+", " ", clean_regeste).strip(), 160)

    # Page title
    if title:
        page_title = f"{docket}, {_truncate(title, 60)} | OpenCaseLaw"
    else:
        page_title = f"{docket}, {court_name} | OpenCaseLaw"

    canonical = f"{BASE_URL}/entscheid/{did}"

    # Schema.org LegalCase
    schema_json = {
        "@context": "https://schema.org",
        "@type": "LegalCase",
        "name": docket,
        "description": _truncate(regeste, 300),
        "datePublished": date,
        "inLanguage": language,
        "court": {
            "@type": "GovernmentOrganization",
            "name": court_name,
            "address": {
                "@type": "PostalAddress",
                "addressCountry": "CH",
            },
        },
        "url": canonical,
        "isPartOf": {
            "@type": "Dataset",
            "name": "Swiss Case Law Open Dataset",
            "url": "https://opencaselaw.ch",
        },
    }
    if source_url:
        schema_json["sameAs"] = source_url

    import json
    schema_str = json.dumps(schema_json, ensure_ascii=False)

    # Language labels
    lang_labels = {"de": "Deutsch", "fr": "Français", "it": "Italiano", "rm": "Rumantsch"}
    lang_label = lang_labels.get(language, language)

    # Regeste paragraphs — multilingual headers split per-language, then
    # each language is reflowed (collapses PDF column line-wraps).
    regeste_html = ""
    if regeste:
        for para in _split_regeste(regeste):
            regeste_html += f"<p>{_esc(para)}</p>\n"

    # Structured fields (Sachverhalt / Erwägungen / Dispositiv) from sidecar DB
    structure = _fetch_structure(did)
    structured_html = ""
    if structure and structure["row"]:
        srow = structure["row"]
        sachverhalt = srow.get("sachverhalt") or ""
        dispositiv = srow.get("dispositiv") or ""
        paragraphs = structure["paragraphs"]
        try:
            import json as _json
            disp_orders = _json.loads(srow["dispositiv_orders"]) if srow.get("dispositiv_orders") else []
        except Exception:
            disp_orders = []

        parts = ['<section class="structured">']

        if sachverhalt:
            parts.append('<details><summary><strong>Sachverhalt</strong></summary>')
            parts.append(f'<div class="section-body">{_paragraphs_html(sachverhalt)}</div></details>')

        if paragraphs:
            parts.append('<details open><summary><strong>Erwägungen</strong> '
                         f'<span class="count">({len(paragraphs)} Absätze)</span></summary>')
            parts.append('<div class="erwaegungen">')
            for p in paragraphs:
                indent_px = (p["depth"] - 1) * 16
                e_label = f"E. {p['e_number']}"
                anchor = f"e-{p['e_number'].replace('.', '-')}"
                # Each Erwägung is paragraph-flowed; the E-number anchor
                # sits inline with the first paragraph for compact scanning.
                e_paras = _split_paragraphs(p["text"])
                if not e_paras:
                    continue
                first_html = (
                    f'<a class="e-num" href="#{anchor}">{_esc(e_label)}</a> '
                    f'{_esc(e_paras[0])}'
                )
                rest_html = "".join(f'<p>{_esc(par)}</p>' for par in e_paras[1:])
                parts.append(
                    f'<div class="erw" id="{anchor}" style="margin-left:{indent_px}px">'
                    f'<p class="e-text">{first_html}</p>{rest_html}</div>'
                )
            parts.append('</div></details>')

        if dispositiv:
            parts.append('<details open><summary><strong>Dispositiv</strong></summary>')
            if disp_orders:
                parts.append('<ol class="dispositiv-orders">')
                for order in disp_orders:
                    parts.append(f'<li>{_esc(order)}</li>')
                parts.append('</ol>')
            else:
                parts.append(f'<div class="section-body">{_paragraphs_html(dispositiv)}</div>')
            parts.append('</details>')

        parts.append('</section>')
        structured_html = "\n".join(parts)

    # Full decision text — collapsed by default when structured view is available
    text_excerpt = ""
    if full_text:
        is_open = "" if structure else " open"
        text_excerpt = f"""
        <details{is_open}>
            <summary>Volltext (verifizierbarer Originaltext)</summary>
            <div class="fulltext">{_paragraphs_html(full_text)}</div>
        </details>"""

    return f"""<!DOCTYPE html>
<html lang="{_esc(language)}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="google-site-verification" content="5eTv5mgNKw8M8vENzS4KPG4aJKYm_zKZJhL3TbQpOGs">
<title>{_esc(page_title)}</title>
<meta name="description" content="{_esc(meta_desc)}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="{_esc(canonical)}">
<meta property="og:type" content="article">
<meta property="og:title" content="{_esc(docket)}, {_esc(court_name)}">
<meta property="og:description" content="{_esc(meta_desc)}">
<meta property="og:url" content="{_esc(canonical)}">
<meta property="og:site_name" content="OpenCaseLaw.ch">
<meta property="og:locale" content="{_esc(language)}_CH">
<script type="application/ld+json">{schema_str}</script>
<style>
    html {{ background: #fff; }}
    body {{
        font-family: 'Times New Roman', Times, serif;
        font-size: 12pt;
        line-height: 1.2;
        max-width: 720px;
        margin: 4rem auto 6rem;
        padding: 0 2rem;
        color: #000;
        background: #fff;
        font-feature-settings: "kern" 1, "liga" 1;
        -webkit-font-smoothing: antialiased;
    }}
    a {{ color: inherit; text-decoration: underline; text-underline-offset: 2px; }}
    a:hover {{ text-decoration-thickness: 2px; }}
    nav {{ font-size: 10.5pt; margin-bottom: 3rem; }}
    nav .sep {{ margin: 0 0.5em; }}
    h1 {{
        font-size: 18pt;
        font-weight: bold;
        line-height: 1.2;
        margin: 0 0 0.4rem 0;
        letter-spacing: -0.005em;
    }}
    .subtitle {{ font-size: 12pt; font-style: italic; margin: 0 0 1.6rem 0; line-height: 1.2; }}
    .meta {{ font-size: 11pt; margin: 0 0 2.4rem 0; }}
    .meta span + span {{ margin-left: 1.5em; }}
    h2 {{ font-size: 13pt; font-weight: bold; margin: 2.4rem 0 0.6rem 0; line-height: 1.2; }}
    .regeste {{ margin: 0 0 2rem 0; font-size: 12pt; line-height: 1.2; }}
    .regeste p {{ margin: 0.5rem 0; }}
    .links {{ margin: 1.6rem 0; font-size: 11pt; }}
    .links a {{ margin-right: 1.4em; }}
    .links.exports {{ margin-top: 0.8rem; font-size: 10.5pt; }}
    details {{ margin: 1.6rem 0; }}
    details summary {{
        cursor: pointer;
        font-weight: bold;
        font-size: 12pt;
        margin: 0;
        padding: 0;
    }}
    details summary .count {{ font-weight: normal; }}
    .structured {{ margin: 0; }}
    /* Body text is now rendered as real <p> elements (server-side reflow
       collapses PDF column line-wraps), so word-wrap is the browser's
       job — no `white-space: pre-wrap`. */
    .section-body {{ margin: 0.8rem 0 0 0; }}
    .section-body p,
    .erw .e-text,
    .erw p,
    .fulltext p {{ margin: 0 0 0.7rem 0; line-height: 1.45; text-align: justify; hyphens: auto; }}
    .section-body p:last-child,
    .erw p:last-child,
    .fulltext p:last-child {{ margin-bottom: 0; }}
    .erwaegungen {{ margin: 0.8rem 0 0 0; }}
    .erw {{ padding: 0.4rem 0; }}
    .erw .e-num {{ font-weight: bold; margin-right: 0.5em; text-decoration: none; }}
    .erw .e-num:hover {{ text-decoration: underline; }}
    .dispositiv-orders {{ margin: 0.8rem 0 0 1.5em; padding: 0; list-style: decimal; }}
    .dispositiv-orders li {{ margin: 0.5rem 0; padding-left: 0.4em; }}
    .fulltext {{
        font-size: 12pt;
        line-height: 1.45;
        margin: 0.8rem 0 0 0;
        padding: 0;
        background: none;
    }}
    /* Print: tighten margins, drop nav/footer chrome. Browsers honour
       hyphens + justify in print, giving exports + paper printouts the
       same cleanly-flowed paragraphs as the screen. */
    @media print {{
        body {{ max-width: none; margin: 1.6cm 1.8cm; padding: 0; font-size: 11pt; }}
        nav, footer, .links.exports, details > summary {{ display: none !important; }}
        details, details[open] {{ display: block; }}
        details > *:not(summary) {{ display: block !important; }}
        .erw, .section-body p, .fulltext p {{ page-break-inside: avoid; }}
    }}
    footer {{ margin-top: 4rem; font-size: 10.5pt; }}
    footer p {{ margin: 0.6rem 0; }}
    @media (max-width: 600px) {{
        body {{ margin: 2rem auto 4rem; padding: 0 1.2rem; font-size: 11.5pt; }}
        h1 {{ font-size: 16pt; }}
        .meta span + span {{ margin-left: 0; display: block; margin-top: 0.2em; }}
    }}
</style>
</head>
<body>
<nav><a href="https://opencaselaw.ch">OpenCaseLaw.ch</a><span class="sep">/</span><a href="{_esc(canonical)}">{_esc(docket)}</a></nav>
<h1>{_esc(docket)}</h1>
{f'<p class="subtitle">{_esc(title)}</p>' if title else ''}
<div class="meta">
    <span>{_esc(court_name)}</span>
    <span>{_esc(date)}</span>
    <span>{_esc(lang_label)}</span>
    {f'<span>{_esc(canton)}</span>' if canton else ''}
</div>
{f'<div class="regeste">{regeste_html}</div>' if regeste_html else ''}
<div class="links">
    {f'<a href="{_esc(source_url)}" rel="noopener">Originalquelle</a>' if source_url else ''}
    {f'<a href="{_esc(pdf_url)}" rel="noopener">Original-PDF</a>' if pdf_url else ''}
    <a href="https://opencaselaw.ch">Alle Entscheide durchsuchen</a>
</div>
<div class="links exports">
    Export:
    <a href="/api/decisions/{_esc(did)}/export.docx" rel="nofollow">Word</a>
    <a href="/api/decisions/{_esc(did)}/export.pdf" rel="nofollow">PDF</a>
    <a href="/api/decisions/{_esc(did)}/export.bib" rel="nofollow">BibTeX</a>
    <a href="/api/decisions/{_esc(did)}/export.ris" rel="nofollow">RIS</a>
</div>
{structured_html}
{text_excerpt}
<footer>
    <p>OpenCaseLaw.ch. Offener Datensatz Schweizer Rechtsprechung.
       Alle Bundesgerichte und 26 Kantone.</p>
    <p><a href="https://opencaselaw.ch">Suche</a>
       <a href="https://huggingface.co/datasets/voilaj/swiss-caselaw" style="margin-left:1.4em">Download</a>
       <a href="https://mcp.opencaselaw.ch/api/docs" style="margin-left:1.4em">API</a></p>
</footer>
</body>
</html>"""


def _render_404(decision_id: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<title>Entscheid nicht gefunden | OpenCaseLaw</title>
<meta name="robots" content="noindex">
<meta name="google-site-verification" content="5eTv5mgNKw8M8vENzS4KPG4aJKYm_zKZJhL3TbQpOGs">
<style>
    body {{
        font-family: 'Times New Roman', Times, serif;
        font-size: 12pt;
        line-height: 1.2;
        max-width: 600px;
        margin: 4rem auto;
        padding: 0 2rem;
        color: #000;
        background: #fff;
    }}
    h1 {{ font-size: 18pt; font-weight: bold; margin: 0 0 1rem 0; line-height: 1.2; }}
    code {{ font-family: 'Courier New', Courier, monospace; font-size: 11pt; }}
    a {{ color: inherit; text-decoration: underline; text-underline-offset: 2px; }}
</style>
</head>
<body>
<h1>Entscheid nicht gefunden</h1>
<p>Der Entscheid <code>{_esc(decision_id)}</code> wurde nicht gefunden.</p>
<p><a href="https://opencaselaw.ch">Zurück zur Suche</a></p>
</body>
</html>"""


MAX_URLS_PER_SITEMAP = 40_000  # Google limit is 50K; stay under


def render_sitemap_index() -> str:
    """Generate sitemap index pointing to per-court chunk sitemaps."""
    conn = _get_db()
    try:
        courts = conn.execute(
            "SELECT court, COUNT(*) as n FROM decisions "
            "WHERE court IS NOT NULL GROUP BY court ORDER BY court"
        ).fetchall()
    finally:
        conn.close()

    lines = ['<?xml version="1.0" encoding="UTF-8"?>']
    lines.append('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    for court, count in courts:
        chunks = (count // MAX_URLS_PER_SITEMAP) + 1
        for chunk in range(chunks):
            if chunks == 1:
                lines.append(f"  <sitemap><loc>{BASE_URL}/sitemap-{court}.xml</loc></sitemap>")
            else:
                lines.append(f"  <sitemap><loc>{BASE_URL}/sitemap-{court}-{chunk}.xml</loc></sitemap>")
    lines.append("</sitemapindex>")
    return "\n".join(lines)


def render_court_sitemap(court_and_chunk: str) -> str:
    """Generate sitemap for a single court's decisions (with optional chunk).

    court_and_chunk can be "bger" or "bger-0", "bger-1", etc.
    """
    # Parse court and chunk from path
    parts = court_and_chunk.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        court = parts[0]
        chunk = int(parts[1])
    else:
        court = court_and_chunk
        chunk = 0

    offset = chunk * MAX_URLS_PER_SITEMAP

    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT decision_id, decision_date FROM decisions "
            "WHERE court = ? ORDER BY decision_date DESC "
            "LIMIT ? OFFSET ?",
            (court, MAX_URLS_PER_SITEMAP, offset),
        ).fetchall()
    finally:
        conn.close()

    lines = ['<?xml version="1.0" encoding="UTF-8"?>']
    lines.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    for row in rows:
        did = row[0]
        lines.append(f"  <url><loc>{BASE_URL}/entscheid/{_esc(did)}</loc></url>")
    lines.append("</urlset>")
    return "\n".join(lines)
