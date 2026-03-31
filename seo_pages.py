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
    page_title = f"{docket} — {court_name}"
    if title:
        page_title = f"{docket} — {_truncate(title, 60)} | OpenCaseLaw"
    else:
        page_title = f"{docket} — {court_name} | OpenCaseLaw"

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

    # Regeste paragraphs
    regeste_html = ""
    if regeste:
        for para in regeste.split("\n"):
            para = para.strip()
            if para:
                regeste_html += f"<p>{_esc(para)}</p>\n"

    # Full decision text
    text_excerpt = ""
    if full_text:
        text_excerpt = f"""
        <details open>
            <summary>Volltext</summary>
            <div class="fulltext">{_esc(full_text)}</div>
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
<meta property="og:title" content="{_esc(docket)} — {_esc(court_name)}">
<meta property="og:description" content="{_esc(meta_desc)}">
<meta property="og:url" content="{_esc(canonical)}">
<meta property="og:site_name" content="OpenCaseLaw.ch">
<meta property="og:locale" content="{_esc(language)}_CH">
<script type="application/ld+json">{schema_str}</script>
<style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
           max-width: 800px; margin: 0 auto; padding: 1rem; line-height: 1.6;
           color: #1a1a1a; background: #fff; }}
    h1 {{ font-size: 1.5rem; margin-bottom: 0.25rem; }}
    .meta {{ color: #555; font-size: 0.9rem; margin-bottom: 1rem; }}
    .meta span {{ margin-right: 1rem; }}
    .regeste {{ background: #f8f8f8; padding: 1rem; border-left: 3px solid #c00;
                margin: 1rem 0; font-size: 0.95rem; }}
    .regeste p {{ margin: 0.5rem 0; }}
    .links {{ margin: 1rem 0; }}
    .links a {{ margin-right: 1rem; color: #0066cc; }}
    .fulltext {{ white-space: pre-wrap; font-size: 0.85rem; color: #333;
                 max-height: 600px; overflow-y: auto; padding: 1rem;
                 background: #fafafa; border: 1px solid #eee; }}
    details summary {{ cursor: pointer; color: #0066cc; margin: 1rem 0; }}
    nav {{ font-size: 0.85rem; margin-bottom: 1rem; }}
    nav a {{ color: #0066cc; text-decoration: none; }}
    footer {{ margin-top: 2rem; padding-top: 1rem; border-top: 1px solid #eee;
              font-size: 0.8rem; color: #888; }}
</style>
</head>
<body>
<nav><a href="https://opencaselaw.ch">OpenCaseLaw.ch</a> &rsaquo; <a href="{_esc(canonical)}">{_esc(docket)}</a></nav>
<h1>{_esc(docket)}</h1>
{f'<p style="color:#555;font-size:0.95rem">{_esc(title)}</p>' if title else ''}
<div class="meta">
    <span>{_esc(court_name)}</span>
    <span>{_esc(date)}</span>
    <span>{_esc(lang_label)}</span>
    {f'<span>{_esc(canton)}</span>' if canton else ''}
</div>
{f'<div class="regeste">{regeste_html}</div>' if regeste_html else ''}
<div class="links">
    {f'<a href="{_esc(source_url)}" rel="noopener">Originalquelle</a>' if source_url else ''}
    {f'<a href="{_esc(pdf_url)}" rel="noopener">PDF</a>' if pdf_url else ''}
    <a href="https://opencaselaw.ch">Alle {962000}+ Entscheide durchsuchen</a>
</div>
{text_excerpt}
<footer>
    <p>OpenCaseLaw.ch — Offener Datensatz Schweizer Rechtsprechung.
       962'000+ Entscheide aller Bundesgerichte und 26 Kantone.
       <a href="https://opencaselaw.ch">Suche</a> |
       <a href="https://huggingface.co/datasets/voilaj/swiss-caselaw">Download</a> |
       <a href="https://mcp.opencaselaw.ch/api/docs">API</a></p>
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
    body {{ font-family: system-ui, sans-serif; max-width: 600px; margin: 2rem auto; padding: 1rem; }}
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
