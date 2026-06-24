"""Tier A: cross-provider law-search highlighting.

Matched terms must read as markdown bold in the text the LLM sees (renders
identically on Claude, ChatGPT, Gemini, Copilot), normalised from whatever
marker the source used: federal '>>>...<<<', cantonal-local '<b>...</b>', or
none for LexFind (on-the-fly fallback). Highlight is presentational: stripping
it must recover the verbatim snippet text (R1 to R3). The same sentinels render
as <mark> for the Tier-B widgets.

Spec: docs/superpowers/specs/2026-06-24-cross-provider-law-search-ux-design.md
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

O, C = m._HL_OPEN, m._HL_CLOSE


# ── render ───────────────────────────────────────────────────────
def test_render_highlight_text_is_markdown_bold():
    assert m._render_highlight(f"der {O}Mietzins{C} ist", "text") == "der **Mietzins** ist"


def test_render_highlight_html_is_mark():
    assert m._render_highlight(f"der {O}Mietzins{C} ist", "html") == "der <mark>Mietzins</mark> ist"


def test_render_highlight_strips_unbalanced_sentinels():
    # snippet truncated mid-mark must never emit a dangling ** or sentinel
    out = m._render_highlight(f"...der {O}Mietz", "text")
    assert O not in out and "**" not in out and out == "...der Mietz"


# ── normalize ────────────────────────────────────────────────────
def test_normalize_federal_markers():
    assert m._normalize_law_snippet("Die >>>Kündigung<<< ist") == f"Die {O}Kündigung{C} ist"


def test_normalize_cantonal_bold_markers():
    assert m._normalize_law_snippet("Die <b>Steuer</b> ist") == f"Die {O}Steuer{C} ist"


def test_normalize_lexfind_plain_uses_terms():
    out = m._normalize_law_snippet("Die Kündigung des Mietverhältnisses", ["kündigung"])
    assert f"{O}Kündigung{C}" in out


def test_highlight_is_verbatim_preserving():
    raw = "Die >>>Kündigung<<< eines Mietverhältnisses"
    norm = m._normalize_law_snippet(raw)
    assert m._strip_highlight(norm) == "Die Kündigung eines Mietverhältnisses"
    assert "**Kündigung**" in m._render_highlight(norm, "text")


# ── helpers ──────────────────────────────────────────────────────
def test_query_terms_filters_operators_quotes_numbers():
    terms = m._query_terms('"miete" OR mietzins AND 220')
    assert "miete" in terms and "mietzins" in terms
    assert "OR" not in terms and "AND" not in terms and "220" not in terms


def test_inline_word_boundary_and_idempotent():
    once = m._highlight_terms_inline("Mietzinsdepot und Mietzins", ["Mietzins"])
    assert once.count(O) == 1  # 'Mietzinsdepot' not marked, standalone 'Mietzins' is
    assert m._highlight_terms_inline(once, ["Mietzins"]) == once  # idempotent


def test_build_law_reference_federal():
    ref = m._build_law_reference(
        {"level": "federal", "canton": "CH", "article_num": "336",
         "abbreviation": "OR", "sr_number": "220"}
    )
    assert ref == "Art. 336 OR"


def test_build_law_reference_cantonal_section_mark():
    ref = m._build_law_reference(
        {"level": "cantonal", "canton": "ZH", "article_num": "12", "sr_number": "131.1"}
    )
    assert ref == "§ 12 ZH 131.1"


# ── formatter wiring (end to end) ────────────────────────────────
def test_format_search_laws_highlights_snippet():
    result = {
        "query": "kündigung", "count": 1, "federal_hits": 1, "cantonal_hits": 0,
        "results": [{
            "level": "federal", "canton": "CH", "article_num": "336",
            "abbreviation": "OR", "sr_number": "220",
            "heading": "Missbräuchliche Kündigung",
            "snippet": "Die >>>Kündigung<<< ist missbräuchlich",
        }],
    }
    out = m._format_search_laws_response(result)
    assert "**Kündigung**" in out
    assert ">>>" not in out and "<<<" not in out


def test_format_search_legislation_highlights_plain_snippet():
    result = {
        "query": "kündigung", "total": 1,
        "laws": [{
            "title": "Obligationenrecht", "systematic_number": "220",
            "entity_name": "Bund", "entity": "CH", "is_active": True,
            "snippet": "Die Kündigung des Arbeitsverhältnisses",
        }],
    }
    out = m._format_search_legislation_response(result)
    assert "**Kündigung**" in out  # on-the-fly fallback (LexFind plain text)


# ── A3: structuredContent payloads (feed the Tier B widgets) ──────
def test_law_hits_structured_shape_and_highlight():
    result = {
        "query": "kündigung", "count": 1, "federal_hits": 1, "cantonal_hits": 0,
        "results": [{
            "level": "federal", "canton": "CH", "article_num": "336",
            "abbreviation": "OR", "sr_number": "220", "heading": "h",
            "snippet": "Die >>>Kündigung<<< ist",
        }],
    }
    sc = m._law_hits_structured(result)
    assert sc["total"] == 1 and len(sc["hits"]) == 1
    h = sc["hits"][0]
    assert h["reference"] == "Art. 336 OR"
    assert h["snippet_text"] == "Die Kündigung ist"               # plain, verbatim
    assert h["snippet_html"] == "Die <mark>Kündigung</mark> ist"  # widget surface


def test_legislation_hits_structured_shape():
    result = {
        "query": "kündigung", "total": 1,
        "laws": [{
            "title": "Obligationenrecht", "systematic_number": "220",
            "entity": "CH", "entity_name": "Bund", "is_active": True,
            "snippet": "Die Kündigung des Arbeitsverhältnisses",
            "original_url": "https://example/or",
        }],
    }
    sc = m._legislation_hits_structured(result)
    h = sc["hits"][0]
    assert h["level"] == "federal" and h["url"] == "https://example/or"
    assert "<mark>Kündigung</mark>" in h["snippet_html"]


def test_with_open_access_note_no_ua_is_noop():
    tok = m._ctx_client_ua.set("")
    try:
        assert m._with_open_access_note("body") == "body"
    finally:
        m._ctx_client_ua.reset(tok)


# ── source links: Fedlex (federal) / LexFind (cantonal) ──────────
def test_fedlex_url_from_work_uri(monkeypatch):
    monkeypatch.setattr(m, "_FEDLEX_WORK_URI_MAP",
                        {"220": "https://fedlex.data.admin.ch/eli/cc/27/317_321_377"})
    assert m._fedlex_url("220", "269d", "de") == \
        "https://www.fedlex.admin.ch/eli/cc/27/317_321_377/de#art_269d"
    assert m._fedlex_url("220", None, "fr") == \
        "https://www.fedlex.admin.ch/eli/cc/27/317_321_377/fr"
    assert m._fedlex_url("999", "1", "de") is None  # sr not in map


def test_lexfind_url():
    assert m._lexfind_url("6275", "fr") == "https://www.lexfind.ch/fe/fr/tol/6275"
    assert m._lexfind_url(None, "de") is None


def test_law_hits_structured_carries_source_links(monkeypatch):
    monkeypatch.setattr(m, "_FEDLEX_WORK_URI_MAP",
                        {"220": "https://fedlex.data.admin.ch/eli/cc/27/317_321_377"})
    result = {
        "query": "x", "count": 2, "federal_hits": 1, "cantonal_hits": 1,
        "results": [
            {"level": "federal", "canton": "CH", "article_num": "269d",
             "abbreviation": "OR", "sr_number": "220", "snippet": "a >>>x<<< b"},
            {"level": "cantonal", "canton": "ZH", "article_num": "5",
             "sr_number": "700.1", "lexfind_id": "6275", "snippet": "c <b>x</b> d"},
        ],
    }
    sc = m._law_hits_structured(result, "de")
    assert sc["query_lang"] == "de"
    fed, can = sc["hits"]
    assert fed["source_label"] == "Fedlex"
    assert fed["source_url"] == "https://www.fedlex.admin.ch/eli/cc/27/317_321_377/de#art_269d"
    assert can["source_label"] == "LexFind"
    assert can["source_url"] == "https://www.lexfind.ch/fe/de/tol/6275"
