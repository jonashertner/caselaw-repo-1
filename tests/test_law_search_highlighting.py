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
