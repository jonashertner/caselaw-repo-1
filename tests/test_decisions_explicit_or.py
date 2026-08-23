"""Explicit OR on the decisions search must reach FTS5 as an operator.

Caught 2026-08-23 while live-verifying the /search/ capabilities panel:
'Mietzins OR Pachtzins' returned 52 results — fewer than either operand
(2000+ / 568) — because _sanitize_fts5 quotes every OR (correct for
natural language, where OR is the Obligationenrecht) and the explicit
branch of _build_query_strategies then executed the sanitised string.
The classifier said "operator query"; the pipeline ran "literal query".

search_laws had the identical disease and cure in #60: build the
explicit MATCH from the ORIGINAL text via _explicit_laws_match. These
tests pin that the decisions strategy builder now does the same — and
that the #88 collision masking (OR next to an article number = the
statute, not the operator) is untouched, since those queries are
classified non-explicit and never enter this branch.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _raw_strategy(original: str):
    sanitized = m._sanitize_fts5(original)
    strategies, _ = m._build_query_strategies(
        sanitized, original_query=original)
    return {s["name"]: s["query"] for s in strategies}


def test_word_or_word_keeps_the_operator():
    q = _raw_strategy("Mietzins OR Pachtzins")
    assert q["raw"] == "Mietzins OR Pachtzins"       # operator, unquoted
    assert '"OR"' not in q["raw"]


def test_not_keeps_the_operator():
    q = _raw_strategy("Mietzins NOT Pachtzins")
    assert q["raw"] == "Mietzins NOT Pachtzins"


def test_phrase_and_prefix_pass_through():
    q = _raw_strategy('"unerlaubte Handlung" Verjähr*')
    assert '"unerlaubte Handlung"' in q["raw"]
    assert "Verjähr*" in q["raw"]


def test_collision_queries_keep_or_as_a_literal():
    """'Verjährung OR 60' means the Obligationenrecht (#88) — classified
    non-explicit, so whatever strategy set runs, no strategy may carry OR
    as a bare operator."""
    for original in ("Verjährung OR 60", "Art. 41 OR Schadenersatz"):
        assert m._has_explicit_fts_syntax(original) is False
        sanitized = m._sanitize_fts5(original)
        strategies, _ = m._build_query_strategies(
            sanitized, original_query=original)
        # Internal recall strategies (nl_or, language-focus) legitimately
        # build their OWN token-disjunctions; the #88 contract binds the
        # verbatim strategies: the user's OR must stay a quoted literal there.
        for s in strategies:
            if s["name"] not in ("raw", "quoted", "raw_fallback"):
                continue
            q = s["query"] or ""
            assert " OR " not in q or '"OR"' in q, (original, s["name"], q)


def test_stray_operators_are_neutralised_by_the_helper():
    """An operator without operands on both sides must degrade to a literal
    so the raw strategy can never hand FTS5 a syntax error."""
    assert not m._explicit_laws_match("OR Mietzins").startswith("OR ")
    assert not m._explicit_laws_match("Mietzins OR").rstrip().endswith(" OR")
