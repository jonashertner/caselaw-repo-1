"""GitHub #89: search_scholarship("corruption pot-de-vin nullité du contrat
droit suisse") returned an introduction to law for secondary-school pupils as
its top hit, matched on the fragments "de" and "droit".

Mechanism: a bare multi-term FTS5 query is an implicit AND (verified against the
scholarship index), so every token is *required*. Requiring "du", "de" and
"droit" excludes focused work that phrases things differently and leaves mainly
long general documents, which contain every common legal word. The stopword list
the Erwägungen scorer already uses was simply never applied here — "droit" is in
it.

Ranking quality at production scale is not asserted by these tests; they pin the
query transformation and its two guards.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _t(q: str) -> str:
    return m._drop_stopwords_for_fts(m._sanitize_fts5(q))


def test_reported_query_keeps_only_substantive_terms():
    out = _t("corruption pot-de-vin nullité du contrat droit suisse")
    for gone in ("du", "de", "droit"):
        assert gone not in out.split(), out
    for kept in ("corruption", "nullité", "contrat", "suisse"):
        assert kept in out.split(), out


def test_hyphenated_term_survives_as_its_parts():
    # "pot-de-vin" is split by the sanitiser, and the index splits document
    # text the same way, so pot/vin still match a document containing it.
    out = _t("pot-de-vin")
    assert "pot" in out.split() and "vin" in out.split()


def test_german_stopwords_dropped_too():
    out = _t("Kündigung nach dem Urteil des Bundesgericht")
    assert "Kündigung" in out
    for gone in ("nach", "dem", "Urteil", "des", "Bundesgericht"):
        assert gone not in out.split(), out


def test_inflected_german_forms_are_not_matched():
    # Documents a real limitation rather than endorsing it. The stopword list is
    # matched on exact tokens, so the genitive "Bundesgerichts" is not
    # recognised while "Bundesgericht" is. Same shape as the umlaut gap in
    # test_pinpoint_suppression_reason. Worth fixing list-wide, not here: any
    # suffix-stripping rule affects the Erwägungen scorer that shares this list
    # and needs its own evidence.
    out = _t("Kündigung nach dem Urteil des Bundesgerichts")
    assert "Bundesgerichts" in out.split()


# ── guard 1: explicit syntax is never rewritten ─────────────────────────────

def test_phrase_query_passed_through():
    q = m._sanitize_fts5('"Treu und Glauben"')
    assert m._drop_stopwords_for_fts(q) == q


def test_operator_queries_passed_through():
    for raw in ("Miete OR Pacht", "Kündigung AND Frist", "Verfahr*"):
        q = m._sanitize_fts5(raw)
        assert m._drop_stopwords_for_fts(q) == q, raw


def test_column_filter_passed_through():
    q = "title:Verjährung"
    assert m._drop_stopwords_for_fts(q) == q


# ── guard 2: an all-stopword query must not collapse to match-everything ────

def test_all_stopword_query_keeps_its_original_form():
    # Collapsing to "" would turn a meaningless query into one that matches the
    # entire corpus, which is worse than returning its own poor results.
    q = m._sanitize_fts5("der die das und")
    assert m._drop_stopwords_for_fts(q) == q
    assert m._drop_stopwords_for_fts(q) != ""


def test_query_that_is_all_legal_stopwords_survives():
    q = m._sanitize_fts5("le droit de la procédure")
    assert m._drop_stopwords_for_fts(q) == q


def test_empty_query_is_untouched():
    assert m._drop_stopwords_for_fts("") == ""


def test_single_substantive_term_unchanged():
    assert _t("Verjährung") == "Verjährung"
