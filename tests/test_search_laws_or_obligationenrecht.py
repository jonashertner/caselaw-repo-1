"""GitHub #88: search_laws("Verjährung OR 60") evaluated `OR` as the FTS5
disjunction operator and returned five articles numbered 60 from unrelated acts
(StHG, ZDG, COTIF, RTVV, VRAB) with nothing from SR 220.

"OR" is both the FTS5 operator and the standard abbreviation of the
Obligationenrecht, so in Swiss usage the collision is unavoidable rather than a
corner case. `_sanitize_fts5` already quotes bare OR, but search_laws
deliberately bypasses the sanitiser for queries it classifies as explicit FTS
syntax, and that classifier only masked the fully written form "Art. 41 OR".

The masking now also covers the abbreviated forms, keyed on an article-shaped
number sitting on exactly one side of the OR. The asymmetry requirement is what
keeps a real numeric disjunction ("2020 OR 2021") working.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


# ── OR as the Obligationenrecht: must NOT be operator syntax ─────────────────

def test_reported_case_is_not_operator_syntax():
    assert m._has_explicit_fts_syntax("Verjährung OR 60") is False


def test_bare_abbreviation_plus_article():
    assert m._has_explicit_fts_syntax("OR 60") is False


def test_suffixed_article_number():
    assert m._has_explicit_fts_syntax("Verjährung OR 60a") is False
    assert m._has_explicit_fts_syntax("OR 322bis") is False


def test_written_out_form_still_masked():
    # QUERY_STATUTE_PATTERN already handled this; guard against regression.
    assert m._has_explicit_fts_syntax("Art. 41 OR") is False


# ── genuine operator syntax: must still be operator syntax ───────────────────

def test_documented_example_still_works():
    # search_laws' own docstring advertises 'Miete OR Pacht'.
    assert m._has_explicit_fts_syntax("Miete OR Pacht") is True


def test_numeric_disjunction_survives():
    # Both sides numeric -> a year range, not a statute reference. This is the
    # case the asymmetry requirement exists to protect.
    assert m._has_explicit_fts_syntax("2020 OR 2021") is True


def test_or_chain_survives():
    assert m._has_explicit_fts_syntax("Miete OR Pacht OR Leihe") is True


def test_other_operators_untouched():
    assert m._has_explicit_fts_syntax("Kündigung AND Frist") is True
    assert m._has_explicit_fts_syntax("Miete NOT Pacht") is True
    assert m._has_explicit_fts_syntax("Verfahr*") is True


# ── the masking helper in isolation ─────────────────────────────────────────

def test_mask_only_fires_on_asymmetry():
    assert "__STATUTE__" in m._mask_or_as_statute("Verjährung OR 60")
    assert "__STATUTE__" not in m._mask_or_as_statute("Miete OR Pacht")
    assert "__STATUTE__" not in m._mask_or_as_statute("2020 OR 2021")


def test_lowercase_or_is_not_touched():
    # Lowercase "or" is an ordinary word to FTS5 and to this classifier alike.
    assert "__STATUTE__" not in m._mask_or_as_statute("Verjährung or 60")


def test_four_digit_number_is_not_article_shaped():
    # Years must not read as article numbers; that is what keeps the
    # disjunction guard honest.
    assert m._ARTICLE_SHAPED.fullmatch("2020") is None
    assert m._ARTICLE_SHAPED.fullmatch("60") is not None
