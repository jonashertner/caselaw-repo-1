"""Unit tests for the Regeste E-hint parser used by
benchmarks/pinpoint_regeste_bench.py.

The parser determines the ground-truth E-numbers for the auto-bench;
silent regressions here would corrupt the benchmark's results."""
from benchmarks.pinpoint_regeste_bench import (
    _extract_e_numbers,
    _strip_e_hints,
    _is_partial_match,
)


# ───────────────────────── _extract_e_numbers ─────────────────────────


def test_extract_simple_german_hint():
    """Basic case: '(E. 3.1)' → {'3.1'}."""
    assert _extract_e_numbers("Some prose (E. 3.1).") == {"3.1"}


def test_extract_simple_french_hint():
    """FR Regeste: '(consid. 4.2)' → {'4.2'}."""
    assert _extract_e_numbers("Le tribunal (consid. 4.2) considère...") == {"4.2"}


def test_extract_range_expanded():
    """'(consid. 3.1-3.5)' → {3.1, 3.2, 3.3, 3.4, 3.5}."""
    out = _extract_e_numbers("Police compétence (consid. 3.1-3.5).")
    assert out == {"3.1", "3.2", "3.3", "3.4", "3.5"}


def test_extract_list_with_und():
    """'(E. 3.1 und 4.2)' → {3.1, 4.2}."""
    out = _extract_e_numbers("Mietrecht (E. 3.1 und 4.2).")
    assert out == {"3.1", "4.2"}


def test_extract_list_with_comma():
    """'(consid. 3.1, 4.2)' → {3.1, 4.2}."""
    out = _extract_e_numbers("Norme (consid. 3.1, 4.2).")
    assert out == {"3.1", "4.2"}


def test_extract_with_ff_suffix():
    """'(E. 3 ff.)' — 'ff.' (folgende) extension is stripped, base captured."""
    out = _extract_e_numbers("Doktrin (E. 3 ff.).")
    assert out == {"3"}


def test_extract_top_level_e_number():
    """Single integer E-number works (not just decimal sub-numbers)."""
    out = _extract_e_numbers("Erwägung (E. 5).")
    assert out == {"5"}


def test_extract_returns_empty_when_no_hint():
    """Regeste with no E-pattern → empty set."""
    assert _extract_e_numbers("Some prose without any pinpoint.") == set()
    assert _extract_e_numbers("") == set()
    assert _extract_e_numbers(None) == set()


def test_extract_filters_malformed():
    """Garbage like '(E. abc)' should not produce a malformed E-number."""
    out = _extract_e_numbers("Bla (E. abc) bla.")
    # Pattern won't capture 'abc' (regex requires \d), so empty.
    assert out == set()


def test_extract_real_world_regeste():
    """Real Regeste shape from BGE 145 IV 50: 'Police compétence ...
    (consid. 3.1-3.5).'"""
    real = (
        "Art. 55 al. 1 LCR; compétence pour ordonner un test préliminaire "
        "selon l'art. 10 al. 2 OCCR. La police est compétente pour ordonner "
        "un test rapide de dépistage de drogues selon l'art. 10 al. 2 OCCR "
        "(consid. 3.1-3.5)."
    )
    assert _extract_e_numbers(real) == {"3.1", "3.2", "3.3", "3.4", "3.5"}


# ───────────────────────── _strip_e_hints ─────────────────────────


def test_strip_removes_german_hint():
    s = _strip_e_hints("Mietrecht (E. 3.1) hier.")
    assert "(E. 3.1)" not in s
    assert "Mietrecht" in s and "hier." in s


def test_strip_removes_french_hint():
    s = _strip_e_hints("Tribunal (consid. 4.2) ici.")
    assert "(consid. 4.2)" not in s
    assert "Tribunal" in s and "ici." in s


def test_strip_collapses_whitespace():
    """After hint removal, runs of whitespace are collapsed to single space."""
    s = _strip_e_hints("Foo (E. 3.1)   bar")
    assert "  " not in s


# ───────────────────────── _is_partial_match ─────────────────────────


def test_partial_match_exact():
    assert _is_partial_match("3.1", {"3.1", "4.2"}) is True


def test_partial_match_predicted_is_parent():
    """Resolver returned 'E. 3' when truth is 'E. 3.1' → partial hit."""
    assert _is_partial_match("3", {"3.1"}) is True


def test_partial_match_predicted_is_child():
    """Resolver returned 'E. 3.1.2' when truth is 'E. 3.1' → partial hit."""
    assert _is_partial_match("3.1.2", {"3.1"}) is True


def test_partial_match_unrelated():
    assert _is_partial_match("5.1", {"3.1", "4.2"}) is False


def test_partial_match_partial_string_not_prefix():
    """'31' is not a prefix of '3.1' (different segment count)."""
    assert _is_partial_match("31", {"3.1"}) is False
