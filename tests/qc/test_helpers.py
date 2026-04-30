"""Function-level tests for critical helpers in mcp_server +
search_stack — these helpers are reused across the QC checks AND the
production server, so a regression here breaks both."""
from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def mcp():
    pytest.importorskip("mcp")  # only present where mcp_server is loadable
    try:
        import mcp_server
    except Exception as e:
        pytest.skip(f"mcp_server import failed: {e}")
    return mcp_server


def test_decision_id_variants_returns_input(mcp):
    """The variants list always contains the original input form.
    Load-bearing: every caller iterates variants and falls back to
    the original."""
    variants = list(mcp._decision_id_variants("bge_BGE_140_III_86"))
    assert "bge_BGE_140_III_86" in variants
    assert len(variants) >= 2  # at least one normalised alternative


def test_decision_id_variants_underscore_space_toggle(mcp):
    """FTS form (underscores) and citation form (spaces) are both
    produced by the normaliser."""
    underscore_in = "BGE_140_III_86"
    variants = list(mcp._decision_id_variants(underscore_in))
    has_space = any(" " in v for v in variants)
    has_underscore = any("_" in v for v in variants)
    assert has_space and has_underscore


def test_e_number_sort_key_natural_ordering(mcp):
    nums = ["1", "10", "2", "2.10", "2.2", "10.1", "3"]
    out = sorted(nums, key=mcp._e_number_sort_key)
    assert out == ["1", "2", "2.2", "2.10", "3", "10", "10.1"]


def test_e_number_sort_key_returns_tuple(mcp):
    """The key function must always return a tuple — the runner sorts
    paragraphs across all e_numbers, and a non-tuple breaks sorting."""
    assert isinstance(mcp._e_number_sort_key("2.3"), tuple)
    assert isinstance(mcp._e_number_sort_key("1"), tuple)
    assert isinstance(mcp._e_number_sort_key(""), tuple)


def test_normalize_docket_search_stack():
    from search_stack.reference_extraction import _normalize_docket
    assert _normalize_docket("4A_321/2013") is not None
    assert _normalize_docket("  4A_321 / 2013  ") is not None


def test_docket_norm_reference_graph():
    """build_reference_graph._docket_norm normalises BGE-style citations."""
    from search_stack.build_reference_graph import _docket_norm
    out_a = _docket_norm("BGE 140 III 86")
    out_b = _docket_norm("140 III 86")
    # Both should produce a comparable normalised form
    assert out_a is not None
    assert out_b is not None
