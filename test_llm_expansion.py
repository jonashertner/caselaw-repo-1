"""Tests for LLM query expansion in mcp_server."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

import mcp_server
from mcp_server import (
    _expand_query_with_llm,
    _build_query_strategies,
    _LLM_EXPANSION_CACHE,
)


# ---------------------------------------------------------------------------
# Unit tests (no API calls)
# ---------------------------------------------------------------------------


class TestExpandQueryDisabled:
    """When disabled or no API key, returns empty list immediately."""

    def test_returns_empty_when_disabled(self):
        orig = mcp_server.LLM_EXPANSION_ENABLED
        try:
            mcp_server.LLM_EXPANSION_ENABLED = False
            result = _expand_query_with_llm("Hundebiss")
            assert result == []
        finally:
            mcp_server.LLM_EXPANSION_ENABLED = orig

    def test_returns_empty_when_no_api_key(self):
        orig_enabled = mcp_server.LLM_EXPANSION_ENABLED
        orig_key = mcp_server.ANTHROPIC_API_KEY
        try:
            mcp_server.LLM_EXPANSION_ENABLED = True
            mcp_server.ANTHROPIC_API_KEY = ""
            result = _expand_query_with_llm("Hundebiss")
            assert result == []
        finally:
            mcp_server.LLM_EXPANSION_ENABLED = orig_enabled
            mcp_server.ANTHROPIC_API_KEY = orig_key


class TestExpandQueryCache:
    """Cache behavior."""

    def test_cache_hit(self):
        _LLM_EXPANSION_CACHE["cached query"] = ["term1", "term2"]
        try:
            orig_enabled = mcp_server.LLM_EXPANSION_ENABLED
            orig_key = mcp_server.ANTHROPIC_API_KEY
            mcp_server.LLM_EXPANSION_ENABLED = True
            mcp_server.ANTHROPIC_API_KEY = "test-key"
            result = _expand_query_with_llm("Cached Query")
            assert result == ["term1", "term2"]
        finally:
            mcp_server.LLM_EXPANSION_ENABLED = orig_enabled
            mcp_server.ANTHROPIC_API_KEY = orig_key
            _LLM_EXPANSION_CACHE.pop("cached query", None)


class TestExpandQueryTimeout:
    """On HTTP errors or timeout, returns empty list gracefully."""

    def test_timeout_returns_empty(self):
        orig_enabled = mcp_server.LLM_EXPANSION_ENABLED
        orig_key = mcp_server.ANTHROPIC_API_KEY
        try:
            mcp_server.LLM_EXPANSION_ENABLED = True
            mcp_server.ANTHROPIC_API_KEY = "test-key"
            # Use a very short timeout and unreachable host
            orig_timeout = mcp_server.LLM_EXPANSION_TIMEOUT
            mcp_server.LLM_EXPANSION_TIMEOUT = 0.01
            result = _expand_query_with_llm("timeout test query xyz")
            assert result == []
        finally:
            mcp_server.LLM_EXPANSION_ENABLED = orig_enabled
            mcp_server.ANTHROPIC_API_KEY = orig_key
            mcp_server.LLM_EXPANSION_TIMEOUT = orig_timeout
            _LLM_EXPANSION_CACHE.pop("timeout test query xyz", None)

    def test_httpx_not_installed(self):
        orig_enabled = mcp_server.LLM_EXPANSION_ENABLED
        orig_key = mcp_server.ANTHROPIC_API_KEY
        try:
            mcp_server.LLM_EXPANSION_ENABLED = True
            mcp_server.ANTHROPIC_API_KEY = "test-key"
            import builtins
            real_import = builtins.__import__

            def mock_import(name, *args, **kwargs):
                if name == "httpx":
                    raise ImportError("mocked")
                return real_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=mock_import):
                result = _expand_query_with_llm("no httpx test xyz")
                assert result == []
        finally:
            mcp_server.LLM_EXPANSION_ENABLED = orig_enabled
            mcp_server.ANTHROPIC_API_KEY = orig_key
            _LLM_EXPANSION_CACHE.pop("no httpx test xyz", None)


class TestBuildQueryStrategiesIntegration:
    """Verify _build_query_strategies returns llm_terms and adds llm_expanded strategy."""

    def test_returns_tuple(self):
        """_build_query_strategies now returns (strategies, llm_terms)."""
        result = _build_query_strategies("Mietrecht Kündigung")
        assert isinstance(result, tuple)
        assert len(result) == 2
        strategies, llm_terms = result
        assert isinstance(strategies, list)
        assert isinstance(llm_terms, list)

    def test_llm_expanded_strategy_with_cached_terms(self):
        """When LLM cache has terms, llm_expanded strategy should appear."""
        _LLM_EXPANSION_CACHE["mietrecht kundigung"] = [
            "bail", "locazione", "contrat de bail"
        ]
        orig_enabled = mcp_server.LLM_EXPANSION_ENABLED
        orig_key = mcp_server.ANTHROPIC_API_KEY
        try:
            mcp_server.LLM_EXPANSION_ENABLED = True
            mcp_server.ANTHROPIC_API_KEY = "test-key"
            strategies, llm_terms = _build_query_strategies("Mietrecht Kundigung")
            assert llm_terms == ["bail", "locazione", "contrat de bail"]
            strategy_names = [s["name"] for s in strategies]
            assert "llm_expanded" in strategy_names
            llm_strat = next(s for s in strategies if s["name"] == "llm_expanded")
            assert llm_strat["weight"] == 0.9
            # Verify the OR query contains normalized terms
            assert "bail" in llm_strat["query"]
            assert "locazione" in llm_strat["query"]
        finally:
            mcp_server.LLM_EXPANSION_ENABLED = orig_enabled
            mcp_server.ANTHROPIC_API_KEY = orig_key
            _LLM_EXPANSION_CACHE.pop("mietrecht kundigung", None)

    def test_no_llm_strategy_when_disabled(self):
        """When LLM expansion is disabled, no llm_expanded strategy."""
        orig = mcp_server.LLM_EXPANSION_ENABLED
        try:
            mcp_server.LLM_EXPANSION_ENABLED = False
            strategies, llm_terms = _build_query_strategies("Mietrecht")
            assert llm_terms == []
            strategy_names = [s["name"] for s in strategies]
            assert "llm_expanded" not in strategy_names
        finally:
            mcp_server.LLM_EXPANSION_ENABLED = orig


# ---------------------------------------------------------------------------
# Live tests (require ANTHROPIC_API_KEY and network access)
# ---------------------------------------------------------------------------


@pytest.mark.live
def test_llm_expansion_returns_terms():
    """LLM expansion should return legal terms for a concept query."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY not set")
    orig_key = mcp_server.ANTHROPIC_API_KEY
    orig_enabled = mcp_server.LLM_EXPANSION_ENABLED
    try:
        mcp_server.ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
        mcp_server.LLM_EXPANSION_ENABLED = True
        _LLM_EXPANSION_CACHE.pop("hundebiss", None)
        terms = _expand_query_with_llm("Hundebiss")
        assert len(terms) >= 2
        all_terms = " ".join(terms).lower()
        assert any(
            t in all_terms
            for t in ["tierhalterhaftung", "art. 56", "haftung", "animal", "morsure"]
        ), f"Expected animal liability terms, got: {terms}"
    finally:
        mcp_server.ANTHROPIC_API_KEY = orig_key
        mcp_server.LLM_EXPANSION_ENABLED = orig_enabled


@pytest.mark.live
def test_llm_expansion_cross_lingual():
    """LLM expansion should provide cross-lingual equivalents."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY not set")
    orig_key = mcp_server.ANTHROPIC_API_KEY
    orig_enabled = mcp_server.LLM_EXPANSION_ENABLED
    try:
        mcp_server.ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
        mcp_server.LLM_EXPANSION_ENABLED = True
        _LLM_EXPANSION_CACHE.pop("responsabilité du détenteur d'animal", None)
        terms = _expand_query_with_llm("responsabilité du détenteur d'animal")
        assert len(terms) >= 2
        all_terms = " ".join(terms).lower()
        # Should include German equivalents
        assert any(
            t in all_terms
            for t in ["tierhalterhaftung", "art. 56", "haftung", "hund"]
        ), f"Expected German legal terms, got: {terms}"
    finally:
        mcp_server.ANTHROPIC_API_KEY = orig_key
        mcp_server.LLM_EXPANSION_ENABLED = orig_enabled


@pytest.mark.live
def test_llm_expansion_caches_result():
    """Second call with same query should use cache (instant)."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY not set")
    import time

    orig_key = mcp_server.ANTHROPIC_API_KEY
    orig_enabled = mcp_server.LLM_EXPANSION_ENABLED
    try:
        mcp_server.ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
        mcp_server.LLM_EXPANSION_ENABLED = True
        _LLM_EXPANSION_CACHE.pop("mietrecht kündigung", None)

        # First call — may take 1-2s
        terms1 = _expand_query_with_llm("Mietrecht Kündigung")
        assert len(terms1) >= 2

        # Second call — should be near-instant from cache
        t0 = time.perf_counter()
        terms2 = _expand_query_with_llm("mietrecht kündigung")
        elapsed = time.perf_counter() - t0
        assert terms2 == terms1
        assert elapsed < 0.01, f"Cache hit should be <10ms, got {elapsed*1000:.1f}ms"
    finally:
        mcp_server.ANTHROPIC_API_KEY = orig_key
        mcp_server.LLM_EXPANSION_ENABLED = orig_enabled
