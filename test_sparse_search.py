"""Tests for sparse search and chunk vector search in mcp_server."""

from __future__ import annotations

from unittest.mock import patch

import pytest

import mcp_server
from mcp_server import (
    _rerank_rows,
    SPARSE_SIGNAL_WEIGHT,
    SPARSE_SEARCH_ENABLED,
)


class TestSparseSearchDisabled:
    """When sparse search is disabled, returns empty dict."""

    def test_sparse_disabled_by_env(self):
        orig = mcp_server.SPARSE_SEARCH_ENABLED
        try:
            mcp_server.SPARSE_SEARCH_ENABLED = "false"
            result = mcp_server._search_sparse("test query")
            assert result == {}
        finally:
            mcp_server.SPARSE_SEARCH_ENABLED = orig


class TestSparseSignalWeight:
    """Verify sparse signal weight is configured correctly."""

    def test_sparse_signal_weight_default(self):
        assert SPARSE_SIGNAL_WEIGHT == 2.5

    def test_sparse_signal_weight_positive(self):
        assert SPARSE_SIGNAL_WEIGHT > 0


class TestChunkVectorSearch:
    """Chunk vector search integration."""

    def test_chunk_search_no_vec_db(self):
        """When no vector DB exists, returns empty dict."""
        result = mcp_server._search_vectors_chunks("test query")
        assert result == {}


class TestRerankWithSparse:
    """Verify _rerank_rows accepts and uses sparse_scores parameter."""

    def test_rerank_accepts_sparse_scores(self):
        """_rerank_rows should accept sparse_scores without error."""
        # We can't easily construct sqlite3.Row objects, but we can verify
        # the function signature accepts the parameter
        import inspect
        sig = inspect.signature(_rerank_rows)
        assert "sparse_scores" in sig.parameters
        param = sig.parameters["sparse_scores"]
        assert param.default is None
