"""
Search pipeline configuration — all tunable parameters in one place.

The optimizer modifies this config between iterations.
The evaluator reads it and sets the corresponding mcp_server globals.
"""

# Default config — matches current production values
DEFAULT_CONFIG = {
    # RRF fusion
    "rrf_rank_constant": 60,

    # Graph signals
    "graph_signals_enabled": True,

    # LLM reranking (set to False for offline-only optimization)
    "llm_rerank_enabled": False,
    "llm_rerank_top_n": 15,
    "llm_rerank_weight": 3.0,
    "llm_rerank_confidence_gate": 2.0,

    # Cross-encoder (disabled by default)
    "cross_encoder_enabled": False,
    "cross_encoder_weight": 1.4,
    "cross_encoder_top_n": 30,

    # Vector search (disabled — no vectors.db)
    "vector_search_enabled": False,
    "vector_weight": 1.0,
    "vector_k": 50,
    "vector_signal_weight": 3.0,

    # Sparse search
    "sparse_search_enabled": False,
    "sparse_signal_weight": 2.5,
    "sparse_rrf_weight": 1.2,
    "sparse_k": 100,

    # Query understanding
    "llm_query_parse_enabled": False,  # offline: no LLM calls
    "synonym_expansion_enabled": True,
    "compound_decomposition_enabled": True,
    "umlaut_bridge_enabled": True,

    # Result scoring weights (applied in _score_and_rank)
    # These are relative — the scoring function combines:
    # - BM25 from FTS5 (base signal)
    # - Metadata match bonus (court, date proximity)
    # - Citation count bonus (log-scaled)
    # - Leading case bonus
    # - BGE authority bonus
}


def apply_config(config: dict):
    """Apply config dict to mcp_server globals."""
    import mcp_server

    mcp_server.RRF_RANK_CONSTANT = config.get("rrf_rank_constant", 60)
    mcp_server.GRAPH_SIGNALS_ENABLED = config.get("graph_signals_enabled", True)
    mcp_server.LLM_RERANK_ENABLED = config.get("llm_rerank_enabled", False)
    mcp_server.LLM_RERANK_TOP_N = config.get("llm_rerank_top_n", 15)
    mcp_server.LLM_RERANK_WEIGHT = config.get("llm_rerank_weight", 3.0)
    mcp_server.LLM_RERANK_CONFIDENCE_GATE = config.get("llm_rerank_confidence_gate", 2.0)
    mcp_server.CROSS_ENCODER_ENABLED = config.get("cross_encoder_enabled", False)
    mcp_server.CROSS_ENCODER_WEIGHT = config.get("cross_encoder_weight", 1.4)
    mcp_server.CROSS_ENCODER_TOP_N = config.get("cross_encoder_top_n", 30)
    mcp_server.VECTOR_SEARCH_ENABLED = "1" if config.get("vector_search_enabled") else "0"
    mcp_server.VECTOR_WEIGHT = config.get("vector_weight", 1.0)
    mcp_server.VECTOR_K = config.get("vector_k", 50)
    mcp_server.VECTOR_SIGNAL_WEIGHT = config.get("vector_signal_weight", 3.0)
    mcp_server.SPARSE_SEARCH_ENABLED = "1" if config.get("sparse_search_enabled") else "0"
    mcp_server.SPARSE_SIGNAL_WEIGHT = config.get("sparse_signal_weight", 2.5)
    mcp_server.SPARSE_RRF_WEIGHT = config.get("sparse_rrf_weight", 1.2)
    mcp_server.SPARSE_K = config.get("sparse_k", 100)
