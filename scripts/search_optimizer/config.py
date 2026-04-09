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

    # ── Rerank signal weights ──
    "w_docket_exact": 6.0,
    "w_docket_partial": 2.0,
    "w_title_cov": 3.0,
    "w_regeste_cov": 2.2,
    "w_snippet_cov": 0.8,
    "w_expanded_regeste_cov": 1.2,
    "w_expanded_title_cov": 0.8,
    "w_phrase_hit": 1.8,
    "w_rrf_score": 32.0,
    "w_strategy_hits": 0.18,
    "strategy_hits_cap": 8,

    # ── Graph signals ──
    "statute_signal_base": 2.2,
    "statute_signal_cap": 1.2,
    "statute_signal_per_mention": 0.25,
    "citation_signal_base": 2.4,
    "citation_signal_cap": 1.2,
    "citation_signal_per_hit": 0.30,
    "authority_signal_per_citation": 0.03,
    "authority_signal_cap": 1.0,
    "in_pool_signal_multiplier": 0.5,
    "in_pool_signal_cap": 1.2,
    "in_pool_min_citations": 2,

    # ── Local reference signals ──
    "local_statute_match_signal": 0.8,
    "local_citation_match_signal": 0.8,

    # ── Court/domain signals ──
    "asylum_bvger_boost": 1.7,
    "asylum_bger_penalty": -0.2,
    "asylum_e_docket_boost": 0.45,
    "decision_intent_boost": 0.65,
    "accelerated_procedure_signal": 0.9,
    "language_match_signal": 0.9,

    # ── Strategy weights ──
    "sw_raw": 1.5,
    "sw_quoted_explicit": 1.1,
    "sw_regeste_focus_explicit": 0.95,
    "sw_title_focus_explicit": 0.85,
    "sw_nl_and_explicit": 0.9,
    "sw_nl_or_explicit": 0.7,
    "sw_nl_and": 1.3,
    "sw_regeste_focus": 1.05,
    "sw_title_focus": 0.95,
    "sw_quoted": 1.15,
    "sw_nl_or": 1.0,
    "sw_nl_or_expanded": 0.85,

    # ── Fusion pipeline weights ──
    "statute_graph_rrf_weight": 1.0,
    "sg_weight_with_keywords": 1.0,
    "sg_weight_pure_statute": 1.5,
    "sg_weight_unstructured_with_keywords": 0.7,
    "llm_bge_rrf_weight": 2.0,
    "structured_bge_rrf_weight": 2.5,

    # ── Doctrine strategy weights ──
    "doctrine_concept_translation_weight": 1.5,
    "doctrine_direct_weight": 1.1,
    "doctrine_regeste_weight": 1.6,
    "doctrine_title_weight": 1.3,
    "doctrine_cross_lingual_weight": 1.3,

    # ── BM25 column weights ──
    "bm25_decision_id": 0.8,
    "bm25_court": 0.8,
    "bm25_canton": 0.8,
    "bm25_docket_number": 2.0,
    "bm25_language": 0.8,
    "bm25_title": 6.0,
    "bm25_regeste": 5.0,
    "bm25_full_text": 1.2,
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

    # LLM query parse — was dead code, now wired up
    mcp_server.LLM_EXPANSION_ENABLED = config.get("llm_query_parse_enabled", False)

    # Patch SCORING_CONFIG dict (all rerank/strategy/fusion/BM25 weights)
    for key in mcp_server.SCORING_CONFIG:
        if key in config:
            mcp_server.SCORING_CONFIG[key] = config[key]
