import sqlite3
from pathlib import Path

import mcp_server


def _row(decision_id: str, *, bm25: float, title: str = "", regeste: str = "", snippet: str = "") -> dict:
    return {
        "decision_id": decision_id,
        "court": "bger",
        "canton": "CH",
        "chamber": None,
        "docket_number": f"X_{decision_id}/2025",
        "decision_date": "2025-01-01",
        "language": "de",
        "title": title,
        "regeste": regeste,
        "snippet": snippet,
        "source_url": None,
        "pdf_url": None,
        "bm25_score": bm25,
    }


def test_query_strategies_include_legal_expansion():
    strategies = mcp_server._build_query_strategies("asile renvoi")
    expanded = next(s for s in strategies if s["name"] == "nl_or_expanded")
    q = expanded["query"]
    assert "asyl" in q
    assert "wegweisung" in q


def test_query_strategies_include_field_focus_queries():
    strategies = mcp_server._build_query_strategies("Asyl und Wegweisung")
    names = {s["name"] for s in strategies}
    assert any(name.startswith("anchor_") for name in names)
    assert "regeste_focus" in names
    assert "title_focus" in names
    ordered_names = [s["name"] for s in strategies]
    assert ordered_names.index("regeste_focus") < ordered_names.index("nl_or")


def test_detect_query_languages_french():
    langs = mcp_server._detect_query_languages(
        "Je cherche un arrêt sur le permis de construire"
    )
    assert "fr" in langs


def test_expand_rank_terms_for_match_uses_legal_expansions():
    expanded = mcp_server._expand_rank_terms_for_match(["beschleunigtes"])
    assert "verkurzte" in expanded


def test_query_strategies_include_language_focus_when_detected():
    strategies = mcp_server._build_query_strategies(
        "Je cherche un arrêt sur le permis de construire"
    )
    names = {s["name"] for s in strategies}
    assert "lang_fr_and" in names


def test_query_strategies_include_anchor_pair_for_windpark_query():
    strategies = mcp_server._build_query_strategies(
        "Je cherche un arrêt sur le permis de construire d'un parc éolien"
    )
    anchor_queries = {
        s["query"]
        for s in strategies
        if s["name"].startswith("anchor_pair_")
    }
    assert "parc AND eolien" in anchor_queries


def test_rerank_uses_fusion_scores_to_break_close_lexical_ties():
    rows = [
        _row("d_fused", bm25=1.25, title="asyl", regeste="wegweisung"),
        _row("d_plain", bm25=1.05, title="asyl", regeste="wegweisung"),
    ]
    results = mcp_server._rerank_rows(
        rows,
        "Asyl Wegweisung",
        limit=2,
        fusion_scores={
            "d_fused": {"rrf_score": 0.05, "strategy_hits": 3},
            "d_plain": {"rrf_score": 0.0, "strategy_hits": 1},
        },
    )
    assert results[0]["decision_id"] == "d_fused"


def test_rerank_boosts_statute_hits_from_graph_signals(tmp_path: Path, monkeypatch):
    graph_db = tmp_path / "reference_graph.db"
    conn = sqlite3.connect(graph_db)
    conn.executescript(
        """
        CREATE TABLE decision_statutes (
            decision_id TEXT NOT NULL,
            statute_id TEXT NOT NULL,
            mention_count INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE decision_citations (
            source_decision_id TEXT NOT NULL,
            target_ref TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_decision_id TEXT,
            mention_count INTEGER NOT NULL DEFAULT 1
        );
        """
    )
    conn.execute(
        "INSERT INTO decision_statutes(decision_id, statute_id, mention_count) VALUES (?, ?, ?)",
        ("d_hit", "ART.8.EMRK", 3),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(mcp_server, "GRAPH_DB_PATH", graph_db)
    monkeypatch.setattr(mcp_server, "GRAPH_SIGNALS_ENABLED", True)

    rows = [
        _row("d_miss", bm25=1.0),
        _row("d_hit", bm25=1.0),
    ]
    results = mcp_server._rerank_rows(
        rows,
        "Art. 8 EMRK",
        limit=2,
        fusion_scores={
            "d_miss": {"rrf_score": 0.0, "strategy_hits": 1},
            "d_hit": {"rrf_score": 0.0, "strategy_hits": 1},
        },
    )
    assert results[0]["decision_id"] == "d_hit"


def test_passage_snippet_prefers_relevant_paragraph():
    text = (
        "Einleitung ohne Treffer.\n\n"
        "Dieser Absatz enthält Art. 8 EMRK und den relevanten Kern der Begründung.\n\n"
        "Schlussteil."
    )
    snippet = mcp_server._select_best_passage_snippet(
        text,
        rank_terms=["art", "8", "emrk"],
        phrase="art 8 emrk",
        fallback="fallback",
    )
    assert snippet is not None
    assert "Art. 8 EMRK" in snippet


def test_asylum_query_boosts_bvger_court():
    rows = [
        _row("d_bger", bm25=1.0, title="Asyl und Wegweisung", regeste="Asyl und Wegweisung"),
        _row("d_bvger", bm25=1.0, title="Asyl und Wegweisung", regeste="Asyl und Wegweisung"),
    ]
    rows[0]["court"] = "bger"
    rows[1]["court"] = "bvger"
    rows[1]["docket_number"] = "E-7414/2015"

    results = mcp_server._rerank_rows(
        rows,
        "Asyl und Wegweisung",
        limit=2,
        fusion_scores={
            "d_bger": {"rrf_score": 0.0, "strategy_hits": 1},
            "d_bvger": {"rrf_score": 0.0, "strategy_hits": 1},
        },
    )
    assert results[0]["decision_id"] == "d_bvger"


def test_asylum_accelerated_query_boosts_equivalent_term():
    rows = [
        _row("d_plain", bm25=1.0, title="Asyl und Wegweisung", regeste="Asyl und Wegweisung"),
        _row(
            "d_equiv",
            bm25=1.0,
            title="Asyl und Wegweisung",
            regeste="Asyl und Wegweisung (verkuerzte Beschwerdefrist)",
        ),
    ]
    rows[0]["court"] = "bvger"
    rows[1]["court"] = "bvger"

    results = mcp_server._rerank_rows(
        rows,
        "Asyl und Wegweisung beschleunigtes Verfahren",
        limit=2,
        fusion_scores={
            "d_plain": {"rrf_score": 0.0, "strategy_hits": 1},
            "d_equiv": {"rrf_score": 0.0, "strategy_hits": 1},
        },
    )
    assert results[0]["decision_id"] == "d_equiv"


def test_decision_intent_query_prefers_high_court():
    rows = [
        _row("d_bger", bm25=1.0, title="permis de construire", regeste="parc eolien"),
        _row("d_canton", bm25=1.0, title="permis de construire", regeste="parc eolien"),
    ]
    rows[0]["court"] = "bger"
    rows[1]["court"] = "ne_gerichte"

    results = mcp_server._rerank_rows(
        rows,
        "Je cherche un arrêt sur le permis de construire",
        limit=2,
        fusion_scores={
            "d_bger": {"rrf_score": 0.0, "strategy_hits": 1},
            "d_canton": {"rrf_score": 0.0, "strategy_hits": 1},
        },
    )
    assert results[0]["decision_id"] == "d_bger"


def test_query_has_numeric_terms_detection():
    assert mcp_server._query_has_numeric_terms("Art. 8 EMRK")
    assert not mcp_server._query_has_numeric_terms("Asyl und Wegweisung")


def test_extract_query_statute_refs_handles_multilingual_paragraph_marker():
    refs = mcp_server._extract_query_statute_refs("selon art. 8 al. 1 CEDH")
    assert "ART.8.ABS.1.CEDH" in refs
    assert "ART.8.AL" not in refs


def test_looks_like_docket_query_is_strict_for_long_nl_query():
    assert mcp_server._looks_like_docket_query("4A_291/2017")
    assert not mcp_server._looks_like_docket_query(
        "ZB.2016.28 (13.04.2017 / Rückweisung 23.08.2018) – "
        "Herabsetzung des Mietzinses (BGer 4A_291/2017)"
    )


def test_extract_inline_docket_candidates():
    cands = mcp_server._extract_inline_docket_candidates(
        "Rückweisung gemäss BGer 4A_291/2017 und Folgeentscheid ZB.2016.28"
    )
    assert "4A_291/2017" in cands
    assert "ZB.2016.28" in cands


def test_detect_query_preferred_courts_bger():
    prefs = mcp_server._detect_query_preferred_courts("BGer 4A_291/2017")
    assert "bger" in prefs
    assert "bge" in prefs


def test_rerank_boosts_language_match():
    rows = [
        _row("d_fr", bm25=1.0, title="permis de construire", regeste="parc eolien"),
        _row("d_de", bm25=1.0, title="baubewilligung", regeste="windpark"),
    ]
    rows[0]["language"] = "fr"
    rows[1]["language"] = "de"

    results = mcp_server._rerank_rows(
        rows,
        "Je cherche un arrêt sur le permis de construire",
        limit=2,
        fusion_scores={
            "d_fr": {"rrf_score": 0.0, "strategy_hits": 1},
            "d_de": {"rrf_score": 0.0, "strategy_hits": 1},
        },
    )
    assert results[0]["decision_id"] == "d_fr"
