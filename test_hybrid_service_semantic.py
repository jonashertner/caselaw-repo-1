import sqlite3
from pathlib import Path

import search_stack.hybrid_service as hs


def test_embed_query_vector_returns_none_when_disabled(monkeypatch):
    monkeypatch.setattr(hs, "SEMANTIC_QUERY_EMBEDDING", False)
    assert hs._embed_query_vector("test query") is None


def test_embed_query_vector_returns_none_on_model_failure(monkeypatch):
    monkeypatch.setattr(hs, "SEMANTIC_QUERY_EMBEDDING", True)
    monkeypatch.setattr(hs, "_EMBED_MODEL", None)
    monkeypatch.setattr(hs, "_EMBED_MODEL_FAILED", True)
    assert hs._embed_query_vector("test query") is None


def test_reference_graph_store_falls_back_to_legacy_target_decision_id(tmp_path: Path):
    db = tmp_path / "graph.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE decision_citations (
            source_decision_id TEXT NOT NULL,
            target_ref TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_decision_id TEXT,
            mention_count INTEGER NOT NULL DEFAULT 1
        );
        INSERT INTO decision_citations(source_decision_id, target_ref, target_type, target_decision_id, mention_count)
        VALUES ('src1', 'VB_2018_00411', 'docket', 'target1', 2);
        """
    )
    conn.commit()
    conn.close()

    store = hs.ReferenceGraphStore(db)
    incoming = store.incoming_citations("target1", limit=5)
    outgoing = store.outgoing_citations("src1", limit=5)

    assert incoming
    assert incoming[0]["source_decision_id"] == "src1"
    assert incoming[0]["target_decision_id"] == "target1"
    assert incoming[0]["match_type"] == "legacy_target_decision_id"

    assert outgoing
    assert outgoing[0]["target_decision_id"] == "target1"
    assert outgoing[0]["confidence_score"] == 1.0


def test_reference_graph_store_unresolved_target_has_no_confidence(tmp_path: Path):
    db = tmp_path / "graph.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE decision_citations (
            source_decision_id TEXT NOT NULL,
            target_ref TEXT NOT NULL,
            target_type TEXT NOT NULL,
            mention_count INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE citation_targets (
            source_decision_id TEXT NOT NULL,
            target_ref TEXT NOT NULL,
            target_decision_id TEXT NOT NULL,
            match_type TEXT NOT NULL,
            confidence_score REAL NOT NULL
        );
        INSERT INTO decision_citations(source_decision_id, target_ref, target_type, mention_count)
        VALUES ('src1', 'VB_2018_00411', 'docket', 2);
        """
    )
    conn.commit()
    conn.close()

    store = hs.ReferenceGraphStore(db)
    outgoing = store.outgoing_citations("src1", limit=5)

    assert outgoing
    assert outgoing[0]["target_decision_id"] is None
    assert outgoing[0]["confidence_score"] is None
    assert outgoing[0]["weighted_mention_count"] is None
