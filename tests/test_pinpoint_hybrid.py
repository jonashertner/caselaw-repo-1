"""Tests for PINPOINT_SEMANTIC_HYBRID — runs both signals and uses
cross-signal agreement to boost confidence.

Mock embeddings throughout; tests pin the agreement-vs-disagreement
semantics + the off-by-default flag behaviour."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pytest

import mcp_server


class _FakeModel:
    def __init__(self, claim_to_vec):
        self._map = claim_to_vec

    def encode(self, claim, **kwargs):
        if claim in self._map:
            return self._map[claim]
        return np.zeros(3, dtype=np.float32)

    def get_sentence_embedding_dimension(self):
        return 3


def _norm(*vals):
    v = np.array(vals, dtype=np.float32)
    n = np.linalg.norm(v)
    return v / (n if n > 0 else 1.0)


def _make_lex_db(tmp_path: Path, paragraphs):
    """Build a tiny FTS5 structure DB with lexical content."""
    sd = tmp_path / "structure.db"
    conn = sqlite3.connect(str(sd))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE erwaegungen_paragraph (
            decision_id TEXT, e_number TEXT, depth INTEGER, parent TEXT,
            text TEXT, PRIMARY KEY (decision_id, e_number)
        );
        CREATE VIRTUAL TABLE erwaegungen_paragraph_fts USING fts5(
            text, content='erwaegungen_paragraph',
            content_rowid='rowid', tokenize='unicode61 remove_diacritics 1'
        );
    """)
    for did, en, txt in paragraphs:
        conn.execute(
            "INSERT INTO erwaegungen_paragraph VALUES (?, ?, ?, ?, ?)",
            (did, en, 1, None, txt),
        )
    conn.execute(
        "INSERT INTO erwaegungen_paragraph_fts(erwaegungen_paragraph_fts) "
        "VALUES ('rebuild')"
    )
    conn.commit()
    return conn


@pytest.fixture
def hybrid_on(monkeypatch):
    monkeypatch.setattr(mcp_server, "PINPOINT_SEMANTIC_ENABLED", True)
    monkeypatch.setattr(mcp_server, "PINPOINT_SEMANTIC_HYBRID", True)
    yield monkeypatch


def test_hybrid_off_by_default_doesnt_alter_lexical_result(tmp_path, monkeypatch):
    """Default: hybrid disabled → result is unmodified lexical (source='lexical')."""
    conn = _make_lex_db(tmp_path, [
        ("d_1", "1", "Mietrecht Kündigung Wohnung — alle drei hier."),
        ("d_1", "2", "Etwas über Strafrecht ohne Bezug."),
        ("d_1", "3", "Sachverhalt Streitsache vor Vorinstanz."),
        ("d_1", "4", "Dispositiv: abgewiesen."),
        ("d_1", "5", "Kostenfolgen."),
    ])
    # Even if model would suggest a different paragraph, hybrid is OFF
    monkeypatch.setattr(mcp_server, "PINPOINT_SEMANTIC_HYBRID", False)
    monkeypatch.setattr(mcp_server, "PINPOINT_SEMANTIC_ENABLED", True)
    monkeypatch.setattr(mcp_server, "_get_semantic_model",
                        lambda: _FakeModel({"Mietrecht Kündigung Wohnung": _norm(1, 0, 0)}))
    monkeypatch.setattr(mcp_server, "_fetch_paragraph_embeddings",
                        lambda did, conn=None: [("99", _norm(1, 0, 0))])
    try:
        out = mcp_server._compute_pinpoint("d_1", "Mietrecht Kündigung Wohnung", conn=conn)
        assert out is not None
        assert out["e_number"] == "1"
        assert out["source"] == "lexical"
        assert "semantic_alternative" not in out
        assert "semantic_score" not in out
    finally:
        conn.close()


def test_hybrid_agreement_boosts_to_high(hybrid_on, tmp_path):
    """When lexical and semantic agree on the same e_number → high confidence."""
    conn = _make_lex_db(tmp_path, [
        ("d_1", "1", "Mietrecht Kündigung Wohnung — alle drei Schlüsselwörter."),
        ("d_1", "2", "Strafrecht ohne Bezug."),
        ("d_1", "3", "Sachverhalt vor Vorinstanz."),
        ("d_1", "4", "Dispositiv: abgewiesen."),
        ("d_1", "5", "Kostenfolgen."),
    ])
    # Semantic ALSO picks "1" (cosine 1.0 vs other paragraphs)
    hybrid_on.setattr(mcp_server, "_get_semantic_model",
                      lambda: _FakeModel({"Mietrecht Kündigung Wohnung": _norm(1, 0, 0)}))
    hybrid_on.setattr(mcp_server, "_fetch_paragraph_embeddings",
                      lambda did, conn=None: [
                          ("1", _norm(1, 0, 0)),     # cosine 1.0 — winner
                          ("2", _norm(0, 1, 0)),     # cosine 0
                      ])
    try:
        out = mcp_server._compute_pinpoint("d_1", "Mietrecht Kündigung Wohnung", conn=conn)
        assert out is not None
        assert out["e_number"] == "1"
        assert out["confidence"] == "high"
        assert out["source"] == "hybrid_agreement"
        assert "semantic_score" in out
        assert out["semantic_score"] > 0.99  # was cosine 1.0
    finally:
        conn.close()


def test_hybrid_disagreement_keeps_lexical_with_alternative(hybrid_on, tmp_path):
    """When lexical picks E.1 but semantic picks E.5 → keep lexical, expose alt."""
    conn = _make_lex_db(tmp_path, [
        ("d_1", "1", "Mietrecht Kündigung Wohnung — alle drei hier."),
        ("d_1", "2", "Strafrecht ohne Bezug."),
        ("d_1", "3", "Sachverhalt vor Vorinstanz."),
        ("d_1", "4", "Dispositiv: abgewiesen."),
        ("d_1", "5", "Kostenfolgen."),
    ])
    # Semantic picks "5" instead — disagreement case
    hybrid_on.setattr(mcp_server, "_get_semantic_model",
                      lambda: _FakeModel({"Mietrecht Kündigung Wohnung": _norm(1, 0, 0)}))
    hybrid_on.setattr(mcp_server, "_fetch_paragraph_embeddings",
                      lambda did, conn=None: [
                          ("1", _norm(0, 1, 0)),  # cosine 0
                          ("5", _norm(1, 0, 0)),  # cosine 1.0 — semantic winner
                      ])
    try:
        out = mcp_server._compute_pinpoint("d_1", "Mietrecht Kündigung Wohnung", conn=conn)
        assert out is not None
        # Lexical winner (E.1) is preserved
        assert out["e_number"] == "1"
        assert out["source"] == "lexical_semantic_disagree"
        # Semantic alternative is exposed for downstream consumption
        assert "semantic_alternative" in out
        assert out["semantic_alternative"]["e_number"] == "5"
        assert out["semantic_alternative"]["score"] > 0.99
    finally:
        conn.close()


def test_hybrid_no_semantic_match_keeps_lexical(hybrid_on, tmp_path):
    """Hybrid ON but semantic returns None (e.g. cosine below threshold)
    → lexical result returned unmodified."""
    conn = _make_lex_db(tmp_path, [
        ("d_1", "1", "Mietrecht Kündigung Wohnung — alle drei hier."),
        ("d_1", "2", "Strafrecht ohne Bezug."),
        ("d_1", "3", "Sachverhalt vor Vorinstanz."),
        ("d_1", "4", "Dispositiv: abgewiesen."),
        ("d_1", "5", "Kostenfolgen."),
    ])
    # All cosines below medium floor (0.55) → semantic returns None
    hybrid_on.setattr(mcp_server, "_get_semantic_model",
                      lambda: _FakeModel({"Mietrecht Kündigung Wohnung": _norm(1, 0, 0)}))
    hybrid_on.setattr(mcp_server, "_fetch_paragraph_embeddings",
                      lambda did, conn=None: [
                          ("1", _norm(0.3, 0.95, 0)),  # cosine 0.3 — below 0.55
                          ("2", _norm(0.2, 0.98, 0)),  # cosine 0.2 — below 0.55
                      ])
    try:
        out = mcp_server._compute_pinpoint("d_1", "Mietrecht Kündigung Wohnung", conn=conn)
        assert out is not None
        assert out["e_number"] == "1"
        assert out["source"] == "lexical"  # unchanged
        assert "semantic_alternative" not in out
    finally:
        conn.close()


def test_hybrid_semantic_failure_falls_back_silently(hybrid_on, tmp_path):
    """If the semantic step throws (e.g. model load fails mid-call),
    the lexical result is still returned — hybrid is enrichment, never
    blocking."""
    conn = _make_lex_db(tmp_path, [
        ("d_1", "1", "Mietrecht Kündigung Wohnung — alle drei hier."),
        ("d_1", "2", "Strafrecht ohne Bezug."),
        ("d_1", "3", "Sachverhalt vor Vorinstanz."),
        ("d_1", "4", "Dispositiv: abgewiesen."),
        ("d_1", "5", "Kostenfolgen."),
    ])

    def _broken_model():
        raise RuntimeError("model load died")

    hybrid_on.setattr(mcp_server, "_get_semantic_model", _broken_model)
    hybrid_on.setattr(mcp_server, "_fetch_paragraph_embeddings",
                      lambda did, conn=None: [("1", _norm(1, 0, 0))])
    try:
        out = mcp_server._compute_pinpoint("d_1", "Mietrecht Kündigung Wohnung", conn=conn)
        assert out is not None
        assert out["e_number"] == "1"
        assert out["source"] == "lexical"  # fallback
    finally:
        conn.close()
