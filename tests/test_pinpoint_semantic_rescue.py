"""Tests for the semantic-rescue branch of the pinpoint resolver.

These cover wiring + threshold logic without loading a real
sentence-transformer model — patches `_get_semantic_model`,
`_fetch_paragraph_embeddings`, and `PINPOINT_SEMANTIC_ENABLED` so each
test controls the (claim, embeddings) → cosine outputs deterministically.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pytest

import mcp_server


class _FakeModel:
    """Stand-in for a sentence-transformer. Returns a fixed embedding
    per claim string — lets tests control cosine outputs precisely."""

    def __init__(self, claim_to_vec: dict[str, np.ndarray]):
        self._map = claim_to_vec

    def encode(self, claim, **kwargs):
        if claim in self._map:
            return self._map[claim]
        # Default: zero vector → cosine 0 against everything → no rescue
        return np.zeros(3, dtype=np.float32)

    def get_sentence_embedding_dimension(self):
        return 3


def _norm(*vals: float) -> np.ndarray:
    v = np.array(vals, dtype=np.float32)
    n = np.linalg.norm(v)
    return v / (n if n > 0 else 1.0)


@pytest.fixture
def patched(monkeypatch):
    """Enable the rescue feature flag for this test scope."""
    monkeypatch.setattr(mcp_server, "PINPOINT_SEMANTIC_ENABLED", True)
    yield monkeypatch


def test_rescue_returns_none_when_flag_disabled(monkeypatch):
    monkeypatch.setattr(mcp_server, "PINPOINT_SEMANTIC_ENABLED", False)
    out = mcp_server._compute_pinpoint_semantic_rescue("d_1", "anything")
    assert out is None


def test_rescue_returns_none_when_no_embeddings(patched):
    """Decision has no precomputed embeddings → graceful no-op."""
    patched.setattr(mcp_server, "_get_semantic_model",
                    lambda: _FakeModel({"foo": _norm(1, 0, 0)}))
    patched.setattr(mcp_server, "_fetch_paragraph_embeddings",
                    lambda did, conn=None: [])
    out = mcp_server._compute_pinpoint_semantic_rescue("d_1", "foo")
    assert out is None


def test_rescue_returns_none_when_model_load_failed(patched):
    """Sentence-transformer not installed / model load raised."""
    patched.setattr(mcp_server, "_get_semantic_model", lambda: None)
    out = mcp_server._compute_pinpoint_semantic_rescue("d_1", "anything")
    assert out is None


def test_rescue_high_confidence_when_cosine_above_high_threshold(patched):
    """Cosine ≥ 0.70 → high. Pin the threshold semantics."""
    claim_vec = _norm(1, 0, 0)
    # Paragraph vector identical → cosine = 1.0 → high
    para_vecs = [
        ("1", _norm(1, 0, 0)),
        ("2", _norm(0, 1, 0)),  # cosine 0
    ]
    patched.setattr(mcp_server, "_get_semantic_model",
                    lambda: _FakeModel({"foo": claim_vec}))
    patched.setattr(mcp_server, "_fetch_paragraph_embeddings",
                    lambda did, conn=None: para_vecs)
    out = mcp_server._compute_pinpoint_semantic_rescue("d_1", "foo")
    assert out is not None
    assert out["e_number"] == "1"
    assert out["confidence"] == "high"
    assert out["source"] == "semantic"
    assert 0.99 < out["score"] <= 1.0


def test_rescue_medium_confidence_in_threshold_band(patched):
    """0.55 ≤ cosine < 0.70 → medium."""
    # Claim ~ 60° from para1 → cos ~ 0.6 (between MEDIUM=0.55 and HIGH=0.70)
    import math
    angle = math.radians(50)  # cos(50°) ≈ 0.643
    claim_vec = _norm(math.cos(angle), math.sin(angle), 0.0)
    para_vecs = [("1", _norm(1, 0, 0))]
    patched.setattr(mcp_server, "_get_semantic_model",
                    lambda: _FakeModel({"foobar": claim_vec}))
    patched.setattr(mcp_server, "_fetch_paragraph_embeddings",
                    lambda did, conn=None: para_vecs)
    out = mcp_server._compute_pinpoint_semantic_rescue("d_1", "foobar")
    assert out is not None
    assert out["confidence"] == "medium"
    assert 0.55 <= out["score"] < 0.70


def test_rescue_suppresses_below_medium_floor(patched):
    """Cosine < 0.55 → None (don't surface noise)."""
    import math
    angle = math.radians(70)  # cos(70°) ≈ 0.34, below medium floor
    claim_vec = _norm(math.cos(angle), math.sin(angle), 0.0)
    para_vecs = [("1", _norm(1, 0, 0))]
    patched.setattr(mcp_server, "_get_semantic_model",
                    lambda: _FakeModel({"foobar": claim_vec}))
    patched.setattr(mcp_server, "_fetch_paragraph_embeddings",
                    lambda did, conn=None: para_vecs)
    out = mcp_server._compute_pinpoint_semantic_rescue("d_1", "foobar")
    assert out is None


def test_rescue_picks_highest_cosine_paragraph(patched):
    """When multiple paragraphs match, returns the rank-1."""
    claim_vec = _norm(1, 0, 0)
    para_vecs = [
        ("1", _norm(0.5, 0.866, 0)),  # cos 0.5
        ("2", _norm(0.95, 0.31, 0)),  # cos 0.95 — winner
        ("3", _norm(0.7, 0.7, 0)),    # cos 0.7
    ]
    patched.setattr(mcp_server, "_get_semantic_model",
                    lambda: _FakeModel({"foobar": claim_vec}))
    patched.setattr(mcp_server, "_fetch_paragraph_embeddings",
                    lambda did, conn=None: para_vecs)
    out = mcp_server._compute_pinpoint_semantic_rescue("d_1", "foobar")
    assert out is not None
    assert out["e_number"] == "2"
    assert out["confidence"] == "high"


def test_rescue_url_includes_e_anchor(patched):
    claim_vec = _norm(1, 0, 0)
    patched.setattr(mcp_server, "_get_semantic_model",
                    lambda: _FakeModel({"foobar": claim_vec}))
    patched.setattr(mcp_server, "_fetch_paragraph_embeddings",
                    lambda did, conn=None: [("2.3", _norm(1, 0, 0))])
    out = mcp_server._compute_pinpoint_semantic_rescue("bge_BGE_140_III_86", "foobar")
    assert out is not None
    assert "e=2.3" in out["url"]


def test_rescue_uses_paragraph_text_lookup_for_snippet(patched):
    claim_vec = _norm(1, 0, 0)
    patched.setattr(mcp_server, "_get_semantic_model",
                    lambda: _FakeModel({"foobar": claim_vec}))
    patched.setattr(mcp_server, "_fetch_paragraph_embeddings",
                    lambda did, conn=None: [("1", _norm(1, 0, 0))])
    text_lookup = {"1": "This is the matched paragraph text."}
    out = mcp_server._compute_pinpoint_semantic_rescue(
        "d_1", "foobar", paragraph_text_lookup=text_lookup
    )
    assert out is not None
    assert "matched paragraph text" in out["matched_sentence"]
    assert "highlight=" in out["url"]


def test_rescue_returns_none_for_empty_or_short_claim(patched):
    patched.setattr(mcp_server, "_get_semantic_model",
                    lambda: _FakeModel({}))
    patched.setattr(mcp_server, "_fetch_paragraph_embeddings",
                    lambda did, conn=None: [("1", _norm(1, 0, 0))])
    assert mcp_server._compute_pinpoint_semantic_rescue("d_1", "") is None
    assert mcp_server._compute_pinpoint_semantic_rescue("d_1", "ab") is None
    assert mcp_server._compute_pinpoint_semantic_rescue("", "anything") is None


def test_compute_pinpoint_falls_back_to_semantic_when_lexical_misses(
    patched, tmp_path,
):
    """End-to-end: lexical resolver finds nothing in a synthetic structure
    DB, semantic rescue surfaces a paragraph by cosine. Confirms the
    wiring in _compute_pinpoint correctly defers to the rescue.
    """
    # Build a tiny lexical structure DB with paragraphs that won't match
    # the claim (no token overlap).
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
    conn.execute(
        "INSERT INTO erwaegungen_paragraph VALUES (?, ?, ?, ?, ?)",
        ("d_1", "1", 1, None,
         "Decision text uses different vocabulary entirely from the claim."),
    )
    conn.execute("INSERT INTO erwaegungen_paragraph_fts(erwaegungen_paragraph_fts) VALUES ('rebuild')")
    conn.commit()

    # Patch lexical to use this connection, semantic to return high cosine.
    patched.setattr(mcp_server, "_get_structure_conn", lambda: conn)
    claim_vec = _norm(1, 0, 0)
    patched.setattr(mcp_server, "_get_semantic_model",
                    lambda: _FakeModel({"vocabulary mismatch query": claim_vec}))
    patched.setattr(mcp_server, "_fetch_paragraph_embeddings",
                    lambda did, conn=None: [("1", _norm(1, 0, 0))])

    try:
        out = mcp_server._compute_pinpoint("d_1", "vocabulary mismatch query")
        # Lexical would return None; semantic rescue should surface high.
        assert out is not None
        assert out["source"] == "semantic"
        assert out["confidence"] == "high"
        assert out["e_number"] == "1"
    finally:
        conn.close()


def test_compute_pinpoint_lexical_winner_not_overridden_by_semantic(
    patched, tmp_path,
):
    """Inverse safety check: when lexical finds a strong match, the
    semantic rescue is NOT consulted (lexical wins). The returned
    source must be 'lexical', not 'semantic'.
    """
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
    paras = [
        ("d_1", "1", "Mietrecht Kündigung Wohnung — alle drei Tokens hier."),
        ("d_1", "2", "Anderes über Strafrecht ohne Bezug."),
        ("d_1", "3", "Sachverhalt Streitsache vor Vorinstanz."),
        ("d_1", "4", "Dispositiv: Beschwerde abgewiesen."),
        ("d_1", "5", "Kostenfolgen verteilt."),
    ]
    for d, e, t in paras:
        conn.execute("INSERT INTO erwaegungen_paragraph VALUES (?, ?, ?, ?, ?)",
                     (d, e, 1, None, t))
    conn.execute("INSERT INTO erwaegungen_paragraph_fts(erwaegungen_paragraph_fts) VALUES ('rebuild')")
    conn.commit()

    patched.setattr(mcp_server, "_get_structure_conn", lambda: conn)
    # If semantic IS called, it would return a (wrong) winner. Test
    # that lexical produces "lexical" source and the wrong semantic
    # winner is never reached.
    fetch_calls = []

    def _track_fetch(*a, **kw):
        fetch_calls.append((a, kw))
        return [("99", _norm(1, 0, 0))]  # semantic would pick e=99

    patched.setattr(mcp_server, "_get_semantic_model",
                    lambda: _FakeModel({"Mietrecht Kündigung Wohnung": _norm(1, 0, 0)}))
    patched.setattr(mcp_server, "_fetch_paragraph_embeddings", _track_fetch)

    try:
        out = mcp_server._compute_pinpoint("d_1", "Mietrecht Kündigung Wohnung")
        assert out is not None
        assert out["source"] == "lexical", f"Lexical should win; got {out}"
        assert out["e_number"] == "1"
        assert fetch_calls == [], "Semantic rescue must NOT fire when lexical wins"
    finally:
        conn.close()
