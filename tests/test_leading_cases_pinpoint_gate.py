"""Result-parity tests for the leading_cases latency fix (2026-07-21).

The auto-pinpoint OR pass now only runs when the claim has a topical (selective)
token — a bare statute reference like "Art. 8 BV" collapses to the ultra-common
token "Art" whose corpus-wide FTS MATCH was the ~18s cost. Selective and mixed
claims must behave BYTE-FOR-BYTE as before.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import mcp_server


def _structure_db(tmp_path: Path, paragraphs):
    conn = sqlite3.connect(str(tmp_path / "structure.db"))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE erwaegungen_paragraph (
            decision_id TEXT, e_number TEXT, depth INTEGER, parent TEXT, text TEXT,
            PRIMARY KEY (decision_id, e_number));
        CREATE INDEX idx_erw_decision ON erwaegungen_paragraph(decision_id);
        CREATE VIRTUAL TABLE erwaegungen_paragraph_fts USING fts5(
            text, content='erwaegungen_paragraph', content_rowid='rowid',
            tokenize='unicode61 remove_diacritics 1');
        """
    )
    for did, en, txt in paragraphs:
        conn.execute(
            "INSERT INTO erwaegungen_paragraph(decision_id,e_number,depth,parent,text) "
            "VALUES(?,?,?,?,?)", (did, en, en.count(".") + 1, None, txt))
    conn.execute("INSERT INTO erwaegungen_paragraph_fts(erwaegungen_paragraph_fts) VALUES('rebuild')")
    conn.commit()
    return conn


def test_bare_statute_ref_claim_bails_even_when_Art_present(tmp_path, monkeypatch):
    # Paragraphs all contain "Art." (as every decision does). Under the OLD code
    # the OR pass would MATCH "Art" and could return a spurious pinpoint; the gate
    # must now return None for the bare statute reference.
    conn = _structure_db(tmp_path, [
        ("bge_x", "1", "Die Vorinstanz hat Art. 5 der Verordnung angewandt."),
        ("bge_x", "2", "Nach Art. 12 ist der Antrag zulaessig."),
    ])
    monkeypatch.setattr(mcp_server, "_compute_pinpoint_semantic_rescue", lambda *a, **k: None)
    assert mcp_server._compute_pinpoint("bge_x", "Art. 8 BV", conn=conn) is None
    assert mcp_server._compute_pinpoint("bge_x", "Art. 47 StGB", conn=conn) is None
    conn.close()


# A 3-paragraph fixture with a clear topical winner (mirrors the known-good
# fixture in test_pinpoint_enrichment) so the confidence-gap floor is satisfied.
_FIX = [
    ("bge_BGE_140_III_86", "1", "Sachverhalt der Streitsache hier."),
    ("bge_BGE_140_III_86", "2.1", "Allgemeine Erwägungen zur Beschwerdebefugnis nach Art. 76 BGG."),
    ("bge_BGE_140_III_86", "3", "Zum Schadenersatz nach Art. 41 OR siehe BGE 134 III 511."),
]


def test_selective_claim_still_matches(tmp_path):
    conn = _structure_db(tmp_path, _FIX)
    r = mcp_server._compute_pinpoint("bge_BGE_140_III_86", "Beschwerdebefugnis", conn=conn)
    assert r is not None and r.get("e_number") == "2.1"
    conn.close()


def test_mixed_statute_plus_topical_is_identical_to_topical(tmp_path):
    # Adding the statute prefix must NOT change the result vs the topical token
    # alone — the selective token "Beschwerdebefugnis" still drives the OR pass.
    conn = _structure_db(tmp_path, _FIX)
    bare = mcp_server._compute_pinpoint("bge_BGE_140_III_86", "Beschwerdebefugnis", conn=conn)
    mixed = mcp_server._compute_pinpoint(
        "bge_BGE_140_III_86", "Art. 76 BGG Beschwerdebefugnis", conn=conn)
    assert mixed is not None and bare is not None
    assert mixed["e_number"] == bare["e_number"] == "2.1"
    assert mixed["confidence"] == bare["confidence"]
    conn.close()


def test_statute_reference_noise_constant_wellformed():
    n = mcp_server._STATUTE_REFERENCE_NOISE
    assert "art" in n and "bv" in n and "stgb" in n
    assert all(t == t.lower() for t in n)  # matched case-insensitively
