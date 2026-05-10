"""Tests for _compute_pinpoint and _pinpoint_enrich_results.

These cover the auto-pinpoint enrichment path that wires the existing
find_relevant_erwaegung resolver into search_decisions / find_leading_cases
results. Confidence floors, URL-anchor shape, batch behaviour.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import mcp_server


def _make_structure_db(tmp_path: Path, paragraphs: list[tuple[str, str, str]]) -> sqlite3.Connection:
    """Build a temp decision_structure-shaped DB with FTS5 paragraph index.

    paragraphs is a list of (decision_id, e_number, text).
    Returns an open connection (caller closes).
    """
    db = tmp_path / "structure.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE erwaegungen_paragraph (
            decision_id TEXT, e_number TEXT, depth INTEGER, parent TEXT,
            text TEXT,
            PRIMARY KEY (decision_id, e_number)
        );
        CREATE INDEX idx_erw_decision ON erwaegungen_paragraph(decision_id);
        CREATE VIRTUAL TABLE erwaegungen_paragraph_fts USING fts5(
            text,
            content='erwaegungen_paragraph',
            content_rowid='rowid',
            tokenize='unicode61 remove_diacritics 1'
        );
        """
    )
    for did, en, txt in paragraphs:
        depth = en.count(".") + 1
        parent = en.rsplit(".", 1)[0] if "." in en else None
        conn.execute(
            "INSERT INTO erwaegungen_paragraph(decision_id, e_number, depth, parent, text) "
            "VALUES (?, ?, ?, ?, ?)",
            (did, en, depth, parent, txt),
        )
    # Rebuild FTS5 from content
    conn.execute(
        "INSERT INTO erwaegungen_paragraph_fts(erwaegungen_paragraph_fts) "
        "VALUES('rebuild')"
    )
    conn.commit()
    return conn


def test_compute_pinpoint_high_confidence_when_one_clear_match(tmp_path):
    conn = _make_structure_db(tmp_path, [
        ("bge_BGE_140_III_86", "1", "Sachverhalt der Streitsache hier."),
        ("bge_BGE_140_III_86", "2.1", "Allgemeine Erwägungen zur Beschwerdebefugnis nach Art. 76 BGG."),
        ("bge_BGE_140_III_86", "3", "Zum Schadenersatz nach Art. 41 OR siehe BGE 134 III 511."),
    ])
    try:
        pp = mcp_server._compute_pinpoint(
            "bge_BGE_140_III_86",
            "Beschwerdebefugnis Art. 76 BGG",
            conn=conn,
        )
        assert pp is not None
        assert pp["e_number"] == "2.1"
        assert pp["confidence"] in {"high", "medium"}
        assert "Beschwerdebefugnis" in pp["matched_sentence"]
        assert pp["url"].startswith("https://")
        assert "highlight=" in pp["url"]
        assert "e=2.1" in pp["url"]
    finally:
        conn.close()


def test_compute_pinpoint_returns_none_when_claim_too_short(tmp_path):
    conn = _make_structure_db(tmp_path, [
        ("d_1", "1", "Etwas hier zum Mietrecht und der Kündigung der Wohnung."),
    ])
    try:
        assert mcp_server._compute_pinpoint("d_1", "ab", conn=conn) is None
        assert mcp_server._compute_pinpoint("d_1", "  ", conn=conn) is None
        assert mcp_server._compute_pinpoint("d_1", "", conn=conn) is None
    finally:
        conn.close()


def test_compute_pinpoint_returns_none_when_no_match(tmp_path):
    conn = _make_structure_db(tmp_path, [
        ("d_1", "1", "Strafrechtliche Erwägungen zum Diebstahl."),
    ])
    try:
        pp = mcp_server._compute_pinpoint(
            "d_1", "Mietvertrag Kündigung Wohnung", conn=conn
        )
        assert pp is None
    finally:
        conn.close()


def test_compute_pinpoint_returns_none_when_empty_decision_id(tmp_path):
    conn = _make_structure_db(tmp_path, [
        ("d_1", "1", "Anything goes here for the test."),
    ])
    try:
        assert mcp_server._compute_pinpoint("", "anything", conn=conn) is None
    finally:
        conn.close()


def test_compute_pinpoint_returns_none_when_no_fts_index(tmp_path):
    db = tmp_path / "nofts.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE erwaegungen_paragraph (
            decision_id TEXT, e_number TEXT, depth INTEGER, parent TEXT,
            text TEXT, PRIMARY KEY (decision_id, e_number)
        );
        """
    )
    conn.execute(
        "INSERT INTO erwaegungen_paragraph VALUES (?, ?, ?, ?, ?)",
        ("d_1", "1", 1, None, "Some text about anything"),
    )
    conn.commit()
    try:
        # No erwaegungen_paragraph_fts table → resolver must return None.
        pp = mcp_server._compute_pinpoint("d_1", "anything", conn=conn)
        assert pp is None
    finally:
        conn.close()


def test_compute_pinpoint_handles_fts_special_chars_via_phrase_quote(tmp_path):
    # Claims with characters FTS5 reserves (parens, asterisks) shouldn't
    # crash; phrase-quoting in the resolver makes them safe.
    conn = _make_structure_db(tmp_path, [
        ("d_1", "1", "Erwägungen zum Schadenersatz nach Art. 41 OR sind hier."),
    ])
    try:
        pp = mcp_server._compute_pinpoint(
            "d_1", "Schadenersatz (Art. 41 OR)", conn=conn
        )
        # May or may not match — but must not raise.
        if pp is not None:
            assert pp["e_number"] == "1"
    finally:
        conn.close()


def test_pinpoint_enrich_results_attaches_to_top_n(tmp_path, monkeypatch):
    conn = _make_structure_db(tmp_path, [
        ("d_1", "1", "Diskussion über Mietrecht und Kündigung der Wohnung."),
        ("d_1", "2.1", "Detailliertes über Mietrecht Kündigung Probezeit."),
        ("d_2", "1", "Allgemeine Mietrecht-Kündigungsfrist Erwägungen."),
        ("d_3", "1", "Mietrecht Kündigung der Familienwohnung."),
        ("d_4", "1", "Anderes Strafrecht Thema hier ohne Match."),
    ])
    monkeypatch.setattr(mcp_server, "_get_structure_conn", lambda: conn)
    try:
        results = [
            {"decision_id": "d_1", "court": "bger"},
            {"decision_id": "d_2", "court": "bger"},
            {"decision_id": "d_3", "court": "bger"},
            {"decision_id": "d_4", "court": "bger"},
            {"decision_id": "d_5_no_struct", "court": "bger"},
            {"decision_id": "d_6_skipped", "court": "bger"},
        ]
        mcp_server._pinpoint_enrich_results(results, "Mietrecht Kündigung", top_n=4)

        # First 4 entries enriched (presence of pinpoint key — value may be None)
        for r in results[:4]:
            assert "pinpoint" in r
        # Entries beyond top_n are untouched
        for r in results[4:]:
            assert "pinpoint" not in r
        # d_4 has no good match → pinpoint should be None.
        assert results[3]["pinpoint"] is None
    finally:
        conn.close()


def test_pinpoint_enrich_results_noop_on_empty_claim(tmp_path, monkeypatch):
    conn = _make_structure_db(tmp_path, [("d_1", "1", "Stuff here")])
    monkeypatch.setattr(mcp_server, "_get_structure_conn", lambda: conn)
    try:
        results = [{"decision_id": "d_1"}]
        mcp_server._pinpoint_enrich_results(results, "", top_n=5)
        assert "pinpoint" not in results[0]
        mcp_server._pinpoint_enrich_results(results, "  ", top_n=5)
        assert "pinpoint" not in results[0]
        mcp_server._pinpoint_enrich_results(results, "ab", top_n=5)
        assert "pinpoint" not in results[0]
    finally:
        conn.close()


def test_pinpoint_enrich_results_noop_when_db_unavailable(tmp_path, monkeypatch):
    monkeypatch.setattr(mcp_server, "_get_structure_conn", lambda: None)
    results = [{"decision_id": "d_1"}]
    mcp_server._pinpoint_enrich_results(results, "anything substantive", top_n=5)
    assert "pinpoint" not in results[0]


def test_url_includes_highlight_and_e_anchor(tmp_path):
    conn = _make_structure_db(tmp_path, [
        ("bge_BGE_140_III_86", "2.3",
         "Die Beschwerdelegitimation nach Art. 76 BGG verlangt ein "
         "schutzwürdiges Interesse."),
    ])
    try:
        pp = mcp_server._compute_pinpoint(
            "bge_BGE_140_III_86",
            "Beschwerdelegitimation Art. 76 BGG",
            conn=conn,
        )
        assert pp is not None
        # URL gets ?highlight=<urlencoded sentence>&e=2.3 — both must be present.
        assert "highlight=" in pp["url"]
        assert "e=2.3" in pp["url"]
        # The matched sentence (used as ?highlight= source) is preserved.
        assert "Beschwerdelegitimation" in pp["matched_sentence"]
    finally:
        conn.close()
