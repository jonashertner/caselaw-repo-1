import sqlite3
from pathlib import Path

import pytest

import mcp_server
from db_schema import INSERT_COLUMNS, INSERT_OR_IGNORE_SQL, SCHEMA_SQL


def _make_row(**overrides):
    row = {col: None for col in INSERT_COLUMNS}
    row.update(
        {
            "decision_id": "placeholder",
            "court": "bger",
            "canton": "CH",
            "docket_number": "X_0/2000",
            "decision_date": "2025-01-01",
            "language": "de",
            "title": "",
            "regeste": "",
            "full_text": "",
            "source_url": "https://example.invalid",
        }
    )
    row.update(overrides)
    return row


@pytest.fixture()
def nl_test_db(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "decisions.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)

    rows = [
        _make_row(
            decision_id="d_asyl",
            court="bvger",
            docket_number="D-8226/2025",
            title="Asyl und Wegweisung",
            regeste="Asyl und Wegweisung im beschleunigten Verfahren",
            full_text="Asyl und Wegweisung beschleunigtes Verfahren und Nichteintreten.",
        ),
        _make_row(
            decision_id="d_fr",
            court="bger",
            docket_number="1A.122/2005",
            language="fr",
            title="Parc eolien; permis de construire",
            regeste="Permis de construire d'un parc eolien",
            full_text="Permis de construire et parc eolien en droit public.",
        ),
        _make_row(
            decision_id="d_art8",
            court="bge",
            docket_number="151 I 62",
            regeste="Art 8 EMRK Umwandlung des Status der vorlaeufigen Aufnahme",
            full_text="Art 8 EMRK und Umwandlung des Status der vorlaeufigen Aufnahme.",
        ),
        _make_row(
            decision_id="d_noise",
            court="zh_verwaltungsgericht",
            docket_number="VB.2010.99999",
            title="Verfahrensrecht",
            regeste="Formelles Verwaltungsrecht",
            full_text=(
                "Asyl Wegweisung Asyl Wegweisung Asyl Wegweisung "
                "beschleunigtes Verfahren Asyl Wegweisung."
            ),
        ),
        _make_row(
            decision_id="d_fr_similar",
            court="bger",
            docket_number="1A.122/2005-ALT",
            language="fr",
            title="Permis de construire - version annexe",
            regeste="Parc eolien et permis de construire, version annexe",
            full_text="Texte annexe sur parc eolien.",
        ),
    ]

    for row in rows:
        values = tuple(row.get(col) for col in INSERT_COLUMNS)
        conn.execute(INSERT_OR_IGNORE_SQL, values)
    conn.commit()
    conn.close()

    def _get_db():
        c = sqlite3.connect(db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA query_only = ON")
        return c

    monkeypatch.setattr(mcp_server, "get_db", _get_db)
    return db_path


def test_search_handles_colon_without_error(nl_test_db):
    results = mcp_server.search_fts5("Asyl: Wegweisung?", limit=5)
    assert any(r["decision_id"] == "d_asyl" for r in results)


def test_search_handles_unmatched_quote_without_error(nl_test_db):
    results = mcp_server.search_fts5('"Asyl und Wegweisung', limit=5)
    assert any(r["decision_id"] == "d_asyl" for r in results)


def test_search_handles_natural_language_french_prompt(nl_test_db):
    results = mcp_server.search_fts5(
        "Je cherche un arret sur le permis de construire d'un parc eolien",
        limit=5,
    )
    assert any(r["decision_id"] == "d_fr" for r in results)


def test_search_handles_legal_natural_language_prompt(nl_test_db):
    results = mcp_server.search_fts5(
        "Art. 8 EMRK Umwandlung des Status der vorlaeufigen Aufnahme",
        limit=5,
    )
    assert any(r["decision_id"] == "d_art8" for r in results)


def test_search_result_shape_is_stable_on_fallback(nl_test_db):
    results = mcp_server.search_fts5("Asyl: Wegweisung?", limit=1)
    assert results
    row = results[0]
    assert "relevance_score" in row
    assert "rank" not in row


def test_explicit_boolean_query_still_works(nl_test_db):
    results = mcp_server.search_fts5("regeste:Asyl AND regeste:Wegweisung", limit=5)
    assert any(r["decision_id"] == "d_asyl" for r in results)


def test_reranker_prefers_title_regeste_over_fulltext_noise(nl_test_db):
    results = mcp_server.search_fts5("Asyl Wegweisung beschleunigtes Verfahren", limit=3)
    assert results
    assert results[0]["decision_id"] == "d_asyl"


def test_reranker_boosts_exact_docket_match(nl_test_db):
    results = mcp_server.search_fts5("1A.122/2005", limit=3)
    assert results
    assert results[0]["decision_id"] == "d_fr"
