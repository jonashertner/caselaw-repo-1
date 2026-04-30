"""Shared fixtures for tests/quality/."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture
def temp_db(tmp_path: Path) -> Path:
    """A minimal-but-realistic decisions DB suitable for QC checks.

    Mirrors the production schema (subset). Populated with hand-curated
    rows that exercise both happy-path and edge-case patterns:
    - One BGE row with a complete regeste
    - One BGer row
    - One cantonal row
    - One row with NULL decision_date (to exercise null-floor checks)
    - One row with German + French + Italian text variants
    """
    db_path = tmp_path / "decisions.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE decisions (
            decision_id TEXT PRIMARY KEY,
            court TEXT NOT NULL,
            canton TEXT,
            chamber TEXT,
            docket_number TEXT,
            decision_date TEXT,
            publication_date TEXT,
            language TEXT,
            title TEXT,
            legal_area TEXT,
            regeste TEXT,
            full_text TEXT,
            decision_type TEXT,
            outcome TEXT,
            source_url TEXT,
            pdf_url TEXT,
            cited_decisions TEXT,
            scraped_at TEXT,
            json_data TEXT
        );
        CREATE VIRTUAL TABLE decisions_fts USING fts5(
            decision_id, court, canton, docket_number, language,
            title, regeste, full_text,
            content='decisions', content_rowid='rowid'
        );
    """)
    rows = [
        ("bge_BGE_140_III_86", "bge", "CH", None, "4A_321/2013",
         "2014-03-15", None, "de", None, None,
         "Art. 42 Abs. 2 BGG; Pflicht zur Begründung der Rechtsverletzungen.",
         "Die Beschwerde wurde fristgerecht erhoben.\n\n2. Spezifisch: ... " * 50,
         None, None,
         "https://www.bger.ch/ext/eurospider/live/de/php/aza/http/...",
         None, None, None, None),
        ("bger_8C_252_2024", "bger", "CH", "I", "8C_252/2024",
         "2024-05-12", None, "de", None, None,
         "Sozialversicherungsrecht. UV.",
         "Erwägungen: ... " * 80,
         None, None,
         "https://www.bger.ch/...", None, None, None, None),
        ("zh_obergericht_LE220012", "zh_obergericht", "ZH", None, "LE220012",
         "2022-08-04", None, "de", None, None,
         None,
         "I. ... II. ... " * 100,
         None, None,
         "https://www.gerichte-zh.ch/...", None, None, None, None),
        ("ti_gerichte_15.2024.124", "ti_gerichte", "TI", None, "15.2024.124",
         None, None, "it", None, None,
         "Decisione del 12 settembre 2024.",
         "Considerando in diritto: ... " * 50,
         None, None,
         "https://sentenze.ti.ch/...", None, None, None, None),
        ("bge_egmr_001-189064", "bge_egmr", "CH", None, "189064",
         "2019-01-22", None, "fr", None, None,
         "ECHR judgment, freedom of expression.",
         "Procedure ... " * 100,
         None, None,
         "https://hudoc.echr.coe.int/...", None, None, None, None),
    ]
    conn.executemany(
        "INSERT INTO decisions (decision_id, court, canton, chamber, "
        "docket_number, decision_date, publication_date, language, title, "
        "legal_area, regeste, full_text, decision_type, outcome, source_url, "
        "pdf_url, cited_decisions, scraped_at, json_data) VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.execute(
        "INSERT INTO decisions_fts (rowid, decision_id, court, canton, "
        "docket_number, language, title, regeste, full_text) "
        "SELECT rowid, decision_id, court, canton, docket_number, language, "
        "title, regeste, full_text FROM decisions"
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def temp_db_conn(temp_db: Path) -> sqlite3.Connection:
    """Read-only connection to the fixture DB."""
    conn = sqlite3.connect(
        f"file:{temp_db}?mode=ro&immutable=1", uri=True,
    )
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture
def sample_decision() -> dict:
    """A complete decision dict for export-rendering tests."""
    return {
        "decision_id": "bge_BGE_140_III_86",
        "citation_string_de": "BGE 140 III 86",
        "citation_string_fr": "ATF 140 III 86",
        "citation_string_it": "DTF 140 III 86",
        "court": "bge",
        "court_name": "Bundesgericht",
        "decision_date": "2014-03-15",
        "docket_number": "4A_321/2013",
        "language": "de",
        "regeste": "Art. 42 Abs. 2 BGG; Pflicht zur Begründung der Rechtsverletzungen.",
        "full_text": "Erwägungen:\n\n1. Allgemein.\n\n2. Spezifisch.\n",
    }
