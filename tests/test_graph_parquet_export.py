"""P2.4: resolved citation edges + statute references leave the MCP silo as
self-contained parquet tables under <output>/graph/ (never under data/,
where a foreign schema breaks HF load_dataset)."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

pq = pytest.importorskip("pyarrow.parquet")
from export_parquet import (CITATION_EDGE_SCHEMA, STATUTE_REF_SCHEMA,  # noqa: E402
                            export_citation_graph)


def test_export_roundtrip(tmp_path):
    g = tmp_path / "reference_graph.db"
    conn = sqlite3.connect(g)
    conn.executescript("""
        CREATE TABLE citation_targets (source_decision_id TEXT, target_ref TEXT,
            target_decision_id TEXT, match_type TEXT, confidence_score REAL);
        CREATE TABLE statutes (statute_id INTEGER PRIMARY KEY, law_code TEXT,
            article TEXT, paragraph TEXT);
        CREATE TABLE decision_statutes (decision_id TEXT, statute_id INTEGER,
            mention_count INTEGER);
        INSERT INTO citation_targets VALUES
            ('bger_5A_1_2024', 'BGE 150 II 1', 'bge_150_II_1', 'exact', 0.99),
            ('bger_5A_1_2024', 'unresolved ref', NULL, NULL, NULL);
        INSERT INTO statutes VALUES (1, 'OR', '329g', NULL);
        INSERT INTO decision_statutes VALUES ('bger_5A_1_2024', 1, 3);
    """)
    conn.commit(); conn.close()

    out = tmp_path / "dataset"
    counts = export_citation_graph(g, out)
    assert counts == {"citations": 1, "statute_references": 1}  # unresolved row excluded

    edges = pq.read_table(out / "graph" / "citations.parquet")
    assert edges.schema.equals(CITATION_EDGE_SCHEMA)
    assert edges.to_pylist()[0]["target_decision_id"] == "bge_150_II_1"

    refs = pq.read_table(out / "graph" / "statute_references.parquet")
    assert refs.schema.equals(STATUTE_REF_SCHEMA)
    r = refs.to_pylist()[0]
    assert (r["law_code"], r["article"], r["mention_count"]) == ("OR", "329g", 3)


def test_missing_graph_db_is_clean_skip(tmp_path):
    assert export_citation_graph(tmp_path / "absent.db", tmp_path / "o") == {}
    assert not (tmp_path / "o" / "graph").exists()
