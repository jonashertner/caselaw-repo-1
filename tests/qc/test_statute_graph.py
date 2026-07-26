"""Regression tests for the reference-graph statute-edge checks.

Three checks used to count a table named `statute_references`. The
graph builder has never created that table — the name only ever
existed as the extractor function `extract_statute_references()` — so
every statute metric silently reported 0 and the per-law checks
yielded nothing at all, for months, without failing anything.

The fixture is built from the builder's own SCHEMA_SQL, so a future
schema rename breaks these tests instead of going unnoticed again.
"""
from __future__ import annotations

import sqlite3

import pytest

from quality.checks import citation_graph, cross_db, statute_graph
from quality.checks._common import count_statute_edges, statute_edge_table
from search_stack.build_reference_graph import SCHEMA_SQL

# One German and one French decision citing the same four acts, each
# under its own language abbreviation. Every act therefore has exactly
# two edges — one per language.
STATUTES = [
    ("OR_41", "OR", "41", None),
    ("CO_41", "CO", "41", None),
    ("ZGB_28", "ZGB", "28", None),
    ("CC_28", "CC", "28", None),
    ("STGB_146", "STGB", "146", None),
    ("CP_146", "CP", "146", None),
    ("BGG_42", "BGG", "42", None),
    ("LTF_42", "LTF", "42", None),
]
DE_STATUTES = ["OR_41", "ZGB_28", "STGB_146", "BGG_42"]
FR_STATUTES = ["CO_41", "CC_28", "CP_146", "LTF_42"]


@pytest.fixture
def graph_db(tmp_path, monkeypatch):
    """A tiny reference_graph.db on the production schema."""
    path = tmp_path / "reference_graph.db"
    conn = sqlite3.connect(str(path))
    conn.executescript(SCHEMA_SQL)
    conn.executemany(
        "INSERT INTO decisions (decision_id, docket_number, docket_norm, "
        "court, canton, language, decision_date) VALUES (?,?,?,?,?,?,?)",
        [
            ("bger_4A_1_2024", "4A_1/2024", "4a_1_2024",
             "bger", "CH", "de", "2024-01-15"),
            ("bger_4A_2_2024", "4A_2/2024", "4a_2_2024",
             "bger", "CH", "fr", "2024-02-20"),
        ],
    )
    conn.executemany(
        "INSERT INTO statutes (statute_id, law_code, article, paragraph) "
        "VALUES (?,?,?,?)", STATUTES,
    )
    conn.executemany(
        "INSERT INTO decision_statutes (decision_id, statute_id, "
        "mention_count) VALUES (?,?,1)",
        [("bger_4A_1_2024", s) for s in DE_STATUTES]
        + [("bger_4A_2_2024", s) for s in FR_STATUTES],
    )
    conn.execute(
        "INSERT INTO decision_citations (source_decision_id, target_ref, "
        "target_type, mention_count) VALUES (?,?,?,1)",
        ("bger_4A_2_2024", "4A_1/2024", "docket"),
    )
    conn.execute(
        "INSERT INTO citation_targets (source_decision_id, target_ref, "
        "target_decision_id) VALUES (?,?,?)",
        ("bger_4A_2_2024", "4A_1/2024", "bger_4A_1_2024"),
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("SWISS_CASELAW_REFERENCE_GRAPH", str(path))
    return path


@pytest.fixture
def graph_conn(graph_db):
    conn = sqlite3.connect(f"file:{graph_db}?mode=ro&immutable=1", uri=True)
    yield conn
    conn.close()


def _collect(maybe_iter):
    return list(maybe_iter or [])


# ── the edge table the builder actually writes ─────────────────────

def test_statute_edge_table_is_decision_statutes(graph_conn):
    assert statute_edge_table(graph_conn) == "decision_statutes"


def test_count_statute_edges_is_not_zero(graph_conn):
    assert count_statute_edges(graph_conn) == 8


# ── cross_db ───────────────────────────────────────────────────────

def test_reference_graph_sanity_counts_statute_edges(graph_db, temp_db_conn):
    r = cross_db.check_reference_graph_sanity(temp_db_conn)
    assert r.extra["statute_edges"] == 8
    assert r.extra["citation_targets"] == 1
    assert "statute_edges: 8" in r.message


# ── citation_graph ─────────────────────────────────────────────────

def test_total_edge_count_reports_statute_edges(graph_db, temp_db_conn):
    rs = _collect(citation_graph.check_total_edge_count(temp_db_conn))
    by_name = {r.name: r for r in rs}
    assert by_name["citation_graph.statute_edges_count"].metric_value == 8


# ── statute_graph ──────────────────────────────────────────────────

def test_top_federal_laws_yield_results(graph_db, temp_db_conn):
    """The check used to yield nothing at all — PRAGMA table_info on a
    non-existent table returns no columns, so it returned early."""
    rs = _collect(statute_graph.check_top_federal_laws_present(temp_db_conn))
    assert {r.name for r in rs} == {
        f"statute_graph.top_law.{k}" for k in statute_graph.TOP_FEDERAL_LAWS
    }


def test_top_federal_laws_count_both_language_variants(graph_db,
                                                       temp_db_conn):
    """Each act has one DE and one FR edge. Counting only the German
    abbreviation would give 1; the merged count is 2."""
    rs = _collect(statute_graph.check_top_federal_laws_present(temp_db_conn))
    for r in rs:
        assert r.metric_value == 2, f"{r.name}: {r.message}"


def test_top_federal_laws_match_upper_cased_law_code(graph_db, temp_db_conn):
    """`law_code` is stored upper-case, so the mixed-case label 'StGB'
    must still match the stored 'STGB'."""
    rs = _collect(statute_graph.check_top_federal_laws_present(temp_db_conn))
    stgb = next(r for r in rs if r.name.endswith(".StGB"))
    assert stgb.metric_value == 2


def test_top_federal_laws_flag_unknown_schema(tmp_path, monkeypatch,
                                              temp_db_conn):
    """An unrecognised schema must produce a visible WARNING, not an
    empty result list."""
    path = tmp_path / "reference_graph.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE unrelated (x INTEGER)")
    conn.commit()
    conn.close()
    monkeypatch.setenv("SWISS_CASELAW_REFERENCE_GRAPH", str(path))
    rs = _collect(statute_graph.check_top_federal_laws_present(temp_db_conn))
    assert [r.name for r in rs] == ["statute_graph.schema_recognised"]
    assert not rs[0].passed
