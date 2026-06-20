"""Tests for the additive BGer↔BGE cross-reference resolution pass.

Proves it (a) resolves a BGer-docket citation to the BGE entry that contains
that docket in its header, (b) resolves separator/decision_id drift the
docket_norm JOIN missed, and (c) is purely additive — never touches an
already-resolved citation.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO / "scripts"))

from resolve_citation_crossref import build_key_index, resolve_crossref  # noqa: E402


def _decisions_db():
    d = sqlite3.connect(":memory:")
    d.execute("CREATE TABLE decisions (decision_id TEXT, docket_number TEXT, "
              "docket_number_2 TEXT, court TEXT, full_text TEXT)")
    return d


def _graph_db(decision_ids=()):
    g = sqlite3.connect(":memory:")
    g.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY)")
    g.executemany("INSERT INTO decisions VALUES (?)", [(d,) for d in decision_ids])
    g.execute("CREATE TABLE citation_targets (source_decision_id TEXT, target_ref TEXT, "
              "target_decision_id TEXT, match_type TEXT, confidence_score REAL, "
              "PRIMARY KEY (source_decision_id, target_ref, target_decision_id))")
    g.execute("CREATE TABLE decision_citations (source_decision_id TEXT, target_ref TEXT, "
              "target_type TEXT)")
    return g


def test_resolves_bger_docket_to_bge_entry():
    d = _decisions_db()
    d.execute("INSERT INTO decisions VALUES ('bge_148 II 233','148 II 233',NULL,'bge',"
              "'Urteilskopf 148 II 233 Auszug aus dem Urteil 2C_663/2021 vom 1. Juni 2022')")
    idx = build_key_index(d)
    assert idx.get("d:2c_663_2021") == "bge_148 II 233"   # cross-reference captured

    g = _graph_db(["bge_148 II 233"])
    g.execute("INSERT INTO decision_citations VALUES ('s1','2C_663/2021','docket')")
    added = resolve_crossref(g, idx)
    assert added == 1
    row = g.execute("SELECT target_decision_id, match_type FROM citation_targets "
                    "WHERE source_decision_id='s1' AND target_ref='2C_663/2021'").fetchone()
    assert row == ("bge_148 II 233", "oracle_xref")


def test_resolves_separator_and_decision_id_drift():
    d = _decisions_db()
    # present as bger_5D_78_2017 with a space-separator docket the JOIN missed
    d.execute("INSERT INTO decisions VALUES ('bger_5D_78_2017','5D 78/2017',NULL,'bger',NULL)")
    idx = build_key_index(d)
    g = _graph_db(["bger_5D_78_2017"])
    g.execute("INSERT INTO decision_citations VALUES ('s2','5D_78/2017','docket')")
    added = resolve_crossref(g, idx)
    assert added == 1
    assert g.execute("SELECT target_decision_id FROM citation_targets "
                     "WHERE source_decision_id='s2'").fetchone()[0] == "bger_5D_78_2017"


def test_skips_target_not_in_graph_fk_safe():
    """If the key index points to a decision the graph doesn't contain (dedup /
    court-filter / limited build), the edge is skipped — never an FK failure."""
    d = _decisions_db()
    d.execute("INSERT INTO decisions VALUES ('bger_2C_663_2021','2C_663/2021',NULL,'bger',NULL)")
    idx = build_key_index(d)
    g = _graph_db(decision_ids=[])  # graph has NO decisions
    g.execute("INSERT INTO decision_citations VALUES ('s9','2C_663/2021','docket')")
    added = resolve_crossref(g, idx)
    assert added == 0  # target absent from graph → skipped, no FK error


def test_never_touches_already_resolved():
    d = _decisions_db()
    d.execute("INSERT INTO decisions VALUES ('bger_4A_101_2014','4A_101/2014',NULL,'bger',NULL)")
    idx = build_key_index(d)
    g = _graph_db(["bger_4A_101_2014", "existing_target"])
    # already resolved by the docket_norm pass to a (hypothetical) other target
    g.execute("INSERT INTO citation_targets VALUES ('s3','4A_101/2014','existing_target','docket_norm',0.9)")
    g.execute("INSERT INTO decision_citations VALUES ('s3','4A_101/2014','docket')")
    added = resolve_crossref(g, idx)
    assert added == 0   # ref already in citation_targets → skipped entirely
    targets = [r[0] for r in g.execute("SELECT target_decision_id FROM citation_targets "
                                       "WHERE source_decision_id='s3'")]
    assert targets == ["existing_target"]   # unchanged


def test_skips_unmatchable_and_self_citation():
    d = _decisions_db()
    d.execute("INSERT INTO decisions VALUES ('bger_4A_101_2014','4A_101/2014',NULL,'bger',NULL)")
    idx = build_key_index(d)
    g = _graph_db(["bger_4A_101_2014"])
    g.execute("INSERT INTO decision_citations VALUES ('x','URK_ 2','docket')")          # noise
    g.execute("INSERT INTO decision_citations VALUES ('x','9Z_999/2099','docket')")     # no corpus match
    g.execute("INSERT INTO decision_citations VALUES ('bger_4A_101_2014','4A_101/2014','docket')")  # self
    added = resolve_crossref(g, idx)
    assert added == 0
