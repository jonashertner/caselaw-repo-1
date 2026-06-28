"""find_leading_cases statute path must rank by TOPICAL authority, not global
citation count. Audit 2026-06-28: asking for leading cases on Art. 41 OR returned
procedural megacases (BGE 126 I 97, right-to-be-heard, 6,535 global citations) that
cite the provision once. Fix: rank by citations FROM other decisions that apply the
same provision (intra-topic), with the global count kept for context.

Reproduced live before the fix; this pins it with a minimal graph fixture.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _make_graph(path: Path) -> str:
    c = sqlite3.connect(path)
    c.executescript(
        """
        CREATE TABLE decisions(decision_id TEXT PRIMARY KEY, court TEXT, decision_date TEXT);
        CREATE TABLE statutes(statute_id INTEGER PRIMARY KEY, law_code TEXT, article TEXT);
        CREATE TABLE decision_statutes(decision_id TEXT, statute_id INTEGER, mention_count INTEGER);
        CREATE TABLE citation_targets(source_decision_id TEXT, target_ref TEXT,
                                      target_decision_id TEXT, match_type TEXT, confidence_score REAL);
        """
    )
    for did in ("HUB", "A", "B", "C", "X1", "X2", "X3"):
        c.execute("INSERT INTO decisions VALUES(?,?,?)", (did, "bge", "2020-01-01"))
    c.execute("INSERT INTO statutes VALUES(1,'OR','41')")
    for did in ("HUB", "A", "B", "C"):           # these apply Art. 41 OR; the X's don't
        c.execute("INSERT INTO decision_statutes VALUES(?,1,1)", (did,))
    # HUB is globally popular: cited by 3 non-topic X's + 1 topic case -> global 4, topic 1
    for s in ("X1", "X2", "X3", "B"):
        c.execute("INSERT INTO citation_targets VALUES(?,?,?,?,?)", (s, "r", "HUB", "docket", 0.9))
    # A is the topical authority: cited by topic cases B and C -> global 2, topic 2
    for s in ("B", "C"):
        c.execute("INSERT INTO citation_targets VALUES(?,?,?,?,?)", (s, "r", "A", "docket", 0.9))
    c.commit()
    c.close()
    return str(path)


def _row_conn(p):
    conn = sqlite3.connect(p)
    conn.row_factory = sqlite3.Row
    return conn


def test_statute_leading_cases_ranks_intra_topic(tmp_path, monkeypatch):
    gp = _make_graph(tmp_path / "graph.db")
    monkeypatch.setattr(m, "_get_graph_conn", lambda: _row_conn(gp))
    monkeypatch.setattr(m, "_fetch_decision_rows_by_ids", lambda ids: [])
    out = m._find_leading_cases(law_code="OR", article="41", limit=5)
    ids = [r["decision_id"] for r in out["results"]]
    # global ranking would put HUB (4) first; intra-topic puts A (topic 2) above HUB (topic 1)
    assert "A" in ids and "HUB" in ids
    assert ids.index("A") < ids.index("HUB"), f"topical ranking failed: {ids}"
    # both counts surfaced: A's global is 2, its topical is 2
    a = next(r for r in out["results"] if r["decision_id"] == "A")
    assert a.get("topic_citation_count") == 2
    assert a.get("citation_count") == 2          # global, for context
