"""Integration tests for _find_appeal_chain in mcp_server.

Builds a small reference graph DB in-memory and verifies
the appeal chain traversal logic.
"""

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

from search_stack.build_reference_graph import build_graph

import mcp_server


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    path.write_text(payload, encoding="utf-8")


def _build_chain_graph(tmp_path: Path) -> Path:
    """Build a 3-level appeal chain: Bezirk → Obergericht → BGer."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d_bezirk",
            "docket_number": "FV.2024.100",
            "court": "zh_gerichte",
            "canton": "ZH",
            "language": "de",
            "decision_date": "2024-06-01",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_ober",
            "docket_number": "LZ250038",
            "court": "zh_obergericht",
            "canton": "ZH",
            "language": "de",
            "decision_date": "2025-10-01",
            "title": "",
            "regeste": "",
            "full_text": (
                "Gegenstand\n"
                "Unterhalt\n"
                "Berufung gegen das Urteil des Bezirksgerichts "
                "vom 1. Juni 2024 (FV.2024.100).\n"
                "Erwägungen:\n"
            ),
        },
        {
            "decision_id": "d_bger",
            "docket_number": "5A_900/2025",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2026-02-01",
            "title": "",
            "regeste": "",
            "full_text": (
                "Gegenstand\n"
                "Unterhalt,\n"
                "Beschwerde gegen den Entscheid des Obergerichts des Kantons Zürich "
                "vom 1. Oktober 2025 (LZ250038).\n"
                "Sachverhalt:\n"
            ),
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)
    build_graph(input_dir=input_dir, db_path=db_path)
    return db_path


def test_find_appeal_chain_from_top(tmp_path: Path):
    """Starting from BGer (top), should find both lower instances."""
    db_path = _build_chain_graph(tmp_path)
    with patch.object(mcp_server, "GRAPH_DB_PATH", db_path):
        # Reset warning flag so _get_graph_conn doesn't skip
        mcp_server._graph_warned = False
        result = mcp_server._find_appeal_chain("d_bger")

    assert "error" not in result
    assert result["decision_id"] == "d_bger"
    chain = result["chain"]
    # Should find d_ober (prior) and d_bezirk (prior of prior)
    chain_ids = {c["decision_id"] for c in chain}
    assert "d_ober" in chain_ids
    assert "d_bezirk" in chain_ids
    # All should be prior_instance relation
    for c in chain:
        assert c["relation"] == "prior_instance"
    # Chain sorted by date
    dates = [c["decision_date"] for c in chain]
    assert dates == sorted(dates)


def test_find_appeal_chain_from_middle(tmp_path: Path):
    """Starting from Obergericht (middle), should find prior and subsequent."""
    db_path = _build_chain_graph(tmp_path)
    with patch.object(mcp_server, "GRAPH_DB_PATH", db_path):
        mcp_server._graph_warned = False
        result = mcp_server._find_appeal_chain("d_ober")

    assert "error" not in result
    chain = result["chain"]
    chain_ids = {c["decision_id"] for c in chain}
    # d_bezirk is a prior instance (walked down)
    assert "d_bezirk" in chain_ids
    # d_bger appealed d_ober (walked up)
    assert "d_bger" in chain_ids


def test_find_appeal_chain_from_bottom(tmp_path: Path):
    """Starting from Bezirksgericht (bottom), should find subsequent instances."""
    db_path = _build_chain_graph(tmp_path)
    with patch.object(mcp_server, "GRAPH_DB_PATH", db_path):
        mcp_server._graph_warned = False
        result = mcp_server._find_appeal_chain("d_bezirk")

    assert "error" not in result
    chain = result["chain"]
    chain_ids = {c["decision_id"] for c in chain}
    # d_ober and d_bger both appealed upward from d_bezirk
    assert "d_ober" in chain_ids
    assert "d_bger" in chain_ids


def test_find_appeal_chain_unknown_decision(tmp_path: Path):
    """Unknown decision_id should return empty chain, not error."""
    db_path = _build_chain_graph(tmp_path)
    with patch.object(mcp_server, "GRAPH_DB_PATH", db_path):
        mcp_server._graph_warned = False
        result = mcp_server._find_appeal_chain("nonexistent")

    assert "error" not in result
    assert result["chain"] == []


def test_find_appeal_chain_no_graph_db():
    """When graph DB is missing, should return error message."""
    with patch.object(mcp_server, "GRAPH_DB_PATH", Path("/nonexistent/graph.db")):
        mcp_server._graph_warned = False
        result = mcp_server._find_appeal_chain("d_bger")

    assert "error" in result
    assert "not available" in result["error"]
