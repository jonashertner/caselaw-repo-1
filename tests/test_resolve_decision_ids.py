# tests/test_resolve_decision_ids.py
"""Tests for the BGE ref → decision_id resolver."""
import json
import pytest
from unittest.mock import patch, MagicMock
from study.resolve_decision_ids import parse_bge_ref, build_fts_query, resolve_all


def test_parse_bge_ref_standard():
    result = parse_bge_ref("BGE 135 III 1")
    assert result == {"volume": "135", "collection": "III", "page": "1"}


def test_parse_bge_ref_two_digit_page():
    result = parse_bge_ref("BGE 84 II 122")
    assert result == {"volume": "84", "collection": "II", "page": "122"}


def test_parse_bge_ref_invalid():
    assert parse_bge_ref("not a bge ref") is None
    assert parse_bge_ref("") is None


def test_build_fts_query():
    query = build_fts_query("BGE 135 III 1")
    assert query == '"135 III 1"'
    assert build_fts_query("not a ref") == ""


def test_parse_bge_ref_ia_collection():
    result = parse_bge_ref("BGE 110 Ia 1")
    assert result == {"volume": "110", "collection": "IA", "page": "1"}


def test_resolve_all_skips_existing(tmp_path):
    """resolve_all should not overwrite non-empty decision_ids."""
    curriculum_dir = tmp_path / "curriculum"
    curriculum_dir.mkdir()
    data = {
        "area_id": "test",
        "area_de": "Test",
        "modules": [{
            "id": "mod1",
            "cases": [{
                "decision_id": "bge_already_resolved",
                "bge_ref": "BGE 135 III 1",
            }]
        }]
    }
    (curriculum_dir / "test.json").write_text(json.dumps(data))

    stats = resolve_all(curriculum_dir=str(curriculum_dir), db_path=":memory:")
    assert stats["already_set"] == 1
    assert stats["resolved"] == 0


def test_resolve_all_fills_blank(tmp_path):
    """resolve_all should fill blank decision_ids."""
    curriculum_dir = tmp_path / "curriculum"
    curriculum_dir.mkdir()
    data = {
        "area_id": "test",
        "area_de": "Test",
        "modules": [{
            "id": "mod1",
            "cases": [{
                "decision_id": "",
                "bge_ref": "BGE 135 III 1",
            }]
        }]
    }
    (curriculum_dir / "test.json").write_text(json.dumps(data))

    mock_result = [{"decision_id": "bge_135 III 1", "docket_number": "135 III 1"}]

    with patch("study.resolve_decision_ids._query_db", return_value=mock_result):
        stats = resolve_all(curriculum_dir=str(curriculum_dir), db_path=":memory:")

    assert stats["resolved"] == 1
    updated = json.loads((curriculum_dir / "test.json").read_text())
    assert updated["modules"][0]["cases"][0]["decision_id"] == "bge_135 III 1"
