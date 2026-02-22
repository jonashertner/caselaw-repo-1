"""Tests for search_stack.build_vectors — text selection helper & sqlite-vec schema."""

import struct

import pytest

from search_stack.build_vectors import (
    EMBEDDING_DIM,
    VEC_TABLE_SQL,
    _select_text,
    create_vec_db,
    serialize_f32,
)

# ---------------------------------------------------------------------------
# Task 2: _select_text tests
# ---------------------------------------------------------------------------


def test_select_text_uses_regeste():
    """When regeste is >= 20 chars, it should be returned as-is."""
    row = {"regeste": "Dies ist eine hinreichend lange Regeste.", "full_text": "Some full text here."}
    result = _select_text(row)
    assert result == "Dies ist eine hinreichend lange Regeste."


def test_select_text_falls_back_to_full_text():
    """When regeste is missing, fall back to full_text truncated to 2000 chars."""
    long_text = "A" * 5000
    row = {"regeste": None, "full_text": long_text}
    result = _select_text(row)
    assert result is not None
    assert len(result) == 2000
    assert result == "A" * 2000


def test_select_text_falls_back_on_short_regeste():
    """When regeste exists but is < 20 chars, fall back to full_text."""
    row = {"regeste": "Too short", "full_text": "This is the full text of the decision."}
    result = _select_text(row)
    assert result == "This is the full text of the decision."


def test_select_text_empty_row():
    """When both regeste and full_text are None, return None."""
    row = {"regeste": None, "full_text": None}
    result = _select_text(row)
    assert result is None


def test_select_text_empty_strings():
    """When both regeste and full_text are empty strings, return None."""
    row = {"regeste": "", "full_text": ""}
    result = _select_text(row)
    assert result is None


# ---------------------------------------------------------------------------
# Task 3: Constants & serialization (non-live)
# ---------------------------------------------------------------------------


def test_embedding_dim():
    """EMBEDDING_DIM should be 1024."""
    assert EMBEDDING_DIM == 1024


def test_vec_table_sql_contains_key_clauses():
    """VEC_TABLE_SQL should define the vec0 virtual table with expected columns."""
    assert "vec_decisions" in VEC_TABLE_SQL
    assert "vec0" in VEC_TABLE_SQL
    assert "decision_id" in VEC_TABLE_SQL
    assert "float[1024]" in VEC_TABLE_SQL
    assert "cosine" in VEC_TABLE_SQL
    assert "partition key" in VEC_TABLE_SQL


def test_serialize_f32_round_trip():
    """serialize_f32 should pack floats into little-endian bytes and round-trip."""
    vec = [1.0, 2.0, 3.0]
    raw = serialize_f32(vec)
    assert isinstance(raw, bytes)
    assert len(raw) == 3 * 4  # 3 floats x 4 bytes
    unpacked = struct.unpack(f"<{len(vec)}f", raw)
    assert list(unpacked) == pytest.approx(vec)


def test_serialize_f32_empty():
    """serialize_f32 with empty list should return empty bytes."""
    assert serialize_f32([]) == b""


# ---------------------------------------------------------------------------
# Task 3: sqlite-vec live tests (require sqlite-vec extension on VPS)
# ---------------------------------------------------------------------------


@pytest.mark.live
def test_vec_db_round_trip(tmp_path):
    """Insert two vectors, run KNN query, verify ordering by distance."""
    db_path = str(tmp_path / "test_vec.db")
    conn = create_vec_db(db_path)

    dim = EMBEDDING_DIM
    # v1: all 1.0, v2: all 0.0 except first element
    v1 = [1.0] * dim
    v2 = [0.0] * dim
    v2[0] = 1.0  # slightly similar to v1

    conn.execute(
        "INSERT INTO vec_decisions (decision_id, embedding, language) VALUES (?, ?, ?)",
        ("dec_a", serialize_f32(v1), "de"),
    )
    conn.execute(
        "INSERT INTO vec_decisions (decision_id, embedding, language) VALUES (?, ?, ?)",
        ("dec_b", serialize_f32(v2), "de"),
    )
    conn.commit()

    # Query with v1 itself — dec_a should be closest
    rows = conn.execute(
        """
        SELECT decision_id, distance
        FROM vec_decisions
        WHERE embedding MATCH ?
        ORDER BY distance
        LIMIT 2
        """,
        (serialize_f32(v1),),
    ).fetchall()

    assert len(rows) == 2
    assert rows[0][0] == "dec_a"  # closest to itself
    assert rows[1][0] == "dec_b"
    assert rows[0][1] < rows[1][1]  # dec_a distance < dec_b distance

    conn.close()


@pytest.mark.live
def test_vec_db_language_partition(tmp_path):
    """Insert DE and FR vectors, query with language filter, verify partition isolation."""
    db_path = str(tmp_path / "test_vec_part.db")
    conn = create_vec_db(db_path)

    dim = EMBEDDING_DIM
    v_de = [1.0] * dim
    v_fr = [1.0] * dim  # identical vector but different language

    conn.execute(
        "INSERT INTO vec_decisions (decision_id, embedding, language) VALUES (?, ?, ?)",
        ("dec_de", serialize_f32(v_de), "de"),
    )
    conn.execute(
        "INSERT INTO vec_decisions (decision_id, embedding, language) VALUES (?, ?, ?)",
        ("dec_fr", serialize_f32(v_fr), "fr"),
    )
    conn.commit()

    # Query with language='de' — should only return dec_de
    rows = conn.execute(
        """
        SELECT decision_id, distance
        FROM vec_decisions
        WHERE embedding MATCH ?
          AND language = ?
        ORDER BY distance
        LIMIT 10
        """,
        (serialize_f32(v_de), "de"),
    ).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == "dec_de"

    conn.close()
