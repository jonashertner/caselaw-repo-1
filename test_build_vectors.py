"""Tests for search_stack.build_vectors — text selection, sqlite-vec, model, and pipeline."""

import json
import struct

import pytest

from search_stack.build_vectors import (
    BGE_M3_MODEL_ID,
    EMBEDDING_DIM,
    ENCODE_BATCH_SIZE,
    ENCODE_MAX_LENGTH,
    VEC_TABLE_SQL,
    _iter_rows_from_jsonl,
    _select_text,
    build_vectors,
    create_vec_db,
    encode_texts,
    load_model,
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


# ---------------------------------------------------------------------------
# Task 4: Constants for model loader
# ---------------------------------------------------------------------------


def test_bge_m3_model_id():
    """BGE_M3_MODEL_ID should reference the BAAI/bge-m3 model."""
    assert BGE_M3_MODEL_ID == "BAAI/bge-m3"


def test_encode_constants():
    """ENCODE_MAX_LENGTH and ENCODE_BATCH_SIZE should have expected defaults."""
    assert ENCODE_MAX_LENGTH == 512
    assert ENCODE_BATCH_SIZE == 32


# ---------------------------------------------------------------------------
# Task 4: Embedding model live tests (require model download)
# ---------------------------------------------------------------------------


@pytest.mark.live
def test_load_embedding_model():
    """Load BGE-M3 model, encode one sentence, verify output shape (1, 1024)."""
    model = load_model()
    result = encode_texts(model, ["This is a test sentence."])
    assert result.shape == (1, EMBEDDING_DIM)


@pytest.mark.live
def test_encode_multilingual():
    """Encode multilingual texts, verify animal-liability texts cluster together."""
    import numpy as np

    model = load_model()
    texts = [
        "Tierhalterhaftung nach Art. 56 OR bei Hundebiss",       # DE: animal liability
        "Responsabilite du detenteur d'animal selon art. 56 CO", # FR: animal liability
        "Responsabilita del detentore di animali art. 56 CO",    # IT: animal liability
        "Steuerbefreiung fuer gemeinnuetzige Organisationen",    # DE: tax exemption (unrelated)
    ]
    embeddings = encode_texts(model, texts)
    assert embeddings.shape == (4, EMBEDDING_DIM)

    # Cosine similarity (embeddings are already L2-normalized, so dot product = cosine)
    def cosine_sim(a, b):
        return float(np.dot(a, b))

    # All three animal-liability texts should be more similar to each other
    # than any of them is to the tax text
    sim_de_fr = cosine_sim(embeddings[0], embeddings[1])
    sim_de_it = cosine_sim(embeddings[0], embeddings[2])
    sim_fr_it = cosine_sim(embeddings[1], embeddings[2])
    sim_de_tax = cosine_sim(embeddings[0], embeddings[3])
    sim_fr_tax = cosine_sim(embeddings[1], embeddings[3])
    sim_it_tax = cosine_sim(embeddings[2], embeddings[3])

    min_animal_sim = min(sim_de_fr, sim_de_it, sim_fr_it)
    max_tax_sim = max(sim_de_tax, sim_fr_tax, sim_it_tax)

    assert min_animal_sim > max_tax_sim, (
        f"Animal-liability cluster similarity ({min_animal_sim:.3f}) should exceed "
        f"max tax similarity ({max_tax_sim:.3f})"
    )


# ---------------------------------------------------------------------------
# Task 5: JSONL iteration (non-live)
# ---------------------------------------------------------------------------


def test_iter_rows_from_jsonl(tmp_path):
    """_iter_rows_from_jsonl should parse valid lines and skip bad JSON."""
    jsonl_file = tmp_path / "test.jsonl"
    lines = [
        json.dumps({"decision_id": "d1", "full_text": "Hello"}),
        "",  # blank line — should be skipped
        "NOT VALID JSON",  # bad JSON — should be skipped with warning
        json.dumps({"decision_id": "d2", "full_text": "World"}),
    ]
    jsonl_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    rows = list(_iter_rows_from_jsonl(tmp_path))
    assert len(rows) == 2
    assert rows[0]["decision_id"] == "d1"
    assert rows[1]["decision_id"] == "d2"


# ---------------------------------------------------------------------------
# Task 5: Build pipeline live test (requires model + sqlite-vec)
# ---------------------------------------------------------------------------


@pytest.mark.live
def test_build_vectors_from_jsonl(tmp_path):
    """Build vectors from a tiny JSONL: 2 with text, 1 without. Verify stats."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir()

    rows = [
        {"decision_id": "dec_1", "regeste": "Mietrecht: Kuendigung wegen Eigenbedarf.", "language": "de"},
        {"decision_id": "dec_2", "full_text": "Das Bundesgericht hat entschieden ...", "language": "de"},
        {"decision_id": "dec_3", "regeste": None, "full_text": None, "language": "de"},  # no text
    ]
    jsonl_file = input_dir / "test.jsonl"
    jsonl_file.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
        encoding="utf-8",
    )

    db_path = tmp_path / "vectors.db"
    stats = build_vectors(input_dir=input_dir, db_path=db_path)

    assert stats["embedded"] == 2
    assert stats["skipped_no_text"] == 1
    assert stats["skipped_dupe"] == 0
    assert db_path.exists()
    assert stats["db_path"] == str(db_path)


# ---------------------------------------------------------------------------
# Task 10: Semantic proximity integration test
# ---------------------------------------------------------------------------


@pytest.mark.live
def test_semantic_finds_related_concept():
    """'Hundebiss' should be semantically closer to 'Tierhalterhaftung' than to 'Mietvertrag'."""
    import numpy as np

    model = load_model()
    texts = [
        "Hundebiss",                                        # query
        "Tierhalterhaftung Art. 56 OR",                     # semantically related
        "Steuerbefreiung gemeinnütziger Organisationen",    # unrelated (tax domain)
    ]
    embs = encode_texts(model, texts)

    # Embeddings are L2-normalized, dot product = cosine similarity
    sim_related = float(np.dot(embs[0], embs[1]))
    sim_unrelated = float(np.dot(embs[0], embs[2]))

    assert sim_related > sim_unrelated, (
        f"'Hundebiss' should be closer to 'Tierhalterhaftung' ({sim_related:.3f}) "
        f"than to 'Steuerbefreiung' ({sim_unrelated:.3f})"
    )
