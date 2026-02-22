# Semantic Search Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add multilingual semantic (vector) search to opencaselaw.ch using BGE-M3 embeddings and sqlite-vec, integrated as a dual-retrieval path alongside FTS5.

**Architecture:** Offline batch pipeline (`build_vectors.py`) embeds 1M+ decisions with BGE-M3, stores them in a sqlite-vec database. At query time, the MCP server embeds the query, runs KNN vector search in parallel with FTS5, and merges candidates via RRF fusion before the existing 15-signal reranking pipeline. Graceful degradation — if vectors.db or model is unavailable, search falls back to pure FTS5.

**Tech Stack:** BGE-M3 (BAAI/bge-m3), sqlite-vec, sentence-transformers, ONNX Runtime (int8 quantized CPU inference)

---

## Context for the Implementer

### Codebase orientation

- **`mcp_server.py`** (5000 lines) — the MCP server. Search entry point is `_search_fts5_inner()`. Candidates are gathered from FTS5 strategies (lines 668-760), then reranked by `_rerank_rows()` (lines 1476-1627) with 15 signals. Cross-encoder is optional (lines 2185-2246). DB connections use `get_db()` (line 450) and `_get_graph_conn()` (line 1177).
- **`search_stack/build_reference_graph.py`** — reference graph builder. Uses atomic `tmp + os.replace()` pattern (line 345-482). Follow this for `build_vectors.py`.
- **`build_fts5.py`** — FTS5 builder. Reads JSONL from `output/decisions/*.jsonl`. Follow its JSONL iteration pattern.
- **`db_schema.py`** — canonical schema. Not modified by this work.
- **`pyproject.toml`** — already has `semantic = ["sentence-transformers>=3.0"]` optional dep.

### Key patterns to follow

1. **Lazy model loading**: See `_get_cross_encoder()` (line 2225) — global var + failure flag + lazy import inside function. Use this exact pattern for BGE-M3.
2. **External DB connection**: See `_get_graph_conn()` (line 1177) — check exists, connect, PRAGMA query_only=ON, return None on error.
3. **Atomic build**: See `build_reference_graph.py` (line 345) — write to `.tmp`, `os.replace()` on success, cleanup on error.
4. **Fusion scores**: See lines 764-770 — `fusion_scores` is a `dict[str, dict]` mapping `decision_id → {"rrf_score": float, "strategy_hits": int}`. Vector results add to this same dict.
5. **Reranking signals**: See lines 1605-1625 — all signals are float values summed together. Add vector similarity as one more term.

### Environment

- VPS: 16 CPU, 64 GB RAM, 200 GB disk (~75% used)
- Python 3.13, SQLite 3.45+
- 4 uvicorn MCP workers (:8770-8773)
- Daily cron at 04:00 UTC: scrapers → `build_fts5.py` → `export_parquet.py` → `generate_stats.py`

---

## Task 1: Add Dependencies

**Files:**
- Modify: `pyproject.toml`

**Step 1: Update pyproject.toml**

Add `sqlite-vec` and `onnxruntime` to the `[semantic]` extra:

```toml
semantic = ["sentence-transformers>=3.0", "sqlite-vec>=0.1.6", "onnxruntime>=1.17"]
```

**Step 2: Install locally**

Run: `pip install sqlite-vec onnxruntime sentence-transformers`
Expected: Installs without error.

**Step 3: Verify sqlite-vec loads**

Run: `python3 -c "import sqlite3; import sqlite_vec; db = sqlite3.connect(':memory:'); db.enable_load_extension(True); sqlite_vec.load(db); print(db.execute('select vec_version()').fetchone())"`
Expected: Prints version tuple like `('v0.1.6',)`

**Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "feat: add sqlite-vec and onnxruntime to semantic deps"
```

---

## Task 2: Text Selection Helper + Tests (TDD)

**Files:**
- Create: `search_stack/build_vectors.py`
- Create: `test_build_vectors.py`

**Step 1: Write the failing test**

Create `test_build_vectors.py`:

```python
"""Tests for vector embedding pipeline."""
import pytest


def test_select_text_uses_regeste():
    from search_stack.build_vectors import _select_text

    row = {"regeste": "Art. 41 OR. Haftpflicht.", "full_text": "Long full text " * 200}
    assert _select_text(row) == "Art. 41 OR. Haftpflicht."


def test_select_text_falls_back_to_full_text():
    from search_stack.build_vectors import _select_text

    row = {"regeste": None, "full_text": "A" * 5000}
    result = _select_text(row)
    assert len(result) == 2000  # truncated


def test_select_text_falls_back_on_short_regeste():
    from search_stack.build_vectors import _select_text

    row = {"regeste": "Too short", "full_text": "Longer text " * 200}
    result = _select_text(row)
    assert result.startswith("Longer text")


def test_select_text_empty_row():
    from search_stack.build_vectors import _select_text

    row = {"regeste": None, "full_text": None}
    assert _select_text(row) is None


def test_select_text_empty_strings():
    from search_stack.build_vectors import _select_text

    row = {"regeste": "", "full_text": ""}
    assert _select_text(row) is None
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest test_build_vectors.py -v`
Expected: FAIL — `ImportError: cannot import name '_select_text'`

**Step 3: Write minimal implementation**

Create `search_stack/build_vectors.py`:

```python
#!/usr/bin/env python3
"""
Build sqlite-vec vector database from scraped decisions.

Reads from:
  - output/decisions/*.jsonl  (same input as build_fts5.py)

Produces:
  - output/vectors.db (SQLite with sqlite-vec KNN index)

Uses BAAI/bge-m3 for multilingual dense embeddings (1024-dim).

Usage:
    python3 -m search_stack.build_vectors                          # default: ./output
    python3 -m search_stack.build_vectors --output /opt/caselaw/repo/output
    python3 -m search_stack.build_vectors --output ./output --limit 1000  # test run
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

logger = logging.getLogger("build_vectors")

MIN_REGESTE_LEN = 20
MAX_TEXT_LEN = 2000


def _select_text(row: dict) -> str | None:
    """Select the best text to embed for a decision.

    Priority: regeste (if long enough) > first 2000 chars of full_text.
    Returns None if no usable text.
    """
    regeste = (row.get("regeste") or "").strip()
    if len(regeste) >= MIN_REGESTE_LEN:
        return regeste

    full_text = (row.get("full_text") or "").strip()
    if not full_text:
        return None

    return full_text[:MAX_TEXT_LEN]
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest test_build_vectors.py -v`
Expected: All 5 PASS

**Step 5: Commit**

```bash
git add search_stack/build_vectors.py test_build_vectors.py
git commit -m "feat: add text selection helper for vector embeddings"
```

---

## Task 3: sqlite-vec Round-Trip Test (TDD)

**Files:**
- Modify: `test_build_vectors.py`
- Modify: `search_stack/build_vectors.py`

**Step 1: Write the failing test**

Append to `test_build_vectors.py`:

```python
import sqlite3
import struct


def _make_vector(dim: int, val: float = 0.0) -> list[float]:
    """Create a constant vector for testing."""
    return [val] * dim


def test_vec_db_round_trip():
    """Insert vectors and retrieve via KNN."""
    from search_stack.build_vectors import (
        VEC_TABLE_SQL,
        serialize_f32,
        create_vec_db,
    )

    conn = create_vec_db(":memory:")

    # Insert two vectors: one "about dogs", one "about taxes"
    dog_vec = _make_vector(1024, 0.1)
    tax_vec = _make_vector(1024, 0.9)

    conn.execute(
        "INSERT INTO vec_decisions(decision_id, embedding, language) VALUES (?, ?, ?)",
        ("dog_case", serialize_f32(dog_vec), "de"),
    )
    conn.execute(
        "INSERT INTO vec_decisions(decision_id, embedding, language) VALUES (?, ?, ?)",
        ("tax_case", serialize_f32(tax_vec), "fr"),
    )

    # Query with a vector close to dog_vec
    query_vec = _make_vector(1024, 0.11)
    results = conn.execute(
        """
        SELECT decision_id, distance
        FROM vec_decisions
        WHERE embedding MATCH ? AND k = 2
        ORDER BY distance
        """,
        (serialize_f32(query_vec),),
    ).fetchall()

    assert len(results) == 2
    assert results[0][0] == "dog_case"  # closest
    assert results[1][0] == "tax_case"


def test_vec_db_language_partition():
    """Partition key filters to one language."""
    from search_stack.build_vectors import (
        VEC_TABLE_SQL,
        serialize_f32,
        create_vec_db,
    )

    conn = create_vec_db(":memory:")

    conn.execute(
        "INSERT INTO vec_decisions(decision_id, embedding, language) VALUES (?, ?, ?)",
        ("de_case", serialize_f32(_make_vector(1024, 0.1)), "de"),
    )
    conn.execute(
        "INSERT INTO vec_decisions(decision_id, embedding, language) VALUES (?, ?, ?)",
        ("fr_case", serialize_f32(_make_vector(1024, 0.9)), "fr"),
    )

    # Query only French partition
    query_vec = _make_vector(1024, 0.5)
    results = conn.execute(
        """
        SELECT decision_id, distance
        FROM vec_decisions
        WHERE embedding MATCH ? AND k = 10
          AND language = 'fr'
        ORDER BY distance
        """,
        (serialize_f32(query_vec),),
    ).fetchall()

    assert len(results) == 1
    assert results[0][0] == "fr_case"
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest test_build_vectors.py::test_vec_db_round_trip -v`
Expected: FAIL — `ImportError: cannot import name 'VEC_TABLE_SQL'`

**Step 3: Write minimal implementation**

Add to `search_stack/build_vectors.py`:

```python
import struct

try:
    import sqlite_vec
except ImportError:
    sqlite_vec = None

EMBEDDING_DIM = 1024

VEC_TABLE_SQL = f"""
CREATE VIRTUAL TABLE IF NOT EXISTS vec_decisions USING vec0(
    decision_id TEXT PRIMARY KEY,
    embedding float[{EMBEDDING_DIM}] distance_metric=cosine,
    language TEXT partition key
);
"""


def serialize_f32(vector: list[float]) -> bytes:
    """Serialize a list of floats to compact binary for sqlite-vec."""
    return struct.pack(f"{len(vector)}f", *vector)


def create_vec_db(path: str) -> sqlite3.Connection:
    """Create a sqlite-vec database with the vector table.

    Args:
        path: Database path, or ":memory:" for testing.

    Returns:
        Connection with sqlite-vec loaded and table created.
    """
    if sqlite_vec is None:
        raise ImportError("sqlite-vec is required: pip install sqlite-vec")

    conn = sqlite3.connect(path)
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    conn.execute(VEC_TABLE_SQL)
    return conn
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest test_build_vectors.py -v`
Expected: All 7 PASS

**Step 5: Commit**

```bash
git add search_stack/build_vectors.py test_build_vectors.py
git commit -m "feat: sqlite-vec table schema and round-trip tests"
```

---

## Task 4: Embedding Model Loader

**Files:**
- Modify: `test_build_vectors.py`
- Modify: `search_stack/build_vectors.py`

**Step 1: Write the failing test**

Append to `test_build_vectors.py`:

```python
@pytest.mark.live
def test_load_embedding_model():
    """Load BGE-M3 model and encode a sentence (requires model download)."""
    from search_stack.build_vectors import load_model, encode_texts

    model = load_model()
    assert model is not None

    embeddings = encode_texts(model, ["Swiss court decision about labor law"])
    assert embeddings.shape == (1, 1024)


@pytest.mark.live
def test_encode_multilingual():
    """BGE-M3 encodes DE/FR/IT and produces similar vectors for similar concepts."""
    import numpy as np
    from search_stack.build_vectors import load_model, encode_texts

    model = load_model()
    texts = [
        "Haftung des Tierhalters für Hundebiss",      # DE
        "Responsabilité du détenteur d'animal",        # FR
        "Responsabilità del detentore di animali",     # IT
        "Steuerrecht Einkommenssteuer",                # DE - unrelated topic
    ]
    embeddings = encode_texts(model, texts)
    assert embeddings.shape == (4, 1024)

    # Animal liability texts (0,1,2) should be closer to each other than to tax (3)
    from numpy.linalg import norm

    def cosine_sim(a, b):
        return float(np.dot(a, b) / (norm(a) * norm(b)))

    sim_de_fr = cosine_sim(embeddings[0], embeddings[1])
    sim_de_it = cosine_sim(embeddings[0], embeddings[2])
    sim_de_tax = cosine_sim(embeddings[0], embeddings[3])

    assert sim_de_fr > sim_de_tax, f"DE-FR ({sim_de_fr:.3f}) should be > DE-Tax ({sim_de_tax:.3f})"
    assert sim_de_it > sim_de_tax, f"DE-IT ({sim_de_it:.3f}) should be > DE-Tax ({sim_de_tax:.3f})"
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest test_build_vectors.py::test_load_embedding_model -v -m live`
Expected: FAIL — `ImportError: cannot import name 'load_model'`

**Step 3: Write minimal implementation**

Add to `search_stack/build_vectors.py`:

```python
import numpy as np

# Model configuration
BGE_M3_MODEL_ID = "BAAI/bge-m3"
ENCODE_MAX_LENGTH = 512
ENCODE_BATCH_SIZE = 32


def load_model(model_id: str = BGE_M3_MODEL_ID):
    """Load BGE-M3 model for dense embedding.

    Tries ONNX backend first (faster on CPU), falls back to PyTorch.

    Returns:
        SentenceTransformer model instance.
    """
    from sentence_transformers import SentenceTransformer

    # Try ONNX backend for better CPU performance
    try:
        model = SentenceTransformer(model_id, backend="onnx")
        logger.info(f"Loaded {model_id} with ONNX backend")
        return model
    except Exception as e:
        logger.debug(f"ONNX backend unavailable ({e}), falling back to PyTorch")

    model = SentenceTransformer(model_id)
    logger.info(f"Loaded {model_id} with PyTorch backend")
    return model


def encode_texts(
    model,
    texts: list[str],
    batch_size: int = ENCODE_BATCH_SIZE,
    max_length: int = ENCODE_MAX_LENGTH,
) -> np.ndarray:
    """Encode texts to dense vectors.

    Args:
        model: SentenceTransformer model from load_model().
        texts: List of strings to encode.
        batch_size: Batch size for encoding.
        max_length: Maximum token length (512 is sufficient for regeste).

    Returns:
        numpy array of shape (len(texts), 1024), dtype float32.
    """
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=False,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    return embeddings
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest test_build_vectors.py::test_load_embedding_model test_build_vectors.py::test_encode_multilingual -v -m live`
Expected: Both PASS (first run downloads ~2GB model, may take several minutes)

**Step 5: Commit**

```bash
git add search_stack/build_vectors.py test_build_vectors.py
git commit -m "feat: BGE-M3 model loader and encoder"
```

---

## Task 5: Full Build Pipeline

**Files:**
- Modify: `search_stack/build_vectors.py`

**Step 1: Write the failing test**

Append to `test_build_vectors.py`:

```python
import tempfile
from pathlib import Path


def test_build_vectors_from_jsonl(tmp_path):
    """Build vectors.db from a small JSONL fixture."""
    from search_stack.build_vectors import build_vectors, serialize_f32

    # Create test JSONL
    decisions_dir = tmp_path / "decisions"
    decisions_dir.mkdir()
    jsonl_file = decisions_dir / "test_court.jsonl"

    rows = [
        {
            "decision_id": "test_1",
            "court": "bger",
            "language": "de",
            "regeste": "Art. 41 OR. Haftung für unerlaubte Handlung. Der Geschädigte hat den Schaden zu beweisen.",
            "full_text": "Long full text here " * 50,
        },
        {
            "decision_id": "test_2",
            "court": "bger",
            "language": "fr",
            "regeste": None,
            "full_text": "Responsabilité civile. Preuve du dommage. " * 50,
        },
        {
            "decision_id": "test_3",
            "court": "bger",
            "language": "de",
            "regeste": None,
            "full_text": None,  # should be skipped
        },
    ]
    with open(jsonl_file, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    db_path = tmp_path / "vectors.db"
    stats = build_vectors(input_dir=decisions_dir, db_path=db_path, limit=100)

    assert stats["embedded"] == 2
    assert stats["skipped_no_text"] == 1
    assert db_path.exists()
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest test_build_vectors.py::test_build_vectors_from_jsonl -v`
Expected: FAIL — `ImportError: cannot import name 'build_vectors'`

**Step 3: Write the full build pipeline**

Add to `search_stack/build_vectors.py`:

```python
def _iter_rows_from_jsonl(input_dir: Path) -> __builtins__.__class__:
    """Iterate over decision rows from JSONL files."""
    for jsonl_path in sorted(input_dir.glob("*.jsonl")):
        with open(jsonl_path, encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    print(
                        f"  WARNING: Skipping bad JSON at {jsonl_path.name}:{line_no}",
                        file=sys.stderr,
                    )


def build_vectors(
    *,
    input_dir: Path,
    db_path: Path,
    model_id: str = BGE_M3_MODEL_ID,
    batch_size: int = ENCODE_BATCH_SIZE,
    limit: int | None = None,
) -> dict:
    """Build the vector database from JSONL decision files.

    Args:
        input_dir: Directory containing *.jsonl decision files.
        db_path: Output path for vectors.db.
        model_id: HuggingFace model ID for embeddings.
        batch_size: Encoding batch size.
        limit: Optional limit for test runs.

    Returns:
        Stats dict with counts.
    """
    t0 = time.time()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = db_path.with_name(f".{db_path.name}.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    logger.info(f"Loading embedding model: {model_id}")
    model = load_model(model_id)

    conn = None
    embedded = 0
    skipped_no_text = 0
    skipped_dupe = 0

    try:
        conn = create_vec_db(str(tmp_path))

        batch_texts: list[str] = []
        batch_ids: list[str] = []
        batch_langs: list[str] = []
        seen_ids: set[str] = set()

        def _flush_batch():
            nonlocal embedded
            if not batch_texts:
                return
            embeddings = encode_texts(model, batch_texts, batch_size=batch_size)
            for i, (did, lang) in enumerate(zip(batch_ids, batch_langs)):
                conn.execute(
                    "INSERT INTO vec_decisions(decision_id, embedding, language) VALUES (?, ?, ?)",
                    (did, serialize_f32(embeddings[i].tolist()), lang),
                )
            embedded += len(batch_texts)
            batch_texts.clear()
            batch_ids.clear()
            batch_langs.clear()

        for row in _iter_rows_from_jsonl(input_dir):
            decision_id = row.get("decision_id")
            if not decision_id:
                continue

            if decision_id in seen_ids:
                skipped_dupe += 1
                continue
            seen_ids.add(decision_id)

            text = _select_text(row)
            if text is None:
                skipped_no_text += 1
                continue

            batch_texts.append(text)
            batch_ids.append(decision_id)
            batch_langs.append(row.get("language", "de"))

            if len(batch_texts) >= batch_size:
                _flush_batch()

                if embedded % 10_000 == 0:
                    conn.commit()
                    elapsed = time.time() - t0
                    logger.info(
                        f"  [{elapsed:.0f}s] {embedded:,} decisions embedded"
                    )

            if limit and embedded + len(batch_texts) >= limit:
                _flush_batch()
                break

        _flush_batch()
        conn.commit()

    except Exception:
        if conn is not None:
            conn.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    else:
        conn.close()
        os.replace(tmp_path, db_path)

    elapsed = time.time() - t0
    stats = {
        "db_path": str(db_path),
        "embedded": embedded,
        "skipped_no_text": skipped_no_text,
        "skipped_dupe": skipped_dupe,
        "elapsed_seconds": round(elapsed, 1),
    }
    logger.info(
        f"Vector DB complete: {embedded:,} embedded, "
        f"{skipped_no_text:,} skipped (no text), "
        f"{skipped_dupe:,} skipped (dupe), "
        f"{elapsed:.0f}s elapsed"
    )
    return stats
```

Note: fix the `_iter_rows_from_jsonl` return type annotation — use `Iterator[dict]`:

```python
from typing import Iterator

def _iter_rows_from_jsonl(input_dir: Path) -> Iterator[dict]:
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest test_build_vectors.py::test_build_vectors_from_jsonl -v`
Expected: PASS (uses the already-downloaded model)

**Step 5: Commit**

```bash
git add search_stack/build_vectors.py test_build_vectors.py
git commit -m "feat: full vector build pipeline with atomic writes"
```

---

## Task 6: CLI Entry Point

**Files:**
- Modify: `search_stack/build_vectors.py`

**Step 1: Add main() and argparse**

Append to `search_stack/build_vectors.py`:

```python
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build sqlite-vec vector database from decision JSONL files"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("output/decisions"),
        help="Directory containing *.jsonl decision files",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/vectors.db"),
        help="Output vector database path",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=BGE_M3_MODEL_ID,
        help=f"HuggingFace model ID (default: {BGE_M3_MODEL_ID})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=ENCODE_BATCH_SIZE,
        help=f"Encoding batch size (default: {ENCODE_BATCH_SIZE})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of decisions to embed (for testing)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    stats = build_vectors(
        input_dir=args.input,
        db_path=args.output,
        model_id=args.model,
        batch_size=args.batch_size,
        limit=args.limit,
    )
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
```

**Step 2: Test locally**

Run: `python3 -m search_stack.build_vectors --limit 10 -v`
Expected: Embeds up to 10 decisions (or fewer if local JSONL files don't exist — that's fine). If no local JSONL, create a small test file first.

**Step 3: Commit**

```bash
git add search_stack/build_vectors.py
git commit -m "feat: CLI entry point for vector builder"
```

---

## Task 7: MCP Server — Vector DB Connection + Model Loader

**Files:**
- Modify: `mcp_server.py` (lines ~112-129 config section, ~444-445 globals section)

**Step 1: Add configuration constants**

After the `GRAPH_SIGNALS_ENABLED` line (line 128), add:

```python
# ── Vector search configuration ──────────────────────────────
VECTOR_DB_PATH = Path(os.environ.get("SWISS_CASELAW_VECTORS_DB", "output/vectors.db"))
VECTOR_SEARCH_ENABLED = os.environ.get("SWISS_CASELAW_VECTOR_SEARCH", "auto").lower()
# "auto" = enabled if vectors.db exists; "0"/"false"/"no" = disabled; "1"/"true"/"yes" = forced
VECTOR_WEIGHT = float(os.environ.get("SWISS_CASELAW_VECTOR_WEIGHT", "1.0"))
VECTOR_K = int(os.environ.get("SWISS_CASELAW_VECTOR_K", "50"))
VECTOR_SIGNAL_WEIGHT = float(os.environ.get("SWISS_CASELAW_VECTOR_SIGNAL_WEIGHT", "3.0"))
```

**Step 2: Add global variables**

After `_CROSS_ENCODER_FAILED = False` (line 445), add:

```python
_VECTOR_MODEL = None
_VECTOR_MODEL_FAILED = False
```

**Step 3: Add vector DB connection helper**

After `_get_graph_conn()` (around line 1188), add:

```python
def _get_vec_conn() -> sqlite3.Connection | None:
    """Open a read-only connection to the vector DB, or None if unavailable."""
    if VECTOR_SEARCH_ENABLED in {"0", "false", "no"}:
        return None
    if not VECTOR_DB_PATH.exists():
        return None
    try:
        import sqlite_vec
    except ImportError:
        logger.debug("sqlite-vec not installed, vector search disabled")
        return None
    try:
        conn = sqlite3.connect(str(VECTOR_DB_PATH), timeout=0.5)
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
        conn.execute("PRAGMA query_only = ON")
        return conn
    except Exception as e:
        logger.debug("Failed to open vector DB: %s", e)
        return None


def _get_vector_model():
    """Lazy-load BGE-M3 embedding model. Returns None if unavailable."""
    global _VECTOR_MODEL, _VECTOR_MODEL_FAILED
    if VECTOR_SEARCH_ENABLED in {"0", "false", "no"}:
        return None
    if _VECTOR_MODEL is not None:
        return _VECTOR_MODEL
    if _VECTOR_MODEL_FAILED:
        return None
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        logger.debug("sentence-transformers not installed, vector search disabled")
        _VECTOR_MODEL_FAILED = True
        return None
    try:
        model_id = "BAAI/bge-m3"
        try:
            _VECTOR_MODEL = SentenceTransformer(model_id, backend="onnx")
            logger.info("Loaded %s with ONNX backend for vector search", model_id)
        except Exception:
            _VECTOR_MODEL = SentenceTransformer(model_id)
            logger.info("Loaded %s with PyTorch backend for vector search", model_id)
        return _VECTOR_MODEL
    except Exception as e:
        logger.warning("Vector model load failed: %s", e)
        _VECTOR_MODEL_FAILED = True
        return None
```

**Step 4: Commit**

```bash
git add mcp_server.py
git commit -m "feat: vector DB connection and model loader in MCP server"
```

---

## Task 8: MCP Server — Vector Search Function

**Files:**
- Modify: `mcp_server.py`

**Step 1: Add the vector search function**

After `_get_vector_model()`, add:

```python
def _search_vectors(
    query: str,
    language: str | None = None,
    k: int | None = None,
) -> dict[str, float]:
    """Run vector KNN search. Returns {decision_id: cosine_distance} or empty dict.

    This is called in parallel with FTS5 search. Returns empty dict if
    vector search is unavailable (missing DB, model, or deps).
    """
    import struct

    model = _get_vector_model()
    if model is None:
        return {}

    vec_conn = _get_vec_conn()
    if vec_conn is None:
        return {}

    k = k or VECTOR_K

    try:
        # Encode query
        embedding = model.encode(
            [query],
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )[0]
        query_bytes = struct.pack(f"{len(embedding)}f", *embedding.tolist())

        # KNN query
        if language:
            rows = vec_conn.execute(
                """
                SELECT decision_id, distance
                FROM vec_decisions
                WHERE embedding MATCH ? AND k = ?
                  AND language = ?
                ORDER BY distance
                """,
                (query_bytes, k, language),
            ).fetchall()
        else:
            rows = vec_conn.execute(
                """
                SELECT decision_id, distance
                FROM vec_decisions
                WHERE embedding MATCH ? AND k = ?
                ORDER BY distance
                """,
                (query_bytes, k),
            ).fetchall()

        return {row[0]: row[1] for row in rows}

    except Exception as e:
        logger.debug("Vector search failed: %s", e)
        return {}
    finally:
        vec_conn.close()
```

**Step 2: Commit**

```bash
git add mcp_server.py
git commit -m "feat: vector KNN search function"
```

---

## Task 9: MCP Server — Integrate Vector Search into Candidate Gathering

This is the core integration. Vector candidates are merged into the FTS5 candidate pool before reranking.

**Files:**
- Modify: `mcp_server.py` (in `_search_fts5_inner()`, around line 762)

**Step 1: Add vector search call after FTS5 candidate gathering**

Find the section at line 762 (`if candidate_meta:`) and add vector integration BEFORE it. The modified section should look like:

```python
    # ── Vector search (parallel candidate source) ──
    # Run vector KNN alongside FTS5 — adds candidates FTS5 may have missed.
    vector_scores: dict[str, float] = {}
    if not is_docket_query and not has_explicit_syntax:
        vector_scores = _search_vectors(
            query=fts_query,
            language=language,
            k=VECTOR_K,
        )
        if vector_scores:
            # Fetch row data for vector-only candidates (not already in FTS5 pool)
            vec_only_ids = set(vector_scores.keys()) - set(candidate_meta.keys())
            if vec_only_ids:
                placeholders = ",".join("?" for _ in vec_only_ids)
                vec_rows = conn.execute(
                    f"""
                    SELECT
                        d.decision_id,
                        d.court,
                        d.canton,
                        d.chamber,
                        d.docket_number,
                        d.decision_date,
                        d.language,
                        d.title,
                        d.regeste,
                        d.full_text AS full_text_raw,
                        '' as snippet,
                        d.source_url,
                        d.pdf_url,
                        0.0 as bm25_score
                    FROM decisions d
                    WHERE d.decision_id IN ({placeholders})
                    """,
                    list(vec_only_ids),
                ).fetchall()
                for row in vec_rows:
                    did = row["decision_id"]
                    candidate_meta[did] = {
                        "row": row,
                        "best_bm25": 0.0,
                        "rrf_score": 0.0,
                        "strategy_hits": 0,
                    }

            # Add vector RRF contribution for ALL vector results
            for rank, (did, dist) in enumerate(
                sorted(vector_scores.items(), key=lambda x: x[1]),
                start=1,
            ):
                if did in candidate_meta:
                    current = candidate_meta[did]
                    current["rrf_score"] = float(current["rrf_score"]) + (
                        VECTOR_WEIGHT / (RRF_RANK_CONSTANT + rank)
                    )
                    current["strategy_hits"] = int(current["strategy_hits"]) + 1

    if candidate_meta:
        rows_for_rerank = [m["row"] for m in candidate_meta.values()]
        # ... (rest unchanged)
```

**Step 2: Add vector cosine similarity as reranking signal**

In `_rerank_rows()`, add a `vector_scores` parameter and use it:

Modify the function signature (line 1476):

```python
def _rerank_rows(
    rows: list[sqlite3.Row],
    raw_query: str,
    limit: int,
    *,
    fusion_scores: dict[str, dict] | None = None,
    vector_scores: dict[str, float] | None = None,
    offset: int = 0,
    sort: str | None = None,
) -> list[dict]:
```

In the per-row loop, after the graph signal calculations (around line 1600), add:

```python
        # Vector similarity signal
        vector_signal = 0.0
        if vector_scores:
            vec_dist = vector_scores.get(decision_id)
            if vec_dist is not None:
                # cosine distance → similarity: sim = 1 - dist
                vec_sim = max(0.0, 1.0 - vec_dist)
                vector_signal = VECTOR_SIGNAL_WEIGHT * vec_sim
```

Add `vector_signal` to the signal sum (line ~1620):

```python
        signal = (
            6.0 * docket_exact
            + 2.0 * docket_partial
            # ... existing signals ...
            + language_signal
            + vector_signal  # <-- NEW
        )
```

**Step 3: Pass vector_scores through the call chain**

In `_search_fts5_inner()`, update the calls to `_rerank_rows()` (around lines 777 and 797) to pass `vector_scores=vector_scores`:

```python
        reranked = _rerank_rows(
            rows_for_rerank,
            fts_query,
            limit,
            fusion_scores=fusion_scores,
            vector_scores=vector_scores,
            offset=offset,
            sort=sort,
        )
```

(Do this for BOTH call sites — the inline_docket_results path and the normal path.)

**Step 4: Run existing tests**

Run: `python3 -m pytest test_mcp_search_nl.py test_search_rerank_signals.py -v`
Expected: All existing tests still PASS (vector search is a no-op when vectors.db doesn't exist)

**Step 5: Commit**

```bash
git add mcp_server.py
git commit -m "feat: integrate vector search into candidate gathering and reranking"
```

---

## Task 10: Semantic Search Integration Test

**Files:**
- Modify: `test_build_vectors.py`

**Step 1: Write the integration test**

Append to `test_build_vectors.py`:

```python
@pytest.mark.live
def test_semantic_finds_related_concept():
    """Embed 'Hundebiss' and 'Tierhalterhaftung', verify semantic proximity."""
    from search_stack.build_vectors import load_model, encode_texts
    import numpy as np
    from numpy.linalg import norm

    model = load_model()

    texts = [
        "Hundebiss",                           # query
        "Tierhalterhaftung Art. 56 OR",        # semantically related
        "Mietvertrag Kündigung",               # unrelated
    ]
    embs = encode_texts(model, texts)

    def cosine_sim(a, b):
        return float(np.dot(a, b) / (norm(a) * norm(b)))

    sim_related = cosine_sim(embs[0], embs[1])
    sim_unrelated = cosine_sim(embs[0], embs[2])

    assert sim_related > sim_unrelated, (
        f"'Hundebiss' should be closer to 'Tierhalterhaftung' ({sim_related:.3f}) "
        f"than to 'Mietvertrag' ({sim_unrelated:.3f})"
    )
```

**Step 2: Run the test**

Run: `python3 -m pytest test_build_vectors.py::test_semantic_finds_related_concept -v -m live`
Expected: PASS

**Step 3: Commit**

```bash
git add test_build_vectors.py
git commit -m "test: semantic proximity integration test"
```

---

## Task 11: Benchmark Extension

**Files:**
- Modify: `benchmarks/golden_queries.json` (or wherever the benchmark golden set lives)

**Step 1: Check current benchmark file**

Run: `ls benchmarks/` and read the golden query file to understand the format.

**Step 2: Add 3-5 semantic queries**

Add queries where FTS5 is expected to fail but vector search should help:

```json
{
    "query": "Hundebiss Haftung",
    "tags": ["semantic", "de", "tort"],
    "relevant": [
        {"docket": "find via search for Art. 56 OR Tierhalterhaftung", "grade": 3}
    ],
    "note": "Tests concept matching: Hundebiss → Tierhalterhaftung"
},
{
    "query": "responsabilité du détenteur d'animal",
    "tags": ["semantic", "fr", "cross-lingual"],
    "relevant": [
        {"docket": "find via search for Tierhalterhaftung", "grade": 2}
    ],
    "note": "Tests cross-lingual: FR query should find DE decisions"
}
```

(Exact decision IDs to be filled in after looking up real decisions.)

**Step 3: Run benchmark before/after**

Run: `python3 benchmarks/run_search_benchmark.py`
Document the baseline metrics. After deploying vector search, re-run and compare.

**Step 4: Commit**

```bash
git add benchmarks/
git commit -m "bench: add semantic search benchmark queries"
```

---

## Task 12: Deploy to VPS

**Step 1: Push all code**

```bash
git push origin main
```

**Step 2: Install deps on VPS**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && git pull --rebase origin main && pip install sqlite-vec onnxruntime'
```

**Step 3: Build vectors.db (overnight job)**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && nohup python3 -m search_stack.build_vectors --input output/decisions --output output/vectors.db -v >> logs/build_vectors.log 2>&1 &'
```

Monitor progress:
```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'tail -5 /opt/caselaw/repo/logs/build_vectors.log'
```

Expected: ~8-12 hours for 1M+ decisions.

**Step 4: After build completes, restart MCP workers**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
```

**Step 5: Verify vector search is active**

Test a semantic query via the MCP tool and check logs for vector search activity:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'journalctl -u mcp-server@8770 --since "1 min ago" | grep -i vector'
```

**Step 6: Add to daily cron**

Add `build_vectors.py` to the publish pipeline or cron. Since embedding 1M decisions takes ~8-12h, this may run as a weekly job rather than daily, or only embed new decisions incrementally (future enhancement).

**Step 7: Commit any cron/config changes**

```bash
git add .env.mcp  # if updated
git commit -m "deploy: enable vector search on VPS"
```

---

## Summary

| Task | Description | Key Files |
|------|-------------|-----------|
| 1 | Add deps | `pyproject.toml` |
| 2 | Text selection helper + tests | `search_stack/build_vectors.py`, `test_build_vectors.py` |
| 3 | sqlite-vec round-trip test | `search_stack/build_vectors.py`, `test_build_vectors.py` |
| 4 | BGE-M3 model loader | `search_stack/build_vectors.py`, `test_build_vectors.py` |
| 5 | Full build pipeline | `search_stack/build_vectors.py`, `test_build_vectors.py` |
| 6 | CLI entry point | `search_stack/build_vectors.py` |
| 7 | MCP: vector DB conn + model loader | `mcp_server.py` |
| 8 | MCP: vector search function | `mcp_server.py` |
| 9 | MCP: integrate into candidate gathering + reranking | `mcp_server.py` |
| 10 | Semantic integration test | `test_build_vectors.py` |
| 11 | Benchmark extension | `benchmarks/` |
| 12 | Deploy to VPS | VPS commands |
