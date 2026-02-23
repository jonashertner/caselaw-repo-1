"""Build and manage vector embeddings for Swiss caselaw semantic search.

This module provides:
- Text selection from decision rows (regeste or full_text fallback)
- sqlite-vec schema creation for cosine-similarity KNN search
- Float32 serialization helpers for sqlite-vec compatibility
- BGE-M3 model loading with ONNX/PyTorch fallback (SentenceTransformer)
- Optional FlagEmbedding backend for BGE-M3 sparse (lexical) weights
- Optional chunk-level indexing for long-document recall
- Full build pipeline: JSONL -> embeddings -> sqlite-vec DB
- CLI entry point for batch embedding generation
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import struct
import sys
import time
from pathlib import Path
from typing import Iterator

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EMBEDDING_DIM = 1024
"""Dimensionality of sentence-transformer embeddings (BGE-M3 default)."""

BGE_M3_MODEL_ID = "BAAI/bge-m3"
"""HuggingFace model identifier for the BGE-M3 multilingual embedding model."""

ENCODE_MAX_LENGTH = 256
"""Maximum token length for input texts during encoding."""

ENCODE_BATCH_SIZE = 32
"""Default batch size for encoding texts."""

SPARSE_WEIGHT_THRESHOLD = 0.01
"""Minimum sparse weight to store (prune near-zero weights)."""

VEC_TABLE_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS vec_decisions USING vec0(
    decision_id TEXT PRIMARY KEY,
    embedding float[1024] distance_metric=cosine,
    language TEXT partition key
)
""".strip()
"""DDL for the sqlite-vec virtual table storing decision embeddings."""

SPARSE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS sparse_terms (
    decision_id TEXT NOT NULL,
    token_id INTEGER NOT NULL,
    weight REAL NOT NULL,
    PRIMARY KEY (decision_id, token_id)
);
CREATE INDEX IF NOT EXISTS idx_sparse_token ON sparse_terms (token_id);
""".strip()
"""DDL for the sparse inverted index table."""

# Chunk-level tables
VEC_CHUNKS_TABLE_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS vec_chunks USING vec0(
    chunk_id TEXT PRIMARY KEY,
    embedding float[1024] distance_metric=cosine,
    language TEXT partition key
)
""".strip()
"""DDL for the sqlite-vec virtual table storing chunk embeddings."""

CHUNK_META_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS chunk_meta (
    chunk_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chunk_meta_decision ON chunk_meta (decision_id);
""".strip()
"""DDL for the chunk metadata table."""


# ---------------------------------------------------------------------------
# Text selection helper
# ---------------------------------------------------------------------------


def _select_text(row: dict) -> str | None:
    """Choose the best text snippet from a decision row for embedding.

    Priority:
    1. ``regeste`` if present and >= 20 characters
    2. ``full_text`` first 500 characters if present and non-empty
    3. ``None`` otherwise

    Args:
        row: A dict with optional ``regeste`` and ``full_text`` keys.

    Returns:
        Selected text string, or None if no usable text is available.
    """
    regeste = row.get("regeste") or ""
    if len(regeste) >= 20:
        return regeste

    full_text = row.get("full_text") or ""
    if full_text:
        return full_text[:500]

    return None


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def serialize_f32(vector: list[float]) -> bytes:
    """Pack a list of floats into little-endian float32 bytes for sqlite-vec.

    Args:
        vector: List of float values (typically length EMBEDDING_DIM).

    Returns:
        Raw bytes in little-endian float32 format.
    """
    return struct.pack(f"<{len(vector)}f", *vector)


# ---------------------------------------------------------------------------
# Database creation
# ---------------------------------------------------------------------------


def create_vec_db(
    path: str,
    *,
    enable_sparse: bool = False,
    enable_chunks: bool = False,
) -> sqlite3.Connection:
    """Create a SQLite database with the sqlite-vec extension and required tables.

    Args:
        path: Filesystem path for the SQLite database file.
        enable_sparse: Create sparse inverted index table.
        enable_chunks: Create chunk-level embedding tables instead of
            decision-level (vec_chunks + chunk_meta).

    Returns:
        An open :class:`sqlite3.Connection` with sqlite-vec loaded and
        tables created.

    Raises:
        RuntimeError: If the sqlite-vec extension cannot be loaded.
    """
    conn = sqlite3.connect(path)
    try:
        conn.enable_load_extension(True)
    except AttributeError:
        conn.close()
        raise RuntimeError(
            "sqlite3.Connection.enable_load_extension() is not available. "
            "This typically happens on macOS system Python. "
            "Use a Python build compiled with --enable-loadable-sqlite-extensions."
        )

    try:
        import sqlite_vec  # type: ignore[import-untyped]

        sqlite_vec.load(conn)
    except ImportError:
        conn.close()
        raise RuntimeError(
            "sqlite-vec Python package is not installed. "
            "Install with: pip install sqlite-vec"
        )
    except Exception as exc:
        conn.close()
        raise RuntimeError(f"Failed to load sqlite-vec extension: {exc}") from exc

    # Always create the decision-level vec table
    conn.execute(VEC_TABLE_SQL)

    if enable_sparse:
        for stmt in SPARSE_TABLES_SQL.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)
        logger.info("Created sparse_terms table")

    if enable_chunks:
        conn.execute(VEC_CHUNKS_TABLE_SQL)
        for stmt in CHUNK_META_TABLE_SQL.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)
        logger.info("Created vec_chunks + chunk_meta tables")

    conn.commit()
    logger.info("Created vec DB at %s (sparse=%s, chunks=%s)", path, enable_sparse, enable_chunks)
    return conn


# ---------------------------------------------------------------------------
# Embedding model
# ---------------------------------------------------------------------------


def load_model(model_id: str = BGE_M3_MODEL_ID, *, use_flagembedding: bool = False):
    """Load an embedding model.

    When use_flagembedding=True, loads BGEM3FlagModel from FlagEmbedding
    (supports dense + sparse output). Otherwise uses SentenceTransformer
    with ONNX/PyTorch fallback.

    Args:
        model_id: HuggingFace model identifier.
        use_flagembedding: Use FlagEmbedding library for sparse support.

    Returns:
        A model instance (BGEM3FlagModel or SentenceTransformer).
    """
    import torch

    # Use all available CPU cores for PyTorch inference
    cpu_count = os.cpu_count() or 8
    torch.set_num_threads(cpu_count)
    logger.info("Set PyTorch threads to %d", cpu_count)

    if use_flagembedding:
        from FlagEmbedding import BGEM3FlagModel  # type: ignore[import-untyped]

        model = BGEM3FlagModel(model_id, use_fp16=False)
        logger.info("Loaded %s with FlagEmbedding (sparse capable)", model_id)
        return model

    from sentence_transformers import SentenceTransformer  # type: ignore[import-untyped]

    try:
        model = SentenceTransformer(model_id, backend="onnx")
        logger.info("Loaded %s with ONNX backend", model_id)
        return model
    except Exception:
        logger.info("ONNX backend unavailable, falling back to PyTorch for %s", model_id)
        model = SentenceTransformer(model_id)
        logger.info("Loaded %s with PyTorch backend", model_id)
        return model


def encode_texts(
    model,
    texts: list[str],
    batch_size: int = ENCODE_BATCH_SIZE,
    max_length: int = ENCODE_MAX_LENGTH,
) -> np.ndarray:
    """Encode a list of strings into normalized embeddings (SentenceTransformer).

    Args:
        model: A SentenceTransformer model instance.
        texts: List of text strings to encode.
        batch_size: Number of texts per encoding batch.
        max_length: Maximum token length for truncation.

    Returns:
        numpy array of shape ``(N, EMBEDDING_DIM)`` with L2-normalized
        embeddings.
    """
    # Set max sequence length to avoid BGE-M3's default of 8192 tokens
    original_max = getattr(model, "max_seq_length", None)
    model.max_seq_length = max_length
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=False,
        truncate_dim=EMBEDDING_DIM,
    )
    if original_max is not None:
        model.max_seq_length = original_max
    return np.asarray(embeddings, dtype=np.float32)


def encode_texts_flag(
    model,
    texts: list[str],
    batch_size: int = ENCODE_BATCH_SIZE,
    max_length: int = ENCODE_MAX_LENGTH,
) -> tuple[np.ndarray, list[dict[int, float]]]:
    """Encode texts using FlagEmbedding, returning both dense and sparse outputs.

    Args:
        model: A BGEM3FlagModel instance.
        texts: List of text strings to encode.
        batch_size: Number of texts per encoding batch.
        max_length: Maximum token length for truncation.

    Returns:
        Tuple of (dense_embeddings, sparse_weights) where:
        - dense_embeddings: numpy array of shape (N, EMBEDDING_DIM)
        - sparse_weights: list of {token_id: weight} dicts
    """
    output = model.encode(
        texts,
        batch_size=batch_size,
        max_length=max_length,
        return_dense=True,
        return_sparse=True,
        return_colbert_vecs=False,
    )
    dense = np.asarray(output["dense_vecs"], dtype=np.float32)
    sparse = output["lexical_weights"]  # list of {token_id: weight} dicts
    return dense, sparse


# ---------------------------------------------------------------------------
# JSONL iteration
# ---------------------------------------------------------------------------


def _iter_rows_from_jsonl(input_dir: Path) -> Iterator[dict]:
    """Iterate over JSONL files in a directory, yielding parsed rows.

    Skips blank lines and logs a warning to stderr for malformed JSON.

    Args:
        input_dir: Directory containing ``*.jsonl`` files.

    Yields:
        Parsed dict for each valid JSON line.
    """
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


# ---------------------------------------------------------------------------
# Batch insert helpers
# ---------------------------------------------------------------------------


def _insert_dense_batch(
    conn: sqlite3.Connection,
    batch_ids: list[str],
    batch_langs: list[str],
    dense_vecs: np.ndarray,
) -> None:
    """Insert a batch of dense vectors into vec_decisions."""
    rows = [
        (batch_ids[i], serialize_f32(dense_vecs[i].tolist()), batch_langs[i])
        for i in range(len(batch_ids))
    ]
    conn.executemany(
        "INSERT INTO vec_decisions (decision_id, embedding, language) "
        "VALUES (?, ?, ?)",
        rows,
    )


def _insert_sparse_batch(
    conn: sqlite3.Connection,
    batch_ids: list[str],
    sparse_weights: list[dict[int, float]],
) -> None:
    """Insert sparse token weights for a batch into sparse_terms."""
    rows = []
    for i in range(len(batch_ids)):
        did = batch_ids[i]
        for token_id, weight in sparse_weights[i].items():
            if weight > SPARSE_WEIGHT_THRESHOLD:
                rows.append((did, int(token_id), float(weight)))
    if rows:
        conn.executemany(
            "INSERT INTO sparse_terms (decision_id, token_id, weight) "
            "VALUES (?, ?, ?)",
            rows,
        )


def _insert_chunk_batch(
    conn: sqlite3.Connection,
    chunk_ids: list[str],
    decision_ids: list[str],
    chunk_indices: list[int],
    chunk_langs: list[str],
    dense_vecs: np.ndarray,
) -> None:
    """Insert chunk embeddings and metadata."""
    for i in range(len(chunk_ids)):
        conn.execute(
            "INSERT INTO vec_chunks (chunk_id, embedding, language) "
            "VALUES (?, ?, ?)",
            (chunk_ids[i], serialize_f32(dense_vecs[i].tolist()), chunk_langs[i]),
        )
        conn.execute(
            "INSERT INTO chunk_meta (chunk_id, decision_id, chunk_index) "
            "VALUES (?, ?, ?)",
            (chunk_ids[i], decision_ids[i], chunk_indices[i]),
        )


# ---------------------------------------------------------------------------
# Build pipeline
# ---------------------------------------------------------------------------


def build_vectors(
    *,
    input_dir: Path,
    db_path: Path,
    model_id: str = BGE_M3_MODEL_ID,
    batch_size: int = ENCODE_BATCH_SIZE,
    limit: int | None = None,
    enable_sparse: bool = False,
    enable_chunks: bool = False,
    shard_index: int | None = None,
    num_shards: int | None = None,
) -> dict:
    """Build a sqlite-vec database of decision embeddings from JSONL files.

    Steps:
    1. Load the embedding model.
    2. Create the vec DB at a temporary path (``<db_path>.tmp``).
    3. Iterate JSONL rows, select text, batch-encode, and insert.
    4. Atomic rename on success; cleanup temp file on error.

    Args:
        input_dir: Directory containing ``*.jsonl`` decision files.
        db_path: Final output path for the sqlite-vec database.
        model_id: HuggingFace model identifier.
        batch_size: Number of texts per encoding batch.
        limit: Maximum number of decisions to process (None = all).
        enable_sparse: Extract and store sparse (lexical) weights
            using FlagEmbedding. Requires the FlagEmbedding package.
        enable_chunks: Enable chunk-level indexing. Splits full_text
            into sections and embeds each chunk separately.
        shard_index: If set, only process decisions where
            hash(decision_id) % num_shards == shard_index.
        num_shards: Total number of shards for parallel builds.

    Returns:
        Stats dict with keys: ``db_path``, ``embedded``, ``skipped_no_text``,
        ``skipped_dupe``, ``elapsed_seconds``, and optionally ``chunks_embedded``,
        ``sparse_terms_inserted``.
    """
    t0 = time.time()
    input_dir = Path(input_dir)
    db_path = Path(db_path)
    tmp_path = db_path.parent / f".{db_path.name}.tmp"

    use_flagembedding = enable_sparse

    # Sharding: limit torch threads when running multiple workers
    if num_shards and num_shards > 1:
        import torch
        cpu_count = os.cpu_count() or 16
        threads_per_shard = max(1, cpu_count // num_shards)
        torch.set_num_threads(threads_per_shard)
        logger.info(
            "Shard %d/%d: using %d PyTorch threads",
            shard_index, num_shards, threads_per_shard,
        )

    logger.info("Loading model %s (flagembedding=%s) ...", model_id, use_flagembedding)
    model = load_model(model_id, use_flagembedding=use_flagembedding)

    logger.info("Creating vec DB at %s", tmp_path)
    conn = create_vec_db(
        str(tmp_path),
        enable_sparse=enable_sparse,
        enable_chunks=enable_chunks,
    )

    # Import chunker if chunks enabled
    chunk_fn = None
    if enable_chunks:
        from search_stack.chunker import chunk_decision
        chunk_fn = chunk_decision

    embedded = 0
    skipped_no_text = 0
    skipped_dupe = 0
    chunks_embedded = 0
    sparse_terms_inserted = 0
    seen_ids: set[str] = set()

    # Accumulate batch for decision-level embeddings
    batch_ids: list[str] = []
    batch_texts: list[str] = []
    batch_langs: list[str] = []

    # Accumulate batch for chunk-level embeddings
    chunk_batch_ids: list[str] = []
    chunk_batch_decision_ids: list[str] = []
    chunk_batch_indices: list[int] = []
    chunk_batch_texts: list[str] = []
    chunk_batch_langs: list[str] = []

    def _flush_decision_batch() -> int:
        nonlocal embedded, sparse_terms_inserted
        if not batch_ids:
            return 0

        if use_flagembedding:
            dense, sparse = encode_texts_flag(
                model, batch_texts, batch_size=batch_size
            )
            _insert_dense_batch(conn, batch_ids, batch_langs, dense)
            if enable_sparse:
                _insert_sparse_batch(conn, batch_ids, sparse)
                sparse_terms_inserted += sum(
                    sum(1 for w in s.values() if w > SPARSE_WEIGHT_THRESHOLD)
                    for s in sparse
                )
        else:
            vecs = encode_texts(model, batch_texts, batch_size=batch_size)
            _insert_dense_batch(conn, batch_ids, batch_langs, vecs)

        count = len(batch_ids)
        embedded += count
        conn.commit()
        batch_ids.clear()
        batch_texts.clear()
        batch_langs.clear()
        return count

    def _flush_chunk_batch() -> int:
        nonlocal chunks_embedded
        if not chunk_batch_ids:
            return 0

        if use_flagembedding:
            dense, _ = encode_texts_flag(
                model, chunk_batch_texts, batch_size=batch_size
            )
        else:
            dense = encode_texts(
                model, chunk_batch_texts, batch_size=batch_size
            )

        _insert_chunk_batch(
            conn,
            chunk_batch_ids,
            chunk_batch_decision_ids,
            chunk_batch_indices,
            chunk_batch_langs,
            dense,
        )
        count = len(chunk_batch_ids)
        chunks_embedded += count
        conn.commit()
        chunk_batch_ids.clear()
        chunk_batch_decision_ids.clear()
        chunk_batch_indices.clear()
        chunk_batch_texts.clear()
        chunk_batch_langs.clear()
        return count

    try:
        for row in _iter_rows_from_jsonl(input_dir):
            if limit is not None and embedded + len(batch_ids) >= limit:
                break

            decision_id = row.get("decision_id") or ""
            if not decision_id:
                continue

            # Shard filter: skip decisions not assigned to this shard
            if num_shards and num_shards > 1 and shard_index is not None:
                if hash(decision_id) % num_shards != shard_index:
                    continue

            if decision_id in seen_ids:
                skipped_dupe += 1
                continue
            seen_ids.add(decision_id)

            text = _select_text(row)
            if text is None:
                skipped_no_text += 1
                continue

            language = row.get("language") or "de"
            batch_ids.append(decision_id)
            batch_texts.append(text)
            batch_langs.append(language)

            # Chunk-level: embed sections of full_text
            if chunk_fn is not None:
                regeste = row.get("regeste") or ""
                full_text = row.get("full_text") or ""

                chunk_texts: list[str] = []
                # Chunk 0: regeste if available
                if len(regeste) >= 20:
                    chunk_texts.append(regeste)
                # Chunks 1-2: from full_text sections
                if full_text:
                    ft_chunks = chunk_fn(full_text, max_chunks=2, max_chunk_chars=500)
                    chunk_texts.extend(ft_chunks)

                for ci, ct in enumerate(chunk_texts[:3]):
                    chunk_id = f"{decision_id}__chunk_{ci}"
                    chunk_batch_ids.append(chunk_id)
                    chunk_batch_decision_ids.append(decision_id)
                    chunk_batch_indices.append(ci)
                    chunk_batch_texts.append(ct)
                    chunk_batch_langs.append(language)

            if len(batch_ids) >= batch_size:
                _flush_decision_batch()
                # Flush accumulated chunks right after decisions
                _flush_chunk_batch()

                if embedded % 10000 == 0:
                    logger.info(
                        "Progress: %d decisions embedded, %d chunks",
                        embedded, chunks_embedded,
                    )

        # Flush remaining batches
        if batch_ids:
            if limit is not None:
                remaining = limit - embedded
                kept_ids = set(batch_ids[:remaining])
                batch_ids[:] = batch_ids[:remaining]
                batch_texts[:] = batch_texts[:remaining]
                batch_langs[:] = batch_langs[:remaining]

                # Trim chunk batch to only kept decisions (avoid orphaned chunks)
                if chunk_batch_ids and kept_ids:
                    keep = [
                        i for i, did in enumerate(chunk_batch_decision_ids)
                        if did in kept_ids
                    ]
                    chunk_batch_ids[:] = [chunk_batch_ids[i] for i in keep]
                    chunk_batch_decision_ids[:] = [chunk_batch_decision_ids[i] for i in keep]
                    chunk_batch_indices[:] = [chunk_batch_indices[i] for i in keep]
                    chunk_batch_texts[:] = [chunk_batch_texts[i] for i in keep]
                    chunk_batch_langs[:] = [chunk_batch_langs[i] for i in keep]

            _flush_decision_batch()

        _flush_chunk_batch()

        conn.close()
        os.replace(str(tmp_path), str(db_path))
        elapsed = time.time() - t0
        logger.info(
            "Done: %d embedded, %d chunks, %d skipped (no text), %d skipped (dupe) in %.1fs",
            embedded,
            chunks_embedded,
            skipped_no_text,
            skipped_dupe,
            elapsed,
        )

    except Exception:
        conn.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    stats: dict = {
        "db_path": str(db_path),
        "embedded": embedded,
        "skipped_no_text": skipped_no_text,
        "skipped_dupe": skipped_dupe,
        "elapsed_seconds": round(elapsed, 2),
    }
    if enable_chunks:
        stats["chunks_embedded"] = chunks_embedded
    if enable_sparse:
        stats["sparse_terms_inserted"] = sparse_terms_inserted
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for building vector embeddings from JSONL decisions."""
    parser = argparse.ArgumentParser(
        description="Build sqlite-vec embedding database from decision JSONL files",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("output/decisions"),
        help="Directory containing *.jsonl decision files (default: output/decisions)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/vectors.db"),
        help="Output sqlite-vec database path (default: output/vectors.db)",
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
        help="Maximum number of decisions to embed (default: all)",
    )
    parser.add_argument(
        "--enable-sparse",
        action="store_true",
        help="Enable sparse (lexical) weight extraction via FlagEmbedding",
    )
    parser.add_argument(
        "--enable-chunks",
        action="store_true",
        help="Enable chunk-level indexing (embed decision sections separately)",
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=None,
        help="Shard index (0-based) for parallel builds",
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        default=None,
        help="Total number of shards for parallel builds",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    stats = build_vectors(
        input_dir=args.input,
        db_path=args.output,
        model_id=args.model,
        batch_size=args.batch_size,
        limit=args.limit,
        enable_sparse=args.enable_sparse,
        enable_chunks=args.enable_chunks,
        shard_index=args.shard_index,
        num_shards=args.num_shards,
    )
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
