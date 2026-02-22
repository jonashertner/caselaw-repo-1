"""Build and manage vector embeddings for Swiss caselaw semantic search.

This module provides:
- Text selection from decision rows (regeste or full_text fallback)
- sqlite-vec schema creation for cosine-similarity KNN search
- Float32 serialization helpers for sqlite-vec compatibility
- BGE-M3 model loading with ONNX/PyTorch fallback
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

ENCODE_MAX_LENGTH = 512
"""Maximum token length for input texts during encoding."""

ENCODE_BATCH_SIZE = 32
"""Default batch size for encoding texts."""

VEC_TABLE_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS vec_decisions USING vec0(
    decision_id TEXT PRIMARY KEY,
    embedding float[1024] distance_metric=cosine,
    language TEXT partition key
)
""".strip()
"""DDL for the sqlite-vec virtual table storing decision embeddings."""

# ---------------------------------------------------------------------------
# Text selection helper
# ---------------------------------------------------------------------------


def _select_text(row: dict) -> str | None:
    """Choose the best text snippet from a decision row for embedding.

    Priority:
    1. ``regeste`` if present and >= 20 characters
    2. ``full_text`` first 2000 characters if present and non-empty
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
        return full_text[:2000]

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


def create_vec_db(path: str) -> sqlite3.Connection:
    """Create a SQLite database with the sqlite-vec extension and vec_decisions table.

    Args:
        path: Filesystem path for the SQLite database file.

    Returns:
        An open :class:`sqlite3.Connection` with sqlite-vec loaded and
        the ``vec_decisions`` virtual table created.

    Raises:
        RuntimeError: If the sqlite-vec extension cannot be loaded (e.g. on
            macOS stock Python which disables ``enable_load_extension``).
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

    conn.execute(VEC_TABLE_SQL)
    conn.commit()
    logger.info("Created vec_decisions table at %s", path)
    return conn


# ---------------------------------------------------------------------------
# Embedding model
# ---------------------------------------------------------------------------


def load_model(model_id: str = BGE_M3_MODEL_ID):
    """Load a SentenceTransformer model, preferring ONNX backend on CPU.

    Tries the ONNX backend first for faster CPU inference.  Falls back to
    the default PyTorch backend if ONNX is unavailable.

    Args:
        model_id: HuggingFace model identifier.

    Returns:
        A :class:`sentence_transformers.SentenceTransformer` instance.
    """
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
    """Encode a list of strings into normalized embeddings.

    Args:
        model: A SentenceTransformer model instance.
        texts: List of text strings to encode.
        batch_size: Number of texts per encoding batch.
        max_length: Maximum token length for truncation.

    Returns:
        numpy array of shape ``(N, EMBEDDING_DIM)`` with L2-normalized
        embeddings.
    """
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=False,
        truncate_dim=EMBEDDING_DIM,
    )
    return np.asarray(embeddings, dtype=np.float32)


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
# Build pipeline
# ---------------------------------------------------------------------------


def build_vectors(
    *,
    input_dir: Path,
    db_path: Path,
    model_id: str = BGE_M3_MODEL_ID,
    batch_size: int = ENCODE_BATCH_SIZE,
    limit: int | None = None,
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

    Returns:
        Stats dict with keys: ``db_path``, ``embedded``, ``skipped_no_text``,
        ``skipped_dupe``, ``elapsed_seconds``.
    """
    t0 = time.time()
    input_dir = Path(input_dir)
    db_path = Path(db_path)
    tmp_path = db_path.parent / f".{db_path.name}.tmp"

    logger.info("Loading model %s ...", model_id)
    model = load_model(model_id)

    logger.info("Creating vec DB at %s", tmp_path)
    conn = create_vec_db(str(tmp_path))

    embedded = 0
    skipped_no_text = 0
    skipped_dupe = 0
    seen_ids: set[str] = set()

    # Accumulate batch
    batch_ids: list[str] = []
    batch_texts: list[str] = []
    batch_langs: list[str] = []

    try:
        for row in _iter_rows_from_jsonl(input_dir):
            if limit is not None and embedded + len(batch_ids) >= limit:
                break

            decision_id = row.get("decision_id") or ""
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

            language = row.get("language") or "de"
            batch_ids.append(decision_id)
            batch_texts.append(text)
            batch_langs.append(language)

            if len(batch_ids) >= batch_size:
                vecs = encode_texts(model, batch_texts, batch_size=batch_size)
                for i in range(len(batch_ids)):
                    conn.execute(
                        "INSERT INTO vec_decisions (decision_id, embedding, language) "
                        "VALUES (?, ?, ?)",
                        (batch_ids[i], serialize_f32(vecs[i].tolist()), batch_langs[i]),
                    )
                conn.commit()
                embedded += len(batch_ids)
                batch_ids.clear()
                batch_texts.clear()
                batch_langs.clear()

                if embedded % 10000 == 0:
                    logger.info("Progress: %d decisions embedded", embedded)

        # Flush remaining batch
        if batch_ids:
            if limit is not None:
                remaining = limit - embedded
                batch_ids = batch_ids[:remaining]
                batch_texts = batch_texts[:remaining]
                batch_langs = batch_langs[:remaining]

            if batch_ids:
                vecs = encode_texts(model, batch_texts, batch_size=batch_size)
                for i in range(len(batch_ids)):
                    conn.execute(
                        "INSERT INTO vec_decisions (decision_id, embedding, language) "
                        "VALUES (?, ?, ?)",
                        (batch_ids[i], serialize_f32(vecs[i].tolist()), batch_langs[i]),
                    )
                conn.commit()
                embedded += len(batch_ids)

        conn.close()
        os.replace(str(tmp_path), str(db_path))
        elapsed = time.time() - t0
        logger.info(
            "Done: %d embedded, %d skipped (no text), %d skipped (dupe) in %.1fs",
            embedded,
            skipped_no_text,
            skipped_dupe,
            elapsed,
        )

    except Exception:
        conn.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    return {
        "db_path": str(db_path),
        "embedded": embedded,
        "skipped_no_text": skipped_no_text,
        "skipped_dupe": skipped_dupe,
        "elapsed_seconds": round(elapsed, 2),
    }


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
    )
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
