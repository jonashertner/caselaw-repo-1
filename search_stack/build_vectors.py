"""Build and manage vector embeddings for Swiss caselaw semantic search.

This module provides:
- Text selection from decision rows (regeste or full_text fallback)
- sqlite-vec schema creation for cosine-similarity KNN search
- Float32 serialization helpers for sqlite-vec compatibility
"""

from __future__ import annotations

import logging
import sqlite3
import struct

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EMBEDDING_DIM = 1024
"""Dimensionality of sentence-transformer embeddings (BGE-M3 default)."""

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
