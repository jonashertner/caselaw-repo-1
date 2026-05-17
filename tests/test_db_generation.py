"""Tests for the db_generation cache-invalidation contract.

See docs/db_contract.md. The contract is:

1. Writers (build_fts5.py, quick_publish.py) set PRAGMA user_version
   to int(time.time()) after the final durable write and before the
   atomic swap.
2. MCP get_db() reads PRAGMA user_version on each call, and clears
   the module-level _query_cache when the value differs from what
   it last saw.
3. /health exposes the last-seen generation via get_db_generation().

These tests don't run the full build / quick_publish pipelines —
that would take hours. They cover the contract surface: the PRAGMA
is bumped at the right point in the writer code, and the reader
correctly clears its cache on transition.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
import time
from pathlib import Path

import pytest

REPO_DIR = Path(__file__).resolve().parents[1]
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))


def _make_tiny_db(path: Path, user_version: int = 0) -> None:
    """Create a minimal decisions.db schema compatible with mcp_server.get_db()."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, court TEXT)"
    )
    conn.execute(
        "INSERT INTO decisions VALUES ('test_1', 'BGer')"
    )
    conn.execute(f"PRAGMA user_version = {user_version}")
    conn.commit()
    conn.close()


def test_pragma_user_version_persists_across_open():
    """Writing user_version on one connection survives close + reopen.

    This is the load-bearing SQLite property the contract depends on.
    If this ever changes (e.g. WAL+immutable interaction), the entire
    db_generation contract breaks.
    """
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "decisions.db"
        _make_tiny_db(p, user_version=0)

        # Write a new value
        c = sqlite3.connect(str(p))
        c.execute("PRAGMA user_version = 1747500000")
        c.close()

        # Read it back via a fresh immutable=1 connection (matches MCP)
        c2 = sqlite3.connect(f"file:{p}?immutable=1", uri=True)
        gen = c2.execute("PRAGMA user_version").fetchone()[0]
        c2.close()

        assert gen == 1747500000


def test_get_db_clears_cache_on_generation_change(monkeypatch):
    """Importing mcp_server and calling get_db() twice with a swapped
    DB clears _query_cache between the two calls.
    """
    import mcp_server

    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "decisions.db"
        _make_tiny_db(p, user_version=100)

        # Point mcp_server at this tiny DB
        monkeypatch.setattr(mcp_server, "DB_PATH", p)
        # Force a known starting state
        mcp_server._last_seen_db_generation = 0
        mcp_server._query_cache.clear()
        mcp_server._query_cache[("decoy",)] = "should_be_cleared"

        # First get_db() — sees transition 0 → 100, clears cache
        c = mcp_server.get_db()
        c.close()
        assert mcp_server._last_seen_db_generation == 100
        assert ("decoy",) not in mcp_server._query_cache

        # Re-seed the cache
        mcp_server._query_cache[("decoy2",)] = "should_be_cleared_again"

        # Second get_db() — generation unchanged, cache untouched
        c = mcp_server.get_db()
        c.close()
        assert mcp_server._query_cache.get(("decoy2",)) == "should_be_cleared_again"

        # Simulate a swap: rewrite the DB with a new user_version
        p.unlink()
        _make_tiny_db(p, user_version=200)

        # Third get_db() — sees transition 100 → 200, clears cache
        c = mcp_server.get_db()
        c.close()
        assert mcp_server._last_seen_db_generation == 200
        assert ("decoy2",) not in mcp_server._query_cache


def test_get_db_generation_returns_last_seen(monkeypatch):
    """get_db_generation() reflects the most recent get_db() observation."""
    import mcp_server

    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "decisions.db"
        _make_tiny_db(p, user_version=12345)
        monkeypatch.setattr(mcp_server, "DB_PATH", p)
        mcp_server._last_seen_db_generation = 0

        # Before any get_db(): 0
        assert mcp_server.get_db_generation() == 0

        # After get_db(): matches on-disk
        c = mcp_server.get_db()
        c.close()
        assert mcp_server.get_db_generation() == 12345


def test_build_fts5_swap_block_calls_user_version():
    """Static check: the build_fts5.py swap block contains a
    `PRAGMA user_version = ` write between journal_mode=DELETE and
    conn.close().

    This is a regression guard against accidentally removing the bump,
    which would silently break MCP cache invalidation.
    """
    src = (REPO_DIR / "build_fts5.py").read_text()
    # Find the journal_mode=DELETE line and the next conn.close()
    delete_idx = src.find('PRAGMA journal_mode=DELETE")')
    assert delete_idx >= 0, "journal_mode=DELETE site not found"

    close_idx = src.find("conn.close()", delete_idx)
    assert close_idx > delete_idx, "conn.close() after journal_mode=DELETE not found"

    block = src[delete_idx:close_idx]
    assert "PRAGMA user_version" in block, (
        "PRAGMA user_version bump missing between "
        "journal_mode=DELETE and conn.close() in build_fts5.py — "
        "see docs/db_contract.md"
    )


def test_quick_publish_calls_user_version_before_swap():
    """Static check: scripts/quick_publish.py sets user_version between
    conn.commit() and conn.close() in the insert path.
    """
    src = (REPO_DIR / "scripts" / "quick_publish.py").read_text()
    commit_idx = src.find("conn.commit()")
    assert commit_idx >= 0, "conn.commit() not found"

    close_idx = src.find("conn.close()", commit_idx)
    assert close_idx > commit_idx, "conn.close() after commit not found"

    block = src[commit_idx:close_idx]
    assert "PRAGMA user_version" in block, (
        "PRAGMA user_version bump missing between "
        "conn.commit() and conn.close() in quick_publish.py — "
        "see docs/db_contract.md"
    )
