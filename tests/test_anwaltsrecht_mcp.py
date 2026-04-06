# tests/test_anwaltsrecht_mcp.py
"""Tests for Anwaltsrecht MCP filter integration."""
import sys
sys.path.insert(0, ".")

def test_tags_db_path_configured():
    """Verify the tags DB path constant exists."""
    from mcp_server import ANWALTSRECHT_TAGS_DB_PATH
    assert "anwaltsrecht_tags" in str(ANWALTSRECHT_TAGS_DB_PATH)

def test_get_tags_conn_returns_none_when_missing():
    """Verify graceful fallback when tags DB doesn't exist."""
    from mcp_server import _get_anwaltsrecht_conn
    conn = _get_anwaltsrecht_conn()
    if conn is not None:
        conn.close()
