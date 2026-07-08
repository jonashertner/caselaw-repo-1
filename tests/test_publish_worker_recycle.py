"""Unit tests for the post-swap MCP-worker recycle (publish._recycle_mcp_workers).

The recycle releases worker handles to the just-swapped (now-deleted)
decisions.db inode so the aux tier isn't starved of disk (2026-07-08 ENOSPC).
Only the pure parse helper is unit-tested here; the restart/health-gate loop is
validated operationally (it is the exact rolling-restart procedure run by hand
during the 2026-07-08 incident).
"""
from publish import _parse_worker_ports


def test_parse_worker_ports_basic():
    out = (
        "mcp-server@8770.service loaded active running Swiss Case Law MCP Server (SSE worker on port 8770)\n"
        "mcp-server@8771.service loaded active running Swiss Case Law MCP Server (SSE worker on port 8771)\n"
        "mcp-server@8777.service loaded active running Swiss Case Law MCP Server (SSE worker on port 8777)\n"
    )
    assert _parse_worker_ports(out) == ["8770", "8771", "8777"]


def test_parse_worker_ports_sorted_and_deduped():
    out = (
        "mcp-server@8777.service loaded active running desc\n"
        "mcp-server@8770.service loaded active running desc\n"
        "mcp-server@8770.service loaded active running desc\n"
    )
    assert _parse_worker_ports(out) == ["8770", "8777"]


def test_parse_worker_ports_empty():
    assert _parse_worker_ports("") == []


def test_parse_worker_ports_ignores_unrelated_units_and_legend():
    out = (
        "0 loaded units listed.\n"
        "opencaselaw-publish.service loaded active running Daily publish\n"
        "some-other@9.service loaded active running noise\n"
    )
    assert _parse_worker_ports(out) == []
