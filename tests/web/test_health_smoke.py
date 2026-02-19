"""Smoke tests for health endpoint and startup."""
from __future__ import annotations

import os
import pytest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient


@pytest.fixture
def client():
    with patch("web_api.main.get_bridge") as mock_get:
        mock_bridge = AsyncMock()
        mock_bridge.is_running = True
        mock_get.return_value = mock_bridge
        from web_api.main import app
        yield TestClient(app)


def test_health_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["mcp_running"] is True
    assert "openai" in data["providers"]
    assert "gemini" in data["providers"]
    assert "claude" in data["providers"]


def test_decision_endpoint(client):
    """Test /decision/{id} calls MCP bridge."""
    with patch("web_api.main.get_bridge") as mock_get:
        mock_bridge = AsyncMock()
        mock_bridge.call_tool = AsyncMock(return_value="# 6B_100/2024\nFull text here.")
        mock_get.return_value = mock_bridge

        resp = client.get("/decision/bger_6B_100_2024")
        assert resp.status_code == 200
        data = resp.json()
        assert data["decision_id"] == "bger_6B_100_2024"
        assert "Full text" in data["content"]


def test_sessions_endpoint(client):
    resp = client.get("/sessions")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


def test_db_path_from_env():
    """Verify MCP server path can be configured via env."""
    custom_path = "/custom/path/mcp_server.py"
    with patch.dict(os.environ, {"MCP_SERVER_PATH": custom_path}):
        # Re-import to pick up env
        import importlib
        import web_api.mcp_bridge
        importlib.reload(web_api.mcp_bridge)
        assert web_api.mcp_bridge.MCP_SERVER_PATH == custom_path

        # Restore
        del os.environ["MCP_SERVER_PATH"]
        importlib.reload(web_api.mcp_bridge)


def test_localhost_cors(client):
    """Verify CORS only allows localhost."""
    resp = client.options(
        "/health",
        headers={
            "Origin": "http://evil.com",
            "Access-Control-Request-Method": "GET",
        },
    )
    # Should not have Access-Control-Allow-Origin for evil.com
    assert resp.headers.get("access-control-allow-origin") != "http://evil.com"

    resp = client.options(
        "/health",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert resp.headers.get("access-control-allow-origin") == "http://localhost:5173"
