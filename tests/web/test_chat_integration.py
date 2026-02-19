"""Integration test for /chat endpoint with mocked MCP and provider."""
from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from fastapi.testclient import TestClient

from web_api.providers.base import ProviderMessage, ProviderResponse, ToolCall


def _make_mock_provider():
    """Create a mock provider that does one tool call then returns text.

    Implements chat_stream (the actual endpoint path) as an async generator,
    matching real provider behaviour.
    """
    call_count = {"n": 0}

    async def mock_chat(messages, tools=None):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return ProviderResponse(
                content=None,
                tool_calls=[ToolCall(
                    id="call_1",
                    name="search_decisions",
                    arguments={"query": "Mietrecht"},
                )],
                finish_reason="tool_calls",
            )
        return ProviderResponse(
            content="Found 2 decisions about Mietrecht.",
            tool_calls=None,
            finish_reason="stop",
        )

    async def mock_chat_stream(messages, tools=None):
        resp = await mock_chat(messages, tools=tools)
        yield resp

    def mock_format(result):
        return ProviderMessage(
            role="tool", content=result.content,
            tool_call_id=result.tool_call_id, name=result.name,
        )

    provider = MagicMock()
    provider.chat = mock_chat
    provider.chat_stream = mock_chat_stream
    provider.format_tool_result = mock_format
    return provider


@pytest.fixture
def client():
    """Create a test client with mocked MCP bridge and provider."""
    mock_bridge = AsyncMock()
    mock_bridge.is_running = True
    mock_bridge.call_tool = AsyncMock(return_value=(
        "Found 2 decisions:\n\n"
        "**1. 6B_100/2024** (2024-03-15) [bger] [de]\n"
        "   Title: Mietrecht Kuendigung\n"
        "   Regeste: Kuendigung des Mietverhaeltnisses\n\n"
        "**2. 4A_200/2024** (2024-02-10) [bger] [de]\n"
        "   Title: Mietrecht Mietzins\n"
    ))

    providers_dict = {
        "openai": _make_mock_provider,
        "claude": _make_mock_provider,
        "gemini": _make_mock_provider,
    }

    with patch("web_api.main.get_bridge", return_value=mock_bridge), \
         patch("web_api.main.PROVIDERS", providers_dict):
        from web_api.main import app
        yield TestClient(app)


def test_chat_streams_tool_and_text(client):
    """Test that /chat streams tool calls and then text."""
    resp = client.post("/chat", json={
        "provider": "openai",
        "message": "Search Mietrecht decisions",
    })
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")

    # Parse SSE events
    chunks = []
    for line in resp.text.split("\n"):
        if line.startswith("data: "):
            chunks.append(json.loads(line[6:]))

    types = [c["type"] for c in chunks]
    assert "tool_start" in types
    assert "tool_end" in types
    assert "done" in types

    # Check session_id was returned
    done_chunk = next(c for c in chunks if c["type"] == "done")
    assert done_chunk.get("session_id")


def test_chat_unknown_provider(client):
    """Unknown provider returns 400."""
    resp = client.post("/chat", json={
        "provider": "unknown",
        "message": "test",
    })
    assert resp.status_code == 400


def test_chat_with_filters(client):
    """Test that filters are accepted."""
    resp = client.post("/chat", json={
        "provider": "openai",
        "message": "test",
        "filters": {
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "collapse_duplicates": True,
            "multilingual": False,
        },
    })
    assert resp.status_code == 200
