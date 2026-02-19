"""Unit tests for provider adapters with mocked APIs."""
from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from web_api.providers.base import ProviderMessage, ToolResult, MCP_TOOLS


# ── OpenAI ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_openai_chat_text_response():
    with patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}):
        with patch("openai.AsyncOpenAI") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client

            # Mock response
            mock_message = MagicMock()
            mock_message.content = "Here are the results."
            mock_message.tool_calls = None
            mock_choice = MagicMock()
            mock_choice.message = mock_message
            mock_choice.finish_reason = "stop"
            mock_resp = MagicMock()
            mock_resp.choices = [mock_choice]

            mock_client.chat = MagicMock()
            mock_client.chat.completions = MagicMock()
            mock_client.chat.completions.create = AsyncMock(return_value=mock_resp)

            from web_api.providers.openai_adapter import OpenAIProvider
            provider = OpenAIProvider()

            messages = [ProviderMessage(role="user", content="Search for BGer decisions")]
            resp = await provider.chat(messages, tools=MCP_TOOLS)

            assert resp.content == "Here are the results."
            assert resp.tool_calls is None
            assert resp.finish_reason == "stop"


@pytest.mark.asyncio
async def test_openai_chat_tool_call():
    with patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}):
        with patch("openai.AsyncOpenAI") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client

            mock_tc = MagicMock()
            mock_tc.id = "call_123"
            mock_tc.function.name = "search_decisions"
            mock_tc.function.arguments = json.dumps({"query": "Mietrecht"})

            mock_message = MagicMock()
            mock_message.content = None
            mock_message.tool_calls = [mock_tc]
            mock_choice = MagicMock()
            mock_choice.message = mock_message
            mock_choice.finish_reason = "tool_calls"
            mock_resp = MagicMock()
            mock_resp.choices = [mock_choice]

            mock_client.chat = MagicMock()
            mock_client.chat.completions = MagicMock()
            mock_client.chat.completions.create = AsyncMock(return_value=mock_resp)

            from web_api.providers.openai_adapter import OpenAIProvider
            provider = OpenAIProvider()

            resp = await provider.chat(
                [ProviderMessage(role="user", content="Search Mietrecht")],
                tools=MCP_TOOLS,
            )
            assert resp.tool_calls is not None
            assert len(resp.tool_calls) == 1
            assert resp.tool_calls[0].name == "search_decisions"
            assert resp.tool_calls[0].arguments == {"query": "Mietrecht"}


@pytest.mark.asyncio
async def test_openai_format_tool_result():
    with patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}):
        with patch("openai.AsyncOpenAI"):
            from web_api.providers.openai_adapter import OpenAIProvider
            provider = OpenAIProvider()
            msg = provider.format_tool_result(ToolResult(
                tool_call_id="call_1", name="search_decisions", content="Found 5 results",
            ))
            assert msg.role == "tool"
            assert msg.tool_call_id == "call_1"
            assert msg.content == "Found 5 results"


# ── Anthropic ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_anthropic_chat_text_response():
    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
        with patch("anthropic.AsyncAnthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client

            mock_block = MagicMock()
            mock_block.type = "text"
            mock_block.text = "Swiss law answer."
            mock_resp = MagicMock()
            mock_resp.content = [mock_block]
            mock_resp.stop_reason = "end_turn"

            mock_client.messages = MagicMock()
            mock_client.messages.create = AsyncMock(return_value=mock_resp)

            from web_api.providers.anthropic_adapter import AnthropicProvider
            provider = AnthropicProvider()

            resp = await provider.chat(
                [ProviderMessage(role="user", content="Test")],
                tools=MCP_TOOLS,
            )
            assert resp.content == "Swiss law answer."
            assert resp.tool_calls is None


@pytest.mark.asyncio
async def test_anthropic_chat_tool_call():
    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
        with patch("anthropic.AsyncAnthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client

            mock_text = MagicMock()
            mock_text.type = "text"
            mock_text.text = "Let me search."
            mock_tool = MagicMock()
            mock_tool.type = "tool_use"
            mock_tool.id = "toolu_123"
            mock_tool.name = "search_decisions"
            mock_tool.input = {"query": "Art. 8 BV"}

            mock_resp = MagicMock()
            mock_resp.content = [mock_text, mock_tool]
            mock_resp.stop_reason = "tool_use"

            mock_client.messages = MagicMock()
            mock_client.messages.create = AsyncMock(return_value=mock_resp)

            from web_api.providers.anthropic_adapter import AnthropicProvider
            provider = AnthropicProvider()

            resp = await provider.chat(
                [ProviderMessage(role="user", content="Art. 8 BV")],
                tools=MCP_TOOLS,
            )
            assert resp.content == "Let me search."
            assert len(resp.tool_calls) == 1
            assert resp.tool_calls[0].name == "search_decisions"


@pytest.mark.asyncio
async def test_anthropic_system_message_extraction():
    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
        with patch("anthropic.AsyncAnthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client

            mock_block = MagicMock()
            mock_block.type = "text"
            mock_block.text = "OK"
            mock_resp = MagicMock()
            mock_resp.content = [mock_block]
            mock_resp.stop_reason = "end_turn"

            mock_client.messages = MagicMock()
            mock_client.messages.create = AsyncMock(return_value=mock_resp)

            from web_api.providers.anthropic_adapter import AnthropicProvider
            provider = AnthropicProvider()

            messages = [
                ProviderMessage(role="system", content="You are a legal assistant."),
                ProviderMessage(role="user", content="Hello"),
            ]
            await provider.chat(messages, tools=MCP_TOOLS)

            # Verify system was extracted and passed separately
            call_kwargs = mock_client.messages.create.call_args[1]
            assert call_kwargs["system"] == "You are a legal assistant."
            assert all(m["role"] != "system" for m in call_kwargs["messages"])


# ── Gemini ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_gemini_chat_text_response():
    with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
        with patch("google.genai.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client

            mock_part = MagicMock()
            mock_part.text = "Gemini answer."
            mock_part.function_call = None
            mock_candidate = MagicMock()
            mock_candidate.content.parts = [mock_part]
            mock_resp = MagicMock()
            mock_resp.candidates = [mock_candidate]

            mock_client.aio = MagicMock()
            mock_client.aio.models = MagicMock()
            mock_client.aio.models.generate_content = AsyncMock(return_value=mock_resp)

            from web_api.providers.gemini_adapter import GeminiProvider
            provider = GeminiProvider()

            resp = await provider.chat(
                [ProviderMessage(role="user", content="Test")],
                tools=MCP_TOOLS,
            )
            assert resp.content == "Gemini answer."
            assert resp.tool_calls is None


@pytest.mark.asyncio
async def test_gemini_chat_tool_call():
    with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
        with patch("google.genai.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client

            mock_fc = MagicMock()
            mock_fc.name = "search_decisions"
            mock_fc.args = {"query": "Arbeitsrecht"}

            mock_part = MagicMock()
            mock_part.text = None
            mock_part.function_call = mock_fc
            mock_candidate = MagicMock()
            mock_candidate.content.parts = [mock_part]
            mock_resp = MagicMock()
            mock_resp.candidates = [mock_candidate]

            mock_client.aio = MagicMock()
            mock_client.aio.models = MagicMock()
            mock_client.aio.models.generate_content = AsyncMock(return_value=mock_resp)

            from web_api.providers.gemini_adapter import GeminiProvider
            provider = GeminiProvider()

            resp = await provider.chat(
                [ProviderMessage(role="user", content="Arbeitsrecht")],
                tools=MCP_TOOLS,
            )
            assert resp.tool_calls is not None
            assert resp.tool_calls[0].name == "search_decisions"
            assert resp.tool_calls[0].arguments == {"query": "Arbeitsrecht"}
