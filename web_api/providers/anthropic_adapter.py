"""Anthropic Claude provider adapter."""
from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator

from .base import ProviderBase, ProviderMessage, ProviderResponse, ToolCall, ToolResult

logger = logging.getLogger("web_api.anthropic")


class AnthropicProvider(ProviderBase):
    def __init__(self):
        import anthropic
        self.client = anthropic.AsyncAnthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        self.model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-5-20250929")

    async def chat(
        self,
        messages: list[ProviderMessage],
        tools: list[dict] | None = None,
    ) -> ProviderResponse:
        system_text, api_messages = self._convert_messages(messages)
        kwargs: dict = {
            "model": self.model,
            "max_tokens": 4096,
            "messages": api_messages,
        }
        if system_text:
            kwargs["system"] = system_text

        if tools:
            kwargs["tools"] = [
                {
                    "name": t["name"],
                    "description": t["description"],
                    "input_schema": t["parameters"],
                }
                for t in tools
            ]

        resp = await self.client.messages.create(**kwargs)

        content_text = None
        tool_calls = []

        for block in resp.content:
            if block.type == "text":
                content_text = (content_text or "") + block.text
            elif block.type == "tool_use":
                tool_calls.append(ToolCall(
                    id=block.id,
                    name=block.name,
                    arguments=block.input if isinstance(block.input, dict) else {},
                ))

        finish = "tool_calls" if resp.stop_reason == "tool_use" else "stop"
        return ProviderResponse(
            content=content_text,
            tool_calls=tool_calls if tool_calls else None,
            finish_reason=finish,
        )

    async def chat_stream(
        self,
        messages: list[ProviderMessage],
        tools: list[dict] | None = None,
    ) -> AsyncIterator[ProviderResponse]:
        system_text, api_messages = self._convert_messages(messages)
        kwargs: dict = {
            "model": self.model,
            "max_tokens": 4096,
            "messages": api_messages,
        }
        if system_text:
            kwargs["system"] = system_text
        if tools:
            kwargs["tools"] = [
                {
                    "name": t["name"],
                    "description": t["description"],
                    "input_schema": t["parameters"],
                }
                for t in tools
            ]

        async with self.client.messages.stream(**kwargs) as stream:
            async for event in stream:
                if event.type == "content_block_delta":
                    if hasattr(event.delta, "text"):
                        yield ProviderResponse(content=event.delta.text)

            # get_final_message() is available after stream is fully consumed
            final = await stream.get_final_message()

        tool_calls = []
        for block in final.content:
            if block.type == "tool_use":
                tool_calls.append(ToolCall(
                    id=block.id,
                    name=block.name,
                    arguments=block.input if isinstance(block.input, dict) else {},
                ))

        # Yield token usage from the final message
        if hasattr(final, "usage") and final.usage:
            yield ProviderResponse(
                input_tokens=final.usage.input_tokens,
                output_tokens=final.usage.output_tokens,
            )

        if tool_calls:
            yield ProviderResponse(
                tool_calls=tool_calls,
                finish_reason="tool_calls",
            )

    def format_tool_result(self, result: ToolResult) -> ProviderMessage:
        return ProviderMessage(
            role="tool",
            content=result.content,
            tool_call_id=result.tool_call_id,
            name=result.name,
        )

    def _convert_messages(self, messages: list[ProviderMessage]) -> tuple[str, list[dict]]:
        system_text = ""
        api_messages = []

        for m in messages:
            if m.role == "system":
                system_text += m.content + "\n"
            elif m.role == "user":
                api_messages.append({"role": "user", "content": m.content or ""})
            elif m.role == "assistant":
                content_blocks = []
                if m.content:
                    content_blocks.append({"type": "text", "text": m.content})
                if m.tool_calls:
                    for tc in m.tool_calls:
                        content_blocks.append({
                            "type": "tool_use",
                            "id": tc.id,
                            "name": tc.name,
                            "input": tc.arguments,
                        })
                if content_blocks:
                    api_messages.append({"role": "assistant", "content": content_blocks})
            elif m.role == "tool":
                # Anthropic expects tool results in a user message
                api_messages.append({
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": m.tool_call_id,
                        "content": m.content,
                    }],
                })

        return system_text.strip(), api_messages
