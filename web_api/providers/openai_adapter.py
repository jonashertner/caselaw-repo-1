"""OpenAI provider adapter."""
from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator

from .base import ProviderBase, ProviderMessage, ProviderResponse, ToolCall, ToolResult


class OpenAIProvider(ProviderBase):
    def __init__(self):
        from openai import AsyncOpenAI
        self.client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        self.model = os.environ.get("OPENAI_MODEL", "gpt-4o")

    async def chat(
        self,
        messages: list[ProviderMessage],
        tools: list[dict] | None = None,
    ) -> ProviderResponse:
        oai_messages = self._convert_messages(messages)
        kwargs: dict = {"model": self.model, "messages": oai_messages}

        if tools:
            kwargs["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": t["name"],
                        "description": t["description"],
                        "parameters": t["parameters"],
                    },
                }
                for t in tools
            ]

        resp = await self.client.chat.completions.create(**kwargs)
        choice = resp.choices[0]
        msg = choice.message

        tool_calls = None
        if msg.tool_calls:
            tool_calls = [
                ToolCall(
                    id=tc.id,
                    name=tc.function.name,
                    arguments=json.loads(tc.function.arguments),
                )
                for tc in msg.tool_calls
            ]

        return ProviderResponse(
            content=msg.content,
            tool_calls=tool_calls,
            finish_reason=choice.finish_reason or "stop",
        )

    async def chat_stream(
        self,
        messages: list[ProviderMessage],
        tools: list[dict] | None = None,
    ) -> AsyncIterator[ProviderResponse]:
        oai_messages = self._convert_messages(messages)
        kwargs: dict = {
            "model": self.model,
            "messages": oai_messages,
            "stream": True,
            "stream_options": {"include_usage": True},
        }

        if tools:
            kwargs["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": t["name"],
                        "description": t["description"],
                        "parameters": t["parameters"],
                    },
                }
                for t in tools
            ]

        stream = await self.client.chat.completions.create(**kwargs)

        # Accumulate tool call fragments across chunks
        tc_accum: dict[int, dict] = {}  # index → {id, name, arguments_json}
        usage_data = None

        async for chunk in stream:
            # Usage-only chunk (no choices) comes at the end with stream_options
            if not chunk.choices and hasattr(chunk, "usage") and chunk.usage:
                usage_data = chunk.usage
                continue

            delta = chunk.choices[0].delta if chunk.choices else None
            if not delta:
                continue

            # Text content
            if delta.content:
                yield ProviderResponse(content=delta.content)

            # Tool call deltas
            if delta.tool_calls:
                for tc_delta in delta.tool_calls:
                    idx = tc_delta.index
                    if idx not in tc_accum:
                        tc_accum[idx] = {"id": "", "name": "", "arguments_json": ""}
                    if tc_delta.id:
                        tc_accum[idx]["id"] = tc_delta.id
                    if tc_delta.function:
                        if tc_delta.function.name:
                            tc_accum[idx]["name"] = tc_delta.function.name
                        if tc_delta.function.arguments:
                            tc_accum[idx]["arguments_json"] += tc_delta.function.arguments

        # Emit token usage
        if usage_data:
            yield ProviderResponse(
                input_tokens=getattr(usage_data, "prompt_tokens", None),
                output_tokens=getattr(usage_data, "completion_tokens", None),
            )

        # Emit accumulated tool calls
        if tc_accum:
            tool_calls = []
            for idx in sorted(tc_accum):
                tc = tc_accum[idx]
                tool_calls.append(ToolCall(
                    id=tc["id"],
                    name=tc["name"],
                    arguments=json.loads(tc["arguments_json"]) if tc["arguments_json"] else {},
                ))
            yield ProviderResponse(tool_calls=tool_calls, finish_reason="tool_calls")

    def format_tool_result(self, result: ToolResult) -> ProviderMessage:
        return ProviderMessage(
            role="tool",
            content=result.content,
            tool_call_id=result.tool_call_id,
            name=result.name,
        )

    def _convert_messages(self, messages: list[ProviderMessage]) -> list[dict]:
        out = []
        for m in messages:
            msg: dict = {"role": m.role, "content": m.content or ""}
            if m.role == "tool":
                msg["tool_call_id"] = m.tool_call_id
                if m.name:
                    msg["name"] = m.name
            elif m.tool_calls:
                msg["tool_calls"] = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": json.dumps(tc.arguments),
                        },
                    }
                    for tc in m.tool_calls
                ]
                msg["content"] = m.content or None
            out.append(msg)
        return out
