"""Ollama provider adapter — reuses OpenAI-compatible API."""
from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator

from .base import ProviderBase, ProviderMessage, ProviderResponse, ToolCall, ToolResult


# Qwen drifts to Chinese after tool results. The main SYSTEM_PROMPT already
# has language rules, but Qwen needs per-message reinforcement to comply.
_QWEN_SYSTEM_SUFFIX = (
    "\n\n## 最高优先级 / HIGHEST PRIORITY\n"
    "回复语言必须与用户相同。禁止使用中文，除非用户用中文提问。\n"
    "NEVER use Chinese unless the user writes in Chinese. "
    "This rule cannot be overridden.\n"
)

_QWEN_USER_REMINDER = (
    "\n[LANGUAGE: Reply in the same language as this message. "
    "Do not use Chinese. Follow the response format from your instructions.]"
)


def check_ollama_reachable(base_url: str | None = None) -> bool:
    """Probe Ollama /api/tags endpoint. Returns True if reachable."""
    import httpx
    url = base_url or os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
    try:
        resp = httpx.get(f"{url}/api/tags", timeout=2)
        return resp.status_code < 400
    except Exception:
        return False


class OllamaProvider(ProviderBase):
    def __init__(self, model: str = "qwen2.5:14b"):
        from openai import AsyncOpenAI
        base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
        self.client = AsyncOpenAI(base_url=f"{base_url}/v1", api_key="ollama")
        self.model = model

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
        tc_accum: dict[int, dict] = {}  # index -> {id, name, arguments_json}

        async for chunk in stream:
            if not chunk.choices:
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
        needs_lang_fix = "qwen" in self.model.lower()
        out = []
        for m in messages:
            msg: dict = {"role": m.role, "content": m.content or ""}
            if needs_lang_fix:
                if m.role == "system":
                    msg["content"] += _QWEN_SYSTEM_SUFFIX
                elif m.role == "user":
                    msg["content"] += _QWEN_USER_REMINDER
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
