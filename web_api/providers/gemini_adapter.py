"""Google Gemini provider adapter."""
from __future__ import annotations

import os
from collections.abc import AsyncIterator

from .base import ProviderBase, ProviderMessage, ProviderResponse, ToolCall, ToolResult


class GeminiProvider(ProviderBase):
    def __init__(self):
        from google import genai
        self._genai = genai
        self.client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
        self.model = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

    async def chat(
        self,
        messages: list[ProviderMessage],
        tools: list[dict] | None = None,
    ) -> ProviderResponse:
        from google.genai import types

        system_text, contents = self._convert_messages(messages)
        kwargs: dict = {"model": self.model, "contents": contents}

        config = self._build_tool_config(tools, types) if tools else types.GenerateContentConfig()
        if system_text:
            config.system_instruction = system_text
        kwargs["config"] = config

        resp = await self.client.aio.models.generate_content(**kwargs)

        content_text = None
        tool_calls = []

        for part in resp.candidates[0].content.parts:
            if part.text:
                content_text = (content_text or "") + part.text
            elif part.function_call:
                fc = part.function_call
                tool_calls.append(ToolCall(
                    id=f"gemini_{fc.name}_{len(tool_calls)}",
                    name=fc.name,
                    arguments=dict(fc.args) if fc.args else {},
                ))

        return ProviderResponse(
            content=content_text,
            tool_calls=tool_calls if tool_calls else None,
            finish_reason="tool_calls" if tool_calls else "stop",
        )

    async def chat_stream(
        self,
        messages: list[ProviderMessage],
        tools: list[dict] | None = None,
    ) -> AsyncIterator[ProviderResponse]:
        from google.genai import types

        system_text, contents = self._convert_messages(messages)
        kwargs: dict = {"model": self.model, "contents": contents}

        config = self._build_tool_config(tools, types) if tools else types.GenerateContentConfig()
        if system_text:
            config.system_instruction = system_text
        kwargs["config"] = config

        tool_calls = []
        async for chunk in await self.client.aio.models.generate_content_stream(**kwargs):
            for part in chunk.candidates[0].content.parts:
                if part.text:
                    yield ProviderResponse(content=part.text)
                elif part.function_call:
                    fc = part.function_call
                    tool_calls.append(ToolCall(
                        id=f"gemini_{fc.name}_{len(tool_calls)}",
                        name=fc.name,
                        arguments=dict(fc.args) if fc.args else {},
                    ))

        if tool_calls:
            yield ProviderResponse(tool_calls=tool_calls, finish_reason="tool_calls")

    def format_tool_result(self, result: ToolResult) -> ProviderMessage:
        return ProviderMessage(
            role="tool",
            content=result.content,
            tool_call_id=result.tool_call_id,
            name=result.name,
        )

    def _build_tool_config(self, tools, types):
        declarations = []
        for t in tools:
            params = t.get("parameters", {})
            # Gemini doesn't accept 'default' in properties
            clean_props = {}
            for k, v in params.get("properties", {}).items():
                prop = {pk: pv for pk, pv in v.items() if pk != "default"}
                clean_props[k] = prop

            declarations.append(types.FunctionDeclaration(
                name=t["name"],
                description=t["description"],
                parameters={
                    "type": "OBJECT",
                    "properties": clean_props,
                    "required": params.get("required", []),
                } if clean_props else None,
            ))
        return types.GenerateContentConfig(
            tools=[types.Tool(function_declarations=declarations)],
        )

    def _convert_messages(self, messages: list[ProviderMessage]) -> tuple[str | None, list]:
        """Convert messages, extracting system prompt separately.

        Returns (system_text, contents) where system_text goes into
        GenerateContentConfig.system_instruction for native handling.
        """
        from google.genai import types

        system_text = None
        contents = []
        for m in messages:
            if m.role == "system":
                # Collect system messages for native system_instruction
                system_text = (system_text or "") + m.content + "\n"
                continue
            elif m.role == "user":
                contents.append(types.Content(
                    role="user",
                    parts=[types.Part.from_text(text=m.content or "")],
                ))
            elif m.role == "assistant":
                parts = []
                if m.content:
                    parts.append(types.Part.from_text(text=m.content))
                if m.tool_calls:
                    for tc in m.tool_calls:
                        parts.append(types.Part.from_function_call(
                            name=tc.name, args=tc.arguments,
                        ))
                if parts:
                    contents.append(types.Content(role="model", parts=parts))
            elif m.role == "tool":
                contents.append(types.Content(
                    role="user",
                    parts=[types.Part.from_function_response(
                        name=m.name or "unknown",
                        response={"result": m.content},
                    )],
                ))
        return system_text, contents
