"""Abstract provider interface for LLM adapters."""
from __future__ import annotations

import abc
from collections.abc import AsyncIterator
from dataclasses import dataclass


@dataclass
class ProviderMessage:
    role: str  # system | user | assistant | tool
    content: str
    tool_call_id: str | None = None
    tool_calls: list[ToolCall] | None = None
    name: str | None = None  # tool name for role=tool


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict


@dataclass
class ToolResult:
    tool_call_id: str
    name: str
    content: str


@dataclass
class ProviderResponse:
    """Non-streaming response from a provider."""
    content: str | None = None
    tool_calls: list[ToolCall] | None = None
    finish_reason: str = "stop"


MCP_TOOLS = [
    {
        "name": "search_decisions",
        "description": (
            "Search Swiss court decisions using full-text search. "
            "Supports keywords, phrases (in quotes), Boolean operators "
            "(AND, OR, NOT), and prefix matching (word*). "
            "Filter by court, canton, language, and date range. "
            "Returns BM25-ranked results with snippets."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "court": {"type": "string", "description": "Filter by court code"},
                "canton": {"type": "string", "description": "Filter by canton (CH, ZH, BE, GE, etc.)"},
                "language": {"type": "string", "description": "Filter by language: de, fr, it, rm", "enum": ["de", "fr", "it", "rm"]},
                "date_from": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                "date_to": {"type": "string", "description": "End date (YYYY-MM-DD)"},
                "chamber": {"type": "string", "description": "Filter by chamber/division"},
                "decision_type": {"type": "string", "description": "Filter by decision type"},
                "limit": {"type": "integer", "description": "Max results (default 50, max 100)", "default": 50},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_decision",
        "description": (
            "Fetch a single court decision with full text. "
            "Look up by decision_id or docket number."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "decision_id": {"type": "string", "description": "Decision ID, docket number, or partial docket"},
            },
            "required": ["decision_id"],
        },
    },
    {
        "name": "list_courts",
        "description": "List all available courts with decision counts.",
        "parameters": {"type": "object", "properties": {}},
    },
    {
        "name": "get_statistics",
        "description": "Get aggregate statistics about the dataset.",
        "parameters": {
            "type": "object",
            "properties": {
                "court": {"type": "string", "description": "Filter by court code"},
                "canton": {"type": "string", "description": "Filter by canton code"},
                "year": {"type": "integer", "description": "Filter by year"},
            },
        },
    },
    {
        "name": "draft_mock_decision",
        "description": (
            "Build a research-only mock decision outline from user facts. "
            "Combines relevant Swiss case law retrieval with statute references."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "facts": {"type": "string", "description": "Detailed facts of the case"},
                "question": {"type": "string", "description": "Optional legal question"},
                "preferred_language": {"type": "string", "enum": ["de", "fr", "it", "rm", "en"]},
                "deciding_court": {"type": "string", "description": "Hypothetical deciding court"},
                "limit": {"type": "integer", "default": 8},
            },
            "required": ["facts"],
        },
    },
]

SYSTEM_PROMPT = (
    "You are a Swiss legal research assistant with access to a database of over 1 million "
    "Swiss court decisions. You can search decisions, retrieve full texts, list courts, "
    "get statistics, and draft mock decision outlines.\n\n"
    "Guidelines:\n"
    "- Always search before answering legal questions — do not rely on general knowledge alone.\n"
    "- Cite decisions by docket number and date.\n"
    "- Respect the user's language — reply in the language they use (DE/FR/IT/EN).\n"
    "- When showing search results, include court, date, docket number, and a brief summary.\n"
    "- For citation chains, follow references between decisions to build a complete picture.\n"
    "- Be precise about jurisdictions (federal vs cantonal) and legal domains.\n"
    "- For broad queries, request up to 100 results to give the user comprehensive coverage.\n"
)


class ProviderBase(abc.ABC):
    """Abstract base for LLM provider adapters."""

    @abc.abstractmethod
    async def chat(
        self,
        messages: list[ProviderMessage],
        tools: list[dict] | None = None,
    ) -> ProviderResponse:
        """Send messages to the LLM and get a response (possibly with tool calls)."""
        ...

    async def chat_stream(
        self,
        messages: list[ProviderMessage],
        tools: list[dict] | None = None,
    ) -> AsyncIterator[ProviderResponse]:
        """Yield ProviderResponse chunks with text deltas and/or tool_calls.

        Default implementation falls back to non-streaming chat().
        Subclasses should override for real token-by-token streaming.
        """
        resp = await self.chat(messages, tools=tools)
        yield resp

    @abc.abstractmethod
    def format_tool_result(self, result: ToolResult) -> ProviderMessage:
        """Format a tool result into the provider's expected message format."""
        ...
