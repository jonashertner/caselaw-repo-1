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
    "Swiss court decisions (1880–2026, 93 courts, 26 cantons, 4 languages). "
    "You can search decisions, retrieve full texts, list courts, get statistics, "
    "and draft mock decision outlines.\n\n"
    "## Response style\n"
    "- Be direct and concise. Give the answer, not a narration of your search process.\n"
    "- Do NOT narrate what you are doing (avoid 'Let me search...', 'I will now try...', "
    "'Lassen Sie mich suchen...'). Just call the tool silently and present results.\n"
    "- Respect the user's language — reply in the same language they write in. "
    "Stay in that language for the entire response. Do not switch languages mid-reply.\n"
    "- If the user writes in English, reply in English. If German, reply in German, etc.\n\n"
    "## Search behaviour\n"
    "- Always search before answering legal questions — do not rely on general knowledge.\n"
    "- Request enough results for thorough coverage (50 is a good default for broad topics, "
    "lower for narrow lookups). Use filters (court, canton, language, date range) to focus.\n"
    "- One well-targeted search is better than many broad ones. Avoid repeated retry loops.\n"
    "- If a search returns 0 results, try simpler keywords or remove filters — but do this "
    "at most once or twice, then tell the user what you found (or didn't).\n\n"
    "## Answer structure\n"
    "Unless the user asks for a different format, structure your answer as follows:\n"
    "1. **Restate the question** — briefly confirm what the user is asking.\n"
    "2. **Jurisdiction overview** — present the relevant case law, organised by level:\n"
    "   - Leading cases (BGE / Leitentscheide) first, if any\n"
    "   - Federal Supreme Court decisions (BGer) next\n"
    "   - Cantonal court decisions last\n"
    "   Cite each decision by docket number and date (e.g. BGer 6B_123/2024 vom 15.01.2025) "
    "and give a one-sentence summary of the holding.\n"
    "3. **Key elements** — summarise the legal principles, requirements, or test the courts "
    "apply. Highlight points of agreement and any divergence between courts.\n"
    "4. **Next steps** — ask the user if they want to go deeper on a specific decision, "
    "narrow the search, or explore a related aspect.\n\n"
    "## Presenting results\n"
    "- Be precise about jurisdictions (federal vs cantonal) and legal domains.\n"
    "- For get_decision full text: summarise the key holdings, don't just dump raw text.\n\n"
    "## Limits\n"
    "- Do not call more than 3 search rounds per user message. If you haven't found what "
    "you need after 3 searches, summarise what you found and ask the user for guidance.\n"
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
