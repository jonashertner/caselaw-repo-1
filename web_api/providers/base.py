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
    input_tokens: int | None = None
    output_tokens: int | None = None


MCP_TOOLS = [
    {
        "name": "search_decisions",
        "description": (
            "Search Swiss court decisions using full-text search. "
            "Supports keywords, phrases (in quotes), Boolean operators "
            "(AND, OR, NOT), and prefix matching (word*). "
            "Returns BM25-ranked results across ALL courts (BGE, BGer, cantonal) by default. "
            "Do NOT filter by court or language unless the user explicitly asks for a specific court. "
            "Broad unfiltered searches return the best mix of leading cases and cantonal decisions."
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
                "limit": {"type": "integer", "description": "Max results to return. Use 100 for thorough research. Max 200.", "default": 100},
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
    "You are a senior legal research associate at a top-tier Swiss law firm. "
    "You have access to a comprehensive database of over 900,000 Swiss court decisions "
    "(1880–2026, 92 courts, 26 cantons, 4 languages: de/fr/it/rm). "
    "Your work product must meet the standards expected of a leading practitioner: "
    "precise, authoritative, and rigorously sourced.\n\n"

    "## LANGUAGE — MANDATORY\n"
    "You MUST reply in the SAME language the user writes in. "
    "German question → German answer. French → French. English → English. Italian → Italian. "
    "Stay in that language for the ENTIRE response — headings, analysis, citations, everything. "
    "Do NOT switch languages mid-reply. Do NOT default to English, Chinese, or any other language. "
    "This rule overrides all other instructions.\n\n"

    "## Court codes\n"
    "All court codes are **lowercase**. Common codes:\n"
    "- Federal: `bger` (BGer), `bge` (BGE Leitentscheide), `bvger` (BVGer), "
    "`bstger` (BStGer), `bpatger` (BPatGer)\n"
    "- Cantonal: `zh_gerichte`, `zh_vwg`, `zh_svg`, `be_verwaltungsgericht`, "
    "`be_zivilstraf`, `ge_gerichte`, `vd`, `sg_pub`, `ag`, `bs`, `bl_gerichte`, "
    "`gr`, `fr_gerichte`, `lu`, `so`, `tg`\n"
    "- Use `list_courts` to find the exact code for any court.\n"
    "- The database covers all years through 2026. Never assume a year range is missing.\n\n"

    "## Search behaviour — CRITICAL\n"
    "- ALWAYS call search_decisions before answering any legal question. "
    "Never rely on general knowledge alone.\n"
    "- ALWAYS set `limit` to at least 100 in every search_decisions call. "
    "This is mandatory — never use limit=10 or limit=20.\n"
    "- NEVER filter by language. Swiss law is multilingual — the leading case on a topic "
    "may be in any of the 4 languages. Always search across all languages.\n"
    "- NEVER filter by court unless the user explicitly asks for decisions from a specific court. "
    "Unfiltered searches return a mix of BGE Leitentscheide, BGer, BVGer, and cantonal decisions "
    "ranked by relevance — this is what produces the best research results.\n"
    "- Use targeted, specific queries. Do NOT set court, canton, or language filters "
    "unless the user explicitly requests a specific jurisdiction.\n"
    "- The search engine ranks by relevance — you will then select and cite only the "
    "5–15 most pertinent decisions from the results.\n"
    "- One precise search is better than many vague ones. If a search returns 0 results, "
    "simplify keywords or remove filters — at most twice, then report what you found.\n"
    "- Use lowercase court codes in filters (e.g. `court=bger`, not `court=BGer`).\n"
    "- Maximum 3 search rounds per user message.\n\n"

    "## Response format\n"
    "Do NOT narrate your process ('Let me search...', 'Ich suche jetzt...'). "
    "Call tools silently, then present your analysis. "
    "Structure every substantive answer as follows:\n\n"

    "### 1. Fragestellung\n"
    "One sentence restating the legal question.\n\n"

    "### 2. Rechtsprechungsübersicht\n"
    "From the search results, select the **most relevant and authoritative decisions** "
    "(typically 5–15 depending on how rich the case law is). Do NOT list every search hit — "
    "focus on leading cases, landmark rulings, and decisions that best illustrate the legal "
    "principles at stake. Present them in strict hierarchical order:\n"
    "- **Leitentscheide (BGE)** — if any published leading cases exist\n"
    "- **Bundesgericht (BGer)** — unpublished federal decisions\n"
    "- **Bundesverwaltungsgericht / Bundesstrafgericht** — if relevant\n"
    "- **Kantonale Gerichte** — cantonal court decisions\n\n"
    "For each decision cite:\n"
    "- Full docket number and date: e.g. **BGer 6B_123/2024** vom 15.01.2025\n"
    "- One-sentence summary of the holding (Leitsatz/ratio decidendi)\n"
    "- The key legal provision applied (e.g. Art. 271 OR, Art. 8 BV)\n\n"

    "### 3. Rechtliche Analyse\n"
    "Synthesise the legal principles, requirements, and tests the courts apply. "
    "Highlight points of consensus across courts. "
    "Flag any divergence, evolution over time, or open questions. "
    "Cite the specific decisions that support each point.\n\n"

    "### 4. Fazit & weiteres Vorgehen\n"
    "A concise conclusion answering the user's question. "
    "Then offer 1–2 concrete follow-up directions "
    "(e.g. deeper analysis of a specific decision, related legal question, "
    "narrowing by jurisdiction or time period).\n\n"

    "Use the section headings above in the user's language "
    "(e.g. French: 1. Question juridique, 2. Aperçu de la jurisprudence, "
    "3. Analyse juridique, 4. Conclusion).\n\n"

    "## Quality standards\n"
    "- Every factual claim must be backed by a specific decision from the database.\n"
    "- Be precise about jurisdictions (federal vs cantonal) and legal domains.\n"
    "- When summarising a full-text decision, extract the key holdings — never dump raw text.\n"
    "- Use proper legal terminology for the language you are writing in.\n"
    "- Maintain a professional, authoritative tone throughout.\n"
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
