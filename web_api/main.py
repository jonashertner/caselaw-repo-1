"""FastAPI backend for Swiss Case Law web UI.

Binds to 127.0.0.1 only. CORS allows localhost only.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from pathlib import Path

import dotenv

_ENV_PATH = Path(__file__).parent.parent / ".env"
dotenv.load_dotenv(_ENV_PATH, override=True)

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from .models import ChatRequest, ChatChunk, ToolTrace, SetKeyRequest
from .mcp_bridge import get_bridge
from .providers import PROVIDERS
from .providers.base import (
    ProviderMessage, ToolCall, ToolResult, MCP_TOOLS, SYSTEM_PROMPT,
)

from search_stack.reference_extraction import extract_case_citations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("web_api")

app = FastAPI(title="Swiss Case Law Chat", version="1.0.0")

FRONTEND_PORT = os.environ.get("FRONTEND_PORT", "5173")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:" + FRONTEND_PORT,
        "http://127.0.0.1:" + FRONTEND_PORT,
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory session store (lightweight, not persisted)
_sessions: dict[str, dict] = {}  # sid → {messages, last_used}
MAX_SESSIONS = 50

MAX_TOOL_ROUNDS = 5

# Provider → env-var mapping for key management
ENV_KEY_MAP = {
    "claude": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "gemini": "GEMINI_API_KEY",
}

SDK_IMPORT_MAP = {
    "claude": "anthropic",
    "openai": "openai",
    "gemini": "google.genai",
}


def _check_sdk(module_name: str) -> bool:
    """Check if a Python SDK is importable."""
    import importlib
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False


def _mask_key(key: str) -> str | None:
    """Show first 6 + last 2 chars of a key."""
    if not key:
        return None
    if len(key) <= 8:
        return key[:2] + "..." + key[-1:]
    return key[:6] + "..." + key[-2:]


def _provider_status() -> dict:
    """Return per-provider configuration status."""
    status = {}
    for provider, env_var in ENV_KEY_MAP.items():
        key_val = os.environ.get(env_var, "")
        sdk_mod = SDK_IMPORT_MAP[provider]
        status[provider] = {
            "configured": bool(key_val),
            "sdk_installed": _check_sdk(sdk_mod),
        }
    return status


def _get_session(session_id: str) -> list[ProviderMessage]:
    """Get or create a session, evicting oldest if over limit."""
    now = time.monotonic()
    if session_id in _sessions:
        _sessions[session_id]["last_used"] = now
        return _sessions[session_id]["messages"]

    # Evict oldest sessions if at capacity
    while len(_sessions) >= MAX_SESSIONS:
        oldest_sid = min(_sessions, key=lambda s: _sessions[s]["last_used"])
        del _sessions[oldest_sid]
        logger.info("Evicted session %s (capacity %d)", oldest_sid, MAX_SESSIONS)

    _sessions[session_id] = {"messages": [], "last_used": now}
    return _sessions[session_id]["messages"]


@app.get("/health")
async def health():
    bridge = await get_bridge()
    return {
        "status": "ok",
        "mcp_running": bridge.is_running,
        "providers": _provider_status(),
    }


@app.get("/settings/keys")
async def get_keys():
    """Return which API keys are set, with masked values."""
    result = {}
    for provider, env_var in ENV_KEY_MAP.items():
        key_val = os.environ.get(env_var, "")
        result[provider] = {
            "configured": bool(key_val),
            "masked": _mask_key(key_val) if key_val else None,
        }
    return result


@app.post("/settings/keys")
async def set_key(req: SetKeyRequest):
    """Set an API key for a provider. Writes to .env and os.environ."""
    provider = req.provider.lower()
    if provider not in ENV_KEY_MAP:
        raise HTTPException(400, f"Unknown provider: {provider}. Choose: {', '.join(ENV_KEY_MAP)}")
    env_var = ENV_KEY_MAP[provider]
    os.environ[env_var] = req.api_key
    dotenv.set_key(str(_ENV_PATH), env_var, req.api_key)
    return {"ok": True, "provider": provider, "configured": True, "masked": _mask_key(req.api_key)}


@app.delete("/settings/keys/{provider}")
async def delete_key(provider: str):
    """Remove an API key for a provider."""
    provider = provider.lower()
    if provider not in ENV_KEY_MAP:
        raise HTTPException(400, f"Unknown provider: {provider}")
    env_var = ENV_KEY_MAP[provider]
    os.environ.pop(env_var, None)
    dotenv.unset_key(str(_ENV_PATH), env_var)
    return {"ok": True, "provider": provider, "configured": False}


@app.get("/decision/{decision_id}")
async def get_decision(decision_id: str):
    bridge = await get_bridge()
    try:
        result = await bridge.call_tool("get_decision", {"decision_id": decision_id})
        return {"decision_id": decision_id, "content": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search")
async def search_endpoint(query: str, limit: int = 20):
    """Direct FTS5 search without LLM involvement."""
    bridge = await get_bridge()
    args = {"query": query, "limit": min(limit, 50)}
    result = await bridge.call_tool("search_decisions", args)
    return {"decisions": _parse_decisions(result)}


@app.get("/sessions")
async def list_sessions():
    return {
        sid: {"message_count": len(data["messages"]), "last_role": data["messages"][-1].role if data["messages"] else None}
        for sid, data in _sessions.items()
    }


@app.post("/chat")
async def chat(request: ChatRequest, raw_request: Request):
    request_id = str(uuid.uuid4())[:8]

    # Validate provider
    provider_name = request.provider.lower()
    if provider_name not in PROVIDERS:
        raise HTTPException(400, f"Unknown provider: {provider_name}. Choose: {', '.join(PROVIDERS)}")

    try:
        provider = PROVIDERS[provider_name]()
    except Exception as e:
        raise HTTPException(500, f"Failed to initialize {provider_name}: {e}")

    session_id = request.session_id or str(uuid.uuid4())[:12]
    session = _get_session(session_id)

    # Add system prompt if new session
    if not session:
        session.append(ProviderMessage(role="system", content=SYSTEM_PROMPT))

    # Inject filter context if provided
    user_msg = request.message
    if request.filters:
        f = request.filters
        parts = []
        if f.court:
            parts.append(f"court={f.court}")
        if f.canton:
            parts.append(f"canton={f.canton}")
        if f.language:
            parts.append(f"language={f.language}")
        if f.date_from:
            parts.append(f"from={f.date_from}")
        if f.date_to:
            parts.append(f"to={f.date_to}")
        if parts:
            user_msg += f"\n\n[Active filters: {', '.join(parts)}]"
        if not f.collapse_duplicates:
            user_msg += "\n[Show all duplicates — do not collapse]"
        if not f.multilingual:
            user_msg += "\n[Single language only — do not expand to other languages]"

    session.append(ProviderMessage(role="user", content=user_msg))

    async def stream():
        try:
            all_text = ""
            emitted_dockets: set[str] = set()

            for _round in range(MAX_TOOL_ROUNDS):
                # Check if client disconnected before each LLM call
                if await raw_request.is_disconnected():
                    logger.info("Client disconnected [%s], stopping", request_id)
                    break

                # Stream text tokens from the provider
                full_text = ""
                final_tool_calls = None

                async for chunk in provider.chat_stream(session, tools=MCP_TOOLS):
                    if chunk.content:
                        full_text += chunk.content
                        yield _sse(ChatChunk(type="text", content=chunk.content))
                    if chunk.tool_calls:
                        final_tool_calls = chunk.tool_calls

                all_text += full_text

                # No tool calls → done
                if not final_tool_calls:
                    session.append(ProviderMessage(role="assistant", content=full_text))
                    break

                # Record assistant message with tool calls
                session.append(ProviderMessage(
                    role="assistant",
                    content=full_text or None,
                    tool_calls=final_tool_calls,
                ))

                # Execute tool calls via MCP
                bridge = await get_bridge()
                for tc in final_tool_calls:
                    # Check disconnect between tool calls
                    if await raw_request.is_disconnected():
                        logger.info("Client disconnected [%s] during tools, stopping", request_id)
                        return

                    yield _sse(ChatChunk(type="tool_start", content=f"Calling {tc.name}..."))

                    tool_t0 = time.monotonic()
                    try:
                        result_text = await bridge.call_tool(tc.name, tc.arguments)
                    except Exception as e:
                        result_text = f"Tool error: {e}"

                    tool_ms = (time.monotonic() - tool_t0) * 1000
                    trace = ToolTrace(
                        tool=tc.name,
                        latency_ms=round(tool_ms, 1),
                        arguments=tc.arguments,
                    )

                    yield _sse(ChatChunk(type="tool_end", tool_trace=trace))

                    # Parse structured decisions from search results
                    if tc.name == "search_decisions":
                        decisions = _parse_decisions(result_text)
                        if decisions:
                            trace.hit_count = len(decisions)
                            for d in decisions:
                                if d.get("docket_number"):
                                    emitted_dockets.add(d["docket_number"])
                            yield _sse(ChatChunk(type="decisions", decisions=decisions))

                    # Add tool result to session
                    session.append(provider.format_tool_result(ToolResult(
                        tool_call_id=tc.id,
                        name=tc.name,
                        content=result_text,
                    )))

            # Auto-resolve citations mentioned in LLM text but not yet in results
            cited = await _resolve_cited_decisions(all_text, emitted_dockets)
            if cited:
                yield _sse(ChatChunk(type="decisions", decisions=cited))

            yield _sse(ChatChunk(type="done", session_id=session_id))

        except Exception as e:
            logger.error("Chat error [%s]: %s", request_id, e, exc_info=True)
            yield _sse(ChatChunk(type="error", content=str(e)))

    return StreamingResponse(stream(), media_type="text/event-stream")


async def _resolve_cited_decisions(
    text: str, already_emitted: set[str],
) -> list[dict]:
    """Find docket/BGE citations in LLM text and fetch their cards."""
    if not text:
        return []

    citations = extract_case_citations(text)
    new_citations = [
        c for c in citations if c.normalized not in already_emitted
    ]
    if not new_citations:
        return []

    # Limit to 10 new citations
    new_citations = new_citations[:10]
    query = " OR ".join(f'"{c.normalized}"' for c in new_citations)

    try:
        bridge = await get_bridge()
        result = await bridge.call_tool("search_decisions", {
            "query": query,
            "limit": 20,
        })
        return _parse_decisions(result)
    except Exception as e:
        logger.warning("Failed to resolve cited decisions: %s", e)
        return []


def _sse(chunk: ChatChunk) -> str:
    return f"data: {chunk.model_dump_json()}\n\n"


def _parse_decisions(text: str) -> list[dict]:
    """Extract structured decision info from MCP search result text."""
    decisions = []
    current: dict | None = None

    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue

        # Match: **1. 6B_123/2024** (2024-01-15) [bger] [de]
        if line.startswith("**") and line.count("**") >= 2 and ". " in line:
            if current:
                decisions.append(current)
            current = {"raw": line}
            # Parse docket, date, court, language
            try:
                parts = line.split("**")
                docket_part = parts[1] if len(parts) > 1 else ""
                rest = parts[2] if len(parts) > 2 else ""
                # "1. 6B_123/2024" → docket
                if ". " in docket_part:
                    current["docket_number"] = docket_part.split(". ", 1)[1].strip()
                # "(2024-01-15) [bger] [de]"
                if "(" in rest and ")" in rest:
                    date_str = rest.split("(")[1].split(")")[0]
                    current["decision_date"] = date_str
                if "[" in rest:
                    brackets = [b.split("]")[0] for b in rest.split("[")[1:]]
                    if len(brackets) >= 1:
                        current["court"] = brackets[0]
                    if len(brackets) >= 2:
                        current["language"] = brackets[1]
            except Exception:
                pass

        elif current:
            if line.startswith("ID:"):
                current["decision_id"] = line[3:].strip()
            elif line.startswith("Title:"):
                current["title"] = line[6:].strip()
            elif line.startswith("Regeste:"):
                current["regeste"] = line[8:].strip()
            elif line.startswith("...") and line.endswith("..."):
                current["snippet"] = line[3:-3].strip()
            elif line.startswith("URL:"):
                current["source_url"] = line[4:].strip()

    if current:
        decisions.append(current)
    return decisions


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("BACKEND_PORT", "8910"))
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")
