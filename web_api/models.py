"""Pydantic models for the web API."""
from __future__ import annotations

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    provider: str = Field(description="LLM provider: openai | gemini | claude")
    message: str = Field(description="User message")
    session_id: str | None = Field(default=None, description="Optional session ID for continuity")
    filters: FilterParams | None = None


class FilterParams(BaseModel):
    court: str | None = None
    canton: str | None = None
    language: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    collapse_duplicates: bool = True
    multilingual: bool = True


class ToolTrace(BaseModel):
    tool: str
    latency_ms: float
    hit_count: int | None = None
    arguments: dict | None = None


class SetKeyRequest(BaseModel):
    provider: str = Field(description="Provider name: claude | openai | gemini")
    api_key: str = Field(description="The API key value")


class ChatChunk(BaseModel):
    """One chunk in the streaming response."""
    type: str = Field(description="text | tool_start | tool_end | decisions | error | done")
    content: str | None = None
    tool_trace: ToolTrace | None = None
    decisions: list[dict] | None = None
    session_id: str | None = None
