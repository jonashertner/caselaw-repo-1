"""
Full-Text Search Server (FastAPI + SQLite FTS5)
=================================================

Provides:
- REST API for searching Swiss court decisions
- MCP-compatible endpoints for Claude integration
- FTS5 full-text search with BM25 ranking
- Filters: court, canton, language, date range

Deploy: Hetzner VPS (~€4/month) or any server with the SQLite DB.

Usage:
    uvicorn fts5_server:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(
    title="Swiss Caselaw Search",
    description="Full-text search over Swiss federal and cantonal court decisions",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = Path("output/decisions.db")


def get_db() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise HTTPException(503, "Database not available. Run pipeline first.")
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# ============================================================
# Response models
# ============================================================


class SearchResult(BaseModel):
    decision_id: str
    court: str
    canton: str | None = None
    docket_number: str
    decision_date: str
    language: str | None = None
    title: str | None = None
    regeste: str | None = None
    snippet: str | None = None
    source_url: str | None = None
    rank: float | None = None


class SearchResponse(BaseModel):
    query: str
    total: int
    results: list[SearchResult]


class StatsResponse(BaseModel):
    total_decisions: int
    courts: dict[str, int]
    cantons: dict[str, int]
    languages: dict[str, int]
    date_range: dict[str, str | None]


# ============================================================
# Endpoints
# ============================================================


@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., description="Search query (FTS5 syntax)"),
    court: Optional[str] = Query(None, description="Filter by court code"),
    canton: Optional[str] = Query(None, description="Filter by canton (2-letter)"),
    language: Optional[str] = Query(None, description="Filter by language (de/fr/it)"),
    date_from: Optional[str] = Query(None, description="Filter: decision date >= (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Filter: decision date <= (YYYY-MM-DD)"),
    limit: int = Query(20, ge=1, le=100, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """
    Search court decisions using FTS5 full-text search.

    Supports FTS5 query syntax: AND, OR, NOT, phrases ("..."), prefix*.
    Results are ranked by BM25 relevance.
    """
    conn = get_db()

    # Build WHERE clause for filters
    filters = []
    params = []

    if court:
        filters.append("d.court = ?")
        params.append(court)
    if canton:
        filters.append("d.canton = ?")
        params.append(canton.upper())
    if language:
        filters.append("d.language = ?")
        params.append(language.lower())
    if date_from:
        filters.append("d.decision_date >= ?")
        params.append(date_from)
    if date_to:
        filters.append("d.decision_date <= ?")
        params.append(date_to)

    where_clause = ""
    if filters:
        where_clause = "AND " + " AND ".join(filters)

    # FTS5 search with BM25 ranking
    query = f"""
        SELECT d.*, rank
        FROM decisions_fts fts
        JOIN decisions d ON d.rowid = fts.rowid
        WHERE decisions_fts MATCH ?
        {where_clause}
        ORDER BY rank
        LIMIT ? OFFSET ?
    """

    try:
        rows = conn.execute(query, [q, *params, limit, offset]).fetchall()
    except sqlite3.OperationalError as e:
        raise HTTPException(400, f"Invalid search query: {e}")

    # Count total
    count_query = f"""
        SELECT COUNT(*)
        FROM decisions_fts fts
        JOIN decisions d ON d.rowid = fts.rowid
        WHERE decisions_fts MATCH ?
        {where_clause}
    """
    total = conn.execute(count_query, [q, *params]).fetchone()[0]

    results = []
    for row in rows:
        # Generate snippet from full text
        snippet = None
        if row["full_text"]:
            text = row["full_text"]
            # Simple snippet: find first occurrence of a query term
            idx = text.lower().find(q.lower().split()[0]) if q else -1
            if idx >= 0:
                start = max(0, idx - 100)
                end = min(len(text), idx + 200)
                snippet = ("..." if start > 0 else "") + text[start:end] + ("..." if end < len(text) else "")

        results.append(
            SearchResult(
                decision_id=row["decision_id"],
                court=row["court"],
                canton=row["canton"],
                docket_number=row["docket_number"],
                decision_date=row["decision_date"],
                language=row["language"],
                title=row["title"],
                regeste=row["regeste"],
                snippet=snippet,
                source_url=row["source_url"],
                rank=row["rank"],
            )
        )

    conn.close()
    return SearchResponse(query=q, total=total, results=results)


@app.get("/decision/{decision_id}")
def get_decision(decision_id: str):
    """Get a single decision by ID with full text."""
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM decisions WHERE decision_id = ?", [decision_id]
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, f"Decision not found: {decision_id}")

    data = dict(row)
    if data.get("json_data"):
        try:
            data["metadata"] = json.loads(data["json_data"])
        except json.JSONDecodeError:
            pass
    return data


@app.get("/stats", response_model=StatsResponse)
def stats():
    """Get database statistics."""
    conn = get_db()

    total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    courts = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT court, COUNT(*) FROM decisions GROUP BY court ORDER BY COUNT(*) DESC"
        ).fetchall()
    }
    cantons = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT canton, COUNT(*) FROM decisions GROUP BY canton ORDER BY COUNT(*) DESC"
        ).fetchall()
    }
    languages = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT language, COUNT(*) FROM decisions GROUP BY language ORDER BY COUNT(*) DESC"
        ).fetchall()
    }

    date_range = conn.execute(
        "SELECT MIN(decision_date), MAX(decision_date) FROM decisions"
    ).fetchone()

    conn.close()

    return StatsResponse(
        total_decisions=total,
        courts=courts,
        cantons=cantons,
        languages=languages,
        date_range={"earliest": date_range[0], "latest": date_range[1]},
    )


# ============================================================
# MCP-compatible endpoints
# ============================================================


@app.get("/mcp/tools")
def mcp_tools():
    """Return MCP tool definitions for Claude integration."""
    return {
        "tools": [
            {
                "name": "search_swiss_caselaw",
                "description": "Search Swiss federal and cantonal court decisions by full text, court, canton, language, and date range.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Full-text search query",
                        },
                        "court": {
                            "type": "string",
                            "description": "Court code filter (bger, bge, bvger, bstger, bpatger, or cantonal)",
                        },
                        "canton": {
                            "type": "string",
                            "description": "Canton filter (2-letter code: ZH, BE, GE, etc.)",
                        },
                        "language": {
                            "type": "string",
                            "description": "Language filter: de, fr, it",
                        },
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "get_decision",
                "description": "Get full text of a specific Swiss court decision by ID.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "decision_id": {
                            "type": "string",
                            "description": "Decision ID (e.g., bger_6B_1234_2025)",
                        },
                    },
                    "required": ["decision_id"],
                },
            },
        ]
    }


@app.post("/mcp/invoke/{tool_name}")
async def mcp_invoke(tool_name: str, body: dict):
    """Invoke MCP tool."""
    if tool_name == "search_swiss_caselaw":
        return search(
            q=body.get("query", ""),
            court=body.get("court"),
            canton=body.get("canton"),
            language=body.get("language"),
        )
    elif tool_name == "get_decision":
        return get_decision(body["decision_id"])
    else:
        raise HTTPException(404, f"Unknown tool: {tool_name}")


@app.get("/health")
def health():
    """Health check."""
    try:
        conn = get_db()
        count = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        conn.close()
        return {"status": "ok", "decisions": count}
    except Exception as e:
        return {"status": "error", "error": str(e)}
