"""
Hybrid local search API for Swiss caselaw.

Backends:
- sqlite (default): reuses current mcp_server search path
- opensearch (optional): hybrid lexical/vector search via query planner

Graph endpoints:
- citations by decision
- decisions by statute reference
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import mcp_server
from search_stack.query_planner import SearchFilters, build_hybrid_search_request

SEMANTIC_QUERY_EMBEDDING = os.environ.get("SWISS_CASELAW_SEMANTIC_QUERY_EMBEDDING", "0").lower() in {
    "1",
    "true",
    "yes",
}
SEMANTIC_EMBED_MODEL = os.environ.get(
    "SWISS_CASELAW_SEMANTIC_EMBED_MODEL",
    "intfloat/multilingual-e5-small",
)

_EMBED_MODEL = None
_EMBED_MODEL_FAILED = False


class SearchRequest(BaseModel):
    query: str = Field(default="", description="Search query")
    court: str | None = None
    canton: str | None = None
    language: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    decision_type: str | None = None
    legal_area: str | None = None
    size: int = Field(default=20, ge=1, le=100)
    include_explain: bool = False


@dataclass
class SearchResponse:
    backend: str
    total: int
    results: list[dict[str, Any]]


class SQLiteSearchBackend:
    name = "sqlite_fts5"

    def search(self, req: SearchRequest) -> SearchResponse:
        rows = mcp_server.search_fts5(
            query=req.query,
            court=req.court,
            canton=req.canton,
            language=req.language,
            date_from=req.date_from,
            date_to=req.date_to,
            limit=req.size,
        )
        return SearchResponse(backend=self.name, total=len(rows), results=rows)

    def get_decision(self, decision_id: str) -> dict[str, Any] | None:
        return mcp_server.get_decision_by_id(decision_id)


class OpenSearchBackend:
    name = "opensearch_hybrid"

    def __init__(self, host: str, index_prefix: str):
        try:
            from opensearchpy import OpenSearch
        except ImportError as e:
            raise RuntimeError("opensearch-py not installed") from e
        self.index = f"{index_prefix}-decisions-v1"
        self.search_pipeline = f"{index_prefix}-hybrid-rrf-v1"
        self.client = OpenSearch(
            hosts=[host],
            use_ssl=host.startswith("https://"),
            verify_certs=host.startswith("https://"),
        )

    def search(self, req: SearchRequest) -> SearchResponse:
        filters = SearchFilters(
            court=req.court,
            canton=req.canton,
            language=req.language,
            date_from=req.date_from,
            date_to=req.date_to,
            decision_type=req.decision_type,
            legal_area=req.legal_area,
        )
        query_vector = None
        if SEMANTIC_QUERY_EMBEDDING:
            query_vector = _embed_query_vector(req.query)
        body = build_hybrid_search_request(
            query=req.query,
            filters=filters,
            size=req.size,
            include_explain=req.include_explain,
            query_vector=query_vector,
            search_pipeline_name=self.search_pipeline,
        )
        response = self.client.search(index=self.index, body=body)
        hits = response.get("hits", {}).get("hits", [])
        results = []
        for hit in hits:
            source = hit.get("_source", {})
            source["relevance_score"] = hit.get("_score")
            source["highlight"] = hit.get("highlight")
            results.append(source)
        total = response.get("hits", {}).get("total", {})
        if isinstance(total, dict):
            total_value = int(total.get("value", 0))
        else:
            total_value = int(total)
        return SearchResponse(backend=self.name, total=total_value, results=results)

    def get_decision(self, decision_id: str) -> dict[str, Any] | None:
        response = self.client.get(index=self.index, id=decision_id, ignore=[404])
        if not response or not response.get("found"):
            return None
        return response.get("_source")


class ReferenceGraphStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        if not self.db_path.exists():
            raise FileNotFoundError(
                f"Reference graph database not found at {self.db_path}. "
                "Run search_stack/build_reference_graph.py first."
            )
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _has_citation_targets(self, conn: sqlite3.Connection) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'citation_targets'"
        ).fetchone()
        return row is not None

    def _has_column(self, conn: sqlite3.Connection, table: str, column: str) -> bool:
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        except sqlite3.Error:
            return False
        return any(str(r["name"]).lower() == column.lower() for r in rows)

    def outgoing_citations(self, decision_id: str, limit: int = 200) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            has_targets = self._has_citation_targets(conn)
            has_confidence = has_targets and self._has_column(
                conn, "citation_targets", "confidence_score"
            )
            has_legacy_target = self._has_column(conn, "decision_citations", "target_decision_id")
            if has_targets:
                confidence_base = (
                    "COALESCE(ct.confidence_score, 1.0)"
                    if has_confidence
                    else "1.0"
                )
                confidence_select = (
                    "CASE "
                    "WHEN ct.target_decision_id IS NULL THEN NULL "
                    f"ELSE {confidence_base} END"
                )
                weighted_select = (
                    "CASE "
                    "WHEN ct.target_decision_id IS NULL THEN NULL "
                    f"ELSE dc.mention_count * ({confidence_base}) END"
                )
                rows = conn.execute(
                    f"""
                    SELECT
                        dc.source_decision_id,
                        dc.target_ref,
                        dc.target_type,
                        ct.target_decision_id,
                        ct.match_type,
                        dc.mention_count,
                        {confidence_select} AS confidence_score,
                        {weighted_select} AS weighted_mention_count
                    FROM decision_citations dc
                    LEFT JOIN citation_targets ct
                        ON ct.source_decision_id = dc.source_decision_id
                       AND ct.target_ref = dc.target_ref
                    WHERE dc.source_decision_id = ?
                    ORDER BY dc.mention_count DESC, dc.target_ref, ct.target_decision_id
                    LIMIT ?
                    """,
                    (decision_id, limit),
                ).fetchall()
            else:
                if has_legacy_target:
                    rows = conn.execute(
                        """
                        SELECT
                            source_decision_id,
                            target_ref,
                            target_type,
                            target_decision_id,
                            'legacy_target_decision_id' AS match_type,
                            mention_count,
                            1.0 AS confidence_score,
                            mention_count AS weighted_mention_count
                        FROM decision_citations
                        WHERE source_decision_id = ?
                        ORDER BY mention_count DESC, target_ref
                        LIMIT ?
                        """,
                        (decision_id, limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT
                            source_decision_id,
                            target_ref,
                            target_type,
                            NULL AS target_decision_id,
                            NULL AS match_type,
                            mention_count,
                            NULL AS confidence_score,
                            NULL AS weighted_mention_count
                        FROM decision_citations
                        WHERE source_decision_id = ?
                        ORDER BY mention_count DESC, target_ref
                        LIMIT ?
                        """,
                        (decision_id, limit),
                    ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def incoming_citations(self, decision_id: str, limit: int = 200) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            has_targets = self._has_citation_targets(conn)
            has_confidence = has_targets and self._has_column(
                conn, "citation_targets", "confidence_score"
            )
            has_legacy_target = self._has_column(conn, "decision_citations", "target_decision_id")
            if has_targets:
                confidence_select = (
                    "COALESCE(ct.confidence_score, 1.0)"
                    if has_confidence
                    else "1.0"
                )
                rows = conn.execute(
                    f"""
                    SELECT
                        ct.source_decision_id,
                        ct.target_ref,
                        dc.target_type,
                        ct.target_decision_id,
                        ct.match_type,
                        dc.mention_count,
                        {confidence_select} AS confidence_score,
                        dc.mention_count * {confidence_select} AS weighted_mention_count
                    FROM citation_targets ct
                    JOIN decision_citations dc
                      ON dc.source_decision_id = ct.source_decision_id
                     AND dc.target_ref = ct.target_ref
                    WHERE ct.target_decision_id = ?
                    ORDER BY dc.mention_count DESC, ct.source_decision_id
                    LIMIT ?
                    """,
                    (decision_id, limit),
                ).fetchall()
            else:
                if has_legacy_target:
                    rows = conn.execute(
                        """
                        SELECT
                            source_decision_id,
                            target_ref,
                            target_type,
                            target_decision_id,
                            'legacy_target_decision_id' AS match_type,
                            mention_count,
                            1.0 AS confidence_score,
                            mention_count AS weighted_mention_count
                        FROM decision_citations
                        WHERE target_decision_id = ?
                        ORDER BY mention_count DESC, source_decision_id
                        LIMIT ?
                        """,
                        (decision_id, limit),
                    ).fetchall()
                else:
                    rows = []
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def decisions_for_statute(self, law_code: str, article: str, limit: int = 200) -> list[dict[str, Any]]:
        law = law_code.upper()
        art = article.lower()
        conn = self._conn()
        try:
            rows = conn.execute(
                """
                SELECT ds.decision_id, ds.mention_count, d.court, d.decision_date, d.docket_number
                FROM decision_statutes ds
                JOIN statutes s ON s.statute_id = ds.statute_id
                JOIN decisions d ON d.decision_id = ds.decision_id
                WHERE s.law_code = ? AND s.article = ?
                ORDER BY ds.mention_count DESC, d.decision_date DESC
                LIMIT ?
                """,
                (law, art, limit),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def _build_backend() -> SQLiteSearchBackend | OpenSearchBackend:
    backend = os.environ.get("SWISS_CASELAW_SEARCH_BACKEND", "sqlite").strip().lower()
    if backend == "opensearch":
        host = os.environ.get("OPENSEARCH_HOST", "http://localhost:9200")
        prefix = os.environ.get("OPENSEARCH_INDEX_PREFIX", "swiss-caselaw")
        return OpenSearchBackend(host=host, index_prefix=prefix)
    return SQLiteSearchBackend()


def _embed_query_vector(query: str) -> list[float] | None:
    model = _get_embed_model()
    if model is None:
        return None
    text = (query or "").strip()
    if not text:
        return None
    if not text.lower().startswith("query:"):
        text = f"query: {text}"
    try:
        vector = model.encode(text, normalize_embeddings=True)
        return [float(v) for v in vector]
    except Exception:
        return None


def _get_embed_model():
    global _EMBED_MODEL, _EMBED_MODEL_FAILED
    if not SEMANTIC_QUERY_EMBEDDING:
        return None
    if _EMBED_MODEL is not None:
        return _EMBED_MODEL
    if _EMBED_MODEL_FAILED:
        return None
    try:
        from sentence_transformers import SentenceTransformer
    except Exception:
        _EMBED_MODEL_FAILED = True
        return None
    try:
        _EMBED_MODEL = SentenceTransformer(SEMANTIC_EMBED_MODEL)
        return _EMBED_MODEL
    except Exception:
        _EMBED_MODEL_FAILED = True
        return None


app = FastAPI(title="Swiss Caselaw Hybrid Search", version="1.0")
SEARCH_BACKEND = _build_backend()
GRAPH_STORE = ReferenceGraphStore(
    Path(os.environ.get("SWISS_CASELAW_GRAPH_DB", "output/reference_graph.db"))
)


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "search_backend": SEARCH_BACKEND.name,
        "graph_db": str(GRAPH_STORE.db_path),
    }


@app.post("/search")
def search(req: SearchRequest) -> dict[str, Any]:
    result = SEARCH_BACKEND.search(req)
    return asdict(result)


@app.get("/decision/{decision_id}")
def decision(decision_id: str) -> dict[str, Any]:
    row = SEARCH_BACKEND.get_decision(decision_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Decision not found: {decision_id}")
    return row


@app.get("/citations/{decision_id}")
def citations(decision_id: str, limit: int = 200) -> dict[str, Any]:
    try:
        outgoing = GRAPH_STORE.outgoing_citations(decision_id, limit=limit)
        incoming = GRAPH_STORE.incoming_citations(decision_id, limit=limit)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {
        "decision_id": decision_id,
        "outgoing": outgoing,
        "incoming": incoming,
    }


@app.get("/statute/{law_code}/{article}")
def statute(law_code: str, article: str, limit: int = 200) -> dict[str, Any]:
    try:
        rows = GRAPH_STORE.decisions_for_statute(law_code, article, limit=limit)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {
        "law_code": law_code.upper(),
        "article": article.lower(),
        "count": len(rows),
        "decisions": rows,
    }
