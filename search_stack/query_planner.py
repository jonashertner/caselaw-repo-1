"""
Query planning for hybrid Swiss caselaw search.

Builds OpenSearch query bodies with:
- intent detection (docket/statute/boolean/natural-language)
- metadata filters
- lexical and optional vector clauses
- hybrid query composition (for RRF search pipeline)
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any


class QueryIntent(str, Enum):
    DOCKET = "docket"
    STATUTE = "statute"
    BOOLEAN = "boolean"
    CITATION = "citation"
    NATURAL_LANGUAGE = "natural_language"


@dataclass
class SearchFilters:
    court: str | None = None
    canton: str | None = None
    language: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    decision_type: str | None = None
    legal_area: str | None = None


DOCKET_PATTERN = re.compile(
    r"""
    (
      \b[A-Z]{1,4}[._-]\d{1,6}[./]\d{4}\b |
      \b[A-Z]{1,4}\.\d{4}\.\d{1,6}\b |
      \b\d+[A-Z]?[._-]\d{1,6}[./]\d{4}\b
    )
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

STATUTE_PATTERN = re.compile(
    r"(?i)\b(?:Art\.?|Artikel)\s*\d+[a-zA-Z]?(?:\s*Abs\.?\s*\d+[a-zA-Z]?)?\s*[A-Z]{2,10}\b"
)

BGE_PATTERN = re.compile(r"(?i)\bBGE\s+\d{2,3}\s+[IVX]+\s+\d+\b")
BOOLEAN_PATTERN = re.compile(r"\b(AND|OR|NOT|NEAR)\b", re.IGNORECASE)


def detect_query_intent(query: str) -> QueryIntent:
    q = (query or "").strip()
    if not q:
        return QueryIntent.NATURAL_LANGUAGE
    if DOCKET_PATTERN.search(q):
        return QueryIntent.DOCKET
    if BGE_PATTERN.search(q):
        return QueryIntent.CITATION
    if STATUTE_PATTERN.search(q):
        return QueryIntent.STATUTE
    if BOOLEAN_PATTERN.search(q) or '"' in q:
        return QueryIntent.BOOLEAN
    return QueryIntent.NATURAL_LANGUAGE


def build_hybrid_search_request(
    *,
    query: str,
    filters: SearchFilters | None = None,
    query_vector: list[float] | None = None,
    size: int = 20,
    num_candidates: int = 300,
    use_rrf_pipeline: bool = True,
    search_pipeline_name: str = "swiss-caselaw-hybrid-rrf-v1",
    include_explain: bool = False,
) -> dict[str, Any]:
    """
    Build an OpenSearch request for lexical or hybrid retrieval.

    If query_vector is provided, returns a hybrid lexical+vector query body.
    Otherwise returns a lexical-only query body.
    """
    filters = filters or SearchFilters()
    intent = detect_query_intent(query)
    lexical = _build_lexical_query(query=query, intent=intent, filters=filters)

    body: dict[str, Any] = {
        "size": size,
        "track_total_hits": True,
        "_source": {
            "includes": [
                "decision_id",
                "court",
                "canton",
                "chamber",
                "docket_number",
                "decision_date",
                "publication_date",
                "language",
                "title",
                "regeste",
                "legal_area",
                "decision_type",
                "source_url",
                "pdf_url",
            ]
        },
        "highlight": {
            "fields": {
                "title": {},
                "regeste": {},
                "full_text": {"fragment_size": 220, "number_of_fragments": 3},
            }
        },
    }

    if query_vector:
        vector = _build_vector_query(query_vector=query_vector, num_candidates=num_candidates)
        body["query"] = {"hybrid": [lexical, vector]}
        if use_rrf_pipeline:
            body["search_pipeline"] = search_pipeline_name
    else:
        body["query"] = lexical

    if include_explain:
        body["explain"] = True
        body["profile"] = True

    return body


def _build_filter_clauses(filters: SearchFilters) -> list[dict[str, Any]]:
    clauses: list[dict[str, Any]] = []
    if filters.court:
        clauses.append({"term": {"court": filters.court.lower()}})
    if filters.canton:
        clauses.append({"term": {"canton": filters.canton.upper()}})
    if filters.language:
        clauses.append({"term": {"language": filters.language.lower()}})
    if filters.decision_type:
        clauses.append({"term": {"decision_type": filters.decision_type.lower()}})
    if filters.legal_area:
        clauses.append({"term": {"legal_area": filters.legal_area.lower()}})
    if filters.date_from or filters.date_to:
        range_body: dict[str, Any] = {}
        if filters.date_from:
            range_body["gte"] = filters.date_from
        if filters.date_to:
            range_body["lte"] = filters.date_to
        clauses.append({"range": {"decision_date": range_body}})
    return clauses


def _build_lexical_query(
    *,
    query: str,
    intent: QueryIntent,
    filters: SearchFilters,
) -> dict[str, Any]:
    filter_clauses = _build_filter_clauses(filters)

    if intent == QueryIntent.DOCKET:
        should = [
            {"term": {"docket_number.raw": _docket_norm(query)}},
            {"term": {"decision_id": query.lower().replace("/", "_").replace(".", "_")}},
            {"match_phrase": {"docket_number": {"query": query, "boost": 6.0}}},
        ]
        must = [{"bool": {"should": should, "minimum_should_match": 1}}]
    elif intent == QueryIntent.STATUTE:
        must = [
            {
                "multi_match": {
                    "query": query,
                    "type": "best_fields",
                    "fields": [
                        "regeste^5",
                        "title^4",
                        "full_text^2",
                    ],
                    "operator": "and",
                }
            }
        ]
    elif intent == QueryIntent.CITATION:
        must = [
            {
                "multi_match": {
                    "query": query,
                    "type": "best_fields",
                    "fields": [
                        "decision_refs^8",
                        "regeste^4",
                        "title^3",
                        "full_text",
                    ],
                    "operator": "or",
                }
            }
        ]
    elif intent == QueryIntent.BOOLEAN:
        must = [
            {
                "query_string": {
                    "query": query,
                    "fields": [
                        "title^4",
                        "regeste^4",
                        "full_text^1.5",
                        "docket_number^6",
                    ],
                    "default_operator": "AND",
                    "lenient": True,
                }
            }
        ]
    else:
        # Natural language retrieval: broad recall with field boosts.
        must = [
            {
                "multi_match": {
                    "query": query,
                    "type": "most_fields",
                    "fields": [
                        "title^5",
                        "regeste^4",
                        "full_text^1.7",
                        "docket_number^5",
                    ],
                    "operator": "or",
                    "fuzziness": "AUTO:4,7",
                    "prefix_length": 1,
                }
            }
        ]

    return {"bool": {"must": must, "filter": filter_clauses}}


def _build_vector_query(*, query_vector: list[float], num_candidates: int) -> dict[str, Any]:
    return {
        "knn": {
            "full_text_embedding": {
                "vector": query_vector,
                "k": min(200, num_candidates),
                "num_candidates": num_candidates,
            }
        }
    }


def _docket_norm(text: str) -> str:
    return re.sub(r"\s+", "", text.strip().lower())
