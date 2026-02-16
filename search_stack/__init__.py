"""
Search stack components for world-class local Swiss caselaw retrieval.

This package provides:
- OpenSearch index templates and search pipeline definitions
- Query planning for hybrid lexical/vector search
- Legal reference extraction (statutes + case citations)
- Reference graph construction for precedent/statute traversals
"""

from .query_planner import QueryIntent, SearchFilters, build_hybrid_search_request, detect_query_intent
from .reference_extraction import extract_case_citations, extract_references, extract_statute_references

__all__ = [
    "QueryIntent",
    "SearchFilters",
    "build_hybrid_search_request",
    "detect_query_intent",
    "extract_case_citations",
    "extract_references",
    "extract_statute_references",
]

