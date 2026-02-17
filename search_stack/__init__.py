"""
Search stack components for local Swiss caselaw retrieval.

This package provides:
- Legal reference extraction (statutes + case citations)
- Reference graph construction for precedent/statute traversals
"""

from .reference_extraction import extract_case_citations, extract_references, extract_statute_references

__all__ = [
    "extract_case_citations",
    "extract_references",
    "extract_statute_references",
]
