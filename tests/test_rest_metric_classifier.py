"""B3+B4 — REST metrics classifier must not charge crawler 4xx to the parent tool.

The middleware previously mapped any /api/decisions/... 4xx to search_decisions and any
/api/laws/... 4xx to laws, so crawler probes of /api/decisions/{id}/export.* and fabricated
/api/laws/{abbr}/{canton} URLs inflated those tools' error rates (~0.6% / ~3%) when the
real error rate is ~0 (0 5xx). Fix: path-depth-aware tool + only 5xx counts as an error.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402

C = mcp_server._classify_rest_metric


def test_search_collection_route():
    assert C("/api/decisions", 200) == ("search_decisions", False)


def test_export_404_is_not_charged_to_search():
    # crawler probing a non-exportable decision -> export_decision, NOT an error
    assert C("/api/decisions/bge_BGE_140_III_86/export.pdf", 404) == ("export_decision", False)
    assert C("/api/decisions/x/export.docx", 404) == ("export_decision", False)


def test_get_decision_subresource_404_not_error():
    assert C("/api/decisions/does-not-exist", 404) == ("get_decision", False)
    assert C("/api/decision/whatever", 404) == ("get_decision", False)


def test_laws_two_segment_crawler_404_not_error():
    # fabricated /api/laws/{abbr}/{canton} crawler URL -> still 'laws' but NOT an error
    assert C("/api/laws/KV/BE", 404) == ("laws", False)


def test_laws_ok():
    assert C("/api/laws/OR", 200) == ("laws", False)


def test_real_5xx_is_error():
    assert C("/api/decisions", 500) == ("search_decisions", True)
    assert C("/api/laws/OR", 503) == ("laws", True)


def test_non_api_and_docs_ignored():
    assert C("/health", 200) == (None, False)
    assert C("/api/docs", 200) == (None, False)
    assert C("/api/openapi.json", 200) == (None, False)
