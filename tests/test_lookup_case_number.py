"""C1 — instant case-number lookup helper (_lookup_case_number).

The public site's case-number box resolves a docket straight to the decision via
search_fts5's exact-docket fast-path (no Haiku/rerank). Verifies the docket guard
(non-dockets never hit the slow path), R1-safe citation/URL extraction, and the lean shape.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402


def test_resolves_case_number(monkeypatch):
    monkeypatch.setattr(mcp_server, "_looks_like_docket_query", lambda q: True)
    row = {
        "decision_id": "ag_verwaltungsgericht_WBE.2026.33", "docket_number": "WBE.2026.33",
        "court": "ag_verwaltungsgericht", "canton": "AG", "decision_date": "2026-01-30",
        "title": "Beschwerdeverfahren",
    }
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **k: ([row], 1))
    monkeypatch.setattr(
        mcp_server, "_build_citation_strings",
        lambda r: {"citation_string_de": "WBE.2026.33",
                   "canonical_url": "https://mcp.opencaselaw.ch/entscheid/ag_verwaltungsgericht_WBE.2026.33"},
    )
    res = mcp_server._lookup_case_number("WBE.2026.33")
    assert res["is_case_number"] is True
    assert res["total"] == 1
    h = res["results"][0]
    assert h["decision_id"] == "ag_verwaltungsgericht_WBE.2026.33"
    assert h["citation"] == "WBE.2026.33"          # R1: from the pipeline, never constructed
    assert "entscheid" in h["url"]
    assert h["court"] == "ag_verwaltungsgericht"


def test_rejects_non_case_number(monkeypatch):
    monkeypatch.setattr(mcp_server, "_looks_like_docket_query", lambda q: False)

    def _boom(**k):
        raise AssertionError("search_fts5 must not run for a non-docket input")

    monkeypatch.setattr(mcp_server, "search_fts5", _boom)
    res = mcp_server._lookup_case_number("Verjaehrung im Mietrecht")
    assert res["is_case_number"] is False
    assert res["total"] == 0
    assert "hint" in res


def test_empty_input():
    res = mcp_server._lookup_case_number("")
    assert res["is_case_number"] is False
    assert res["total"] == 0
    assert res["results"] == []
