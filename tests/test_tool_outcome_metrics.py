"""Outcome labelling for tool calls (mcp_server._classify_outcome).

Motivation (2026-08-06): call counts measure demand, not whether the tool
answered. An empty search, a cite resolving to exists=false and a get_law
miss are all HTTP 200 and were indistinguishable from a hit in the metrics,
so "15.4M calls" could not be turned into "how often did we answer".

The payload shapes below are taken from the handlers, not invented — see
the marker list in mcp_server for the grep that produced them.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402

_classify = mcp_server._classify_outcome


class _Txt:
    """Stand-in for mcp.types.TextContent (only .text is read)."""

    def __init__(self, text):
        self.text = text


def _res(text):
    return [_Txt(text)]


# ── empty: handled "nothing found" paths ─────────────────────────────

@pytest.mark.parametrize("text", [
    "No decisions found matching your query (total: 0).",
    "Ergebnisse:\n\nNo results found.\n",
    "No data found.\n",
    "No articles found.\n",
    "Citations\n\nNo outgoing citations found.\n",
    "- No sufficiently similar decisions found.\n\n",
    "Unknown tool: search_everything",
])
def test_text_no_result_paths_are_empty(text):
    assert _classify(_res(text)) == "empty"


@pytest.mark.parametrize("payload", [
    {"error": "Decision not found: 'bger_9C_999_9999'"},
    {"error": "No law found with abbreviation 'XYZ'."},
    {"error": "No structured Erwägungen found for 'x'."},
    {"note": "No leading cases found for this topic."},
    {"exists": False, "queried": "BGE 148 I 356", "resolved_id": "BGE 148 I 356"},
    {},
    {"note": "", "hint": None},
])
def test_json_no_answer_payloads_are_empty(payload):
    assert _classify(_res(json.dumps(payload, ensure_ascii=False))) == "empty"


def test_empty_list_payload_is_empty():
    assert _classify(_res("[]")) == "empty"


def test_blank_and_missing_results_are_empty():
    assert _classify(_res("")) == "empty"
    assert _classify(_res("   \n ")) == "empty"
    assert _classify([]) == "empty"


# ── substantive: real answers ────────────────────────────────────────

def test_cite_hit_is_substantive():
    payload = {
        "exists": True,
        "decision_id": "bge_BGE_148_IV_356",
        "citation_string_de": "BGE 148 IV 356",
        "_note": "Copy citation_string verbatim into your response.",
    }
    assert _classify(_res(json.dumps(payload))) == "substantive"


def test_note_alongside_content_is_substantive():
    """A _note is boilerplate on almost every payload — its presence must
    not make a real answer look empty."""
    payload = {"note": "Use markdown_link.", "results": [{"id": "x"}]}
    assert _classify(_res(json.dumps(payload))) == "substantive"


def test_formatted_search_hits_are_substantive():
    text = ("Found 12 decisions\n\n1. BGE 148 IV 356 vom 25.08.2022\n"
            "   Untersuchungsgrundsatz; Strafregisterauszug\n")
    assert _classify(_res(text)) == "substantive"


def test_long_decision_text_is_never_empty():
    """A full decision can contain a phrase like 'not found:' in its own
    reasoning. The size guard is what keeps that from flipping the label."""
    body = ("Die Vorinstanz erwog, der Beweis sei not found: " + "x" * 60_000)
    assert _classify(_res(body)) == "substantive"


def test_marker_inside_a_long_but_real_answer_is_ignored():
    text = "Zitate\n\nNo outgoing citations found.\n\n" + ("Eingehend: " * 900)
    assert len(text) > mcp_server._EMPTY_MAX_CHARS
    assert _classify(_res(text)) == "substantive"


def test_deep_research_tuple_result_is_unwrapped():
    payload = json.dumps({"results": [{"id": "a"}]})
    assert _classify(([_Txt(payload)], {"results": [{"id": "a"}]})) == "substantive"
    assert _classify(([_Txt("[]")], {})) == "empty"


def test_multipart_result_is_joined():
    assert _classify([_Txt("No results found."), _Txt("")]) == "empty"


def test_malformed_json_falls_back_to_text_rules():
    assert _classify(_res('{"error": broken')) == "substantive"
    assert _classify(_res('{"x": not json, decision not found: y')) == "empty"


# ── recording ────────────────────────────────────────────────────────

def test_record_tool_outcome_accumulates(monkeypatch):
    import collections
    fake = collections.defaultdict(collections.Counter)
    monkeypatch.setitem(mcp_server._metrics, "tool_outcomes", fake)
    mcp_server._record_tool_outcome("cite", "substantive")
    mcp_server._record_tool_outcome("cite", "substantive")
    mcp_server._record_tool_outcome("cite", "empty")
    assert dict(fake["cite"]) == {"substantive": 2, "empty": 1}


def test_record_tool_outcome_never_raises(monkeypatch):
    """Telemetry must not be able to fail a request that already answered."""
    class _Boom(dict):
        def __getitem__(self, k):
            raise RuntimeError("counter exploded")

    monkeypatch.setattr(mcp_server, "_metrics", _Boom())
    mcp_server._record_tool_outcome("cite", "substantive")   # must not raise


def test_metrics_snapshot_exposes_outcomes(monkeypatch):
    import collections
    monkeypatch.setitem(mcp_server._metrics, "tool_calls",
                        collections.Counter({"cite": 5}))
    monkeypatch.setitem(mcp_server._metrics, "tool_errors", collections.Counter())
    monkeypatch.setitem(mcp_server._metrics, "tool_latency_ms",
                        collections.defaultdict(list, {"cite": [10.0]}))
    monkeypatch.setitem(
        mcp_server._metrics, "tool_outcomes",
        collections.defaultdict(collections.Counter,
                                {"cite": collections.Counter(
                                    {"substantive": 3, "empty": 1})}))
    snap = mcp_server._get_metrics()
    assert snap["tools"]["cite"]["substantive"] == 3
    assert snap["tools"]["cite"]["empty"] == 1
    # 5 calls, 4 labelled: the shortfall stays visible instead of being
    # silently folded into "answered".
    assert snap["tools"]["cite"]["calls"] == 5
