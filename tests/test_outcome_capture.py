"""Every tool call becomes a recorded (input, output) pair.

Capture logged what was asked and, outside search, nothing about what
came back: no result identifiers, no status, no latency. A `get_law` that
answered and one that found nothing were identical records, and a call
that raised left no trace of the arguments that broke it — which is
exactly the material that would improve a tool description.

The answer text itself is deliberately not stored. The documents are CC0
and already in the corpus, so the identifiers are the whole signal.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _tc(payload):
    from mcp.types import TextContent
    return [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False))]


def test_identifiers_are_recovered_from_a_list_answer():
    got = m._returned_ids(_tc({"results": [
        {"decision_id": "bger_4A_1_2020", "title": "x"},
        {"decision_id": "bge_BGE_145_II_49"},
    ]}))
    assert got == ["bger_4A_1_2020", "bge_BGE_145_II_49"]


def test_identifiers_are_recovered_from_a_nested_answer():
    got = m._returned_ids(_tc({"law": {"sr_number": "220", "articles": [
        {"id": "art-41"}, {"id": "art-42"}]}}))
    assert "220" in got and "art-41" in got


def test_duplicates_collapse_and_the_list_is_bounded():
    got = m._returned_ids(_tc({"r": [{"id": "a"} for _ in range(200)]}))
    assert got == ["a"]
    many = m._returned_ids(_tc({"r": [{"id": f"x{i}"} for i in range(200)]}))
    assert len(many) <= 30


def test_a_huge_answer_is_skipped_rather_than_reparsed():
    """Full-text fetches name their id in the arguments already, so the
    cost of walking them buys nothing."""
    assert m._returned_ids(_tc({"full_text": "x" * (m._OUTCOME_MAX_CHARS + 10),
                                "decision_id": "bger_4A_1_2020"})) == []


def test_identifiers_are_recovered_from_a_markdown_answer():
    """Most tools answer in Markdown, so a JSON parse alone would leave
    the majority of the surface blank. R1 guarantees every decision is
    named with a verbatim /entscheid/<id> link, which is what makes the
    ids readable out of prose."""
    from mcp.types import TextContent
    md = ("**1.** [BGE 125 V 351](https://mcp.opencaselaw.ch/entscheid/"
          "bge_BGE_125_V_351) (1999) — 77298 citations\n"
          "**2.** [125 V 351](https://mcp.opencaselaw.ch/entscheid/"
          "bge_125%20V%20351)\n")
    got = m._returned_ids([TextContent(type="text", text=md)])
    assert got == ["bge_BGE_125_V_351", "bge_125 V 351"], \
        "percent-escapes must be decoded to the real id"


def test_markdown_ids_deduplicate_and_stay_bounded():
    from mcp.types import TextContent
    md = " ".join(f"[x](https://mcp.opencaselaw.ch/entscheid/bger_{i})"
                  for i in range(80)) * 2
    got = m._returned_ids([TextContent(type="text", text=md)])
    assert len(got) <= 30 and len(set(got)) == len(got)


def test_non_json_and_empty_answers_are_harmless():
    from mcp.types import TextContent
    assert m._returned_ids([TextContent(type="text", text="plain prose")]) == []
    assert m._returned_ids([]) == []


def test_the_outcome_record_carries_status_latency_and_ids(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)
    monkeypatch.setattr(m, "_FULL_CAPTURE", True)
    m._capture_outcome("search_decisions", "substantive", m.time.monotonic() - 0.25,
                       result=_tc({"results": [{"decision_id": "bger_4A_1_2020"}]}))
    rec = json.loads(next(tmp_path.glob("capture_*.jsonl")).read_text().strip())
    assert rec["src"] == "outcome" and rec["tool"] == "search_decisions"
    assert rec["outcome"] == "substantive"
    assert rec["returned_ids"] == ["bger_4A_1_2020"]
    assert rec["ms"] >= 200
    assert rec["error"] is None


def test_a_failure_records_the_error(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)
    monkeypatch.setattr(m, "_FULL_CAPTURE", True)
    m._capture_outcome("get_law", "error", m.time.monotonic(),
                       error="KeyError: 'sr_number'")
    rec = json.loads(next(tmp_path.glob("capture_*.jsonl")).read_text().strip())
    assert rec["outcome"] == "error"
    assert "KeyError" in rec["error"]
    assert rec["returned_ids"] == []


def test_capture_outcome_never_raises(monkeypatch):
    monkeypatch.setattr(m, "_FULL_CAPTURE", True)
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", Path("/nonexistent\0/x"))
    m._capture_outcome("t", "substantive", m.time.monotonic())   # must not raise


@pytest.mark.asyncio
async def test_a_raised_tool_is_captured_then_re_raised(tmp_path, monkeypatch):
    """The failure must be recorded without swallowing it."""
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)
    monkeypatch.setattr(m, "_FULL_CAPTURE", True)

    async def _boom(name, arguments):
        raise ValueError("bad argument")

    monkeypatch.setattr(m, "_handle_call_tool_inner", _boom)
    with pytest.raises(ValueError):
        await m._dispatch_with_timeout("get_law", {"sr_number": "220"})
    rec = json.loads(next(tmp_path.glob("capture_*.jsonl")).read_text().strip())
    assert rec["outcome"] == "error" and "bad argument" in rec["error"]
