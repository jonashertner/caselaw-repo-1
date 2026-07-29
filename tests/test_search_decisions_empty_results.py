"""search_decisions must answer "nothing found", not raise.

Regression, found in production 2026-07-29 by probing every tool's empty
path: `_echr_note` was bound only where results existed, but read on the
shared return, so EVERY zero-result search returned

    Error: cannot access local variable '_echr_note' …

with isError=false — i.e. the model was handed an error string as if it were
an answer. The ECtHR-attribution work introduced it; no test covered the
zero-result path of the busiest search tool, and the attribution tests only
ever passed rows in.

These tests pin both halves: the empty path answers, and the attribution
still rides along when ECtHR rows are present.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402


def _call(args: dict):
    out = asyncio.run(mcp_server._handle_call_tool_inner("search_decisions", args))
    content = out[0] if isinstance(out, tuple) else out
    return "".join(c.text for c in content)


def _no_error(text: str):
    assert not text.lstrip().startswith("Error"), text[:300]
    assert "cannot access local variable" not in text, text[:300]


def test_zero_results_answers_instead_of_erroring(monkeypatch):
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **k: ([], 0))
    text = _call({"query": "zzqqxwv nichtwort 9182734"})
    _no_error(text)
    assert "No decisions found" in text


def test_zero_results_on_a_capped_total_answers(monkeypatch):
    """The lower-bound branch reaches the same return and was equally broken."""
    def _stub(**k):
        (k.get("meta") if k.get("meta") is not None else {})["total_is_lower_bound"] = True
        return ([], 5000)
    monkeypatch.setattr(mcp_server, "search_fts5", _stub)
    text = _call({"query": "x", "offset": 900})
    _no_error(text)
    assert "No decisions on this page" in text


def test_zero_results_compact_fields_answers(monkeypatch):
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **k: ([], 0))
    _no_error(_call({"query": "x", "fields": "compact"}))


def test_ecthr_attribution_still_appended_when_results_exist(monkeypatch):
    rows = [{
        "decision_id": "ecthr_001", "court": "ecthr_chamber",
        "docket_number": "30696/09", "decision_date": "2011-01-21",
        "language": "de", "title": "M.S.S.", "snippet": "Art. 3 EMRK",
    }]
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **k: (rows, 1))
    monkeypatch.setattr(mcp_server, "_pinpoint_enrich_results", lambda *a, **k: None)
    text = _call({"query": "Art. 3 EMRK"})
    _no_error(text)
    assert mcp_server._ECHR_ATTRIBUTION in text


def test_non_ecthr_results_carry_no_attribution(monkeypatch):
    rows = [{
        "decision_id": "bge_BGE_140_III_86", "court": "bge",
        "docket_number": "140 III 86", "decision_date": "2014-01-01",
        "language": "de", "title": "X gegen Y", "snippet": "Mietrecht",
    }]
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **k: (rows, 1))
    monkeypatch.setattr(mcp_server, "_pinpoint_enrich_results", lambda *a, **k: None)
    text = _call({"query": "Mietrecht"})
    _no_error(text)
    assert mcp_server._ECHR_ATTRIBUTION not in text
