"""get_decisions — batch retrieval (2026-07-29).

Built from telemetry, not assumption. Production /metrics/all showed
get_decision at 90.2% of all tool calls (31,896 calls, ~20 per search),
while cite — which the MCP spec review and I both assumed was the driver —
was 0.27% (95 calls). A research answer reading ten decisions cost ten
model turns; clients cap tool calls per turn, which is what users hit.

Additive by design: the single-decision get_decision path carries that 90%
of traffic and is not touched.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _dec(did, court="bger", **kw):
    d = {"decision_id": did, "court": court, "canton": "CH",
         "docket_number": "6B_1234/2025", "decision_date": "2025-05-04",
         "language": "de", "title": "Testentscheid",
         "regeste": "Leitsatz zum Testfall.",
         "full_text": "A" * 60000}
    d.update(kw)
    return d


def test_batch_returns_all_requested(monkeypatch):
    store = {"a": _dec("a"), "b": _dec("b"), "c": _dec("c")}
    monkeypatch.setattr(m, "get_decision_by_id", lambda x: store.get(x))
    out = m._handle_get_decisions(decision_ids=["a", "b", "c"])
    assert "Retrieved 3 of 3" in out
    assert out.count("## ") == 3


def test_missing_ids_reported_without_failing_the_batch(monkeypatch):
    monkeypatch.setattr(m, "get_decision_by_id",
                        lambda x: _dec(x) if x == "good" else None)
    monkeypatch.setattr(m, "_overlay_enabled", lambda: False)
    out = m._handle_get_decisions(decision_ids=["good", "nope"])
    assert "Retrieved 1 of 2" in out
    assert "Not found: nope" in out
    assert "search_decisions" in out          # tells the model what to do next
    assert "## " in out                        # the good one still rendered


def test_citation_strings_come_from_the_pipeline(monkeypatch):
    """R1: the batch tool must not construct citations differently from the
    single tool."""
    monkeypatch.setattr(m, "get_decision_by_id", lambda x: _dec(x))
    out = m._handle_get_decisions(decision_ids=["a"])
    expected = m._build_citation_strings(_dec("a"))
    assert expected["citation_string_de"] in out
    assert expected["citation_string_fr"] in out
    assert f"]({expected['canonical_url']})" in out   # markdown link present
    assert "do NOT reconstruct" in out


def test_full_text_off_by_default(monkeypatch):
    monkeypatch.setattr(m, "get_decision_by_id", lambda x: _dec(x))
    out = m._handle_get_decisions(decision_ids=["a"])
    assert "A" * 1000 not in out
    assert "Regeste" in out
    assert "full_text=true" in out            # discloses how to get more


def test_full_text_is_excerpted_and_disclosed(monkeypatch):
    monkeypatch.setattr(m, "get_decision_by_id", lambda x: _dec(x))
    out = m._handle_get_decisions(decision_ids=["a"], full_text=True,
                                  max_chars_per_decision=5000)
    assert "first 5,000 of 60,000 chars" in out
    assert "operative part" in out            # the #55 disclosure pattern
    assert "Volltext" in out


def test_batch_size_is_capped(monkeypatch):
    monkeypatch.setattr(m, "get_decision_by_id", lambda x: _dec(x))
    out = m._handle_get_decisions(decision_ids=[str(i) for i in range(11)])
    assert "Too many ids (11)" in out
    assert "at most 10" in out


def test_empty_and_blank_input(monkeypatch):
    assert "at least one" in m._handle_get_decisions(decision_ids=[])
    assert "at least one" in m._handle_get_decisions(decision_ids=["", "  "])


def test_char_cap_is_bounded(monkeypatch):
    """A caller asking for 10M chars must not be able to blow the response."""
    monkeypatch.setattr(m, "get_decision_by_id", lambda x: _dec(x))
    out = m._handle_get_decisions(decision_ids=["a"], full_text=True,
                                  max_chars_per_decision=10_000_000)
    assert "first 50,000" in out              # clamped to the 50k ceiling
    out2 = m._handle_get_decisions(decision_ids=["a"], full_text=True,
                                   max_chars_per_decision="not-a-number")
    assert "first 20,000" in out2             # falls back to the default


def test_ecthr_batch_carries_attribution(monkeypatch):
    monkeypatch.setattr(m, "get_decision_by_id",
                        lambda x: _dec(x, court="ecthr_chamber", canton="CE"))
    out = m._handle_get_decisions(decision_ids=["a"])
    assert "© ECHR-CEDH" in out


def test_swiss_only_batch_has_no_attribution(monkeypatch):
    monkeypatch.setattr(m, "get_decision_by_id", lambda x: _dec(x))
    out = m._handle_get_decisions(decision_ids=["a"])
    assert "ECHR-CEDH" not in out


def test_tool_is_declared_and_within_description_budget():
    [t] = [x for x in m._list_tools() if x.name == "get_decisions"]
    assert len(t.description) <= 1024
    p = t.inputSchema["properties"]["decision_ids"]
    assert p["maxItems"] == 10 and p["minItems"] == 1
    assert t.inputSchema["properties"]["full_text"]["default"] is False
    # names the reason it exists, so the model prefers it
    assert "one tool call" in t.description or "ONE call" in t.description
