"""Full-capture mode: everything the user sends, minus the two carve-outs.

Policy 2026-08-19: for a first evaluation period, gather every data
point, then prune by measured usefulness. The mode is OFF by default and
env-gated — the amended /datenschutz/ must be live before the flag is
set, never the reverse. The carve-outs are structural, not
configuration: document bodies (client work product; the add-in's own
policy says they are not stored) and credentials never land on disk.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def test_capture_is_off_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)
    monkeypatch.setattr(m, "_FULL_CAPTURE", False)
    m._capture_event({"src": "mcp", "tool": "x"})
    assert not list(tmp_path.iterdir()), "no capture without the flag"


def test_capture_writes_one_json_line_when_enabled(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)
    monkeypatch.setattr(m, "_FULL_CAPTURE", True)
    m._capture_event({"src": "rest", "tool": "search_decisions",
                      "params": {"query": "Mietzins Zürich"}})
    files = list(tmp_path.glob("capture_*.jsonl"))
    assert len(files) == 1
    rec = json.loads(files[0].read_text().strip())
    assert rec["params"]["query"] == "Mietzins Zürich", \
        "full query text is the point of the period"


def test_document_bodies_are_never_captured():
    """attest/verify-claim carry draft documents. Length, never content."""
    body = "x" * 500
    out = m._capture_args({"response_text": body,
                           "claim": "some claim text",
                           "query": "Mietrecht", "limit": 5})
    assert "response_text" not in out and "claim" not in out
    assert out["response_text_len"] == 500
    assert out["claim_len"] == len("some claim text")
    assert out["query"] == "Mietrecht"     # queries ARE captured
    assert out["limit"] == 5


def test_attest_responses_draft_text_is_a_body():
    """The one that got missed. attest_response names its document
    `draft_text`; it was absent from the set, and 166 drafted legal
    opinions reached disk — including a named taxpayer and successive
    revisions of a criminal defence."""
    out = m._capture_args({"draft_text": "Nach Art. 9 DSG..." * 40,
                           "audit_grounding": True})
    assert "draft_text" not in out
    assert out["draft_text_len"] > 0
    assert out["audit_grounding"] is True


def test_a_long_string_is_a_body_whatever_it_is_called():
    """A denylist of names cannot be trusted alone — that is precisely how
    draft_text got through. Length is the backstop, so the next unlisted
    parameter fails safe instead of silently landing on disk."""
    out = m._capture_args({"unforeseen_new_param": "y" * 5000})
    assert "unforeseen_new_param" not in out
    assert out["unforeseen_new_param_len"] == 5000


def test_a_real_query_is_still_captured_in_full():
    """The backstop must not eat the data the notice says we collect. The
    longest query seen in a day of live traffic was 214 characters."""
    q = ("Bearbeitung besonders schützenswerter Personendaten Gesundheit "
         "Einwilligung Amtsgeheimnis Entbindung Sozialhilfe " * 2)
    assert len(q) < m._CAPTURE_MAX_ARG_CHARS
    out = m._capture_args({"query": q})
    assert out["query"] == q


def test_credentials_are_redacted():
    out = m._capture_args({"license_key": "OCL-PRO-123", "query": "x",
                           "Authorization": "Bearer abc"})
    assert out["license_key"] == "<redacted>"
    assert out["Authorization"] == "<redacted>"


def test_capture_never_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(m, "_FULL_CAPTURE", True)
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path / "no\0dir")
    m._capture_event({"src": "mcp"})   # must not raise


def test_no_ip_field_in_the_mcp_capture_site():
    """The carve-out in code, pinned: the MCP capture record carries sid
    and client class, never the IP the handler also has in scope."""
    import ast
    import inspect
    src = inspect.getsource(m._handle_call_tool_inner)
    for node in ast.walk(ast.parse(src)):
        if (isinstance(node, ast.Call)
                and getattr(node.func, "id", "") == "_capture_event"):
            keys = {k.value for d in ast.walk(node) if isinstance(d, ast.Dict)
                    for k in d.keys if isinstance(k, ast.Constant)}
            assert "sid" in keys and "ip" not in keys
            return
    raise AssertionError("MCP capture site not found")


def test_export_is_crawler_only_without_a_session():
    """The classifier and the interaction join must agree about
    export_decision: a session-less bulk export is a crawler, the same
    tool inside a session is an agent reading a result it found (which
    the join counts as a fetch)."""
    assert m._traffic_class(None, "rest", "export_decision", False) == "crawler"
    assert m._traffic_class("claude.ai", "mcp", "export_decision", True) == "agent"


def test_traffic_segments():
    assert m._traffic_class("bot", "rest", "search_decisions", False) == "crawler"
    assert m._traffic_class("claude.ai", "mcp", "search_decisions", True) == "agent"
    assert m._traffic_class("chatgpt", "rest", "search_decisions", False) == "direct"


def test_result_set_id_is_opaque_and_unique():
    a, b = m._new_result_set_id(), m._new_result_set_id()
    assert a.startswith("rs_") and a != b and len(a) > 8
