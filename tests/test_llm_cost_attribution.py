"""LLM cost attribution + per-IP quota on the Sonnet-backed MCP tools.

Until 2026-08-23, answering "who is using check_claim_support" took a
forensic elimination across four log systems: llm_usage carried no
source, the MCP path had no per-IP cap (the REST twins were 429-gated for
months), and no record joined IP to LLM cost. Two burst days (08-18,
08-20) consumed two-thirds of a week's Sonnet spend unattributed.

Contracts pinned here: usage records carry source+client; edge-originated
calls also land in the 90-day per-IP ledger while internal calls never
do; the MCP gate shares the REST quota buckets, throttles with the
custom-access contact, gates attest_response only when grounding is
requested, and fails open.
"""
from __future__ import annotations

import json
import re
import sys
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


@pytest.fixture
def logs(tmp_path, monkeypatch):
    usage = tmp_path / "usage.jsonl"
    ledger = tmp_path / "ledger.jsonl"
    monkeypatch.setattr(m, "LLM_USAGE_LOG_PATH", usage)
    monkeypatch.setattr(m, "LLM_LEDGER_LOG_PATH", ledger)
    return usage, ledger


def _fire(feature="check_claim_support"):
    m._llm_usage_log(model="claude-sonnet-4-6", feature=feature,
                     response_json={"usage": {"input_tokens": 1000,
                                              "output_tokens": 100}})


def _rows(p: Path):
    return [json.loads(l) for l in p.open()] if p.exists() else []


def test_mcp_call_lands_in_both_files_with_attribution(logs):
    usage, ledger = logs
    m._ctx_llm_source.set("mcp")
    m._ctx_client_ip.set("203.0.113.7")
    m._ctx_client_ua.set("python-httpx/0.27")
    try:
        _fire()
    finally:
        m._ctx_llm_source.set("internal")
        m._ctx_client_ip.set("")
        m._ctx_client_ua.set("")
    u = _rows(usage)[-1]
    assert u["source"] == "mcp" and u["client"] != "-"
    assert "ip" not in u                       # usage file stays impersonal
    l = _rows(ledger)[-1]
    # The ledger carries a daily-rotating pseudonym, never the address
    # (2026-08-26): it was the address half of a timestamp join against the
    # query-bearing search traces. See tests/test_ip_pseudonym_ledger.py.
    assert "ip" not in l, "ledger must not persist a raw address"
    assert re.fullmatch(r"[0-9a-f]{16}", l["ip_pseudonym"])
    assert "203.0.113.7" not in json.dumps(l)
    assert l["cost_usd"] > 0


def test_internal_call_never_reaches_the_ledger(logs):
    usage, ledger = logs
    m._ctx_llm_source.set("internal")
    m._ctx_client_ip.set("203.0.113.7")        # even with an IP in context
    try:
        _fire()
    finally:
        m._ctx_client_ip.set("")
    assert _rows(usage)[-1]["source"] == "internal"
    assert not ledger.exists()


class _Deny:
    allowed, calls, limit, label = False, 201, 200, None


class _Allow:
    allowed, calls, limit, label = True, 3, 200, None


def _dispatch(name, arguments, quota_result):
    """Run only the quota-gate prologue of the dispatch via its pieces."""
    calls = {}

    def _check(ip, endpoint, api_key=None):
        calls["args"] = (ip, endpoint)
        return quota_result

    fake = types.SimpleNamespace(check_and_increment=_check)
    return fake, calls


def test_gate_throttles_check_claim_support(monkeypatch, logs):
    fake, calls = _dispatch("check_claim_support", {}, _Deny())
    monkeypatch.setattr(m, "_mcp_quota", fake)
    monkeypatch.setattr(m, "_MCP_QUOTA_AVAILABLE", True)
    m._ctx_client_ip.set("203.0.113.9")
    try:
        import asyncio
        out = asyncio.run(m._handle_call_tool_inner(
            "check_claim_support", {"claim": "x", "decision_id": "y"}))
    finally:
        m._ctx_client_ip.set("")
    text = out[0].text
    assert "Daily limit reached" in text
    assert "team@jonashertner.com" in text
    assert calls["args"] == ("203.0.113.9", "verify_claim")   # shared bucket


def test_ungrounded_attest_is_not_gated(monkeypatch):
    """attest without audit_grounding costs no Sonnet call — it must never
    burn quota."""
    hit = {}
    fake = types.SimpleNamespace(
        check_and_increment=lambda **kw: hit.setdefault("called", True) or _Deny())
    monkeypatch.setattr(m, "_mcp_quota", fake)
    monkeypatch.setattr(m, "_MCP_QUOTA_AVAILABLE", True)
    m._ctx_client_ip.set("203.0.113.9")
    try:
        import asyncio
        out = asyncio.run(m._handle_call_tool_inner(
            "attest_response", {"draft_text": "Kein Zitat."}))
    finally:
        m._ctx_client_ip.set("")
    assert "called" not in hit
    assert "Daily limit reached" not in out[0].text


def test_quota_failure_fails_open(monkeypatch):
    def boom(**kw):
        raise RuntimeError("quota db on fire")
    monkeypatch.setattr(m, "_mcp_quota",
                        types.SimpleNamespace(check_and_increment=boom))
    monkeypatch.setattr(m, "_MCP_QUOTA_AVAILABLE", True)
    m._ctx_client_ip.set("203.0.113.9")
    try:
        import asyncio
        out = asyncio.run(m._handle_call_tool_inner(
            "check_claim_support", {"claim": "x", "decision_id": "nonexistent"}))
    finally:
        m._ctx_client_ip.set("")
    assert "Daily limit reached" not in out[0].text   # served, not throttled
