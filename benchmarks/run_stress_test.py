#!/usr/bin/env python3
"""Comprehensive stress test of mcp.opencaselaw.ch — every REST path, every
MCP tool, error paths, CORS, adversarial inputs, concurrency. Reports
pass/fail + p50/p95 latency per endpoint.
"""
from __future__ import annotations

import concurrent.futures as cf
import json
import re
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Callable

import httpx

BASE = "https://mcp.opencaselaw.ch"
ORIGIN = "https://peakprivacy.ch"  # exercise CORS on every call
TIMEOUT = 30.0

# Known-good sample identifiers (verified to exist in the corpus).
SAMPLE_BGER = "bger_4A_747_2012"
SAMPLE_BGE = "bge_BGE_140_III_86"
SAMPLE_MKG = "mkg_MKGE_16_Nr_1"
SAMPLE_LAW = "OR"  # Obligationenrecht
SAMPLE_ARTICLE = "41"


@dataclass
class Result:
    label: str
    method: str
    path: str
    status_expected: list[int]
    status_got: int = 0
    latency_ms: float = 0.0
    error: str = ""
    body_summary: str = ""

    @property
    def passed(self) -> bool:
        return self.status_got in self.status_expected and not self.error


RESULTS: list[Result] = []
LATENCIES_BY_GROUP: dict[str, list[float]] = {}


def record(r: Result, group: str = "") -> None:
    RESULTS.append(r)
    if group:
        LATENCIES_BY_GROUP.setdefault(group, []).append(r.latency_ms)


def http_call(
    method: str, url: str, *,
    params: dict | None = None,
    json_body: dict | None = None,
    headers: dict | None = None,
    expect: list[int] = [200],
    label: str = "",
    timeout: float = TIMEOUT,
) -> Result:
    h = {"Origin": ORIGIN, "Accept": "application/json"}
    if json_body is not None:
        h["Content-Type"] = "application/json"
    if headers:
        h.update(headers)
    r = Result(label=label or f"{method} {url}",
               method=method, path=url.replace(BASE, ""),
               status_expected=expect)
    t0 = time.perf_counter()
    try:
        with httpx.Client(timeout=timeout, follow_redirects=False) as c:
            resp = c.request(method, url, params=params, json=json_body, headers=h)
        r.status_got = resp.status_code
        r.latency_ms = (time.perf_counter() - t0) * 1000
        # Summarise body for reporting
        ct = resp.headers.get("content-type", "")
        if "json" in ct:
            try:
                data = resp.json()
                if isinstance(data, dict):
                    keys = list(data.keys())[:6]
                    r.body_summary = "dict keys=" + ",".join(keys)
                elif isinstance(data, list):
                    r.body_summary = f"list len={len(data)}"
            except Exception:
                r.body_summary = f"invalid-json:{resp.text[:80]}"
        else:
            r.body_summary = f"{ct} size={len(resp.content)}"
    except Exception as e:
        r.status_got = 0
        r.latency_ms = (time.perf_counter() - t0) * 1000
        r.error = f"{type(e).__name__}: {e}"
    return r


# ============================================================
# Section A — REST: every path with a realistic input
# ============================================================
def section_a_rest_paths() -> None:
    print("\n── A. REST paths (every endpoint, realistic input)")
    cases: list[tuple[str, str, dict]] = [
        # Case Law
        ("GET", "/api/decisions",              {"params": {"query": "Mietrecht", "limit": 3}}),
        ("GET", f"/api/decisions/{SAMPLE_BGER}", {"params": {"full_text": "false"}}),
        ("GET", "/api/courts",                 {}),
        ("GET", "/api/statistics",             {}),
        # Citation graph
        ("GET", f"/api/citations/{SAMPLE_BGER}", {}),
        ("GET", f"/api/appeal-chain/{SAMPLE_BGER}", {}),
        ("GET", "/api/leading-cases",          {"params": {"law_code": SAMPLE_LAW, "article": SAMPLE_ARTICLE, "limit": 3}}),
        # Analysis
        ("GET", "/api/trends",                 {"params": {"query": "Mietrecht"}}),
        ("POST","/api/mock-decision",          {"json_body": {"facts": "Sample facts", "legal_area": "Vertragsrecht"}}),
        # Statutes
        ("GET", "/api/laws/search",            {"params": {"query": "Obligationenrecht", "limit": 3}}),
        ("GET", f"/api/laws/{SAMPLE_LAW}",     {"params": {"article": SAMPLE_ARTICLE}}),
        ("GET", "/api/amendment-ref",          {"params": {"as_ref": "AS 2020 1"}, "expect":[200,404,422]}),
        # Commentaries
        ("GET", "/api/commentaries/search",    {"params": {"query": "Miete", "limit": 3}}),
        ("GET", "/api/commentaries/OR",        {"params": {"article": SAMPLE_ARTICLE}, "expect":[200,404]}),
        # Materialien
        ("GET", "/api/materialien",            {"params": {"query": "Mietrecht", "limit": 3}}),
        ("GET", "/api/materialien/OR",         {"params": {"article": SAMPLE_ARTICLE}, "expect":[200,404]}),
        # Legislation
        ("GET", "/api/legislation/search",     {"params": {"query": "Mietrecht", "limit": 3}}),
        ("GET", "/api/legislation/changes",    {}),
        ("GET", "/api/legislation/7020",       {"expect": [200, 404]}),
        # Research
        ("GET", "/api/doctrine",               {"params": {"query": "Schadenersatz"}}),
        ("GET", f"/api/case-brief/{SAMPLE_BGER}", {}),
        ("GET", "/api/exam-question",          {"params": {"topic": "Mietrecht"}, "expect":[200,500]}),  # slow + Haiku; 500 acceptable if Haiku miss
        # Citation Integrity (new today)
        ("GET", "/api/cite",                   {"params": {"reference": "BGE 140 III 86", "pinpoint": "2.3"}}),
        ("POST","/api/attest",                 {"json_body": {"draft_text": "Per BGE 140 III 86 E. 4 gilt X."}}),
        ("POST","/api/verify-claim",           {"json_body": {"claim": "Die Auslegung des Vertreters wird dem Vertretenen zugerechnet.", "decision_id": SAMPLE_BGE, "pinpoint": "4"}, "timeout": 30}),
        # Decision Structure (new today)
        (f"GET", f"/api/erwaegung/{SAMPLE_BGER}/2", {}),
        (f"GET", f"/api/regeste/{SAMPLE_BGER}", {}),
        (f"GET", f"/api/structure/{SAMPLE_BGER}", {}),
        # Spec + docs UIs
        ("GET", "/api/openapi.json",           {}),
        ("GET", "/api/docs",                   {}),
        ("GET", "/api/redoc",                  {}),
    ]
    for method, path, kwargs in cases:
        url = BASE + path
        expect = kwargs.pop("expect", [200])
        timeout = kwargs.pop("timeout", TIMEOUT)
        r = http_call(method, url, expect=expect, timeout=timeout, **kwargs)
        record(r, group="rest")
        icon = "✓" if r.passed else "✗"
        print(f"  {icon} {method:<5} {path:<45} {r.status_got} {r.latency_ms:>6.0f}ms {r.body_summary[:45]}")


# ============================================================
# Section B — MCP tools via JSON-RPC
# ============================================================
def _mcp(tool: str, args: dict, timeout: float = 30.0) -> Result:
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": tool, "arguments": args}}
    r = Result(label=f"mcp/{tool}", method="POST", path="/",
               status_expected=[200])
    t0 = time.perf_counter()
    try:
        with httpx.Client(timeout=timeout) as c:
            resp = c.post(BASE + "/", json=body,
                          headers={"Content-Type": "application/json",
                                   "Accept": "application/json, text/event-stream",
                                   "Origin": ORIGIN})
        r.status_got = resp.status_code
        r.latency_ms = (time.perf_counter() - t0) * 1000
        m = re.search(r"data: (.+)", resp.text, re.DOTALL)
        if m:
            payload = json.loads(m.group(1).split("\n\n", 1)[0])
            res = payload.get("result", {})
            is_err = res.get("isError", False)
            content = res.get("content", [{}])[0].get("text", "")
            r.body_summary = f"isError={is_err} len={len(content)}"
            if is_err:
                r.error = f"tool returned isError: {content[:80]}"
        else:
            r.error = "no data chunk"
    except Exception as e:
        r.status_got = 0
        r.latency_ms = (time.perf_counter() - t0) * 1000
        r.error = f"{type(e).__name__}: {e}"
    return r


def section_b_mcp_tools() -> None:
    print("\n── B. MCP tools (one call per tool via JSON-RPC)")
    tools: list[tuple[str, dict, float]] = [
        ("search_decisions",         {"query": "Mietrecht", "limit": 2}, 15),
        ("get_decision",             {"decision_id": SAMPLE_BGER}, 15),
        ("get_case_brief",           {"case": SAMPLE_BGER}, 15),
        ("list_courts",              {}, 10),
        ("get_statistics",           {}, 10),
        ("find_citations",           {"decision_id": SAMPLE_BGER, "limit": 3}, 15),
        ("find_appeal_chain",        {"decision_id": SAMPLE_BGER}, 15),
        ("find_leading_cases",       {"law_code": SAMPLE_LAW, "article": SAMPLE_ARTICLE, "limit": 3}, 15),
        ("analyze_legal_trend",      {"query": "Mietrecht"}, 15),
        ("get_law",                  {"abbreviation": SAMPLE_LAW, "article": SAMPLE_ARTICLE}, 10),
        ("search_laws",              {"query": "Obligationen", "limit": 3}, 10),
        ("get_commentary",           {"abbreviation": SAMPLE_LAW, "article": SAMPLE_ARTICLE}, 10),
        ("search_commentaries",      {"query": "Miete", "limit": 3}, 10),
        ("get_legislation",          {"lexfind_id": 7020}, 10),
        ("search_legislation",       {"query": "Mietrecht", "limit": 3}, 15),
        ("browse_legislation_changes", {}, 10),
        ("get_materialien",          {"law_code": SAMPLE_LAW, "article": SAMPLE_ARTICLE}, 10),
        ("search_materialien",       {"query": "Mietrecht", "limit": 3}, 10),
        ("get_doctrine",             {"query": "Schadenersatz"}, 30),
        ("get_decision_structure",   {"decision_id": SAMPLE_BGER}, 10),
        ("get_erwaegung",            {"decision_id": SAMPLE_BGER, "e_number": "2"}, 10),
        ("get_regeste",              {"decision_id": SAMPLE_BGER}, 10),
        ("cite",                     {"reference": "BGE 140 III 86", "pinpoint": "2.3"}, 10),
        ("attest_response",          {"draft_text": "Per BGE 140 III 86 E. 4 gilt X."}, 15),
        ("check_claim_support",      {"claim": "Die Auslegung des Vertreters wird dem Vertretenen zugerechnet.",
                                       "decision_id": SAMPLE_BGE, "pinpoint": "4"}, 30),
        ("draft_mock_decision",      {"facts": "Sample facts", "legal_area": "Vertragsrecht"}, 30),
        ("generate_exam_question",   {"topic": "Mietrecht"}, 30),
    ]
    for tool, args, tmo in tools:
        time.sleep(1.1)  # respect nginx mcp_sse 1r/s limit
        r = _mcp(tool, args, timeout=tmo)
        record(r, group="mcp")
        icon = "✓" if r.passed else "✗"
        print(f"  {icon} {tool:<28} {r.status_got} {r.latency_ms:>6.0f}ms {r.body_summary[:55]}")


# ============================================================
# Section C — Error / negative paths
# ============================================================
def section_c_errors() -> None:
    print("\n── C. Error / negative paths (expect graceful failures)")
    cases = [
        ("GET", "/api/decisions/DOES_NOT_EXIST", {}, [404]),
        ("GET", "/api/cite", {"params": {"reference": ""}}, [200, 422]),  # empty ref
        ("POST","/api/attest", {"json_body": {}}, [422]),
        ("POST","/api/attest", {"json_body": {"draft_text": ""}}, [200]),
        ("POST","/api/verify-claim", {"json_body": {"claim": "x", "decision_id": "FABRICATED_ID_999"}}, [200]),
        ("GET", "/api/erwaegung/DOES_NOT_EXIST/1", {}, [200]),
        ("GET", "/api/leading-cases", {"params": {}}, [200]),  # no filters
        ("GET", "/api/decisions", {"params": {"query": "test", "limit": 9999}}, [200, 422]),  # over limit
        ("GET", "/api/decisions", {"params": {"query": "test", "limit": -1}}, [422]),
        ("GET", "/api/decisions", {"params": {"query": "test", "date_from": "not-a-date"}}, [200, 422]),
    ]
    for method, path, kwargs, expect in cases:
        url = BASE + path
        r = http_call(method, url, expect=expect, **kwargs)
        record(r, group="err")
        icon = "✓" if r.passed else "✗"
        print(f"  {icon} {method:<5} {path:<45} {r.status_got} {r.latency_ms:>6.0f}ms exp={expect}")


# ============================================================
# Section D — CORS preflight across methods
# ============================================================
def section_d_cors() -> None:
    print("\n── D. CORS preflight (OPTIONS) from a foreign origin")
    endpoints = ["/api/openapi.json", "/api/decisions", "/api/cite",
                 "/api/attest", "/api/verify-claim", "/mcp", "/sse", "/messages/"]
    for path in endpoints:
        r = Result(label=f"OPTIONS {path}", method="OPTIONS", path=path,
                   status_expected=[200, 204])
        t0 = time.perf_counter()
        try:
            with httpx.Client(timeout=10) as c:
                resp = c.request("OPTIONS", BASE + path, headers={
                    "Origin": ORIGIN,
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "Content-Type",
                })
            r.status_got = resp.status_code
            r.latency_ms = (time.perf_counter() - t0) * 1000
            acao = resp.headers.get("access-control-allow-origin", "")
            r.body_summary = f"ACAO={acao!r}"
            if acao != "*":
                r.error = f"ACAO not wildcard: {acao!r}"
        except Exception as e:
            r.error = str(e)
            r.latency_ms = (time.perf_counter() - t0) * 1000
        record(r, group="cors")
        icon = "✓" if r.passed else "✗"
        print(f"  {icon} OPTIONS {path:<40} {r.status_got} {r.body_summary}")


# ============================================================
# Section E — Adversarial / special-char inputs
# ============================================================
def section_e_adversarial() -> None:
    print("\n── E. Adversarial inputs (injection, XSS, unicode, huge)")
    cases = [
        ("SQL-ish in query",     "/api/decisions?query=" + "' OR 1=1--"),
        ("XSS in query",         "/api/decisions?query=%3Cscript%3Ealert(1)%3C/script%3E"),
        ("Null byte",            "/api/decisions?query=%00"),
        ("Very long query (5k)", "/api/decisions?query=" + "A" * 5000),
        ("Emoji in query",       "/api/decisions?query=%F0%9F%98%80"),
        ("FTS5 reserved",        "/api/decisions?query=AND+OR+NOT"),
        ("Hyphen compound",      "/api/decisions?query=%C3%B6ffentlich-rechtliche"),
        ("Art.-with-dot",        "/api/cite?reference=Art.+41+OR"),
        ("Double quotes",        '/api/decisions?query=%22Treu+und+Glauben%22'),
        ("Path traversal attempt","/api/decisions/../admin"),
    ]
    for label, path in cases:
        url = BASE + path
        # Accept 200 (sanitised), 400, 404 — any graceful non-500 is fine.
        r = http_call("GET", url, expect=[200, 400, 404, 422])
        r.label = label
        record(r, group="adv")
        icon = "✓" if r.passed else "✗"
        print(f"  {icon} {label:<30} {r.status_got} {r.latency_ms:>6.0f}ms")


# ============================================================
# Section F — Concurrency burst
# ============================================================
def section_f_concurrency() -> None:
    print("\n── F. Concurrency — 20 parallel search_decisions + 4 parallel verify-claim")

    def one_search():
        return http_call("GET", BASE + "/api/decisions",
                         params={"query": "Mietrecht", "limit": 1})

    def one_verify():
        return http_call("POST", BASE + "/api/verify-claim",
                         json_body={"claim": "Test-Behauptung.",
                                    "decision_id": SAMPLE_BGE,
                                    "pinpoint": "4"},
                         timeout=45)

    with cf.ThreadPoolExecutor(max_workers=20) as ex:
        search_results = list(ex.map(lambda _: one_search(), range(20)))
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        verify_results = list(ex.map(lambda _: one_verify(), range(4)))

    ok_search = sum(1 for r in search_results if r.passed)
    ok_verify = sum(1 for r in verify_results if r.passed)
    p50_s = statistics.median(r.latency_ms for r in search_results)
    p95_s = sorted(r.latency_ms for r in search_results)[int(20 * 0.95) - 1]
    p50_v = statistics.median(r.latency_ms for r in verify_results)

    print(f"  search_decisions  × 20:  {ok_search}/20 OK   p50={p50_s:.0f}ms   p95={p95_s:.0f}ms")
    print(f"  verify-claim       × 4:  {ok_verify}/4 OK    p50={p50_v:.0f}ms")
    for r in search_results + verify_results:
        record(r, group="concurrency")


# ============================================================
# Section G — post-stress health
# ============================================================
def section_g_health() -> None:
    print("\n── G. Post-stress health")
    for path in ("/health",):
        with httpx.Client(timeout=10) as c:
            resp = c.get(BASE + path)
        print(f"  GET {path}: {resp.status_code}  body: {resp.text[:120]}")


# ============================================================
# Summary
# ============================================================
def summary() -> int:
    print("\n══════════════════════════════════════════════════════════════")
    print("  SUMMARY")
    print("══════════════════════════════════════════════════════════════")
    total = len(RESULTS)
    passed = sum(1 for r in RESULTS if r.passed)
    failed = total - passed
    print(f"  Total tests:    {total}")
    print(f"  Pass:           {passed}  ({passed / total * 100:.1f}%)")
    print(f"  Fail:           {failed}")
    print()
    for group, lats in LATENCIES_BY_GROUP.items():
        p50 = statistics.median(lats)
        p95 = sorted(lats)[int(len(lats) * 0.95) - 1] if len(lats) >= 20 else max(lats)
        print(f"  {group:<14}  n={len(lats):<4}  p50={p50:>6.0f}ms  p95={p95:>6.0f}ms")
    print()
    if failed:
        print("  ✗ FAILURES:")
        for r in RESULTS:
            if not r.passed:
                print(f"    - {r.method} {r.path}  expected {r.status_expected}, got {r.status_got}  {r.error or r.body_summary[:80]}")
    return failed


if __name__ == "__main__":
    section_a_rest_paths()
    section_b_mcp_tools()
    section_c_errors()
    section_d_cors()
    section_e_adversarial()
    section_f_concurrency()
    section_g_health()
    sys.exit(summary())
