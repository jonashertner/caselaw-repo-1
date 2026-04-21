#!/usr/bin/env python3
"""Run the hallucination-probe golden set against a live MCP endpoint.

Tool-level audit: feeds each probe's input to cite / attest_response /
check_claim_support via the public JSON-RPC endpoint and compares the
response against the probe's `expect` block. Produces a pass/fail
summary plus a per-probe breakdown.

This is NOT an end-to-end LLM test (which would require driving an
actual client like claude.ai). It measures whether our SERVER-SIDE
tools return the right signals so that a well-behaved LLM following
the server instructions WOULD NOT hallucinate. A future extension can
add a client-driven harness that actually runs an LLM through the
probes and measures fabrication rate in its final output.

Usage:
    python3 benchmarks/run_hallucination_probe.py
    python3 benchmarks/run_hallucination_probe.py --endpoint http://127.0.0.1:8770/
    python3 benchmarks/run_hallucination_probe.py --probes benchmarks/hallucination_probe_v1.json
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import httpx


DEFAULT_ENDPOINT = "https://mcp.opencaselaw.ch/"
DEFAULT_PROBES = "benchmarks/hallucination_probe_v1.json"


def _parse_sse_response(raw: str) -> dict:
    """Extract the JSON-RPC result from the SSE-formatted MCP response."""
    m = re.search(r"data: (.+)", raw, re.DOTALL)
    if not m:
        raise ValueError(f"No 'data:' chunk in response: {raw[:200]!r}")
    # The "data:" line may be multi-line; grab only the first JSON object.
    payload = m.group(1).split("\n\n", 1)[0].strip()
    return json.loads(payload)


def _mcp_call(endpoint: str, tool: str, arguments: dict, timeout: float = 30.0) -> dict:
    """Invoke an MCP tool via JSON-RPC over HTTP, return the parsed result."""
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": arguments},
    }
    with httpx.Client(timeout=timeout) as client:
        resp = client.post(
            endpoint,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            },
            json=body,
        )
        resp.raise_for_status()
    envelope = _parse_sse_response(resp.text)
    # The tool's own JSON is the .result.content[0].text string (not a dict).
    content = envelope.get("result", {}).get("content", [])
    if not content:
        return {}
    text = content[0].get("text", "")
    # If the tool returns JSON, parse it; otherwise return as markdown string.
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Peel off the closing OpenCaseLaw footer, then try again.
        cleaned = text.rsplit("\n\n---\nℹ️", 1)[0]
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return {"_markdown": text}


def _check_expect(response: dict, expect: dict, extras: dict | None = None) -> tuple[bool, str]:
    """Compare a tool response against the probe's expect block.

    Returns (passed, detail). Supported expect keys:
      - exists:             bool
      - exists_any:         true (accepts any)
      - close_matches_nonempty: bool
      - close_matches_includes_real: expected decision_id in close_matches
      - ok:                 bool
      - citations_found:    int
      - issues_count_min:   int
      - issue_problem:      str (first issue's problem must equal)
      - has_error:          bool (response has an 'error' key)
      - expect_supports_in: list[str] (check_claim_support's supports field ∈ list)
      - citation_string_contains: substring match on any of the DE/FR/IT strings
    """
    if "has_error" in expect:
        if expect["has_error"] != ("error" in response):
            return False, f"expected has_error={expect['has_error']}; got error={response.get('error')!r}"
    if "exists" in expect:
        if response.get("exists") != expect["exists"]:
            return False, f"expected exists={expect['exists']}; got {response.get('exists')!r}"
    if expect.get("exists_any"):
        if "exists" not in response:
            return False, f"response has no 'exists' key: {list(response)}"
    if expect.get("close_matches_nonempty"):
        cms = response.get("close_matches") or []
        if not cms:
            return False, "expected non-empty close_matches"
    if "close_matches_includes_real" in expect:
        target = expect["close_matches_includes_real"]
        cms = response.get("close_matches") or []
        if not any(cm.get("decision_id") == target for cm in cms):
            return False, f"expected {target} in close_matches; got {[c.get('decision_id') for c in cms]}"
    if "ok" in expect:
        if response.get("ok") != expect["ok"]:
            return False, f"expected ok={expect['ok']}; got {response.get('ok')!r}"
    if "citations_found" in expect:
        if response.get("citations_found") != expect["citations_found"]:
            return False, f"expected citations_found={expect['citations_found']}; got {response.get('citations_found')}"
    if "issues_count_min" in expect:
        if (response.get("issues_count") or 0) < expect["issues_count_min"]:
            return False, f"expected ≥{expect['issues_count_min']} issues; got {response.get('issues_count')}"
    if "issue_problem" in expect:
        issues = response.get("issues") or []
        if not issues or issues[0].get("problem") != expect["issue_problem"]:
            return False, f"expected first issue.problem={expect['issue_problem']!r}; got {issues[:1]}"
    if "expect_supports_in" in expect:
        supports = response.get("supports")
        if supports not in expect["expect_supports_in"]:
            return False, f"expected supports ∈ {expect['expect_supports_in']}; got {supports!r}"
    if "citation_string_contains" in expect:
        needle = expect["citation_string_contains"]
        hay = " ".join(str(response.get(f, "")) for f in (
            "citation_string", "citation_string_de", "citation_string_fr", "citation_string_it"
        ))
        if needle not in hay:
            return False, f"expected {needle!r} in some citation_string; got {hay!r}"
    return True, "ok"


def run_probes(endpoint: str, probes: list[dict]) -> dict:
    results: list[dict] = []
    passed = failed = skipped = 0
    started = time.time()

    for probe in probes:
        probe_id = probe["id"]
        tool = probe["tool"]
        args = probe["arguments"]
        expect = probe.get("expect", {})
        if "expect_supports_in" in probe:
            expect["expect_supports_in"] = probe["expect_supports_in"]

        t0 = time.time()
        try:
            response = _mcp_call(endpoint, tool, args)
        except Exception as e:
            results.append({
                "id": probe_id, "tool": tool, "status": "ERROR",
                "detail": f"{type(e).__name__}: {e}",
                "elapsed": time.time() - t0,
            })
            failed += 1
            continue
        elapsed = time.time() - t0

        if not expect:
            results.append({
                "id": probe_id, "tool": tool, "status": "SKIP",
                "detail": "no expect block — smoke test only", "elapsed": elapsed,
            })
            skipped += 1
            continue

        ok, detail = _check_expect(response, expect)
        results.append({
            "id": probe_id, "tool": tool, "status": "PASS" if ok else "FAIL",
            "detail": detail if not ok else "",
            "category": probe.get("category", ""),
            "elapsed": elapsed,
        })
        if ok:
            passed += 1
        else:
            failed += 1

    return {
        "endpoint": endpoint,
        "total": len(probes),
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "total_elapsed_s": round(time.time() - started, 2),
        "pass_rate": round(passed / max(1, passed + failed), 3),
        "results": results,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    p.add_argument("--probes", default=DEFAULT_PROBES)
    p.add_argument("--output", default=None,
                   help="Save full JSON results here (default: stdout)")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    probes_data = json.loads(Path(args.probes).read_text())
    probes = probes_data["probes"]

    if not args.quiet:
        print(f"Running {len(probes)} probes against {args.endpoint} …",
              file=sys.stderr)

    report = run_probes(args.endpoint, probes)

    # Console summary
    if not args.quiet:
        print(f"\n{'─' * 70}", file=sys.stderr)
        print(f"  TOTAL {report['total']}  ·  "
              f"PASS {report['passed']}  ·  FAIL {report['failed']}  ·  "
              f"SKIP {report['skipped']}  ·  "
              f"pass_rate {report['pass_rate'] * 100:.1f}%  ·  "
              f"elapsed {report['total_elapsed_s']}s",
              file=sys.stderr)
        print("─" * 70, file=sys.stderr)
        for r in report["results"]:
            icon = {"PASS": "✓", "FAIL": "✗", "SKIP": "·", "ERROR": "!"}.get(r["status"], "?")
            print(f"  {icon} {r['id']:<5} {r['tool']:<22} {r['status']:<5} "
                  f"{r['detail'][:80]}",
                  file=sys.stderr)

    payload = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        Path(args.output).write_text(payload)
        if not args.quiet:
            print(f"\nFull report → {args.output}", file=sys.stderr)
    else:
        print(payload)

    return 0 if report["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
