#!/usr/bin/env python3
"""Live contract check for the research CLI against production (or --base URL).

Runs the client from clients/python/src (no install needed) through the calls a
user makes and checks the MCP/REST contract points the release depends on. Read
only, bounded (about 12 requests), exits 1 on any failure. Not part of `make test`
(network); run it with `make smoke-cli` after a deploy or on a timer.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "clients" / "python" / "src"
SIX = ("search_decisions", "get_decision", "get_erwaegung", "get_law", "find_citations", "cite")


def ocl(base: str, *args: str) -> tuple[int, str, str]:
    proc = subprocess.run([sys.executable, "-m", "opencaselaw_cli", "--base-url", base, "--timeout", "30", "--retries", "0", *args],
                          capture_output=True, text=True, env={"PYTHONPATH": str(SRC), "PATH": "/usr/bin:/bin", "NO_COLOR": "1"})
    return proc.returncode, proc.stdout, proc.stderr


def mcp(base: str, method: str, params: dict, sid: str | None = None):
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    if sid:
        headers["Mcp-Session-Id"] = sid
    req = urllib.request.Request(base + "/mcp", data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(), headers=headers)
    with urllib.request.urlopen(req, timeout=60) as r:
        body = r.read().decode()
        sid = r.headers.get("Mcp-Session-Id")
    data = next((line[5:] for line in body.splitlines() if line.startswith("data:")), body)
    return sid, json.loads(data)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", default="https://mcp.opencaselaw.ch")
    args = ap.parse_args()
    base = args.base.rstrip("/")
    failures: list[str] = []
    started = time.time()

    def check(name: str, ok: bool, detail: str = ""):
        print(f"  {'OK  ' if ok else 'FAIL'} {name}" + (f"  {detail}" if detail else ""))
        if not ok:
            failures.append(name)

    print(f"CLI live check against {base}")
    code, out, err = ocl(base, "decisions", "search", "--court", "bge", "--sort", "date_desc", "--max-results", "3", "--format", "jsonl", "--fields", "decision_id")
    rows = [json.loads(l) for l in out.splitlines() if l.strip()]
    check("filter search pages honestly", code == 0 and rows and rows[-1].get("_type") == "pagination" and rows[-1]["_client"]["ranked_single_request"] is False, err.strip()[:80])
    code, out, _ = ocl(base, "decisions", "search", "Rachekündigung", "--max-results", "5", "--format", "jsonl", "--fields", "decision_id")
    rows = [json.loads(l) for l in out.splitlines() if l.strip()]
    check("ranked search is one request", code == 0 and rows and len(rows[-1]["_client"]["pages"]) == 1 and rows[-1]["_client"]["ranked_single_request"] is True)
    code, out, _ = ocl(base, "decisions", "passage", "bge_BGE_136_III_513", "2.3", "--format", "json", "--fields", "decision_id,e_number")
    check("passage verbatim", code == 0 and json.loads(out).get("e_number") == "2.3")
    code, out, _ = ocl(base, "citations", "resolve", "BGE 136 III 513", "bge_BGE_136_III_513", "4A_747/2012", "ATF 136 III 513", "--format", "jsonl", "--fields", "reference")
    rows = [json.loads(l) for l in out.splitlines() if l.strip()]
    statuses = {r.get("reference"): r.get("status") for r in rows if r.get("_type") != "pagination"}
    check("resolve: BGE, canonical id, docket, ATF all resolved", code == 0 and set(statuses.values()) == {"resolved"}, str(statuses)[:120])
    code, out, err = ocl(base, "decisions", "passage", "247/2020", "2")
    check("docket fragment rejected", code == 4 and not out.strip() and '"kind": "resolution"' in err, err.strip()[:100])
    code, out, _ = ocl(base, "citations", "resolve", "BGer 4A_255/2012", "Obergericht ZH NG190020 vom 30. November 2020",
                       "BGE 136 III 510 E. 99", "BGer 4A_714/2014 vom 22. Mai 2016", "--format", "jsonl", "--fields", "reference")
    rows = {r.get("reference"): r.get("status") for r in (json.loads(l) for l in out.splitlines() if l.strip()) if r.get("_type") != "pagination"}
    check("resolve: long forms, inline pinpoint and a wrong date are told apart",
          code == 4 and list(rows.values()) == ["resolved", "resolved", "pinpoint_unavailable", "discrepancy"], str(rows)[:160])
    code, out, _ = ocl(base, "cite", "BGE 140 III 86", "--pinpoint", "2.3", "--format", "json", "--fields", "pinpoint_exists")
    check("cite verifies a pinpoint that the index lacks", code == 4 and json.loads(out).get("pinpoint_exists") is False)
    with tempfile.TemporaryDirectory() as tmp:
        code, out, err = ocl(base, "bundle", "create", "", "--court", "bge", "--date-from", "2010-10-07", "--date-to", "2010-10-07", "--max-results", "1", "--law", "OR:41", "--out", tmp + "/b", "--format", "json", "--fields", "status")
        payload = json.loads(out) if out.strip() else {}
        check("filtered bundle complete with corpus generation", code == 0 and payload.get("status") == "complete" and payload["completeness"].get("corpus_generation"), err.strip()[:100])
        code, out, _ = ocl(base, "bundle", "verify", tmp + "/b", "--format", "json")
        check("bundle verify passes on a fresh bundle", code == 0 and json.loads(out).get("status") == "verified")
    try:
        sid, init = mcp(base, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "cli-live-check", "version": "1"}})
        _, tools = mcp(base, "tools/list", {}, sid)
        names = {t["name"]: t for t in tools["result"]["tools"]}
        check("MCP: six research tools advertise outputSchema", all(names.get(n, {}).get("outputSchema") for n in SIX), f"{len(names)} tools")
        _, call = mcp(base, "tools/call", {"name": "cite", "arguments": {"reference": "BGE 136 III 513"}}, sid)
        check("MCP: cite returns structuredContent", (call.get("result") or {}).get("structuredContent", {}).get("exists") is True)
    except Exception as exc:  # noqa: BLE001
        check("MCP transport", False, str(exc)[:120])
    try:
        spec = json.load(urllib.request.urlopen(base + "/api/research/openapi.json", timeout=30))
        check("research OpenAPI: 7 paths, 3.0.3", len(spec.get("paths", {})) == 7 and spec.get("openapi") == "3.0.3")
    except Exception as exc:  # noqa: BLE001
        check("research OpenAPI", False, str(exc)[:120])
    print(f"{len(failures)} failure(s) in {time.time() - started:.1f}s")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
