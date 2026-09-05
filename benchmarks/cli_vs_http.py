#!/usr/bin/env python3
"""Decision test from the interface strategy: the same two research tasks done with
(a) `ocl` and (b) a plain HTTP script over the public REST API, measured on wall
time, requests, and correctness of the reported statuses. MCP code execution is
the third arm of the strategy's test and needs an agent runtime; it is not
measured here. Bounded live calls (about 30 requests); run manually, not in CI.

Usage: python benchmarks/cli_vs_http.py [--base URL] [--json OUT]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "clients" / "python" / "src"
REFERENCES = [("BGE 136 III 513", "2.3"), ("4A_747/2012", None), ("BGE 999 III 1", None),
              ("BGE 140 III 86", "2.3"), ("ATF 141 III 433", None), ("6B_1/2020", None),
              ("BGE 119 II 380", "4"), ("bge_BGE_136_III_513", None)]
EXPECTED = {"BGE 136 III 513": "resolved", "4A_747/2012": "resolved", "BGE 999 III 1": "missing",
            "BGE 140 III 86": "pinpoint_unavailable", "ATF 141 III 433": "resolved", "6B_1/2020": "resolved",
            "BGE 119 II 380": "resolved", "bge_BGE_136_III_513": "resolved"}
QUERY = "Rachekündigung Art. 336 OR"


class Counter:
    def __init__(self):
        self.requests = 0
    def get(self, base, path, params=None):
        self.requests += 1
        url = base + path + ("?" + urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None}) if params else "")
        with urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": "application/json"}), timeout=60) as r:
            return json.load(r)


def http_citation_check(base):
    """What a careful script author writes without the client: cite, then verify the pinpoint."""
    c = Counter(); rows = []
    for ref, pin in REFERENCES:
        cite = c.get(base, "/api/cite", {"reference": ref, "pinpoint": pin})
        if cite.get("exists") is not True:
            rows.append({"reference": ref, "status": "missing"}); continue
        status = "resolved"
        if pin:
            passage = c.get(base, f"/api/erwaegung/{urllib.parse.quote(cite['decision_id'], safe='')}/{pin}")
            if passage.get("error") or not passage.get("text"):
                status = "pinpoint_unavailable"
        rows.append({"reference": ref, "status": status, "decision_id": cite.get("decision_id")})
    return rows, c.requests


def http_bundle(base, out):
    c = Counter(); out.mkdir()
    page = c.get(base, "/api/decisions", {"query": QUERY, "limit": 3, "fields": "compact", "include_pinpoint": "false"})
    saved = 0
    for hit in page["results"]:
        d = c.get(base, "/api/decisions/" + urllib.parse.quote(hit["decision_id"], safe=""))
        (out / (hit["decision_id"].replace("/", "_") + ".json")).write_text(json.dumps(d, ensure_ascii=False))
        saved += 1
    law = c.get(base, "/api/laws/OR", {"article": "336"})
    (out / "OR_336.json").write_text(json.dumps(law, ensure_ascii=False))
    return saved + 1, c.requests


def ocl(base, *args):
    t = time.time()
    p = subprocess.run([sys.executable, "-m", "opencaselaw_cli", "--base-url", base, "--retries", "0", "--timeout", "60", *args],
                       capture_output=True, text=True, env={"PYTHONPATH": str(SRC), "PATH": "/usr/bin:/bin", "NO_COLOR": "1", "OCL_CONFIG": "/nonexistent"})
    return p, time.time() - t


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", default="https://mcp.opencaselaw.ch")
    ap.add_argument("--json")
    args = ap.parse_args(); base = args.base.rstrip("/")
    report = {"base": base, "tasks": {}}
    with tempfile.TemporaryDirectory() as tmp:
        refs = Path(tmp) / "refs.jsonl"
        refs.write_text("".join(json.dumps({"reference": r, **({"pinpoint": p} if p else {})}) + "\n" for r, p in REFERENCES))
        # Task 1: citation check
        t = time.time(); rows, n = http_citation_check(base); http_t = time.time() - t
        http_ok = sum(1 for r in rows if EXPECTED[r["reference"]] == r["status"])
        p, cli_t = ocl(base, "citations", "resolve", "--input", str(refs), "--format", "jsonl", "--fields", "reference")
        cli_rows = [json.loads(l) for l in p.stdout.splitlines() if l.strip() and '"_type"' not in l]
        cli_ok = sum(1 for r in cli_rows if EXPECTED.get(r.get("reference")) == r.get("status"))
        report["tasks"]["citation_check"] = {
            "references": len(REFERENCES),
            "http": {"seconds": round(http_t, 1), "requests": n, "correct": http_ok, "lines_of_code": 14, "identity_checked": False},
            "ocl": {"seconds": round(cli_t, 1), "correct": cli_ok, "exit_code": p.returncode, "identity_checked": True,
                    "command": "ocl citations resolve --input refs.jsonl"}}
        # Task 2: evidence bundle
        t = time.time(); files, n = http_bundle(base, Path(tmp) / "http"); http_t = time.time() - t
        p, cli_t = ocl(base, "bundle", "create", QUERY, "--max-results", "3", "--law", "OR:336", "--out", str(Path(tmp) / "cli"), "--format", "json", "--fields", "status")
        manifest = json.loads((Path(tmp) / "cli" / "manifest.json").read_text())
        report["tasks"]["evidence_bundle"] = {
            "http": {"seconds": round(http_t, 1), "requests": n, "files": files, "manifest": False, "hashes": False, "resumable": False, "lines_of_code": 12},
            "ocl": {"seconds": round(cli_t, 1), "files": len(manifest["artifacts"]), "manifest": True, "hashes": True, "resumable": True,
                    "corpus_generation": manifest.get("corpus_snapshot", {}).get("db_generation"), "exit_code": p.returncode,
                    "command": f"ocl bundle create '{QUERY}' --max-results 3 --law OR:336 --out cli"}}
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
