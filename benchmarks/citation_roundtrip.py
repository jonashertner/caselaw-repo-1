#!/usr/bin/env python3
"""Citation round-trip benchmark: does `ocl citations resolve` still give the expected
verdict for the references the 2026-09 field test and review wrote?

Run by hand (there is no scheduled run):

    make bench-citations                     # against production
    python benchmarks/citation_roundtrip.py --base-url https://staging.example

Exit 1 when a real reference regressed (expected resolved / discrepancy /
pinpoint_unavailable but got missing, unrecognized, ambiguous or error), when a
deliberately wrong reference now resolves, or when an expected decision_id
changed. --update-expected rewrites the expectations after a deliberate change.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SET = ROOT / "benchmarks" / "citation_roundtrip" / "references.jsonl"
RESULTS = ROOT / "benchmarks" / "results"
GOOD = {"resolved", "discrepancy", "pinpoint_unavailable"}
BAD = {"missing", "unrecognized", "ambiguous", "error", "resolution_incomplete", "skipped"}


def load_set(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def compare(expected: list[dict], rows: list[dict]) -> dict:
    """Pure comparison; rows are the resolve output rows (with `input`)."""
    by_id, by_key = {}, {}
    for row in rows:
        inp = row.get("input") or {}
        if inp.get("bench_id"):
            by_id[inp["bench_id"]] = row
        by_key[(row.get("reference"), row.get("pinpoint") or None)] = row
    mismatches, regressions = [], []
    counts = {"checked": 0, "match": 0}
    for exp in expected:
        # rows echo the set's extra keys under `input`; an inline pinpoint the
        # resolver read from the reference must not break the match
        row = by_id.get(exp.get("bench_id")) or by_key.get((exp["reference"], exp.get("pinpoint") or None))
        counts["checked"] += 1
        if row is None:
            mismatches.append({**exp, "got_status": None, "problem": "no result row"}); regressions.append(exp["reference"]); continue
        got = row.get("status")
        ok = got == exp["expected_status"]
        if ok and exp.get("expected_decision_id") and row.get("decision_id") != exp["expected_decision_id"]:
            ok = False
        if ok:
            counts["match"] += 1
            continue
        item = {**exp, "got_status": got, "got_decision_id": row.get("decision_id")}
        mismatches.append(item)
        if exp["kind"] == "deliberately_wrong" and got in GOOD:
            item["problem"] = "a wrong reference now resolves"; regressions.append(exp["reference"])
        elif exp["expected_status"] in GOOD and got in BAD:
            item["problem"] = "a real reference is lost"; regressions.append(exp["reference"])
        elif exp.get("expected_decision_id") and row.get("decision_id") not in (None, exp["expected_decision_id"]):
            item["problem"] = "resolved to another decision"; regressions.append(exp["reference"])
        else:
            item["problem"] = "status changed"
    counts["mismatch"] = len(mismatches); counts["regressions"] = len(regressions)
    return {"counts": counts, "mismatches": mismatches, "regressions": regressions}


def run_resolve(base_url: str | None, set_path: Path, timeout: int, jobs: int) -> tuple[list[dict], dict]:
    env = dict(os.environ, PYTHONPATH=str(ROOT / "clients" / "python" / "src"), OCL_CONFIG="/nonexistent", OCL_JOBS=str(jobs))
    cmd = [sys.executable, "-m", "opencaselaw_cli", "citations", "resolve", "--input", str(set_path), "--format", "jsonl",
           "--timeout", str(timeout), "--jobs", str(jobs)]
    if base_url:
        cmd += ["--base-url", base_url]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    rows, meta = [], {}
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        if r.get("_type") == "pagination":
            meta = r
        else:
            rows.append(r)
    meta["exit_code"] = proc.returncode
    return rows, meta


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url"); ap.add_argument("--set", type=Path, default=SET)
    ap.add_argument("--timeout", type=int, default=40); ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--update-expected", action="store_true", help="rewrite the expectations from this run")
    args = ap.parse_args()
    expected = load_set(args.set)
    started = time.time()
    rows, meta = run_resolve(args.base_url, args.set, args.timeout, args.jobs)
    report = compare(expected, rows)
    report.update(generated_at=datetime.now(timezone.utc).isoformat(), base_url=args.base_url or "https://mcp.opencaselaw.ch",
                  references=len(expected), requests=meta.get("requests"), wall_seconds=round(time.time() - started, 1),
                  client_version=meta.get("client_version"), resolve_exit_code=meta.get("exit_code"))
    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / f"citation_roundtrip-{datetime.now(timezone.utc).date()}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    c = report["counts"]
    print(f"{c['match']}/{c['checked']} as expected, {c['mismatch']} mismatch(es), {c['regressions']} regression(s); "
          f"{report['requests']} requests in {report['wall_seconds']} s -> {out.relative_to(ROOT)}")
    for item in report["mismatches"][:40]:
        print(f"  {item['problem']:28} {item['reference'][:60]:60} expected {item['expected_status']} got {item.get('got_status')}")
    if args.update_expected:
        by_key = {(r.get("reference"), r.get("pinpoint") or None): r for r in rows}
        by_id = {(r.get("input") or {}).get("bench_id"): r for r in rows if (r.get("input") or {}).get("bench_id")}
        for exp in expected:
            row = by_id.get(exp.get("bench_id")) or by_key.get((exp["reference"], exp.get("pinpoint") or None))
            if row:
                exp["expected_status"] = row.get("status")
                if row.get("decision_id"):
                    exp["expected_decision_id"] = row["decision_id"]
        args.set.write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in expected), encoding="utf-8")
        print("expectations rewritten")
        return 0
    return 1 if report["regressions"] else 0


if __name__ == "__main__":
    sys.exit(main())
