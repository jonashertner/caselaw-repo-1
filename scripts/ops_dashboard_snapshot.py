"""Operator-dashboard snapshot: every platform subsystem into one ops.json.

Runs on the VPS via ocl-ops-dashboard.timer (60 s). Each section is
collected independently and defensively — a failing source yields
{"error": ...} for that section, never a missing file. The page at
/ops/ polls the JSON; nothing here is public (nginx basic auth).

Sources (all local, read-only):
  workers      GET 127.0.0.1:8770-8777/health
  build        systemd state + publish.log tail
  usage        scripts/metrics_report.py (cached 5 min — it scans the
               cumulative-per-boot daily_metrics.jsonl, the only correct
               source; /metrics/all undercounts ~75x after recycles)
  uniques      /var/lib/ocl-uniq/state.json (HLL registers -> estimates)
  intake       logs/scraper_health.json
  integrity    logs/citation_anomalies_latest.json + toolcheck unit state
  ops          disk, meminfo, opencaselaw-* timer schedule
  corpus       docs/stats.json (nightly generate_stats.py output)
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

OUT = Path(os.environ.get("OCL_OPS_OUT", "/opt/caselaw/ops-dashboard/ops.json"))
USAGE_CACHE = Path("/var/tmp/ops-usage-cache.json")
USAGE_TTL_S = 300
WORKER_PORTS = range(8770, 8778)


def _run(cmd: list[str], timeout: int = 10) -> str:
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=timeout).stdout


def _section(fn):
    try:
        return fn()
    except Exception as e:                              # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}"}


# ── serving ────────────────────────────────────────────────────────────
def serving() -> dict:
    per = []
    rows = None
    for p in WORKER_PORTS:
        t0 = time.time()
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{p}/health",
                                        timeout=1.5) as r:
                body = json.loads(r.read().decode())
            ms = round((time.time() - t0) * 1000)
            per.append({"port": p, "ok": True, "ms": ms})
            rows = body.get("decisions", rows)
        except Exception:                               # noqa: BLE001
            per.append({"port": p, "ok": False, "ms": None})
    oks = [w["ms"] for w in per if w["ok"]]
    return {"workers_up": len(oks), "workers_total": len(per),
            "latency_ms_min": min(oks) if oks else None,
            "latency_ms_max": max(oks) if oks else None,
            "corpus_rows": rows, "per_worker": per}


# ── build ──────────────────────────────────────────────────────────────
def build() -> dict:
    state = _run(["systemctl", "is-active",
                  "opencaselaw-publish.service"]).strip()
    show = _run(["systemctl", "show", "opencaselaw-publish.service",
                 "-p", "ExecMainStartTimestamp", "-p", "MemoryPeak"])
    started = mem_peak = None
    for line in show.splitlines():
        k, _, v = line.partition("=")
        if k == "ExecMainStartTimestamp":
            started = v.strip() or None
        elif k == "MemoryPeak" and v.strip().isdigit():
            mem_peak = round(int(v) / 2**30, 1)
    phase = last_total_s = None
    log = REPO / "logs/publish.log"
    if log.exists():
        with log.open("rb") as f:
            f.seek(max(0, log.stat().st_size - 16384))
            tail = f.read().decode("utf-8", "replace")
        lines = [l for l in tail.splitlines() if l.strip()]
        if lines:
            phase = lines[-1][-220:]
        m = None
        for m in re.finditer(r"Total time: ([\d.]+)s", tail):
            pass
        if m:
            last_total_s = float(m.group(1))
    return {"state": state, "started": started, "mem_peak_g": mem_peak,
            "phase": phase, "last_total_s": last_total_s}


# ── usage (cached — metrics_report scans the full jsonl) ───────────────
def usage() -> dict:
    if USAGE_CACHE.exists() and time.time() - USAGE_CACHE.stat().st_mtime < USAGE_TTL_S:
        return json.loads(USAGE_CACHE.read_text())
    out = _run(["nice", "-n", "19", "/usr/bin/python3",
                str(REPO / "scripts/metrics_report.py")], timeout=120)
    days = []
    for line in out.splitlines():
        m = re.match(r"^(\d{4}-\d{2}-\d{2})\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)", line)
        if m:
            days.append({"date": m.group(1), "calls": int(m.group(2)),
                         "sessions": int(m.group(3)),
                         "claude_ai": int(m.group(4)),
                         "chatgpt": int(m.group(5)),
                         "claude_code": int(m.group(6)),
                         "other": int(m.group(7))})
    totals = re.search(r"30-day totals: ([\d,]+) calls, ([\d,]+) sessions", out)
    data = {"days": days[-14:],
            "today": days[-1] if days else None,
            "calls_30d": int(totals.group(1).replace(",", "")) if totals else None,
            "sessions_30d": int(totals.group(2).replace(",", "")) if totals else None}
    USAGE_CACHE.write_text(json.dumps(data))
    return data


# ── unique consumers ───────────────────────────────────────────────────
def uniques() -> dict:
    from scripts.unique_users_daemon import HLL
    st = json.loads(Path("/var/lib/ocl-uniq/state.json").read_text())

    def est(regs: dict) -> dict:
        return {k: HLL(bytearray.fromhex(v)).estimate()
                for k, v in regs.items()}
    return {"day": st.get("day"), "month": st.get("month"),
            "today": est(st.get("day_reg", {})),
            "month_to_date": est(st.get("month_reg", {}))}


# ── intake ─────────────────────────────────────────────────────────────
def intake() -> dict:
    d = json.loads((REPO / "logs/scraper_health.json").read_text())
    sc = d.get("scrapers", d)
    rows = {k: v for k, v in sc.items() if isinstance(v, dict)}
    failed = [k for k, v in rows.items()
              if str(v.get("status", "")).upper() == "FAILED"]
    return {"scrapers": len(rows), "failed": failed,
            "new_last_round": sum(int(v.get("new_count") or 0)
                                  for v in rows.values())}


# ── integrity ──────────────────────────────────────────────────────────
def integrity() -> dict:
    rep = json.loads((REPO / "logs/citation_anomalies_latest.json").read_text())
    show = _run(["systemctl", "show", "opencaselaw-toolcheck.service",
                 "-p", "ExecMainStatus", "-p", "ExecMainExitTimestamp"])
    tc_status = tc_when = None
    for line in show.splitlines():
        k, _, v = line.partition("=")
        if k == "ExecMainStatus" and v.strip().isdigit():
            tc_status = int(v)
        elif k == "ExecMainExitTimestamp":
            tc_when = v.strip() or None
    return {"audit_generated": rep.get("generated"),
            "window_findings": rep.get("nonexistent_bge_total"),
            "anachronistic": rep.get("anachronistic_total"),
            "corpus_qa_total": rep.get("nonexistent_bge_corpus_total"),
            "toolcheck_pass": tc_status == 0 if tc_status is not None else None,
            "toolcheck_when": tc_when}


# ── ops ────────────────────────────────────────────────────────────────
def parse_meminfo(text: str) -> dict:
    kv = {}
    for line in text.splitlines():
        m = re.match(r"(\w+):\s+(\d+) kB", line)
        if m:
            kv[m.group(1)] = int(m.group(2))
    return {"mem_avail_g": round(kv.get("MemAvailable", 0) / 2**20, 1),
            "mem_cache_g": round((kv.get("Buffers", 0) + kv.get("Cached", 0)) / 2**20, 1)}


def parse_timers(text: str) -> list[dict]:
    out = []
    for line in text.splitlines():
        if "opencaselaw" not in line and "ocl-" not in line:
            continue
        toks = line.split()
        unit = next((t for t in toks if t.endswith(".timer")), None)
        if unit and len(toks) >= 4:
            out.append({"unit": unit.replace("opencaselaw-", "").replace(".timer", ""),
                        "next": " ".join(toks[:4])})
    return out


def ops() -> dict:
    du = shutil.disk_usage("/")
    mem = parse_meminfo(Path("/proc/meminfo").read_text())
    timers = parse_timers(_run(["systemctl", "list-timers",
                                "--no-pager", "--plain"]))
    return {"disk_used_pct": round(du.used / du.total * 100),
            "disk_free_g": round(du.free / 2**30), **mem, "timers": timers}


# ── corpus (nightly stats.json) ────────────────────────────────────────
def corpus() -> dict:
    s = json.loads((REPO / "docs/stats.json").read_text())
    flat = {}
    for key in ("decisions", "laws", "citations", "scholarship",
                "total_decisions", "total_laws", "citation_edges"):
        v = s.get(key)
        if isinstance(v, (int, float, str)):
            flat[key] = v
        elif isinstance(v, dict) and "total" in v:
            flat[key] = v["total"]
    return flat or {"note": "stats.json shape unrecognized"}


def main() -> int:
    snap = {"generated": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "serving": _section(serving), "build": _section(build),
            "usage": _section(usage), "uniques": _section(uniques),
            "intake": _section(intake), "integrity": _section(integrity),
            "ops": _section(ops), "corpus": _section(corpus)}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(snap, separators=(",", ":")))
    os.replace(tmp, OUT)
    print(f"ops.json written ({OUT.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
