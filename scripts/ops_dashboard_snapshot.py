"""Operator-dashboard snapshot v2: verdicts, real user signals, trends.

Design (2026-08-03, operator-driven): the page must answer, in order —
is everything OK · are users getting full functionality right now · where
is the build and when does it end · is usage normal · is data flowing in ·
what did the integrity chain find · anything trending toward trouble.

Sources, all local and read-only:
  probe        one real search request per run against a rotating worker
  tier2 log    per-request actor class / status / response time -> last-hour
               p50/p95 (human vs all), error rate  (no IPs in this log)
  workers      GET 127.0.0.1:8770-8777/health
  build        systemd unit state + publish.log tail + build_last sidecar
  usage        scripts/metrics_report.py (cached 5 min; the only correct
               source — /metrics/all undercounts ~75x after recycles)
  uniques      /var/lib/ocl-uniq/state.json (HLL registers -> estimates)
  intake       logs/scraper_health.json (per-scraper movers/errors)
  integrity    citation-anomaly report + reporter log + toolcheck unit
  host         disk, meminfo, timers; 24 h history ring for deltas

Every section is collected independently; a failing source yields
{"error": ...} for that section only. The verdict aggregates named checks
so the page can lead with one line and its reasons.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

OUT = Path(os.environ.get("OCL_OPS_OUT", "/opt/caselaw/ops-dashboard/ops.json"))
USAGE_CACHE = Path("/var/tmp/ops-usage-cache.json")
USAGE_TTL_S = 300
HISTORY = OUT.parent / "history.jsonl"
BUILD_LAST = OUT.parent / "build_last.json"
TIER2_LOG = Path("/var/log/nginx/tier2.log")
WORKER_PORTS = tuple(range(8770, 8778))
PROBE_QUERY = "missbräuchliche Kündigung Art. 336 OR"
DEFAULT_BUILD_S = 13.5 * 3600


def _run(cmd: list[str], timeout: int = 10) -> str:
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=timeout).stdout


def _section(fn):
    try:
        return fn()
    except Exception as e:                              # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}"}


def _systemd_props(unit: str, *props: str) -> dict:
    out = _run(["systemctl", "show", unit, *[f"-p{p}" for p in props]])
    kv = {}
    for line in out.splitlines():
        k, _, v = line.partition("=")
        kv[k] = v.strip()
    return kv


# ── serving + live probe ───────────────────────────────────────────────
def serving() -> dict:
    per, rows = [], None
    for p in WORKER_PORTS:
        t0 = time.time()
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{p}/health",
                                        timeout=1.5) as r:
                body = json.loads(r.read().decode())
            per.append({"port": p, "ok": True,
                        "ms": round((time.time() - t0) * 1000)})
            rows = body.get("decisions", rows)
        except Exception:                               # noqa: BLE001
            per.append({"port": p, "ok": False, "ms": None})
    oks = [w["ms"] for w in per if w["ok"]]
    port = WORKER_PORTS[int(time.time() // 60) % len(WORKER_PORTS)]
    probe = {"query": PROBE_QUERY, "worker": port, "ok": False,
             "ms": None, "results": None}
    t0 = time.time()
    try:
        q = urllib.parse.quote(PROBE_QUERY)
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/api/decisions?q={q}"
                f"&fields=compact&limit=3", timeout=25) as r:
            body = json.loads(r.read().decode())
        probe.update(ok=True, ms=round((time.time() - t0) * 1000),
                     results=len(body.get("results", body.get("decisions", []))))
    except Exception as e:                              # noqa: BLE001
        probe["error"] = f"{type(e).__name__}"
        probe["ms"] = round((time.time() - t0) * 1000)
    return {"workers_up": len(oks), "workers_total": len(per),
            "latency_ms_min": min(oks) if oks else None,
            "latency_ms_max": max(oks) if oks else None,
            "corpus_rows": rows, "per_worker": per, "probe": probe}


# ── last-hour traffic from tier2 (actor-classified, no IPs) ────────────
def parse_tier2_window(text: str, now_epoch: float, window_s: int = 3600) -> dict:
    reqs = r5xx = 0
    times_all: list[float] = []
    times_human: list[float] = []
    human_reqs = 0
    actors: dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 7:
            continue
        ts, actor, _endpoint, _method, status, rt = parts[:6]
        try:
            t = time.mktime(time.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S"))
        except ValueError:
            continue
        # tier2 timestamps are +00:00; mktime is local — treat host as UTC
        if now_epoch - t > window_s:
            continue
        reqs += 1
        actors[actor] = actors.get(actor, 0) + 1
        if status.startswith("5"):
            r5xx += 1
        try:
            rt_f = float(rt)
            times_all.append(rt_f)
            if not actor.startswith("bot_") and actor != "monitor":
                times_human.append(rt_f)
                human_reqs += 1
        except ValueError:
            pass

    def pct(vals: list[float], p: float):
        if not vals:
            return None
        vals = sorted(vals)
        return round(vals[min(len(vals) - 1, int(p * len(vals)))] * 1000)

    return {"requests": reqs, "r5xx": r5xx,
            "err_rate_pct": round(r5xx / reqs * 100, 2) if reqs else None,
            "p50_ms": pct(times_all, 0.50), "p95_ms": pct(times_all, 0.95),
            "human_requests": human_reqs,
            "human_p50_ms": pct(times_human, 0.50),
            "human_p95_ms": pct(times_human, 0.95),
            "top_actors": sorted(actors.items(), key=lambda kv: -kv[1])[:6]}


def traffic_1h() -> dict:
    with TIER2_LOG.open("rb") as f:
        f.seek(max(0, TIER2_LOG.stat().st_size - 12 * 2**20))
        text = f.read().decode("utf-8", "replace")
    return parse_tier2_window(text, time.time())


# ── build ──────────────────────────────────────────────────────────────
def friendly_build_state(raw: str, exec_status: int | None) -> tuple[str, bool | None]:
    """systemd oneshot vocabulary -> operator vocabulary.
    'activating' = the nightly is running; 'inactive' = idle between runs
    (healthy); 'failed' = last run died."""
    if raw == "activating":
        return "running", None
    if raw == "failed":
        return "failed", False
    ok = (exec_status == 0) if exec_status is not None else None
    return ("idle", ok)


def build() -> dict:
    props = _systemd_props("opencaselaw-publish.service",
                           "ActiveState", "ExecMainStartTimestamp",
                           "ExecMainExitTimestamp", "ExecMainStatus",
                           "MemoryPeak")
    raw = props.get("ActiveState", "unknown")
    exec_status = (int(props["ExecMainStatus"])
                   if props.get("ExecMainStatus", "").lstrip("-").isdigit()
                   else None)
    state, result_ok = friendly_build_state(raw, exec_status)
    mem_peak = (round(int(props["MemoryPeak"]) / 2**30, 1)
                if props.get("MemoryPeak", "").isdigit() else None)

    phase = step = None
    total_s = None
    log = REPO / "logs/publish.log"
    if log.exists():
        with log.open("rb") as f:
            f.seek(max(0, log.stat().st_size - 32768))
            tail = f.read().decode("utf-8", "replace")
        lines = [l for l in tail.splitlines() if l.strip()]
        if lines:
            phase = lines[-1][-220:]
        for m in re.finditer(r"Step ([0-9]+[a-z]?)[: ]([^|]*)", tail):
            step = f"{m.group(1)} {m.group(2).strip()[:60]}"
        m = None
        for m in re.finditer(r"Total time: ([\d.]+)s", tail):
            pass
        if m:
            total_s = float(m.group(1))

    # sidecar: remember the last completed run's wall time + end
    last = {}
    if BUILD_LAST.exists():
        try:
            last = json.loads(BUILD_LAST.read_text())
        except Exception:                               # noqa: BLE001
            last = {}
    ended = props.get("ExecMainExitTimestamp") or None
    if state == "idle" and total_s and ended and ended != last.get("ended"):
        last = {"ended": ended, "total_s": total_s}
        BUILD_LAST.write_text(json.dumps(last))

    started = props.get("ExecMainStartTimestamp") or None
    eta = elapsed_h = None
    if state == "running" and started:
        try:
            st = time.mktime(time.strptime(started[4:24], "%Y-%m-%d %H:%M:%S"))
            elapsed_h = round((time.time() - st) / 3600, 1)
            expect = last.get("total_s", DEFAULT_BUILD_S)
            eta = time.strftime("%H:%M UTC", time.gmtime(st + expect))
        except ValueError:
            pass
    return {"state_raw": raw, "state": state, "result_ok": result_ok,
            "started": started, "ended": ended, "elapsed_h": elapsed_h,
            "eta": eta, "last_total_s": last.get("total_s"),
            "last_ended": last.get("ended"), "mem_peak_g": mem_peak,
            "step": step, "phase": phase}


# ── usage (cached; canonical reconstruction) ───────────────────────────
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
    prior = days[:-1][-7:]                     # last 7 complete days
    med = {"calls": int(statistics.median(d["calls"] for d in prior)),
           "sessions": int(statistics.median(d["sessions"] for d in prior))} if prior else None
    data = {"days": days[-14:], "today": days[-1] if days else None,
            "yesterday": days[-2] if len(days) > 1 else None,
            "week_median": med,
            "calls_30d": int(totals.group(1).replace(",", "")) if totals else None,
            "sessions_30d": int(totals.group(2).replace(",", "")) if totals else None}
    USAGE_CACHE.write_text(json.dumps(data))
    return data


# ── unique consumers ───────────────────────────────────────────────────
def uniques() -> dict:
    from scripts.unique_users_daemon import HLL
    p = Path("/var/lib/ocl-uniq/state.json")
    st = json.loads(p.read_text())

    def est(regs: dict) -> dict:
        return {k: HLL(bytearray.fromhex(v)).estimate()
                for k, v in regs.items()}
    return {"day": st.get("day"), "month": st.get("month"),
            "state_age_s": round(time.time() - p.stat().st_mtime),
            "today": est(st.get("day_reg", {})),
            "month_to_date": est(st.get("month_reg", {}))}


# ── intake ─────────────────────────────────────────────────────────────
def shape_intake(d: dict) -> dict:
    sc = d.get("scrapers", {})
    rows = {k: v for k, v in sc.items() if isinstance(v, dict)}
    failed = [k for k, v in rows.items() if v.get("success") is False]
    errors = sorted(((k, v.get("error_count") or 0) for k, v in rows.items()
                     if (v.get("error_count") or 0) > 0),
                    key=lambda kv: -kv[1])[:5]
    top_new = sorted(((k, v.get("new_count") or 0) for k, v in rows.items()
                      if (v.get("new_count") or 0) > 0),
                     key=lambda kv: -kv[1])[:6]
    return {"run_at": d.get("run_at"), "duration_s": d.get("run_duration_s"),
            "scrapers": len(rows), "failed": failed,
            "errors": errors, "top_new": top_new,
            "new_last_round": sum(v.get("new_count") or 0 for v in rows.values())}


def intake() -> dict:
    return shape_intake(json.loads((REPO / "logs/scraper_health.json").read_text()))


# ── integrity ──────────────────────────────────────────────────────────
def integrity() -> dict:
    rep = json.loads((REPO / "logs/citation_anomalies_latest.json").read_text())
    tc = _systemd_props("opencaselaw-toolcheck.service",
                        "ExecMainStatus", "ExecMainExitTimestamp")
    rep_log = REPO / "logs/citation_anomaly_report.log"
    reporter_last = None
    if rep_log.exists():
        lines = rep_log.read_text().strip().splitlines()
        if lines:
            reporter_last = lines[-1][-160:]
    gen = rep.get("generated")
    fresh = gen == time.strftime("%Y-%m-%d", time.gmtime()) or \
        gen == time.strftime("%Y-%m-%d", time.gmtime(time.time() - 86400))
    return {"audit_generated": gen, "audit_fresh": fresh,
            "window_findings": rep.get("nonexistent_bge_total"),
            "anachronistic": rep.get("anachronistic_total"),
            "corpus_qa_total": rep.get("nonexistent_bge_corpus_total"),
            "reporter_last": reporter_last,
            "toolcheck_pass": (int(tc["ExecMainStatus"]) == 0
                               if tc.get("ExecMainStatus", "").lstrip("-").isdigit()
                               else None),
            "toolcheck_when": tc.get("ExecMainExitTimestamp") or None}


# ── host + history ─────────────────────────────────────────────────────
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
            nxt = " ".join(toks[:4]) if re.match(r"^\w{3}$", toks[0]) else "-"
            out.append({"unit": unit.replace("opencaselaw-", "").replace(".timer", ""),
                        "next": nxt})
    return out


def update_history(entry: dict, path: Path = None, min_gap_s: int = 600,
                   keep: int = 200) -> list[dict]:
    """Append at most one compact record per min_gap_s; return the ring."""
    path = path or HISTORY
    hist: list[dict] = []
    if path.exists():
        for line in path.read_text().splitlines()[-keep:]:
            try:
                hist.append(json.loads(line))
            except Exception:                           # noqa: BLE001
                continue
    if not hist or entry["ts"] - hist[-1]["ts"] >= min_gap_s:
        hist.append(entry)
        path.write_text("\n".join(json.dumps(h, separators=(",", ":"))
                                  for h in hist[-keep:]) + "\n")
    return hist[-keep:]


def host(serving_d: dict) -> dict:
    du = shutil.disk_usage("/")
    mem = parse_meminfo(Path("/proc/meminfo").read_text())
    timers = parse_timers(_run(["systemctl", "list-timers",
                                "--no-pager", "--plain"]))
    probe = (serving_d or {}).get("probe") or {}
    hist = update_history({"ts": int(time.time()),
                           "disk_free_g": round(du.free / 2**30, 1),
                           "corpus_rows": (serving_d or {}).get("corpus_rows"),
                           "probe_ms": probe.get("ms")})
    return {"disk_used_pct": round(du.used / du.total * 100),
            "disk_free_g": round(du.free / 2**30), **mem,
            "timers": timers, "history": hist}


# ── corpus ─────────────────────────────────────────────────────────────
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


# ── verdict ────────────────────────────────────────────────────────────
def verdict(snap: dict) -> dict:
    checks: list[dict] = []

    def add(name: str, level: str, detail: str):
        checks.append({"name": name, "level": level, "detail": detail})

    s = snap.get("serving", {})
    up, tot = s.get("workers_up"), s.get("workers_total", 8)
    if isinstance(up, int):
        add("workers", "ok" if up == tot else ("warn" if up >= tot - 1 else "fail"),
            f"{up}/{tot} up")
    probe = s.get("probe") or {}
    if probe.get("ok"):
        ms = probe.get("ms") or 0
        lvl = "ok" if ms < 8000 else "warn"
        add("search probe", lvl, f"{ms} ms"
            + (" (build hours)" if lvl == "warn" and
               snap.get("build", {}).get("state") == "running" else ""))
    elif probe:
        add("search probe", "fail", probe.get("error", "no response"))

    t = snap.get("traffic_1h", {})
    if isinstance(t.get("requests"), int) and t["requests"] >= 100:
        er = t.get("err_rate_pct") or 0
        add("error rate 1h", "ok" if er < 0.5 else ("warn" if er < 2 else "fail"),
            f"{er}% of {t['requests']}")

    b = snap.get("build", {})
    if b.get("state") == "failed":
        add("build", "fail", "last run failed")
    elif b.get("state") == "running":
        lvl = "warn" if (b.get("elapsed_h") or 0) > 16 else "ok"
        add("build", lvl, f"running {b.get('elapsed_h')} h, eta {b.get('eta')}")
    elif b.get("state") == "idle":
        add("build", "ok" if b.get("result_ok") in (True, None) else "fail",
            "idle · last run " + ("OK" if b.get("result_ok") in (True, None) else "FAILED"))

    i = snap.get("intake", {})
    if i.get("failed"):
        add("scrapers", "warn", ", ".join(i["failed"][:4]))
    elif "scrapers" in i:
        add("scrapers", "ok", f"{i['scrapers']} ok")

    g = snap.get("integrity", {})
    if g.get("audit_fresh") is False:
        add("citation audit", "warn", f"stale ({g.get('audit_generated')})")
    if g.get("toolcheck_pass") is False:
        add("toolcheck", "fail", "last run failed")

    u = snap.get("uniques", {})
    if isinstance(u.get("state_age_s"), int) and u["state_age_s"] > 1800:
        add("uniq counter", "warn", f"checkpoint {u['state_age_s'] // 60} min old")

    h = snap.get("host", {})
    if isinstance(h.get("disk_used_pct"), int):
        d = h["disk_used_pct"]
        add("disk", "ok" if d < 80 else ("warn" if d < 90 else "fail"),
            f"{d}% used")

    order = {"fail": 2, "warn": 1, "ok": 0}
    level = max((c["level"] for c in checks), key=lambda l: order[l],
                default="ok")
    return {"level": level, "checks": checks}


def main() -> int:
    snap = {"generated": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())}
    snap["serving"] = _section(serving)
    snap["traffic_1h"] = _section(traffic_1h)
    snap["build"] = _section(build)
    snap["usage"] = _section(usage)
    snap["uniques"] = _section(uniques)
    snap["intake"] = _section(intake)
    snap["integrity"] = _section(integrity)
    snap["host"] = _section(lambda: host(snap.get("serving")))
    snap["corpus"] = _section(corpus)
    snap["verdict"] = _section(lambda: verdict(snap))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(snap, separators=(",", ":")))
    os.replace(tmp, OUT)
    print(f"ops.json written ({OUT.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
