"""Per-day usage from daily_metrics.jsonl — the numbers /metrics/all cannot give.

/metrics/all reports in-memory counters SINCE THE LAST WORKER RESTART (it says
so, via uptime_since, but it is easy to misread). On 2026-07-29 it showed
4,335 calls while the flush records put the day at ~327k — a 75x misread that
made real traffic look negligible during an ops review.

The durable store is daily_metrics.jsonl: every worker appends a snapshot of
its cumulative counters every ~10 minutes. Counters reset when a worker
restarts, so a day's true total is the sum, over worker boots (keyed by
uptime_since), of that boot's per-day counter growth. This script does that
reconstruction and prints a per-day table, client mix, and 30-day tool totals.

    .venv/bin/python scripts/metrics_report.py [path/to/daily_metrics.jsonl]
    ssh vps 'python3 /opt/caselaw/repo/scripts/metrics_report.py'

(On the VPS the default resolves via SWISS_CASELAW_DIR to
/opt/caselaw/repo/output/research_logs/daily_metrics.jsonl.)

Direction of error: a boot whose counters only ever shrink (impossible short
of clock skew) contributes zero, never negative; flushes lost to a crash
undercount the final partial day. Both are small and conservative.

Not to be confused with scripts/usage_report.py (nginx-log based, web tiers).
"""
from __future__ import annotations

import collections
import json
import os
import sys
from pathlib import Path

def _default_path() -> Path:
    """First existing candidate. SWISS_CASELAW_DIR is set in the worker units
    but not in an interactive SSH shell, so an ops run on the VPS must also
    find the repo-relative location on its own."""
    candidates = []
    if os.environ.get("SWISS_CASELAW_DIR"):
        candidates.append(Path(os.environ["SWISS_CASELAW_DIR"])
                          / "research_logs" / "daily_metrics.jsonl")
    repo = Path(__file__).resolve().parents[1]
    candidates.append(repo / "output" / "research_logs" / "daily_metrics.jsonl")
    candidates.append(Path.home() / ".swiss-caselaw" / "research_logs"
                      / "daily_metrics.jsonl")
    for c in candidates:
        if c.exists():
            return c
    return candidates[0]


DEFAULT = _default_path()


def reconstruct(lines):
    """flush records -> (daily_calls, daily_sessions, daily_clients,
    daily_tool_calls, daily_tool_errors).

    Each record: {flushed_at, uptime_since, sessions, tools: {name: {calls,
    errors}}, clients: {name: n}} with counters cumulative per worker boot
    (keyed by uptime_since).
    """
    boots = collections.defaultdict(dict)  # uptime_since -> {day: last record}
    for line in lines:
        try:
            d = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(d, dict):
            continue
        day = (d.get("flushed_at") or "")[:10]
        if not day:
            continue
        # Later flushes overwrite earlier ones: the last flush of a day holds
        # that day's maximum (counters are cumulative within a boot).
        boots[d.get("uptime_since", "?")][day] = d

    daily = collections.Counter()
    dsess = collections.Counter()
    dcli = collections.defaultdict(collections.Counter)
    dtools = collections.defaultdict(collections.Counter)
    derr = collections.defaultdict(collections.Counter)
    dsub = collections.defaultdict(collections.Counter)   # answered
    dempty = collections.defaultdict(collections.Counter)  # ran, no answer
    # Provenance and diagnosis. An answered rate is only worth reading
    # alongside how much of it was measured rather than inferred from an
    # HTTP status, and an empty rate is only actionable with a reason.
    ddecl = collections.defaultdict(collections.Counter)   # route said so
    dstat = collections.defaultdict(collections.Counter)   # status guessed
    dreason: dict = collections.defaultdict(
        lambda: collections.defaultdict(collections.Counter))

    for _boot, bydays in boots.items():
        prev_tools: dict = {}
        prev_sess = 0
        prev_cli: dict = {}
        for day in sorted(bydays):
            d = bydays[day]
            tools = d.get("tools") or {}
            for name, v in tools.items():
                pc = (prev_tools.get(name) or {}).get("calls", 0)
                pe = (prev_tools.get(name) or {}).get("errors", 0)
                ps = (prev_tools.get(name) or {}).get("substantive", 0)
                pn = (prev_tools.get(name) or {}).get("empty", 0)
                delta_c = max(0, v.get("calls", 0) - pc)
                delta_e = max(0, v.get("errors", 0) - pe)
                dtools[day][name] += delta_c
                derr[day][name] += delta_e
                # Absent on records written before outcome labelling shipped;
                # those days simply report 0 labelled, which the coverage
                # line below makes visible rather than hiding.
                dsub[day][name] += max(0, v.get("substantive", 0) - ps)
                dempty[day][name] += max(0, v.get("empty", 0) - pn)
                pd = (prev_tools.get(name) or {}).get("outcome_declared", 0)
                pst = (prev_tools.get(name) or {}).get("outcome_from_status", 0)
                ddecl[day][name] += max(0, v.get("outcome_declared", 0) - pd)
                dstat[day][name] += max(0, v.get("outcome_from_status", 0) - pst)
                pr = (prev_tools.get(name) or {}).get("empty_reasons") or {}
                for reason, cnt in (v.get("empty_reasons") or {}).items():
                    dreason[day][name][reason] += max(0, cnt - pr.get(reason, 0))
                daily[day] += delta_c
            sess = d.get("sessions", 0) or 0
            dsess[day] += max(0, sess - prev_sess)
            for k, v in (d.get("clients") or {}).items():
                dcli[day][k] += max(0, v - prev_cli.get(k, 0))
            prev_tools, prev_sess = tools, sess
            prev_cli = dict(d.get("clients") or {})
    return daily, dsess, dcli, dtools, derr, dsub, dempty, ddecl, dstat, dreason


def main() -> int:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT
    if not path.exists():
        print(f"not found: {path}", file=sys.stderr)
        return 1
    with open(path, encoding="utf-8", errors="replace") as f:
        (daily, dsess, dcli, dtools, derr, dsub, dempty,
         ddecl, dstat, dreason) = reconstruct(f)
    if not daily:
        print("no parseable flush records")
        return 1

    days = sorted(daily)
    print(f"{path}  ({len(days)} days: {days[0]} .. {days[-1]})\n")
    print("day         calls  sessions  claude.ai  chatgpt  claude-code   other")
    for day in days[-14:]:
        c = dcli[day]
        print(f"{day} {daily[day]:7d} {dsess[day]:9d} {c.get('claude.ai', 0):10d}"
              f" {c.get('chatgpt', 0):8d} {c.get('claude-code', 0):12d}"
              f" {c.get('other', 0):7d}")

    cut = days[-30] if len(days) >= 30 else days[0]
    window = [d for d in days if d >= cut]
    print(f"\n{len(window)}-day totals: "
          f"{sum(daily[d] for d in window):,} calls, "
          f"{sum(dsess[d] for d in window):,} sessions")

    tools = collections.Counter()
    errs = collections.Counter()
    subs = collections.Counter()
    empt = collections.Counter()
    for d in window:
        tools.update(dtools[d])
        errs.update(derr[d])
        subs.update(dsub[d])
        empt.update(dempty[d])

    decl = collections.Counter()
    stat = collections.Counter()
    reasons: dict = collections.defaultdict(collections.Counter)
    for d in window:
        decl.update(ddecl[d])
        stat.update(dstat[d])
        for name, c in dreason[d].items():
            reasons[name].update(c)

    print(f"\ntop tools over {len(window)} days:")
    print(f"  {'tool':30s} {'calls':>9s}  {'err':>7s}  {'answered':>9s}  "
          f"{'empty':>8s}  {'labelled':>8s}  {'measured':>8s}")
    for name, n in tools.most_common(15):
        e, s, m = errs.get(name, 0), subs.get(name, 0), empt.get(name, 0)
        labelled = s + m
        cov = f"{100 * labelled / max(1, n):.0f}%"
        ans = f"{100 * s / labelled:.1f}%" if labelled else "-"
        # `measured` is the share of outcomes the route declared. The
        # remainder was inferred from the HTTP status, which cannot see a
        # 200 carrying an empty list — so a low figure here means the
        # answered rate beside it is optimistic, not wrong.
        srcs = decl.get(name, 0) + stat.get(name, 0)
        meas = f"{100 * decl.get(name, 0) / srcs:.0f}%" if srcs else "-"
        print(f"  {name:30s} {n:9,}  {100 * e / max(1, n):6.2f}%  {ans:>9s}  "
              f"{m:8,}  {cov:>8s}  {meas:>8s}")

    if reasons:
        print("\nwhy calls answered nothing:")
        ranked = sorted(reasons.items(), key=lambda kv: -sum(kv[1].values()))
        for name, c in ranked[:8]:
            top = ", ".join(f"{r} {v:,}" for r, v in c.most_common(4))
            print(f"  {name:30s} {top}")

    tot_calls = sum(tools.values())
    tot_lab = sum(subs.values()) + sum(empt.values())
    tot_sub = sum(subs.values())
    print(f"\nanswered rate: ", end="")
    if tot_lab:
        print(f"{tot_sub:,} of {tot_lab:,} labelled calls returned substance "
              f"({100 * tot_sub / tot_lab:.1f}%); "
              f"{100 * tot_lab / max(1, tot_calls):.0f}% of all calls carry a label")
    else:
        print("no labelled calls yet (outcome labelling ships with the next deploy)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
