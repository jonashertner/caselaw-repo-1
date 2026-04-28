#!/usr/bin/env python3
"""
Daily / per-feature roll-up of Anthropic API spend logged by the MCP
server.

Reads the JSONL receipts at $OCL_LLM_USAGE_LOG (default
`logs/llm_usage.jsonl`) and prints a summary by day, model, and feature.

Usage:
    python3 scripts/llm_usage_report.py                    # all-time summary
    python3 scripts/llm_usage_report.py --days 7           # last 7 days
    python3 scripts/llm_usage_report.py --since 2026-04-01
    python3 scripts/llm_usage_report.py --by feature       # group by feature
    python3 scripts/llm_usage_report.py --json             # machine-readable
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

DEFAULT_LOG_PATH = Path(os.environ.get(
    "OCL_LLM_USAGE_LOG",
    str(Path(__file__).resolve().parent.parent / "logs" / "llm_usage.jsonl"),
))


def _load_records(path: Path, since: date | None) -> list[dict]:
    if not path.exists():
        return []
    records: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = r.get("ts") or ""
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                continue
            if since and dt.date() < since:
                continue
            r["_dt"] = dt
            records.append(r)
    return records


def _aggregate(records: list[dict], group_by: str) -> dict:
    """group_by ∈ {'day', 'feature', 'model', 'day_model', 'day_feature'}"""
    out: dict = defaultdict(lambda: {"calls": 0, "in": 0, "out": 0,
                                      "cache_r": 0, "cache_w": 0,
                                      "cost_usd": 0.0, "errors": 0})

    def _key(r: dict) -> str:
        d = r["_dt"].date().isoformat()
        if group_by == "day":         return d
        if group_by == "feature":     return r.get("feature", "?")
        if group_by == "model":       return r.get("model", "?")
        if group_by == "day_model":   return f"{d}  {r.get('model', '?')}"
        if group_by == "day_feature": return f"{d}  {r.get('feature', '?')}"
        return d

    for r in records:
        k = _key(r)
        out[k]["calls"] += 1
        out[k]["in"] += int(r.get("in") or 0)
        out[k]["out"] += int(r.get("out") or 0)
        out[k]["cache_r"] += int(r.get("cache_r") or 0)
        out[k]["cache_w"] += int(r.get("cache_w") or 0)
        out[k]["cost_usd"] += float(r.get("cost_usd") or 0.0)
        if not r.get("ok", True):
            out[k]["errors"] += 1
    return out


def _print_table(rows: dict, key_label: str) -> None:
    keys = sorted(rows.keys())
    print(f"{key_label:38s} {'calls':>7} {'in_tok':>9} {'out_tok':>8} "
          f"{'errors':>7} {'cost_usd':>10}")
    print("─" * 84)
    total_calls = total_in = total_out = total_err = 0
    total_cost = 0.0
    for k in keys:
        v = rows[k]
        print(f"{k[:38]:38s} {v['calls']:>7} {v['in']:>9} {v['out']:>8} "
              f"{v['errors']:>7} ${v['cost_usd']:>9.4f}")
        total_calls += v["calls"]; total_in += v["in"]
        total_out += v["out"]; total_err += v["errors"]
        total_cost += v["cost_usd"]
    print("─" * 84)
    print(f"{'TOTAL':38s} {total_calls:>7} {total_in:>9} {total_out:>8} "
          f"{total_err:>7} ${total_cost:>9.4f}")


def main():
    ap = argparse.ArgumentParser(description="LLM usage / cost roll-up")
    ap.add_argument("--log", default=str(DEFAULT_LOG_PATH),
                    help=f"Path to llm_usage.jsonl (default: {DEFAULT_LOG_PATH})")
    ap.add_argument("--days", type=int, default=None,
                    help="Limit to last N days")
    ap.add_argument("--since", default=None,
                    help="Limit to records on/after YYYY-MM-DD")
    ap.add_argument("--by", choices=("day", "feature", "model",
                                      "day_model", "day_feature"),
                    default="day",
                    help="Group by (default: day)")
    ap.add_argument("--json", action="store_true",
                    help="Output JSON instead of a table")
    args = ap.parse_args()

    log_path = Path(args.log)
    since: date | None = None
    if args.since:
        since = date.fromisoformat(args.since)
    elif args.days:
        since = (datetime.now(timezone.utc).date()
                 - timedelta(days=args.days - 1))

    records = _load_records(log_path, since)
    if not records:
        print(f"No records in {log_path}"
              + (f" since {since}" if since else ""), file=sys.stderr)
        sys.exit(0 if log_path.exists() else 1)

    agg = _aggregate(records, args.by)

    if args.json:
        print(json.dumps({
            "log": str(log_path),
            "since": since.isoformat() if since else None,
            "n_records": len(records),
            "group_by": args.by,
            "rows": agg,
        }, indent=2, ensure_ascii=False))
        return

    label = {"day": "date", "feature": "feature", "model": "model",
             "day_model": "date · model", "day_feature": "date · feature"}[args.by]
    print(f"\nLLM usage from {log_path} "
          f"({len(records)} records"
          + (f", since {since}" if since else "") + ")\n")
    _print_table(agg, label)


if __name__ == "__main__":
    main()
