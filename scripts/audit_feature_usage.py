#!/usr/bin/env python3
"""Audit per-tool / per-REST-route call counts from metrics.db.

Phase 0 of the refactor plan needs production-traffic evidence
before deleting any feature. This script aggregates the last N days
of `metrics.db` and emits Markdown:

- All endpoints by call volume
- A verdict section for each dead-code candidate the plan lists

`metrics.db` is the persistent counter store from `_record_tool_call`
in `mcp_server.py`. The `daily_tools.tool` column mixes MCP-tool
names (snake_case, e.g. `search_decisions`) with REST/web route
names (kebab-case, e.g. `case-brief`) and Word-add-in / Copilot
subset labels — a single source of truth for the public surface.

Usage:
    # On the VPS (default DB path works):
    python3 scripts/audit_feature_usage.py --output refactor/AUDIT.md

    # Locally against a copied DB:
    python3 scripts/audit_feature_usage.py --db /tmp/metrics.db --days 30
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = Path(
    os.environ.get("SWISS_CASELAW_DIR", "/opt/caselaw/repo/output")
) / "metrics.db"

# Each entry: (display label, list of LIKE substrings on daily_tools.tool).
DEAD_CODE_CANDIDATES = [
    ("anwaltsrecht_tags",        ["anwaltsrecht"]),
    ("Vector search",            ["vector"]),
    ("paragraph_embeddings",     ["paragraph_embed"]),
    ("study/curriculum_engine",  ["generate_exam_question", "curriculum", "study"]),
]


def open_db(path: Path) -> sqlite3.Connection:
    if not path.exists():
        sys.exit(f"metrics.db not found at {path}. Override with --db.")
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--output", type=Path, default=None,
                    help="Where to write the Markdown. Stdout if absent.")
    ap.add_argument("--top", type=int, default=200,
                    help="Cap the all-endpoints table at this many rows.")
    args = ap.parse_args()

    conn = open_db(args.db)
    now = dt.datetime.now(dt.timezone.utc)

    earliest, latest = conn.execute(
        "SELECT MIN(date), MAX(date) FROM daily_tools"
    ).fetchone()

    cutoff = f"date('now', '-{args.days} days')"

    rows = conn.execute(f"""
        SELECT tool,
               SUM(calls)            AS calls,
               SUM(errors)           AS errors,
               COUNT(DISTINCT date)  AS active_days,
               MIN(date)             AS first_seen,
               MAX(date)             AS last_seen
        FROM daily_tools
        WHERE date >= {cutoff}
        GROUP BY tool
        ORDER BY calls DESC
        LIMIT ?
    """, (args.top,)).fetchall()

    total_endpoints, total_calls = conn.execute(f"""
        SELECT COUNT(DISTINCT tool), COALESCE(SUM(calls), 0)
        FROM daily_tools
        WHERE date >= {cutoff}
    """).fetchone()

    verdicts = []
    for label, substrings in DEAD_CODE_CANDIDATES:
        matched = []
        seen = set()
        for ss in substrings:
            for r in conn.execute(f"""
                SELECT tool, SUM(calls), SUM(errors), MAX(date)
                FROM daily_tools
                WHERE date >= {cutoff} AND tool LIKE ?
                GROUP BY tool
            """, (f"%{ss}%",)):
                if r[0] not in seen:
                    seen.add(r[0])
                    matched.append(r)
        total = sum(r[1] for r in matched) if matched else 0
        verdict = (
            "REMOVE-CANDIDATE — zero traffic"
            if total == 0
            else f"KEEP — {total:,} call(s)"
        )
        verdicts.append((label, matched, total, verdict))

    out = []
    out.append("# Feature usage audit")
    out.append("")
    out.append(f"Generated: `{now.strftime('%Y-%m-%d %H:%M:%S UTC')}`")
    out.append(f"Window: last **{args.days} days**")
    out.append(f"Source: `{args.db}` (retention `{earliest}` → `{latest}`)")
    out.append("")
    out.append("`daily_tools.tool` mixes MCP-tool names (snake_case),")
    out.append("REST route names (kebab-case), Word-add-in labels")
    out.append("(`word-addin:*`), and Copilot subset labels — one source of")
    out.append("truth for the public surface.")
    out.append("")
    out.append("## Summary")
    out.append("")
    out.append(f"- Distinct endpoints called: **{total_endpoints}**")
    out.append(f"- Total calls in window: **{total_calls:,}**")
    out.append("")
    out.append("## Dead-code candidates (refactor plan Phase 1)")
    out.append("")
    out.append(f"| Feature | Matching endpoints | Calls in {args.days}d | Verdict |")
    out.append("|---|---|---:|---|")
    for label, matched, total, verdict in verdicts:
        eps = ", ".join(f"`{m[0]}`" for m in matched) if matched else "_(none)_"
        out.append(f"| {label} | {eps} | {total:,} | {verdict} |")
    out.append("")
    out.append(f"## All endpoints by call volume (top {len(rows)})")
    out.append("")
    out.append("| Endpoint | Calls | Errors | Active days | First | Last |")
    out.append("|---|---:|---:|---:|---|---|")
    for tool, calls, errors, active_days, first_seen, last_seen in rows:
        out.append(
            f"| `{tool}` | {calls:,} | {errors:,} | {active_days} "
            f"| {first_seen} | {last_seen} |"
        )
    out.append("")

    text = "\n".join(out)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
        print(f"wrote {args.output} ({len(rows)} rows)", file=sys.stderr)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
