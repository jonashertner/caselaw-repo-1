#!/usr/bin/env python3
"""Append an evidence record to docs/agent-loop/LOG.md."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DEFAULT_LOG = REPO / "docs" / "agent-loop" / "LOG.md"


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _section(title: str, body: str | None) -> str:
    lines = [f"{title}:"]
    if not body:
        lines.append("- Not provided.")
        return "\n".join(lines)
    for line in body.splitlines():
        stripped = line.strip()
        if stripped:
            lines.append(f"- {stripped}")
    if len(lines) == 1:
        lines.append("- Not provided.")
    return "\n".join(lines)


def format_entry(*, timestamp: str, situation: str | None, action: str, evidence: str, outcome: str) -> str:
    return (
        f"\n## {timestamp}\n\n"
        f"{_section('Situation report', situation)}\n\n"
        f"{_section('Action', action)}\n\n"
        f"{_section('Evidence', evidence)}\n\n"
        f"{_section('Outcome', outcome)}\n"
    )


def append_entry(
    *,
    log_path: Path,
    situation: str | None,
    action: str,
    evidence: str,
    outcome: str,
    timestamp: str | None = None,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not log_path.exists():
        log_path.write_text("# OpenCaseLaw Agent Loop Log\n", encoding="utf-8")
    entry = format_entry(
        timestamp=timestamp or _timestamp(),
        situation=situation,
        action=action,
        evidence=evidence,
        outcome=outcome,
    )
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(entry)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", default=str(DEFAULT_LOG), help="log path")
    parser.add_argument("--timestamp", help="override UTC timestamp")
    parser.add_argument("--situation", help="situation report text")
    parser.add_argument("--action", required=True, help="action taken")
    parser.add_argument("--evidence", required=True, help="verification evidence")
    parser.add_argument("--outcome", required=True, help="outcome")
    args = parser.parse_args()

    append_entry(
        log_path=Path(args.log),
        timestamp=args.timestamp,
        situation=args.situation,
        action=args.action,
        evidence=args.evidence,
        outcome=args.outcome,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

