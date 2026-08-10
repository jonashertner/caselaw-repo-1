#!/usr/bin/env python3
"""Best-effort Codex hook for obvious high-risk shell commands.

The authoritative guard is scripts/agent_safe_deploy.py. This hook is a small
front-line brake for project-local Codex sessions that trust .codex/hooks.json.
If Codex changes the hook event payload shape, the hook exits 0 rather than
blocking unknown safe work.
"""
from __future__ import annotations

import json
import re
import sys
from typing import Any


BLOCK_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bgit\s+reset\s+--hard\b"), "destructive git reset"),
    (re.compile(r"\bgit\s+checkout\s+--\b"), "destructive checkout of local changes"),
    (re.compile(r"\brm\s+-[^\n]*r[^\n]*f\b"), "recursive force delete"),
    (re.compile(r"\bsystemctl\s+restart\s+mcp-server@"), "production worker restart"),
    (re.compile(r"\bsystemctl\s+start\s+opencaselaw-publish\b"), "manual publish trigger"),
    (re.compile(r"\bcrontab\s+-l\b"), "crontab may contain secrets"),
    (
        re.compile(r"\bsqlite3\b.*\b(decisions|reference_graph|decision_structure)\.db\b.*\b(INSERT|UPDATE|DELETE|DROP|ALTER|VACUUM)\b", re.I),
        "write-like operation against served SQLite DB",
    ),
    (
        re.compile(r"\b(cp|mv)\b.*\b(decisions|reference_graph|decision_structure)\.db\b"),
        "manual copy/move of served SQLite DB",
    ),
]


def _walk(obj: Any) -> list[str]:
    values: list[str] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in {"command", "cmd", "shell_command"} and isinstance(value, str):
                values.append(value)
            else:
                values.extend(_walk(value))
    elif isinstance(obj, list):
        for value in obj:
            values.extend(_walk(value))
    return values


def _extract_command(raw: str) -> str:
    if not raw.strip():
        return ""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    commands = _walk(payload)
    return "\n".join(commands)


def main() -> int:
    command = _extract_command(sys.stdin.read())
    if not command:
        return 0
    for pattern, reason in BLOCK_PATTERNS:
        if pattern.search(command):
            print(f"Blocked by OpenCaseLaw Codex policy: {reason}", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

