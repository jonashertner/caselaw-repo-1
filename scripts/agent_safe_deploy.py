#!/usr/bin/env python3
"""Classify the current diff against the OpenCaseLaw autonomy policy.

This script does not deploy. It answers one question: are the changed paths in
the small autonomous-safe surface, or must this stop for a human/proposal?
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
DEFAULT_POLICY = REPO / "ops" / "autonomy-policy.json"


def load_policy(path: Path = DEFAULT_POLICY) -> dict[str, Any]:
    """Load the autonomy policy. FAIL CLOSED: a missing or invalid policy returns
    a minimal policy under which every path classifies 'unknown' → disallowed
    (allowed:false), rather than crashing the guard with FileNotFoundError (which
    would happen on a clean checkout if the policy file isn't committed)."""
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {
            "path_classes": [],
            "deploy": {"allowed_without_restart_globs": []},
            "required_verification": [],
        }


def _matches(path: str, pattern: str) -> bool:
    path = path.strip("/")
    pattern = pattern.strip("/")
    return fnmatch.fnmatchcase(path, pattern)


def classify_path(path: str, policy: dict[str, Any]) -> tuple[str, str]:
    for group in policy.get("path_classes", []):
        for pattern in group.get("globs", []):
            if _matches(path, pattern):
                return str(group["class"]), str(group.get("reason", ""))
    return "unknown", "not covered by autonomy policy"


def changed_files(repo: Path) -> list[str]:
    """All non-committed paths — modified/staged tracked files AND untracked
    files — via `git status --porcelain=v1 -uall`. FAIL CLOSED: untracked files
    are ALWAYS scanned, so an unreviewed untracked path (a new control-plane
    script, guardrail, secret) blocks the deploy instead of slipping through an
    opt-in flag. Ignored files (.gitignore) are excluded by git, as intended."""
    proc = subprocess.run(
        ["git", "status", "--porcelain=v1", "-uall"],
        cwd=str(repo),
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    files: list[str] = []
    for line in proc.stdout.splitlines():
        if len(line) < 4:
            continue
        path = line[3:].strip()
        if " -> " in path:  # rename status: "R  old -> new"
            path = path.split(" -> ", 1)[1].strip()
        if path:
            files.append(path)
    return sorted(set(files))


def deploy_without_restart(path: str, policy: dict[str, Any]) -> bool:
    patterns = policy.get("deploy", {}).get("allowed_without_restart_globs", [])
    return any(_matches(path, pattern) for pattern in patterns)


def evaluate(paths: list[str], policy: dict[str, Any]) -> dict[str, Any]:
    classifications: dict[str, list[dict[str, str]]] = {
        "safe_candidate": [],
        "proposal_only": [],
        "always_human": [],
        "unknown": [],
    }
    for path in sorted(set(paths)):
        klass, reason = classify_path(path, policy)
        classifications.setdefault(klass, []).append({"path": path, "reason": reason})

    disallowed = (
        classifications.get("proposal_only", [])
        + classifications.get("always_human", [])
        + classifications.get("unknown", [])
    )
    all_safe = bool(paths) and not disallowed
    no_restart = all(deploy_without_restart(path, policy) for path in paths) if paths else False
    allowed = all_safe and no_restart

    reasons: list[str] = []
    if not paths:
        reasons.append("no changed files to deploy")
    if classifications.get("always_human"):
        reasons.append("one or more paths are always-human")
    if classifications.get("proposal_only"):
        reasons.append("one or more paths are proposal-only")
    if classifications.get("unknown"):
        reasons.append("one or more paths are outside the autonomous policy")
    if all_safe and not no_restart:
        reasons.append("one or more safe-candidate paths are not deploy-without-restart paths")
    if allowed:
        reasons.append("all changed paths are autonomous safe candidates and deploy-without-restart")

    return {
        "schema_version": 1,
        "allowed": allowed,
        "changed_files": sorted(set(paths)),
        "classifications": classifications,
        "required_verification": policy.get("required_verification", []),
        "requires_restart": False if allowed else None,
        "reasons": reasons,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=str(REPO), help="repository root")
    parser.add_argument("--policy", default=str(DEFAULT_POLICY), help="policy JSON path")
    parser.add_argument("--changed-files", nargs="*", help="explicit paths to classify")
    parser.add_argument("--include-untracked", action="store_true",
                        help="(deprecated; untracked files are now always scanned — fail-closed)")
    parser.add_argument("--json", action="store_true", help="emit JSON (default; kept for runner clarity)")
    args = parser.parse_args(argv)

    repo = Path(args.repo).resolve()
    policy = load_policy(Path(args.policy))
    paths = args.changed_files if args.changed_files is not None else changed_files(repo)
    result = evaluate(paths, policy)
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0 if result["allowed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

