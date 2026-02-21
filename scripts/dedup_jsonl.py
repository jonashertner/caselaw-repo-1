#!/usr/bin/env python3
"""
Deduplicate a JSONL file by decision_id, keeping the last occurrence.

Streams line-by-line so it works on large files without loading everything
into memory. Writes to a temp file then does an atomic replace.

Usage:
    python3 scripts/dedup_jsonl.py output/decisions/bl_gerichte.jsonl
    python3 scripts/dedup_jsonl.py output/decisions/bl_gerichte.jsonl --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path


def dedup_jsonl(path: Path, dry_run: bool = False) -> tuple[int, int, int]:
    """Deduplicate JSONL file in-place, keeping last occurrence per decision_id.

    Returns (total_lines, unique_lines, duplicates_removed).
    """
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        return 0, 0, 0

    # Pass 1: find which decision_ids appear more than once,
    # and for each, record the last line number.
    seen: dict[str, int] = {}  # decision_id -> last line number
    total = 0
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                obj = json.loads(line)
                did = obj.get("decision_id", "")
                if did:
                    seen[did] = lineno
            except json.JSONDecodeError:
                pass

    unique = len(seen)
    dupes = total - unique

    print(f"File: {path}")
    print(f"  Total lines: {total}")
    print(f"  Unique decision_ids: {unique}")
    print(f"  Duplicates to remove: {dupes}")

    if dupes == 0:
        print("  No duplicates found.")
        return total, unique, 0

    if dry_run:
        print("  [dry-run] No changes made.")
        return total, unique, dupes

    # Pass 2: rewrite file keeping only the last occurrence of each decision_id.
    # For lines without a decision_id or invalid JSON, keep them.
    kept = 0
    dir_path = path.parent
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=dir_path, suffix=".tmp", delete=False
    ) as tmp:
        tmp_path = Path(tmp.name)
        with open(path, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    obj = json.loads(stripped)
                    did = obj.get("decision_id", "")
                    if did and seen.get(did) != lineno:
                        continue  # skip earlier duplicate
                except json.JSONDecodeError:
                    pass  # keep malformed lines
                tmp.write(stripped + "\n")
                kept += 1

    # Atomic replace
    original_stat = path.stat()
    os.replace(tmp_path, path)

    new_size = path.stat().st_size
    print(f"  Kept: {kept} lines")
    print(f"  Removed: {total - kept} duplicates")
    print(f"  File size: {original_stat.st_size / 1024 / 1024:.1f} MB -> {new_size / 1024 / 1024:.1f} MB")

    return total, kept, total - kept


def main():
    parser = argparse.ArgumentParser(description="Deduplicate JSONL by decision_id")
    parser.add_argument("files", nargs="+", type=Path, help="JSONL files to deduplicate")
    parser.add_argument("--dry-run", action="store_true", help="Show stats without modifying")
    args = parser.parse_args()

    total_dupes = 0
    for path in args.files:
        _, _, dupes = dedup_jsonl(path, dry_run=args.dry_run)
        total_dupes += dupes
        print()

    if total_dupes > 0 and not args.dry_run:
        print(f"Total duplicates removed: {total_dupes}")
    elif total_dupes > 0:
        print(f"Total duplicates found: {total_dupes} (dry-run, no changes)")


if __name__ == "__main__":
    main()
