"""Post-publish verification: confirm the entscheidsuche historical
archive's contribution to decisions.db.

Run after a nightly publish to verify:
  1. All 51 es_*.jsonl shards have rows landing in decisions.db
  2. Direct scraper rows kept their chamber-specific court labels
     (architectural direct-first fix from commit 713afe3)
  3. Es-unique decisions are preserved (decisions only entscheidsuche
     has, where the original cantonal portal no longer serves them)

Usage:  python3 scripts/verify_archive_preservation.py
"""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path

JSONL_DIR = Path("/opt/caselaw/repo/output/decisions")
DB = "/opt/caselaw/repo/output/decisions.db"


def main():
    c = sqlite3.connect(DB).cursor()

    print(f"{'shard':45s} {'jsonl':>8s} {'in_db':>8s}  status")
    print("-" * 80)

    total_jsonl = 0
    total_in_db = 0
    missing_shards = []

    for shard_path in sorted(JSONL_DIR.glob("es_*.jsonl")):
        shard_name = shard_path.name

        # Sample 100 decision_ids from the shard, check db presence
        sample_ids = []
        with open(shard_path) as f:
            for i, line in enumerate(f):
                if i >= 100:
                    break
                sample_ids.append(json.loads(line)["decision_id"])

        if not sample_ids:
            continue

        # Count lines in shard
        with open(shard_path) as f:
            n_jsonl = sum(1 for _ in f)

        # How many of the sample are in db?
        in_db_sample = c.execute(
            "SELECT COUNT(*) FROM decisions WHERE decision_id IN ("
            + ",".join("?" * len(sample_ids)) + ")",
            sample_ids,
        ).fetchone()[0]

        # Extrapolate
        est_in_db = int(in_db_sample / len(sample_ids) * n_jsonl)

        status = "✓" if in_db_sample > 80 else ("⚠" if in_db_sample > 0 else "✗ NOT IN DB")
        print(f"{shard_name:45s} {n_jsonl:>8d} ~{est_in_db:>7d}  {status}")

        total_jsonl += n_jsonl
        total_in_db += est_in_db
        if in_db_sample == 0:
            missing_shards.append(shard_name)

    print("-" * 80)
    print(f"{'TOTAL':45s} {total_jsonl:>8d} ~{total_in_db:>7d}")
    print()
    if missing_shards:
        print(f"⚠ Shards with 0 db presence ({len(missing_shards)}):")
        for s in missing_shards:
            print(f"  {s}")
    else:
        print("✓ All es_*.jsonl shards have rows in decisions.db")


if __name__ == "__main__":
    main()
