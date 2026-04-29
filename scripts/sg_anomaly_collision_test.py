"""SG anomaly root cause: decision_id collisions across shards?

90% of sg_verwaltungsrekurskommission and sg_kantonsgericht JSONL
rows fail to land in db, while sg_versicherungsgericht and
sg_verwaltungsgericht reach parity.  Test whether the missing rows
have decision_ids that collide with rows from another shard.
"""
import json
import sqlite3
from pathlib import Path

JSONL_DIR = Path("/opt/caselaw/repo/output/decisions")
DB = "/opt/caselaw/repo/output/decisions.db"


def main():
    c = sqlite3.connect(DB).cursor()

    # Pull all decision_ids from sg_publikationen.jsonl
    direct_rows = []
    with open(JSONL_DIR / "sg_publikationen.jsonl") as f:
        for line in f:
            d = json.loads(line)
            direct_rows.append((d["decision_id"], d.get("court")))

    print(f"sg_publikationen.jsonl: {len(direct_rows)} rows")

    # Check for internal duplicates
    seen = set()
    dups = 0
    for did, _ in direct_rows:
        if did in seen:
            dups += 1
        seen.add(did)
    print(f"  unique decision_ids: {len(seen)}, internal duplicates: {dups}")

    # For each chamber, sample 5 missing rows and check db
    print()
    for chamber in ["sg_verwaltungsrekurskommission", "sg_kantonsgericht",
                    "sg_handelsgericht", "sg_versicherungsgericht"]:
        chamber_ids = [did for did, c in direct_rows if c == chamber]
        in_db = c.execute(
            "SELECT decision_id, court, source FROM decisions WHERE decision_id IN (" +
            ",".join("?" * len(chamber_ids[:1000])) + ")",
            chamber_ids[:1000],
        ).fetchall()
        print(f"--- {chamber} ---")
        print(f"  jsonl rows: {len(chamber_ids)}")
        print(f"  of first 1000 jsonl IDs, in db: {len(in_db)}")
        # How many landed under the EXPECTED court vs different court
        same_court = sum(1 for r in in_db if r[1] == chamber)
        diff_court = len(in_db) - same_court
        print(f"    same court ({chamber}): {same_court}")
        print(f"    different court (collision!): {diff_court}")
        if diff_court > 0:
            for r in in_db:
                if r[1] != chamber:
                    print(f"      example: {r[0]} → court={r[1]} source={r[2]}")
                    break
        print()


if __name__ == "__main__":
    main()
