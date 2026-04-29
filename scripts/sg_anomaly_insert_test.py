"""Try to manually insert one of the missing SG rows.
If it succeeds → insert path is fine, issue is elsewhere
If it fails with specific error → that's the bug.
"""
import json
import sqlite3
import sys
sys.path.insert(0, "/opt/caselaw/repo")

from build_fts5 import insert_decision  # noqa: E402

# Find a missing SG decision_id
JSONL = "/opt/caselaw/repo/output/decisions/sg_publikationen.jsonl"
DB = "/opt/caselaw/repo/output/decisions.db"

# Open DB read-only-ish (for testing — we'll roll back)
conn = sqlite3.connect(DB)
c = conn.cursor()

# Find first 5 missing rows of court=sg_verwaltungsrekurskommission
attempts = 0
successes = 0
failures = []
with open(JSONL) as f:
    for line in f:
        d = json.loads(line)
        if d.get("court") != "sg_verwaltungsrekurskommission":
            continue
        did = d["decision_id"]
        # Already in db?
        existing = c.execute(
            "SELECT 1 FROM decisions WHERE decision_id=?", (did,)
        ).fetchone()
        if existing:
            continue
        # Try insert
        attempts += 1
        try:
            ok = insert_decision(conn, d)
            if ok:
                successes += 1
                print(f"  SUCCESS: {did} inserted")
            else:
                # rowcount was 0 — means INSERT OR IGNORE collided
                conflict = c.execute(
                    "SELECT decision_id, court FROM decisions WHERE decision_id=?",
                    (did,),
                ).fetchone()
                failures.append((did, "INSERT IGNORE → conflict", conflict))
                print(f"  IGNORE: {did} → existing={conflict}")
        except Exception as e:
            failures.append((did, type(e).__name__, str(e)))
            print(f"  EXCEPTION: {did}: {type(e).__name__}: {e}")
        if attempts >= 10:
            break

# Rollback so we don't pollute the DB
conn.rollback()
conn.close()

print()
print(f"=== summary ===")
print(f"  attempts: {attempts}, successes: {successes}, failures: {len(failures)}")
