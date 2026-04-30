"""Run import_jsonl on JUST sg_publikationen.jsonl into a fresh empty DB.
If all 12,652 rows land → bug is cross-shard interaction during full rebuild.
If <12,652 land → bug is in the per-file processing.
"""
import sys
import sqlite3
import json
from pathlib import Path
import shutil
import tempfile

sys.path.insert(0, "/opt/caselaw/repo")

from db_schema import SCHEMA_SQL, COVERAGE_SCHEMA_SQL  # noqa: E402
from build_fts5 import insert_decision  # noqa: E402

JSONL = Path("/opt/caselaw/repo/output/decisions/sg_publikationen.jsonl")

# Create temp DB with full schema
tmp_db = Path(tempfile.mkdtemp()) / "test.db"
print(f"Creating fresh DB at {tmp_db}")
conn = sqlite3.connect(str(tmp_db))
conn.executescript(SCHEMA_SQL)
try:
    conn.executescript(COVERAGE_SCHEMA_SQL)
except Exception:
    pass

# Import sg_publikationen.jsonl
print(f"Importing {JSONL.name} ...")
imported = 0
ignored = 0
exception_count = 0
parse_errors = 0
exception_examples = []
chamber_imported = {}

with open(JSONL, "rb") as fb:
    import io
    f = io.TextIOWrapper(fb, encoding="utf-8", errors="replace")
    for lineno, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as e:
            parse_errors += 1
            if parse_errors <= 3:
                print(f"  parse error at line {lineno}: {e}")
            continue

        court = row.get("court", "(none)")
        try:
            ok = insert_decision(conn, row)
            if ok:
                imported += 1
                chamber_imported[court] = chamber_imported.get(court, 0) + 1
            else:
                ignored += 1
        except Exception as e:
            exception_count += 1
            if len(exception_examples) < 3:
                exception_examples.append((row.get("decision_id"), type(e).__name__, str(e)))

conn.commit()

# Final db count
db_count = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
db_per_chamber = dict(conn.execute(
    "SELECT court, COUNT(*) FROM decisions GROUP BY court ORDER BY 2 DESC"
).fetchall())

conn.close()
shutil.rmtree(tmp_db.parent)

print()
print("=== isolated import results ===")
print(f"  imported: {imported}")
print(f"  ignored (rowcount=0): {ignored}")
print(f"  exceptions: {exception_count}")
print(f"  parse errors: {parse_errors}")
print(f"  total db rows: {db_count}")
print()
print("=== per-chamber JSONL parse vs db ===")
for court, n in sorted(chamber_imported.items(), key=lambda x: -x[1]):
    db_n = db_per_chamber.get(court, 0)
    print(f"  {court:35s} imported={n:>5d} in_db={db_n:>5d}")

if exception_examples:
    print()
    print("=== exception examples ===")
    for did, et, em in exception_examples:
        print(f"  {did}: {et}: {em}")
