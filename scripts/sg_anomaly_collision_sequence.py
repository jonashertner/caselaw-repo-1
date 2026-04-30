"""Test the actual collision sequence.

In production rebuild: alphabetical glob means es_sg_publikationen.jsonl
is processed BEFORE sg_publikationen.jsonl.  Reproduce that ordering in
a fresh DB and count what lands.
"""
import sys
import sqlite3
import json
import io
import shutil
import tempfile
from pathlib import Path

sys.path.insert(0, "/opt/caselaw/repo")

from db_schema import SCHEMA_SQL, COVERAGE_SCHEMA_SQL  # noqa: E402
from build_fts5 import insert_decision  # noqa: E402

JSONL_DIR = Path("/opt/caselaw/repo/output/decisions")

tmp_db = Path(tempfile.mkdtemp()) / "test.db"
conn = sqlite3.connect(str(tmp_db))
conn.executescript(SCHEMA_SQL)
try:
    conn.executescript(COVERAGE_SCHEMA_SQL)
except Exception:
    pass


def import_file(path: Path):
    n_imported, n_ignored, n_exc = 0, 0, 0
    with open(path, "rb") as fb:
        f = io.TextIOWrapper(fb, encoding="utf-8", errors="replace")
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                ok = insert_decision(conn, row)
                if ok:
                    n_imported += 1
                else:
                    n_ignored += 1
            except Exception:
                n_exc += 1
    conn.commit()
    return n_imported, n_ignored, n_exc


# Step 1: process es_sg_publikationen.jsonl first
es_path = JSONL_DIR / "es_sg_publikationen.jsonl"
print(f"--- step 1: import {es_path.name} (would-be first in alpha order) ---")
es_imported, es_ignored, es_exc = import_file(es_path)
print(f"  imported={es_imported}, ignored={es_ignored}, exceptions={es_exc}")

# Step 2: process sg_publikationen.jsonl
direct_path = JSONL_DIR / "sg_publikationen.jsonl"
print(f"--- step 2: import {direct_path.name} ---")
d_imported, d_ignored, d_exc = import_file(direct_path)
print(f"  imported={d_imported}, ignored={d_ignored}, exceptions={d_exc}")

# Final breakdown
print()
print("=== final db breakdown by court ===")
for court, n in conn.execute(
    "SELECT court, COUNT(*) FROM decisions GROUP BY court ORDER BY 2 DESC"
).fetchall():
    print(f"  {court:35s} {n:>6d}")

# Per-chamber check for sg_publikationen.jsonl rows
print()
print("=== sg_publikationen.jsonl per-chamber db landing ===")
chamber_jsonl = {}
chamber_landed = {}
with open(direct_path) as f:
    for line in f:
        d = json.loads(line)
        chamber_jsonl[d["court"]] = chamber_jsonl.get(d["court"], 0) + 1
        r = conn.execute(
            "SELECT court FROM decisions WHERE decision_id=?",
            (d["decision_id"],),
        ).fetchone()
        if r and r[0] == d["court"]:
            chamber_landed[d["court"]] = chamber_landed.get(d["court"], 0) + 1

for chamber in sorted(chamber_jsonl):
    j = chamber_jsonl[chamber]
    lan = chamber_landed.get(chamber, 0)
    print(f"  {chamber:35s} jsonl={j:>5d}  landed_under_same_court={lan:>5d}  loss={j-lan:>5d} ({100*(j-lan)/j:.0f}%)")

conn.close()
shutil.rmtree(tmp_db.parent)
