"""Diagnose multiple cantons for the alphabetical-collision pattern.
For each (direct_shard, es_shard) pair: check if their decision_id prefixes
align AND simulate the collision to see how many direct rows get IGNOREd.
"""
import sys
import sqlite3
import json
import io
import shutil
import tempfile
from pathlib import Path
from collections import Counter

sys.path.insert(0, "/opt/caselaw/repo")

from db_schema import SCHEMA_SQL, COVERAGE_SCHEMA_SQL  # noqa: E402
from build_fts5 import insert_decision  # noqa: E402

JSONL_DIR = Path("/opt/caselaw/repo/output/decisions")


def run_test(label, direct_shard, es_shards):
    """Replay the alphabetical processing order for these specific shards."""
    print(f"\n{'='*72}")
    print(f"  {label}: direct={direct_shard.name}  es={[p.name for p in es_shards]}")
    print('='*72)

    # Sample direct shard chamber distribution
    direct_chambers = Counter()
    direct_id_prefixes = Counter()
    with open(direct_shard) as f:
        for line in f:
            d = json.loads(line)
            direct_chambers[d.get("court", "(none)")] += 1
            did = d.get("decision_id", "")
            # Capture first segment of decision_id (before second underscore)
            parts = did.split("_", 2)
            prefix = "_".join(parts[:2]) if len(parts) >= 2 else did
            direct_id_prefixes[prefix] += 1

    print(f"\n  direct shard chamber distribution:")
    for c, n in direct_chambers.most_common():
        print(f"    {c:35s} {n:>6d}")
    print(f"  direct shard decision_id prefix distribution (top 5):")
    for p, n in direct_id_prefixes.most_common(5):
        print(f"    {p:35s} {n:>6d}")

    if not es_shards:
        print(f"\n  no es shards — no collision possible")
        return

    # Reproduce alphabetical-order processing
    tmp_db = Path(tempfile.mkdtemp()) / "test.db"
    conn = sqlite3.connect(str(tmp_db))
    conn.executescript(SCHEMA_SQL)
    try:
        conn.executescript(COVERAGE_SCHEMA_SQL)
    except Exception:
        pass

    files_in_order = sorted([direct_shard] + list(es_shards), key=lambda p: p.name)
    print(f"\n  alphabetical processing order:")
    for f in files_in_order:
        print(f"    {f.name}")

    for path in files_in_order:
        n_imp, n_ig = 0, 0
        with open(path, "rb") as fb:
            text_f = io.TextIOWrapper(fb, encoding="utf-8", errors="replace")
            for line in text_f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    ok = insert_decision(conn, row)
                    if ok:
                        n_imp += 1
                    else:
                        n_ig += 1
                except Exception:
                    pass
        conn.commit()
        print(f"    {path.name}: imported={n_imp} ignored={n_ig}")

    # Per-chamber landing for direct
    chamber_landed = Counter()
    with open(direct_shard) as f:
        for line in f:
            d = json.loads(line)
            r = conn.execute(
                "SELECT court FROM decisions WHERE decision_id=?",
                (d["decision_id"],),
            ).fetchone()
            if r and r[0] == d["court"]:
                chamber_landed[d["court"]] += 1

    print(f"\n  direct per-chamber LANDING (correct court vs jsonl count):")
    significant_loss = False
    for chamber in sorted(direct_chambers):
        j = direct_chambers[chamber]
        lan = chamber_landed.get(chamber, 0)
        loss = j - lan
        pct = 100 * loss / j if j else 0
        marker = "  *** BUG ***" if pct > 50 else ""
        if pct > 50:
            significant_loss = True
        print(f"    {chamber:35s} jsonl={j:>5d} landed={lan:>5d} loss={loss:>5d} ({pct:.0f}%){marker}")

    if significant_loss:
        print(f"  ⚠ SIGNIFICANT LOSS DETECTED — same pattern as SG bug")

    conn.close()
    shutil.rmtree(tmp_db.parent)


# Test cases — direct shard + matching es shards
TESTS = [
    ("TG", JSONL_DIR / "tg_gerichte.jsonl",
     [JSONL_DIR / "es_tg_obergericht.jsonl"]),
    ("BE-Verwaltungsgericht",
     JSONL_DIR / "be_verwaltungsgericht.jsonl",
     [JSONL_DIR / "es_be_verwaltungsgericht.jsonl"]),
    ("BE-Zivilstraf",
     JSONL_DIR / "be_zivilstraf.jsonl",
     [JSONL_DIR / "es_be_zivilstraf.jsonl"]),
    ("GE", JSONL_DIR / "ge_gerichte.jsonl",
     [JSONL_DIR / "es_ge_gerichte.jsonl"]),
    ("BL", JSONL_DIR / "bl_gerichte.jsonl",
     [JSONL_DIR / "es_bl_gerichte.jsonl"]),
]

for label, direct, es_list in TESTS:
    if not direct.exists():
        print(f"\n{label}: direct shard {direct.name} not found, skipping")
        continue
    es_existing = [p for p in es_list if p.exists()]
    run_test(label, direct, es_existing)
