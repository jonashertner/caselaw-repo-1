"""V2 per-court audit — count via JSONL files (each shard's actual contribution),
NOT via decisions.db's `source` column (which is post-dedup-merge label that
loses provenance for rows where the es_* shard happened to win the merge).
"""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path

JSONL_DIR = Path("/opt/caselaw/repo/output/decisions")

# (canton, court_code, has_es_shard)
COURTS = [
    ("AG", "ag_gerichte"),
    ("AI", "ai_gerichte"),
    ("AR", "ar_gerichte"),
    ("BE", "be_zivilstraf"),
    ("BE", "be_verwaltungsgericht"),
    ("BE", "be_anwaltsaufsicht"),
    ("BE", "be_steuerrekurs"),
    ("BL", "bl_gerichte"),
    ("BS", "bs_gerichte"),
    ("FR", "fr_gerichte"),
    ("GE", "ge_gerichte"),
    ("GL", "gl_gerichte"),
    ("GR", "gr_gerichte"),
    ("JU", "ju_gerichte"),
    ("LU", "lu_gerichte"),
    ("NE", "ne_gerichte"),
    ("NW", "nw_gerichte"),
    ("OW", "ow_gerichte"),
    ("SG", "sg_publikationen"),
    ("SH", "sh_gerichte"),
    ("SO", "so_gerichte"),
    ("SZ", "sz_gerichte"),
    ("SZ", "sz_verwaltungsgericht"),
    ("TG", "tg_gerichte"),
    ("TI", "ti_gerichte"),
    ("UR", "ur_gerichte"),
    ("VD", "vd_gerichte"),
    ("VS", "vs_gerichte"),
    ("ZG", "zg_obergericht"),
    ("ZG", "zg_verwaltungsgericht"),
    ("ZH", "zh_gerichte"),
    ("ZH", "zh_verwaltungsgericht"),
    ("ZH", "zh_sozialversicherungsgericht"),
    ("ZH", "zh_steuerrekursgericht"),
    ("ZH", "zh_baurekursgericht"),
]


def count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    n = 0
    with open(path, encoding="utf-8") as f:
        for _ in f:
            n += 1
    return n


def main():
    db = "/opt/caselaw/repo/output/decisions.db"
    cur = sqlite3.connect(db).cursor()

    print(f"{'canton':6} {'court':32} "
          f"{'direct_jsonl':>12} {'es_jsonl':>10} {'db_total':>10} "
          f"verdict")
    print("-" * 100)

    for canton, court in COURTS:
        direct_jsonl = count_jsonl(JSONL_DIR / f"{court}.jsonl")
        es_jsonl = count_jsonl(JSONL_DIR / f"es_{court}.jsonl")
        db_total = cur.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=?", (court,)
        ).fetchone()[0]

        if direct_jsonl == 0 and es_jsonl == 0:
            verdict = "EMPTY"
        elif direct_jsonl == 0:
            verdict = "ES-ONLY (need direct scraper or accept archive)"
        elif es_jsonl == 0:
            verdict = "DIRECT-ONLY ✓"
        else:
            # Compare
            if direct_jsonl >= es_jsonl * 0.95:
                verdict = f"PARITY → safe to retire es_ ({100*es_jsonl/(direct_jsonl+es_jsonl):.0f}% es)"
            elif direct_jsonl >= es_jsonl * 0.5:
                verdict = f"DIRECT < ES, both substantial"
            else:
                verdict = f"ES > DIRECT — investigate (es {100*es_jsonl/max(1,direct_jsonl):.0f}% of direct)"

        print(f"{canton:6} {court:32} "
              f"{direct_jsonl:>12} {es_jsonl:>10} {db_total:>10}  "
              f"{verdict}")


if __name__ == "__main__":
    main()
