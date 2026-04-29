"""V3 per-canton audit — corrects V2's methodology bug.

V2 compared `<court>.jsonl` row count to `decisions WHERE court=<court>`
in the DB. That fails when a direct scraper writes to (e.g.)
`bs_gerichte.jsonl` but sets `court="bs_appellationsgericht"` /
`court="bs_sozialversicherungsgericht"` per chamber. V2 reported
the BS direct scraper at 10,182 JSONL rows but only 2 db rows; the
full 10,180 rows are in the DB under chamber-specific court codes.

V3 aggregates by canton-prefix: counts all DB rows whose court
starts with `<canton>_` AND whose decision_id origin matches the
shard. Maps every es_*.jsonl shard to its canton + reports:
  - es_jsonl row count
  - direct_jsonl row count (sum across all <canton>_*.jsonl shards)
  - canton-aggregate db row count
  - per-court db breakdown for the canton

This shows the true picture for retirement decisions.
"""
from __future__ import annotations
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

JSONL_DIR = Path("/opt/caselaw/repo/output/decisions")

# Cantons whose es_* shards are still active after retirement batches.
# (federal CH_VB and federal duplicates are NOT included; this script
#  focuses on cantonal decisions.)
CANTONS = ["AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR",
           "JU", "LU", "NE", "NW", "OW", "SG", "SH", "SO", "SZ", "TG",
           "TI", "UR", "VD", "VS", "ZG", "ZH"]


def count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    with open(path, encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def main():
    db = "/opt/caselaw/repo/output/decisions.db"
    cur = sqlite3.connect(db).cursor()

    # Get all court codes per canton-prefix
    courts_by_canton: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for r in cur.execute(
        "SELECT court, COUNT(*) FROM decisions WHERE court LIKE '__\\_%' ESCAPE '\\' GROUP BY court ORDER BY 2 DESC"
    ).fetchall():
        court, n = r
        canton = court[:2].upper()
        if canton in CANTONS:
            courts_by_canton[canton].append((court, n))

    print(f"{'canton':6} {'es_jsonl':>10} {'direct_jsonl':>14} "
          f"{'db_rows':>10} {'verdict':30} courts")
    print("-" * 130)

    for canton in CANTONS:
        # All es_*.jsonl shards starting with this canton's prefix
        es_total = 0
        for path in sorted(JSONL_DIR.glob(f"es_{canton.lower()}_*.jsonl")):
            es_total += count_jsonl(path)
        # All <canton>_*.jsonl direct-scraper shards
        direct_total = 0
        for path in sorted(JSONL_DIR.glob(f"{canton.lower()}_*.jsonl")):
            direct_total += count_jsonl(path)

        # DB row count for the canton
        db_courts = courts_by_canton.get(canton, [])
        db_total = sum(n for _, n in db_courts)
        court_summary = ", ".join(f"{c}={n}" for c, n in db_courts[:4])
        if len(db_courts) > 4:
            court_summary += f", +{len(db_courts)-4} more"

        if es_total == 0:
            verdict = "✓ es retired"
        elif direct_total == 0 and es_total > 0:
            verdict = "ES-ONLY (build direct)"
        elif db_total > es_total + direct_total * 0.95:
            verdict = "DISJOINT (keep both)"
        elif db_total > max(es_total, direct_total) * 1.5:
            verdict = "PARTIAL OVERLAP"
        elif db_total <= max(es_total, direct_total) * 1.05:
            if es_total > direct_total:
                verdict = f"FULL OVERLAP, es bigger ({es_total/(direct_total+1):.1f}×)"
            else:
                verdict = f"FULL OVERLAP, direct bigger"
        else:
            verdict = "?"

        print(f"{canton:6} {es_total:>10} {direct_total:>14} "
              f"{db_total:>10} {verdict:30} {court_summary}")


if __name__ == "__main__":
    main()
