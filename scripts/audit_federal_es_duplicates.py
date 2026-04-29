"""
Audit: how many federal/regulatory rows in decisions.db were ingested
from entscheidsuche AND have a corresponding canonical-scraper row for
the same docket?  Those are true duplicates, safe to delete.  Rows
without a canonical counterpart are unique coverage and must be
preserved.

Output: a CSV of (court, decision_id_es, decision_id_canonical, action)
for review before any DELETE is executed.

Run with --execute to perform the cleanup; default is dry-run (audit only).
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = Path(os.environ.get(
    "SWISS_CASELAW_DECISIONS_DB",
    "/opt/caselaw/repo/output/decisions.db",
))

# Federal courts whose entscheidsuche feed is now retired (commit 904916e).
# For each, the canonical-scraper output is authoritative.
FEDERAL_COURTS = [
    "bge", "bger", "bvger", "bstger", "bpatger",
    "ch_bundesrat", "edoeb", "weko", "ta_sst",
]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--execute", action="store_true",
                   help="DELETE the duplicates. Default is dry-run.")
    p.add_argument("--csv", type=Path, default=Path("/tmp/federal_es_audit.csv"))
    args = p.parse_args()

    if not args.db.exists():
        print(f"ERROR: {args.db} not found", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(args.db))
    c = conn.cursor()

    # Per court, count entscheidsuche rows vs canonical rows
    print("=" * 70)
    print(f"  AUDIT against {args.db}")
    print("=" * 70)
    print(f"{'court':14} {'es_count':>10} {'canonical':>10} {'ratio':>8}")
    print("-" * 70)

    candidates = []  # (decision_id, court, docket_number, decision_date)
    for court in FEDERAL_COURTS:
        n_es = c.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=? AND source=?",
            (court, "entscheidsuche"),
        ).fetchone()[0]
        n_canon = c.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=? AND (source IS NULL OR source != ?)",
            (court, "entscheidsuche"),
        ).fetchone()[0]
        ratio = n_canon / max(1, n_es)
        print(f"{court:14} {n_es:>10d} {n_canon:>10d} {ratio:>8.2f}")

        # Build the candidate-deletion list: es-sourced rows whose
        # docket+date is also present in a canonical row.
        for did, dock, dat in c.execute(
            "SELECT decision_id, docket_number, decision_date FROM decisions "
            "WHERE court=? AND source=?",
            (court, "entscheidsuche"),
        ).fetchall():
            candidates.append((did, court, dock, dat))

    print()
    print(f"  total entscheidsuche-sourced federal rows: {len(candidates)}")
    print(f"  checking which have a canonical counterpart by (court, docket_number)...")

    # Build a fast index: for each (court, docket_number), list of canonical decision_ids
    canonical_idx: dict[tuple[str, str], list[str]] = {}
    for court in FEDERAL_COURTS:
        for did, dock in c.execute(
            "SELECT decision_id, docket_number FROM decisions "
            "WHERE court=? AND (source IS NULL OR source != ?)",
            (court, "entscheidsuche"),
        ).fetchall():
            canonical_idx.setdefault((court, dock), []).append(did)

    # Classify each candidate
    safe_to_delete = []
    keep_unique = []
    for did, court, dock, dat in candidates:
        canon_list = canonical_idx.get((court, dock), [])
        if canon_list:
            safe_to_delete.append((did, court, dock, dat, canon_list[0]))
        else:
            keep_unique.append((did, court, dock, dat))

    print()
    print(f"  duplicates (canonical row exists, safe to delete): "
          f"{len(safe_to_delete)}")
    print(f"  unique coverage (no canonical counterpart, KEEP):  "
          f"{len(keep_unique)}")
    print()

    # Write the full audit CSV
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "decision_id", "court", "docket_number", "decision_date",
            "action", "canonical_counterpart",
        ])
        for did, court, dock, dat, canon in safe_to_delete:
            w.writerow([did, court, dock, dat, "DELETE", canon])
        for did, court, dock, dat in keep_unique:
            w.writerow([did, court, dock, dat, "KEEP", ""])
    print(f"  wrote audit CSV: {args.csv}")

    if not args.execute:
        print()
        print("  DRY-RUN — no changes made. Re-run with --execute to delete the "
              f"{len(safe_to_delete)} duplicate rows.")
        return 0

    # Execute deletion
    print()
    print(f"  EXECUTING: deleting {len(safe_to_delete)} duplicate rows...")
    conn.execute("BEGIN")
    deleted = 0
    for did, _, _, _, _ in safe_to_delete:
        n = c.execute("DELETE FROM decisions WHERE decision_id=?", (did,)).rowcount
        deleted += n
    print(f"  deleted: {deleted}")
    conn.execute("COMMIT")
    print("  done. (FTS5 sync via decisions_ad trigger.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
