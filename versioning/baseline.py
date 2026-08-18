"""Baseline pass: start the version clock for the whole corpus.

Records version 1 for every decision using the content_hash ALREADY stored
in decisions.db, so the pass touches no court portal and reads no
full_text. History cannot be reconstructed backwards — whatever is not
baselined today is a change we can never see — so this runs corpus-wide
rather than on a subset.

Read-only on decisions.db (mode=ro&immutable=1). Writes only to the
sidecar version store. Idempotent: re-running baselines whatever is new
and leaves existing rows untouched.

  python3 -m versioning.baseline --decisions output/decisions.db \
      --store output/decision_versions.db [--limit N] [--dry-run]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timezone

from versioning.store import VersionStore


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}",
          file=sys.stderr, flush=True)


def run(decisions_db: str, store_path: str, *, limit: int = 0,
        dry_run: bool = False, batch: int = 20000) -> dict:
    """Baseline every decision that has a usable content_hash.

    Decisions with a NULL/short hash are counted and skipped, not
    invented: a wrong baseline hash would report a phantom change on the
    first refresh, which at ~5 real events a year would be pure noise.
    """
    src = sqlite3.connect(f"file:{decisions_db}?mode=ro&immutable=1", uri=True)
    store = VersionStore(store_path)
    now = datetime.now(timezone.utc).isoformat()

    existing = {r[0] for r in store.conn.execute(
        "SELECT decision_id FROM decision_versions WHERE version_no=1")}
    _log(f"store already holds {len(existing):,} baselined decisions")

    stats = {"scanned": 0, "baselined": 0, "already": 0,
             "no_hash": 0, "elapsed_s": 0.0}
    t0 = time.time()
    pending: list[tuple] = []
    sql = "SELECT decision_id, content_hash, court, source_url FROM decisions"
    if limit:
        sql += f" LIMIT {int(limit)}"

    for did, chash, court, url in src.execute(sql):
        stats["scanned"] += 1
        if did in existing:
            stats["already"] += 1
            continue
        if not chash or len(chash) != 64:
            stats["no_hash"] += 1
            continue
        pending.append((did, 1, chash, now, None, None, 0, url, None,
                        "baseline"))
        if len(pending) >= batch and not dry_run:
            _flush(store, pending)
            stats["baselined"] += len(pending)
            pending.clear()
            _log(f"  {stats['scanned']:,} scanned, "
                 f"{stats['baselined']:,} baselined")

    if pending and not dry_run:
        _flush(store, pending)
        stats["baselined"] += len(pending)
    elif dry_run:
        stats["baselined"] = len(pending)

    stats["elapsed_s"] = round(time.time() - t0, 1)
    src.close()
    store.close()
    return stats


def _flush(store: VersionStore, rows: list[tuple]) -> None:
    store.conn.executemany(
        "INSERT OR IGNORE INTO decision_versions(decision_id, version_no, "
        "content_hash, observed_at, superseded_at, reverse_diff, char_delta, "
        "source_url, merkle_leaf, classification) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    store.conn.executemany(
        "INSERT OR IGNORE INTO verification_log(decision_id, last_checked, "
        "last_changed, check_count) VALUES (?,?,NULL,0)",
        [(r[0], r[3]) for r in rows])
    store.conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--decisions", required=True)
    ap.add_argument("--store", required=True)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    stats = run(a.decisions, a.store, limit=a.limit, dry_run=a.dry_run)
    _log("done: " + ", ".join(f"{k}={v:,}" if isinstance(v, int) else
                              f"{k}={v}" for k, v in stats.items()))
    if stats["no_hash"]:
        _log(f"NOTE {stats['no_hash']:,} decisions carry no usable "
             f"content_hash and were skipped — they will baseline on the "
             f"next run after build_fts5 hashes them.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
