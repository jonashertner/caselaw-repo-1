#!/usr/bin/env python3
"""Rank the decisions Swiss courts cite but we do not hold.

A citation whose target never resolved to a decision in the corpus is a
piece of demand the courts themselves express: this ruling is important
enough to cite, and it is missing. Aggregated by how many distinct
decisions cite it, the unresolved-citation set is a corpus-acquisition
queue ranked by evidence — the Wikipedia-GapFinder pattern applied to
case law (Wulczyn et al., WWW 2016), and it needs no user data at all.

Measured 2026-08-19: 276,321 distinct unresolved targets across ~9.8M
citation rows; the most-cited missing decision (BGer 4C.310/1996) is
referenced by over a thousand others.

Source: output/reference_graph.db — `decision_citations` (every extracted
reference) minus the `target_ref`s that `citation_targets` resolved to a
held decision. Read-only.

Output: output/datasets/demand_queue/YYYY-MM-DD.jsonl, one row per
missing target: {target_ref, target_type, citing_count, example_sources}.
This directly prices the corpus backfills (Vaud, ch_vb, BGer pre-2007):
sort the queue, and the top of it IS the work list.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

log = logging.getLogger("demand_queue")


def build(db: Path, limit: int | None = None,
          min_citations: int = 2) -> list[dict]:
    con = sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True)
    con.row_factory = sqlite3.Row
    try:
        log.info("loading resolved reference set…")
        resolved = {r[0] for r in con.execute(
            "SELECT DISTINCT target_ref FROM citation_targets "
            "WHERE target_decision_id IS NOT NULL AND target_decision_id != ''")}
        log.info("resolved refs: %d", len(resolved))

        count: Counter = Counter()
        typ: dict[str, str] = {}
        examples: dict[str, list] = defaultdict(list)
        scanned = 0
        for row in con.execute(
                "SELECT source_decision_id, target_ref, target_type "
                "FROM decision_citations"):
            scanned += 1
            ref = row["target_ref"]
            if not ref or ref in resolved:
                continue
            count[ref] += 1
            typ.setdefault(ref, row["target_type"] or "")
            if len(examples[ref]) < 3:
                examples[ref].append(row["source_decision_id"])
        log.info("scanned %d citation rows; %d distinct unresolved targets",
                 scanned, len(count))
    finally:
        con.close()

    rows = []
    for ref, n in count.most_common(limit):
        if n < min_citations:
            break  # most_common is descending, so the tail is all below
        rows.append({
            "target_ref": ref,
            "target_type": typ.get(ref, ""),
            "citing_count": n,
            "example_sources": examples[ref],
        })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--db", type=Path,
                    default=Path("output/reference_graph.db"))
    ap.add_argument("--out", type=Path,
                    default=Path("output/datasets/demand_queue"))
    ap.add_argument("--limit", type=int, default=None,
                    help="cap the queue (default: all with >= min-citations)")
    ap.add_argument("--min-citations", type=int, default=2)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(message)s")
    if not args.db.exists():
        log.error("reference graph not found: %s", args.db)
        return 1
    rows = build(args.db, limit=args.limit, min_citations=args.min_citations)
    args.out.mkdir(parents=True, exist_ok=True)
    day = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
    path = args.out / f"{day}.jsonl"
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    top = rows[0] if rows else {}
    log.info("wrote %d demand rows -> %s (top: %s ×%s)", len(rows), path,
             top.get("target_ref"), top.get("citing_count"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
