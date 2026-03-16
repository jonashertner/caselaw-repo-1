#!/usr/bin/env python3
"""Automated citation-pair retrieval regression check.

Samples citation pairs from the graph, uses one decision's regeste
as a query, and checks if the cited decision appears in top-k results.

This is a REGRESSION DETECTION tool, NOT a quality metric.
It tests retrieval similarity, not user-facing search quality.
The golden set (run_search_benchmark.py) is the source of truth.
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def sample_citation_pairs(
    graph_db: Path,
    fts_db: Path,
    n: int = 5000,
    seed: int = 42,
) -> list[tuple[str, str, str]]:
    """Sample (query_regeste, target_decision_id, source_decision_id) triples."""
    graph = sqlite3.connect(str(graph_db))
    rows = graph.execute(
        """
        SELECT ct.source_decision_id, ct.target_decision_id
        FROM citation_targets ct
        WHERE ct.source_decision_id LIKE 'bge_%'
          AND ct.target_decision_id LIKE 'bge_%'
          AND ct.source_decision_id != ct.target_decision_id
        LIMIT 100000
        """
    ).fetchall()
    graph.close()

    fts = sqlite3.connect(str(fts_db))
    random.seed(seed)
    random.shuffle(rows)

    pairs: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()

    for source_id, target_id in rows:
        if len(pairs) >= n:
            break
        key = (source_id, target_id)
        if key in seen:
            continue
        seen.add(key)

        src = fts.execute(
            "SELECT regeste FROM decisions WHERE decision_id = ?", (source_id,)
        ).fetchone()
        if not src or not src[0] or len(src[0]) < 50:
            continue

        tgt = fts.execute(
            "SELECT regeste FROM decisions WHERE decision_id = ?", (target_id,)
        ).fetchone()
        if not tgt or not tgt[0] or len(tgt[0]) < 50:
            continue

        pairs.append((src[0][:200], target_id, source_id))

    fts.close()
    return pairs


def run_retrieval_check(
    pairs: list[tuple[str, str, str]],
    k: int = 10,
) -> dict:
    """Use source regeste as query, check if target in top-k."""
    from mcp_server import _search_fts5_inner, get_db

    conn = get_db()
    hits = 0
    rr_list: list[float] = []
    total = len(pairs)
    errors = 0
    t0 = time.time()

    for i, (query_regeste, target_id, _source_id) in enumerate(pairs):
        query = query_regeste[:100]
        try:
            results, _ = _search_fts5_inner(
                conn, query,
                court="", canton="", language="",
                date_from="", date_to="",
                chamber="", decision_type="",
                limit=k,
            )
            result_ids = [r["decision_id"] for r in results]
            if target_id in result_ids:
                rank = result_ids.index(target_id) + 1
                hits += 1
                rr_list.append(1.0 / rank)
            else:
                rr_list.append(0.0)
        except Exception:
            rr_list.append(0.0)
            errors += 1

        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            mrr_so_far = sum(rr_list) / len(rr_list)
            rate = (i + 1) / elapsed
            print(
                f"  [{i+1}/{total}] hits={hits} MRR={mrr_so_far:.3f} "
                f"({rate:.1f} q/s, errors={errors})"
            )

    conn.close()

    mrr = sum(rr_list) / len(rr_list) if rr_list else 0.0
    hit_rate = hits / total if total > 0 else 0.0

    return {
        "total_pairs": total,
        "hits_at_k": hits,
        "hit_rate": round(hit_rate, 4),
        "mrr": round(mrr, 4),
        "k": k,
        "errors": errors,
        "elapsed_s": round(time.time() - t0, 1),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Citation-pair retrieval regression check"
    )
    parser.add_argument("--graph-db", type=Path, required=True)
    parser.add_argument("--fts-db", type=Path, required=True)
    parser.add_argument("-n", type=int, default=5000)
    parser.add_argument("-k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()

    print(f"Sampling {args.n} citation pairs (seed={args.seed})...")
    pairs = sample_citation_pairs(
        args.graph_db, args.fts_db, n=args.n, seed=args.seed
    )
    print(f"Sampled {len(pairs)} valid pairs")

    if not pairs:
        print("ERROR: No valid pairs found. Check DB paths.")
        sys.exit(1)

    print(f"Running retrieval check (k={args.k})...")
    results = run_retrieval_check(pairs, k=args.k)

    print(f"\nCitation-Pair Regression Check @ {args.k}")
    print(f"  Pairs:    {results['total_pairs']}")
    print(f"  Hit rate: {results['hit_rate']}")
    print(f"  MRR:      {results['mrr']}")
    print(f"  Errors:   {results['errors']}")
    print(f"  Time:     {results['elapsed_s']}s")

    if args.json_output:
        with open(args.json_output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"  Saved to {args.json_output}")


if __name__ == "__main__":
    main()
