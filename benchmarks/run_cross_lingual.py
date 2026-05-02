#!/usr/bin/env python3
"""Cross-lingual leading-case retrieval evaluation.

Reads a JSONL question set authored against the OpenCaseLaw citation graph
(target_decision_id is one of the most-cited BGE leading decisions) and
measures retrieval quality by (query language × target decision language)
cell using BM25 + RRF (no LLM rerank).

Output:
- per-query results with rank of target in top-k
- aggregated MRR, Recall@10, Hit@1, Hit@10 by cell
- saved to benchmarks/swiss_legal_rag_bench/results/cross_lingual_v1_<date>.json
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _configure_search(db_path: Path):
    import mcp_server
    mcp_server.DB_PATH = db_path
    mcp_server.DATA_DIR = db_path.parent
    mcp_server.PARQUET_DIR = db_path.parent / "parquet"
    mcp_server.GRAPH_DB_PATH = db_path.parent / "reference_graph.db"
    mcp_server.VECTOR_DB_PATH = db_path.parent / "vectors.db"
    return mcp_server


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cross-lingual leading-case retrieval eval")
    p.add_argument("--questions", type=Path,
                   default=REPO_ROOT / "benchmarks/swiss_legal_rag_bench/cross_lingual_v1.jsonl")
    p.add_argument("--db", type=Path, default=Path.home() / ".swiss-caselaw" / "decisions.db")
    p.add_argument("-k", type=int, default=10)
    p.add_argument("--output", type=Path,
                   default=REPO_ROOT / "benchmarks/swiss_legal_rag_bench/results/cross_lingual_v1.json")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.questions.exists():
        print(f"Questions file not found: {args.questions}", file=sys.stderr)
        return 1
    if not args.db.exists():
        print(f"Database not found: {args.db}", file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)

    queries = []
    with open(args.questions, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                queries.append(json.loads(line))
    print(f"Loaded {len(queries)} queries from {args.questions}", file=sys.stderr)

    mcp_server = _configure_search(args.db)

    per_query = []
    cell_aggregates = defaultdict(lambda: {"n": 0, "rr_sum": 0.0, "hit1": 0, "hit10": 0,
                                            "ranks": []})
    overall = {"n": 0, "rr_sum": 0.0, "hit1": 0, "hit10": 0, "ranks": []}
    latencies = []

    for i, q in enumerate(queries):
        q_text = q["q_text"]
        q_lang = q["q_lang"]
        target = q["target_decision_id"]
        target_lang = q["target_lang"]
        cell = (q_lang, target_lang)

        start = time.perf_counter()
        try:
            results, _total = mcp_server.search_fts5(query=q_text, limit=args.k)
        except Exception as e:
            results = []
            print(f"[{q['q_id']}] error: {e}", file=sys.stderr)
        latency_ms = (time.perf_counter() - start) * 1000.0
        latencies.append(latency_ms)

        topk_ids = [r.get("decision_id") for r in results if r.get("decision_id")]
        rank = topk_ids.index(target) + 1 if target in topk_ids else None
        rr = 1.0 / rank if rank else 0.0
        hit1 = 1 if rank == 1 else 0
        hit10 = 1 if rank else 0

        per_query.append({
            "q_id": q["q_id"], "q_lang": q_lang, "q_text": q_text,
            "target_decision_id": target, "target_lang": target_lang,
            "in_degree": q.get("in_degree"), "legal_area": q.get("legal_area"),
            "rank": rank, "rr": rr, "hit1": hit1, "hit10": hit10,
            "topk_ids": topk_ids[:5], "latency_ms": round(latency_ms, 1),
        })

        # Aggregate
        for agg in (cell_aggregates[cell], overall):
            agg["n"] += 1
            agg["rr_sum"] += rr
            agg["hit1"] += hit1
            agg["hit10"] += hit10
            if rank:
                agg["ranks"].append(rank)

        if (i + 1) % 25 == 0:
            print(f"  ... {i+1}/{len(queries)} queries done", file=sys.stderr)

    # Build summary
    def cell_stats(agg):
        n = agg["n"]
        if n == 0:
            return {"n": 0}
        return {
            "n": n,
            "mrr_at_k": agg["rr_sum"] / n,
            "hit_at_1": agg["hit1"] / n,
            "hit_at_10": agg["hit10"] / n,
            "median_rank_when_found": sorted(agg["ranks"])[len(agg["ranks"]) // 2] if agg["ranks"] else None,
            "n_found_in_topk": len(agg["ranks"]),
        }

    summary = {
        "questions_path": str(args.questions),
        "db_path": str(args.db),
        "k": args.k,
        "queries_total": len(queries),
        "overall": cell_stats(overall),
        "by_cell": {f"{q}_to_{t}": cell_stats(cell_aggregates[(q, t)])
                    for q in ("de", "fr", "it") for t in ("de", "fr", "it")
                    if cell_aggregates.get((q, t), {}).get("n", 0) > 0},
        "latency_ms_avg": sum(latencies) / len(latencies) if latencies else 0,
        "latency_ms_max": max(latencies) if latencies else 0,
    }

    output = {"summary": summary, "per_query": per_query}
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # Print key numbers
    print("\n=== OVERALL ===")
    print(json.dumps(summary["overall"], indent=2))
    print("\n=== BY CELL (q_lang -> target_lang) ===")
    for cell, stats in summary["by_cell"].items():
        print(f"  {cell}: n={stats['n']}, MRR={stats['mrr_at_k']:.3f}, Hit@1={stats['hit_at_1']:.3f}, Hit@10={stats['hit_at_10']:.3f}, median rank when found={stats['median_rank_when_found']}")
    print(f"\nWrote: {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
