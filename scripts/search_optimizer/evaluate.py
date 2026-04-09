#!/usr/bin/env python3
"""
Evaluate search pipeline with a given config and produce execution traces.

Returns MRR@10, Hit@1, and per-query traces showing WHY each query
succeeded or failed — the key input for the Meta-Harness proposer.
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _dcg(grades: list[int]) -> float:
    return sum((2**g - 1) / math.log2(i + 2) for i, g in enumerate(grades))


def _normalize_docket(value: str) -> str:
    return re.sub(r"[^0-9a-z]+", "", value.lower())


def evaluate(
    config: dict,
    db_path: Path,
    golden_path: Path,
    k: int = 10,
    trace_limit: int = 20,
) -> dict:
    """
    Run the 100-query benchmark with a given config.

    Returns:
        {
            "mrr": float,
            "hit1": float,
            "recall": float,
            "ndcg": float,
            "evaluated": int,
            "per_query": [...],
            "failed_traces": [...],  # detailed traces for failed queries (top N worst)
            "config": dict,
        }
    """
    # Configure mcp_server with this config
    from scripts.search_optimizer.config import apply_config
    import mcp_server

    mcp_server.DB_PATH = db_path
    mcp_server.DATA_DIR = db_path.parent
    mcp_server.GRAPH_DB_PATH = db_path.parent / "reference_graph.db"
    mcp_server.VECTOR_DB_PATH = db_path.parent / "vectors.db"
    apply_config(config)

    # Load golden set
    with open(golden_path) as f:
        golden = json.load(f)
    queries = golden["queries"]

    # Resolve relevant IDs
    conn = sqlite3.connect(str(db_path))
    existing_ids = {
        row[0] for row in conn.execute("SELECT decision_id FROM decisions").fetchall()
    }

    per_query = []
    rr_scores = []
    hit1_scores = []
    recall_scores = []
    ndcg_scores = []

    for q in queries:
        qid = q.get("id", "")
        query_text = q.get("query", "")
        tags = q.get("tags", [])

        # Resolve relevant decisions
        rel_grades = {}
        for rel in q.get("relevant", []):
            rid = rel.get("decision_id", "")
            grade = int(rel.get("grade", 1))
            if rid in existing_ids:
                rel_grades[rid] = max(rel_grades.get(rid, 0), grade)
            else:
                # Try docket lookup
                parts = rid.split("_", 1)
                if len(parts) > 1:
                    docket = parts[1].rsplit("_", 1)
                    if len(docket) == 2:
                        docket_str = docket[0] + "/" + docket[1]
                        row = conn.execute(
                            "SELECT decision_id FROM decisions WHERE docket_number = ? LIMIT 1",
                            (docket_str,),
                        ).fetchone()
                        if row:
                            rel_grades[row[0]] = max(rel_grades.get(row[0], 0), grade)

        if not rel_grades:
            per_query.append({
                "id": qid, "query": query_text, "tags": tags,
                "status": "skipped", "rr": None,
            })
            continue

        # Run search
        t0 = time.perf_counter()
        try:
            results, _total = mcp_server.search_fts5(query=query_text, limit=k)
        except Exception as e:
            per_query.append({
                "id": qid, "query": query_text, "tags": tags,
                "status": "error", "error": str(e), "rr": 0.0,
            })
            rr_scores.append(0.0)
            hit1_scores.append(0.0)
            recall_scores.append(0.0)
            ndcg_scores.append(0.0)
            continue
        latency_ms = (time.perf_counter() - t0) * 1000

        topk_ids = [r.get("decision_id") for r in results]
        matched_ranks = {
            rid: topk_ids.index(rid) + 1
            for rid in rel_grades if rid in topk_ids
        }

        rr = 1.0 / min(matched_ranks.values()) if matched_ranks else 0.0
        hit1 = 1.0 if topk_ids and topk_ids[0] in rel_grades else 0.0
        recall = len(matched_ranks) / len(rel_grades)

        graded = [rel_grades[rid] for rid, _ in sorted(matched_ranks.items(), key=lambda x: x[1])]
        dcg = _dcg(graded)
        ideal = sorted(rel_grades.values(), reverse=True)[:k]
        idcg = _dcg(ideal)
        ndcg = dcg / idcg if idcg > 0 else 0.0

        rr_scores.append(rr)
        hit1_scores.append(hit1)
        recall_scores.append(recall)
        ndcg_scores.append(ndcg)

        # Build trace
        trace = {
            "id": qid,
            "query": query_text,
            "tags": tags,
            "status": "ok",
            "rr": rr,
            "hit1": hit1,
            "recall": recall,
            "ndcg": ndcg,
            "latency_ms": round(latency_ms, 1),
            "relevant_ids": list(rel_grades.keys()),
            "relevant_grades": rel_grades,
            "matched_ranks": matched_ranks,
            "topk": [
                {
                    "rank": i + 1,
                    "decision_id": r.get("decision_id", ""),
                    "court": r.get("court", ""),
                    "docket": r.get("docket_number", ""),
                    "date": r.get("decision_date", ""),
                    "title": (r.get("title") or "")[:100],
                    "is_relevant": r.get("decision_id") in rel_grades,
                }
                for i, r in enumerate(results[:k])
            ],
        }
        per_query.append(trace)

    conn.close()

    evaluated = len(rr_scores)
    mrr = sum(rr_scores) / evaluated if evaluated else 0.0
    hit1 = sum(hit1_scores) / evaluated if evaluated else 0.0
    recall = sum(recall_scores) / evaluated if evaluated else 0.0
    ndcg = sum(ndcg_scores) / evaluated if evaluated else 0.0

    # Extract failed traces (worst queries by RR) for the proposer
    evaluated_queries = [q for q in per_query if q.get("status") == "ok"]
    failed = sorted(evaluated_queries, key=lambda q: q.get("rr", 0))[:trace_limit]

    return {
        "mrr": round(mrr, 4),
        "hit1": round(hit1, 4),
        "recall": round(recall, 4),
        "ndcg": round(ndcg, 4),
        "evaluated": evaluated,
        "total_queries": len(queries),
        "per_query": per_query,
        "failed_traces": failed,
        "config": config,
    }


if __name__ == "__main__":
    import argparse
    from scripts.search_optimizer.config import DEFAULT_CONFIG

    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--golden", type=Path, default=Path("benchmarks/search_relevance_golden.json"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("-k", type=int, default=10)
    args = parser.parse_args()

    result = evaluate(DEFAULT_CONFIG, args.db, args.golden, k=args.k)
    print("MRR@{}: {:.4f}  Hit@1: {:.4f}  Recall: {:.4f}  nDCG: {:.4f}  (n={})".format(
        args.k, result["mrr"], result["hit1"], result["recall"], result["ndcg"], result["evaluated"]))

    if args.output:
        with open(args.output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print("Written to {}".format(args.output))
