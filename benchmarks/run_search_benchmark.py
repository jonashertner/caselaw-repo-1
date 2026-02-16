#!/usr/bin/env python3
"""
Evaluate mcp_server search relevance using a fixed golden query set.

Metrics:
- MRR@k
- Recall@k
- nDCG@k
- Hit@1
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Swiss caselaw search relevance benchmark")
    parser.add_argument(
        "--golden",
        type=Path,
        default=Path("benchmarks/search_relevance_golden.json"),
        help="Path to golden relevance JSON",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path.home() / ".swiss-caselaw" / "decisions.db",
        help="Path to SQLite decisions.db",
    )
    parser.add_argument(
        "-k",
        type=int,
        default=10,
        help="Top-k cutoff for metrics (default: 10)",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        help="Optional path to write machine-readable benchmark report JSON",
    )
    parser.add_argument(
        "--min-mrr",
        type=float,
        help="Fail (exit 1) if MRR@k is below this threshold",
    )
    parser.add_argument(
        "--min-recall",
        type=float,
        help="Fail (exit 1) if Recall@k is below this threshold",
    )
    parser.add_argument(
        "--min-ndcg",
        type=float,
        help="Fail (exit 1) if nDCG@k is below this threshold",
    )
    parser.add_argument(
        "--show-misses",
        action="store_true",
        help="Print per-query misses where no relevant judgment appears in top-k",
    )
    return parser.parse_args()


def _dcg(grades: list[int]) -> float:
    score = 0.0
    for idx, grade in enumerate(grades, start=1):
        gain = (2 ** grade) - 1
        score += gain / math.log2(idx + 1)
    return score


def _load_golden(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    if "queries" not in payload or not isinstance(payload["queries"], list):
        raise ValueError(f"Invalid golden file format: {path}")
    return payload


def _existing_ids(conn: sqlite3.Connection, ids: set[str]) -> set[str]:
    if not ids:
        return set()
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"SELECT decision_id FROM decisions WHERE decision_id IN ({placeholders})",
        tuple(ids),
    ).fetchall()
    return {r[0] for r in rows}


def _configure_search_db(db_path: Path):
    import mcp_server

    mcp_server.DB_PATH = db_path
    mcp_server.DATA_DIR = db_path.parent
    mcp_server.PARQUET_DIR = db_path.parent / "parquet"
    return mcp_server


def main() -> int:
    args = parse_args()
    k = max(1, args.k)
    db_path = args.db.expanduser().resolve()
    golden_path = args.golden.expanduser().resolve()

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return 1
    if not golden_path.exists():
        print(f"Golden file not found: {golden_path}", file=sys.stderr)
        return 1

    golden = _load_golden(golden_path)
    queries = golden["queries"]
    if not queries:
        print("Golden file contains no queries.", file=sys.stderr)
        return 1

    all_relevant_ids = {
        rel["decision_id"]
        for q in queries
        for rel in q.get("relevant", [])
        if isinstance(rel, dict) and rel.get("decision_id")
    }

    with sqlite3.connect(str(db_path)) as conn:
        existing_relevant = _existing_ids(conn, all_relevant_ids)
        total_rows = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    mcp_server = _configure_search_db(db_path)

    per_query = []
    rr_scores = []
    recall_scores = []
    ndcg_scores = []
    hit1_scores = []
    latencies_ms = []
    skipped = 0

    for q in queries:
        qid = q.get("id", "")
        query = q.get("query", "")
        relevant_items = q.get("relevant", [])

        rel_grades = {}
        for rel in relevant_items:
            if not isinstance(rel, dict):
                continue
            rid = rel.get("decision_id")
            if not rid or rid not in existing_relevant:
                continue
            rel_grades[rid] = int(rel.get("grade", 1))

        if not rel_grades:
            skipped += 1
            per_query.append(
                {
                    "id": qid,
                    "query": query,
                    "status": "skipped_no_relevant_docs_in_db",
                    "rr": None,
                    "recall": None,
                    "ndcg": None,
                    "hit1": None,
                    "latency_ms": None,
                    "topk_ids": [],
                    "matched_ranks": {},
                }
            )
            continue

        start = time.perf_counter()
        results = mcp_server.search_fts5(query=query, limit=k)
        latency_ms = (time.perf_counter() - start) * 1000.0
        latencies_ms.append(latency_ms)

        topk_ids = [r.get("decision_id") for r in results if r.get("decision_id")]
        matched_ranks = {
            rid: (topk_ids.index(rid) + 1)
            for rid in rel_grades
            if rid in topk_ids
        }

        if matched_ranks:
            best_rank = min(matched_ranks.values())
            rr = 1.0 / best_rank
        else:
            rr = 0.0

        recall = len(matched_ranks) / len(rel_grades)

        graded_found = [
            rel_grades[rid]
            for rid, _rank in sorted(matched_ranks.items(), key=lambda x: x[1])
        ]
        dcg = _dcg(graded_found)
        ideal_grades = sorted(rel_grades.values(), reverse=True)[:k]
        idcg = _dcg(ideal_grades)
        ndcg = (dcg / idcg) if idcg > 0 else 0.0

        hit1 = 1.0 if topk_ids and topk_ids[0] in rel_grades else 0.0

        rr_scores.append(rr)
        recall_scores.append(recall)
        ndcg_scores.append(ndcg)
        hit1_scores.append(hit1)

        per_query.append(
            {
                "id": qid,
                "query": query,
                "status": "ok",
                "rr": rr,
                "recall": recall,
                "ndcg": ndcg,
                "hit1": hit1,
                "latency_ms": latency_ms,
                "topk_ids": topk_ids,
                "matched_ranks": matched_ranks,
            }
        )

    evaluated = len(rr_scores)
    if evaluated == 0:
        print("No benchmark queries could be evaluated (all skipped).", file=sys.stderr)
        return 1

    summary = {
        "k": k,
        "db_path": str(db_path),
        "db_rows": total_rows,
        "golden_path": str(golden_path),
        "queries_total": len(queries),
        "queries_evaluated": evaluated,
        "queries_skipped": skipped,
        "mrr_at_k": statistics.mean(rr_scores),
        "recall_at_k": statistics.mean(recall_scores),
        "ndcg_at_k": statistics.mean(ndcg_scores),
        "hit_at_1": statistics.mean(hit1_scores),
        "latency_ms_avg": statistics.mean(latencies_ms),
        "latency_ms_p95": sorted(latencies_ms)[max(0, int(0.95 * len(latencies_ms)) - 1)],
    }

    print(f"Search Benchmark @ {k}")
    print(f"- DB: {db_path} ({total_rows} decisions)")
    print(f"- Golden: {golden_path}")
    print(
        f"- Evaluated: {evaluated}/{len(queries)} "
        f"(skipped {skipped} missing relevance docs)"
    )
    print(f"- MRR@{k}:    {summary['mrr_at_k']:.4f}")
    print(f"- Recall@{k}: {summary['recall_at_k']:.4f}")
    print(f"- nDCG@{k}:   {summary['ndcg_at_k']:.4f}")
    print(f"- Hit@1:      {summary['hit_at_1']:.4f}")
    print(f"- Latency:    avg {summary['latency_ms_avg']:.2f} ms, p95 {summary['latency_ms_p95']:.2f} ms")

    if args.show_misses:
        misses = [q for q in per_query if q["status"] == "ok" and q["rr"] == 0.0]
        if misses:
            print("\nMisses (no relevant result in top-k):")
            for m in misses:
                print(f"- {m['id']}: {m['query']}")
                if m["topk_ids"]:
                    print(f"  topk: {', '.join(m['topk_ids'][:5])}")
                else:
                    print("  topk: <empty>")

    if args.json_output:
        payload = {
            "summary": summary,
            "per_query": per_query,
        }
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.json_output, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        print(f"\nWrote JSON report: {args.json_output}")

    failed = False
    if args.min_mrr is not None and summary["mrr_at_k"] < args.min_mrr:
        print(
            f"Threshold fail: MRR@{k} {summary['mrr_at_k']:.4f} < {args.min_mrr:.4f}",
            file=sys.stderr,
        )
        failed = True
    if args.min_recall is not None and summary["recall_at_k"] < args.min_recall:
        print(
            f"Threshold fail: Recall@{k} {summary['recall_at_k']:.4f} < {args.min_recall:.4f}",
            file=sys.stderr,
        )
        failed = True
    if args.min_ndcg is not None and summary["ndcg_at_k"] < args.min_ndcg:
        print(
            f"Threshold fail: nDCG@{k} {summary['ndcg_at_k']:.4f} < {args.min_ndcg:.4f}",
            file=sys.stderr,
        )
        failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
