#!/usr/bin/env python3
"""Nightly research benchmark — ablation study for search pipeline components.

Runs the golden set under 5 configurations and logs MRR/Hit@1/Hit@5/NDCG@10.
Appends results to a history file for tracking component contribution over time.

Configurations:
  full          — all components enabled
  no_parse      — disable LLM structured query parsing
  no_rerank     — disable Haiku reranking
  no_crossling  — disable cross-lingual strategies
  no_citation   — disable citation-graph boosting

Output: benchmarks/ablation_history.jsonl (one JSON line per run)
"""
import json
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

GOLDEN_SET = REPO / "benchmarks" / "search_relevance_golden.json"
HISTORY = REPO / "benchmarks" / "ablation_history.jsonl"
DB_PATH = Path(os.environ.get("SWISS_CASELAW_DIR", str(Path.home() / ".swiss-caselaw"))) / "decisions.db"


def load_golden():
    with open(GOLDEN_SET) as f:
        data = json.load(f)
    # v2 schema wraps queries under "queries". Older callers assumed a list
    # at the top level — unwrap to stay compatible.
    if isinstance(data, dict) and "queries" in data:
        return data["queries"]
    return data


def dcg(relevances, k=10):
    return sum(r / math.log2(i + 2) for i, r in enumerate(relevances[:k]))


def ndcg(relevances, k=10):
    ideal = dcg(sorted(relevances, reverse=True), k)
    if ideal == 0:
        return 0
    return dcg(relevances, k) / ideal


def run_config(golden, config_name, env_overrides):
    """Run golden set with specific env overrides. Returns metrics dict."""
    # Set env vars for this config
    old_env = {}
    for k, v in env_overrides.items():
        old_env[k] = os.environ.get(k)
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    # Re-import search to pick up env changes
    # Simpler: just call the search function directly with flags
    from mcp_server import search_fts5, _parse_query_structured

    hits_at_1 = 0
    hits_at_5 = 0
    reciprocal_ranks = []
    ndcgs = []
    latencies = []
    cross_lingual_positions = []

    for entry in golden:
        query = entry["query"]
        # v2 schema stores judgments under "relevant" with per-result grades;
        # v1 used a flat list under "expected_ids". Support both.
        rel = entry.get("relevant") or []
        expected_ids = set(entry.get("expected_ids")
                            or (r.get("decision_id") for r in rel
                                 if isinstance(r, dict) and r.get("grade", 0) > 0))
        if not expected_ids:
            continue

        t0 = time.monotonic()
        try:
            results, total = search_fts5(query=query, limit=20)
        except Exception as e:
            print(f"  ERROR {query[:40]}: {e}")
            continue
        elapsed = (time.monotonic() - t0) * 1000
        latencies.append(elapsed)

        result_ids = [r["decision_id"] for r in results[:20]]

        # MRR
        rr = 0
        for i, rid in enumerate(result_ids):
            if rid in expected_ids:
                rr = 1.0 / (i + 1)
                break
        reciprocal_ranks.append(rr)

        # Hit@1, Hit@5
        if result_ids and result_ids[0] in expected_ids:
            hits_at_1 += 1
        if any(rid in expected_ids for rid in result_ids[:5]):
            hits_at_5 += 1

        # NDCG@10
        relevances = [1.0 if rid in expected_ids else 0.0 for rid in result_ids[:10]]
        ndcgs.append(ndcg(relevances))

        # Cross-lingual: detect query language and result languages
        query_lang = entry.get("language", "de")
        for i, r in enumerate(results[:20]):
            if r.get("language") and r["language"] != query_lang:
                cross_lingual_positions.append(i + 1)
                break

    # Restore env
    for k, v in old_env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    n = len(reciprocal_ranks)
    return {
        "config": config_name,
        "queries": n,
        "mrr": round(sum(reciprocal_ranks) / max(n, 1), 4),
        "hit_at_1": round(hits_at_1 / max(n, 1), 4),
        "hit_at_5": round(hits_at_5 / max(n, 1), 4),
        "ndcg_at_10": round(sum(ndcgs) / max(n, 1), 4),
        "avg_latency_ms": round(sum(latencies) / max(n, 1), 1),
        "p95_latency_ms": round(sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0, 1),
        "cross_lingual_avg_pos": round(sum(cross_lingual_positions) / max(len(cross_lingual_positions), 1), 1) if cross_lingual_positions else None,
        "cross_lingual_queries": len(cross_lingual_positions),
    }


def main():
    if not GOLDEN_SET.exists():
        print(f"Golden set not found: {GOLDEN_SET}")
        sys.exit(1)

    golden = load_golden()
    print(f"Loaded {len(golden)} golden queries")

    configs = {
        "full": {},
        "no_parse": {"LLM_EXPANSION_ENABLED": "false"},
        "no_rerank": {"LLM_RERANK_ENABLED": "false"},
        "no_crossling": {"LLM_EXPANSION_ENABLED": "false"},  # cross-lingual comes from parse
        "no_citation": {"CITATION_BOOST_ENABLED": "false"},
    }

    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    results = []

    for name, env in configs.items():
        print(f"\n=== {name} ===")
        t0 = time.monotonic()
        metrics = run_config(golden, name, env)
        metrics["run_id"] = run_id
        metrics["timestamp"] = datetime.now(timezone.utc).isoformat()
        elapsed = time.monotonic() - t0
        print(f"  MRR={metrics['mrr']:.3f}  Hit@1={metrics['hit_at_1']:.3f}  "
              f"Hit@5={metrics['hit_at_5']:.3f}  NDCG@10={metrics['ndcg_at_10']:.3f}  "
              f"({elapsed:.1f}s)")
        results.append(metrics)

    # Write to history
    with open(HISTORY, "a") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    print(f"\nResults appended to {HISTORY}")

    # Summary table
    print("\n" + "="*80)
    print(f"{'Config':<16} {'MRR':>8} {'Hit@1':>8} {'Hit@5':>8} {'NDCG@10':>8} {'P95ms':>8}")
    print("-"*80)
    for r in results:
        print(f"{r['config']:<16} {r['mrr']:>8.3f} {r['hit_at_1']:>8.3f} "
              f"{r['hit_at_5']:>8.3f} {r['ndcg_at_10']:>8.3f} {r['p95_latency_ms']:>8.0f}")


if __name__ == "__main__":
    main()
