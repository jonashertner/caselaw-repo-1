#!/usr/bin/env python3
"""Aggregate research traces into paper-ready statistics.

Reads search_traces_*.jsonl files and produces a summary JSON
with everything needed for paper tables and figures.

Output: benchmarks/research_summary_YYYY-MM-DD.json
"""
import collections
import glob
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LOG_DIR = Path(os.environ.get("SWISS_CASELAW_DIR", str(Path.home() / ".swiss-caselaw"))) / "research_logs"
OUTPUT_DIR = REPO / "benchmarks"


def load_traces(days=None):
    """Load all trace files, optionally limited to last N days."""
    files = sorted(glob.glob(str(LOG_DIR / "search_traces_*.jsonl")))
    if days:
        files = files[-days:]
    traces = []
    rerank_traces = []
    for f in files:
        with open(f) as fh:
            for line in fh:
                try:
                    d = json.loads(line)
                    if d.get("type") == "rerank":
                        rerank_traces.append(d)
                    else:
                        traces.append(d)
                except json.JSONDecodeError:
                    pass
    return traces, rerank_traces


def compute_summary(traces, rerank_traces):
    summary = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "trace_count": len(traces),
        "rerank_count": len(rerank_traces),
    }

    if not traces:
        return summary

    # === Query language distribution ===
    lang_dist = collections.Counter(t.get("query_language", "de") for t in traces)
    summary["query_languages"] = dict(lang_dist.most_common())

    # === Latency distribution ===
    latencies = [t["total_ms"] for t in traces if "total_ms" in t]
    if latencies:
        latencies.sort()
        n = len(latencies)
        summary["latency"] = {
            "p50_ms": round(latencies[n // 2]),
            "p95_ms": round(latencies[int(n * 0.95)]),
            "p99_ms": round(latencies[int(n * 0.99)]),
            "avg_ms": round(sum(latencies) / n),
        }

    # === Parse latency (Haiku structured parse) ===
    parse_times = [t["parse_ms"] for t in traces if "parse_ms" in t and t["parse_ms"] > 0]
    if parse_times:
        parse_times.sort()
        n = len(parse_times)
        summary["haiku_parse_latency"] = {
            "p50_ms": round(parse_times[n // 2]),
            "p95_ms": round(parse_times[int(n * 0.95)]),
            "calls": n,
        }

    # === Structured parse stats ===
    parsed = [t for t in traces if t.get("structured_parse")]
    summary["structured_parse"] = {
        "total_queries": len(traces),
        "parsed": len(parsed),
        "parse_rate": round(len(parsed) / max(len(traces), 1), 3),
        "avg_statutes_found": round(
            sum(len(t["structured_parse"].get("statutes", [])) for t in parsed) / max(len(parsed), 1), 2
        ),
        "avg_synonyms": round(
            sum(len(t["structured_parse"].get("synonyms", [])) for t in parsed) / max(len(parsed), 1), 2
        ),
    }

    # === Cross-lingual retrieval ===
    cross_queries = [t for t in traces if t.get("cross_lingual_positions")]
    all_positions = []
    for t in cross_queries:
        all_positions.extend(t["cross_lingual_positions"])
    summary["cross_lingual"] = {
        "queries_with_cross_results": len(cross_queries),
        "total_queries": len(traces),
        "rate": round(len(cross_queries) / max(len(traces), 1), 3),
        "avg_first_position": round(sum(min(t["cross_lingual_positions"]) for t in cross_queries) / max(len(cross_queries), 1), 1) if cross_queries else None,
    }

    # === Result language distribution (across all results) ===
    result_langs = collections.Counter()
    for t in traces:
        for lang in t.get("result_langs", []):
            result_langs[lang] += 1
    summary["result_languages"] = dict(result_langs.most_common())

    # === Docket vs concept queries ===
    docket = sum(1 for t in traces if t.get("is_docket"))
    summary["query_types"] = {
        "docket_lookup": docket,
        "concept_search": len(traces) - docket,
        "docket_rate": round(docket / max(len(traces), 1), 3),
    }

    # === Top queries ===
    qc = collections.Counter(t.get("query", "")[:100] for t in traces)
    summary["top_queries"] = [{"query": q, "count": n} for q, n in qc.most_common(20)]

    # === Haiku rerank impact ===
    if rerank_traces:
        changed = sum(1 for t in rerank_traces if t.get("changed"))
        summary["rerank"] = {
            "total": len(rerank_traces),
            "changed_top": changed,
            "change_rate": round(changed / len(rerank_traces), 3),
        }

    # === Result count distribution ===
    counts = [t.get("result_count", 0) for t in traces]
    zero = sum(1 for c in counts if c == 0)
    summary["results"] = {
        "avg_count": round(sum(counts) / max(len(counts), 1), 1),
        "zero_result_rate": round(zero / max(len(counts), 1), 3),
        "zero_results": zero,
    }

    return summary


def main():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else None
    traces, reranks = load_traces(days)
    print(f"Loaded {len(traces)} search traces, {len(reranks)} rerank traces")

    summary = compute_summary(traces, reranks)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = OUTPUT_DIR / f"research_summary_{today}.json"
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"Written to {out_path}")

    # Print key stats
    print(f"\n{'='*60}")
    print(f"Traces: {summary['trace_count']}")
    if "latency" in summary:
        l = summary["latency"]
        print(f"Latency: P50={l['p50_ms']}ms  P95={l['p95_ms']}ms  P99={l['p99_ms']}ms")
    if "structured_parse" in summary:
        p = summary["structured_parse"]
        print(f"Parse rate: {p['parse_rate']:.1%}  ({p['parsed']}/{p['total_queries']})")
    if "cross_lingual" in summary:
        c = summary["cross_lingual"]
        print(f"Cross-lingual: {c['rate']:.1%} of queries got cross-lingual results")
        if c["avg_first_position"]:
            print(f"  Avg first cross-lingual position: {c['avg_first_position']}")
    if "rerank" in summary:
        r = summary["rerank"]
        print(f"Rerank: {r['change_rate']:.1%} changed top result ({r['changed_top']}/{r['total']})")
    if "results" in summary:
        print(f"Zero-result rate: {summary['results']['zero_result_rate']:.1%}")
    if "query_types" in summary:
        print(f"Docket lookups: {summary['query_types']['docket_rate']:.1%}")


if __name__ == "__main__":
    main()
