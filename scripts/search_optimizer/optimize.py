#!/usr/bin/env python3
"""
Meta-Harness Search Optimizer
==============================
Iteratively optimizes the search pipeline configuration by:
1. Evaluating current config against the 100-query benchmark
2. Feeding execution traces (especially failed queries) to Claude
3. Claude proposes config changes based on trace analysis
4. Repeat until convergence or max iterations

Inspired by: Lee et al., "Meta-Harness: End-to-End Optimization of Model Harnesses" (2026)
Key insight: giving the proposer full execution traces (not just scores) enables
10x faster convergence than score-only optimization.

Usage:
    python3 -m scripts.search_optimizer.optimize --db output/decisions.db
    python3 -m scripts.search_optimizer.optimize --db output/decisions.db --iterations 20
    python3 -m scripts.search_optimizer.optimize --db output/decisions.db --dry-run
"""
from __future__ import annotations

import argparse
import copy
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.search_optimizer.config import DEFAULT_CONFIG
from scripts.search_optimizer.evaluate import evaluate

logger = logging.getLogger("search_optimizer")

RESULTS_DIR = REPO_ROOT / "scripts" / "search_optimizer" / "results"

PROPOSER_SYSTEM = """You are an expert search engineer optimizing a Swiss legal case law retrieval system.

The system searches 965,000+ court decisions using:
- FTS5 full-text search with BM25 ranking (column weights: bm25_title, bm25_regeste, bm25_full_text, etc.)
- Reciprocal Rank Fusion (RRF) to merge multiple retrieval strategies (each with its own weight: sw_*)
- Reranking signals: term coverage (w_title_cov, w_regeste_cov, w_snippet_cov), phrase hits, docket matching
- Citation graph signals (statute_signal_*, citation_signal_*, authority_signal_*, in_pool_signal_*)
- Court/domain priors (asylum_bvger_boost, decision_intent_boost, language_match_signal)
- Statute graph retrieval (statute_graph_rrf_weight, sg_weight_*)
- Doctrine strategies from LLM parse (doctrine_*_weight)
- Optional LLM reranking

TUNABLE PARAMETER GROUPS:
1. **Scoring weights (w_*)**: importance of each match signal in final ranking formula
2. **Graph signals (*_signal_*)**: citation/statute graph contribution (base + cap + rate)
3. **Strategy weights (sw_*)**: RRF fusion weight per retrieval strategy (nl_and, regeste_focus, quoted, etc.)
4. **Fusion pipeline (sg_weight_*, *_bge_rrf_weight)**: statute-graph and BGE injection strength
5. **BM25 column weights (bm25_*)**: FTS5 column importance (title=6.0 is highest, full_text=1.2 is lowest)
6. **Doctrine weights (doctrine_*)**: LLM-derived concept translation and cross-lingual strategy strength

Your job: analyze execution traces from failed queries and propose config changes that improve MRR@10.

IMPORTANT CONSTRAINTS:
- Only modify numeric parameters (weights, thresholds, constants)
- Do not add new features or change the pipeline architecture
- Vector search is disabled (no vectors.db) — don't enable it
- Small incremental changes are better than large jumps
- Always explain your reasoning based on the trace data
- Pay attention to the per-tag breakdown — target the weakest categories"""

PROPOSER_PROMPT = """## Current Performance
MRR@10: {mrr:.4f} | Hit@1: {hit1:.4f} | Recall@10: {recall:.4f} | nDCG@10: {ndcg:.4f}
Evaluated: {evaluated} queries

## Per-Tag MRR Breakdown
{per_tag_breakdown}

## Best So Far
MRR@10: {best_mrr:.4f} (iteration {best_iter})

## Current Config
```json
{config}
```

## Top 5 Successful Queries (for reference — what works)
{successes}

## Failed Query Traces (worst {n_traces} by MRR)

{traces}

## Iteration History
{history}

## Your Task

1. Look at the per-tag breakdown to identify which query categories are weakest.
2. Analyze the failed query traces — what was expected vs. what was returned.
3. Compare with successful queries to understand what signals work.
4. Propose weight changes that address the failures WITHOUT degrading successes.

Output a MODIFIED CONFIG (JSON) wrapped in ```json ... ``` markers.
Keep analysis concise (under 300 words)."""


def _format_traces(traces: list[dict], limit: int = 15) -> str:
    """Format failed traces for the proposer."""
    lines = []
    for t in traces[:limit]:
        rr = t.get("rr", 0)
        lines.append("### Query: \"{}\" [{}] — RR={:.2f}".format(
            t["query"], ", ".join(t.get("tags", [])), rr))
        lines.append("  Expected: {}".format(
            ", ".join("{} (grade {})".format(rid, t["relevant_grades"].get(rid, "?"))
                      for rid in t.get("relevant_ids", []))))
        matched = t.get("matched_ranks", {})
        if matched:
            lines.append("  Found at ranks: {}".format(
                ", ".join("{} → rank {}".format(rid, rank) for rid, rank in matched.items())))
        else:
            lines.append("  NOT FOUND in top 10")
        # Show top 5 returned results
        topk = t.get("topk", [])[:5]
        if topk:
            lines.append("  Top 5 returned:")
            for r in topk:
                marker = " ✓" if r.get("is_relevant") else ""
                lines.append("    #{}: {} | {} | {} | {}{}".format(
                    r["rank"], r["decision_id"], r["court"],
                    r["docket"], r["title"][:60], marker))
        lines.append("")
    return "\n".join(lines)


def _format_history(history: list[dict]) -> str:
    """Format iteration history."""
    if not history:
        return "(first iteration)"
    lines = []
    for h in history[-5:]:  # last 5 iterations
        lines.append("  Iter {}: MRR={:.4f} Hit@1={:.4f} — {}".format(
            h["iteration"], h["mrr"], h["hit1"],
            h.get("summary", "")[:100]))
    return "\n".join(lines)


def _call_proposer(prompt: str) -> dict | None:
    """Call Claude to propose a new config."""
    try:
        import anthropic
        client = anthropic.Anthropic()
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            system=PROPOSER_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text

        # Extract JSON from response
        import re
        json_match = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
        if json_match:
            proposed = json.loads(json_match.group(1))
            return proposed

        # Try parsing the whole response as JSON
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        logger.warning("Could not extract JSON config from proposer response")
        logger.debug("Response: %s", text[:500])
        return None

    except ImportError:
        logger.error("anthropic package not installed. Install with: pip install anthropic")
        return None
    except Exception as e:
        logger.error("Proposer call failed: %s", e)
        return None


def optimize(
    db_path: Path,
    golden_path: Path,
    max_iterations: int = 20,
    k: int = 10,
    trace_limit: int = 15,
    dry_run: bool = False,
    initial_config: dict | None = None,
) -> dict:
    """Main optimization loop."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir = RESULTS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    config = copy.deepcopy(initial_config or DEFAULT_CONFIG)
    best_config = copy.deepcopy(config)
    best_mrr = 0.0
    best_iter = 0
    history = []

    logger.info("=== Search Optimizer — %s ===", run_id)
    logger.info("DB: %s", db_path)
    logger.info("Golden: %s (%d queries)", golden_path,
                len(json.load(open(golden_path))["queries"]))
    logger.info("Max iterations: %d", max_iterations)

    for iteration in range(max_iterations):
        logger.info("\n--- Iteration %d ---", iteration)

        # Step 1: Evaluate
        t0 = time.time()
        result = evaluate(config, db_path, golden_path, k=k, trace_limit=trace_limit)
        eval_time = time.time() - t0

        mrr = result["mrr"]
        hit1 = result["hit1"]
        recall = result["recall"]
        ndcg = result["ndcg"]

        logger.info("MRR@%d: %.4f  Hit@1: %.4f  Recall: %.4f  nDCG: %.4f  (%.1fs)",
                     k, mrr, hit1, recall, ndcg, eval_time)

        # Save evaluation
        eval_path = run_dir / "iter_{:03d}_eval.json".format(iteration)
        with open(eval_path, "w") as f:
            json.dump(result, f, indent=2, default=str)

        # Track best
        improved = mrr > best_mrr
        if improved:
            best_mrr = mrr
            best_config = copy.deepcopy(config)
            best_iter = iteration
            logger.info("NEW BEST: MRR=%.4f", best_mrr)

        history.append({
            "iteration": iteration,
            "mrr": mrr,
            "hit1": hit1,
            "recall": recall,
            "ndcg": ndcg,
            "improved": improved,
            "config": copy.deepcopy(config),
        })

        # Step 2: Check convergence
        if iteration >= 3:
            recent_mrrs = [h["mrr"] for h in history[-3:]]
            if max(recent_mrrs) - min(recent_mrrs) < 0.002:
                logger.info("Converged (MRR stable within 0.002 for 3 iterations)")
                break

        if dry_run:
            logger.info("(dry-run: skipping proposer)")
            break

        # Step 3: Propose new config
        traces_text = _format_traces(result["failed_traces"], limit=trace_limit)
        history_text = _format_history(history)

        # Per-tag MRR breakdown
        per_tag = result.get("per_tag_mrr", {})
        if per_tag:
            tag_lines = ["{:25s} {:.4f}".format(tag, val)
                         for tag, val in sorted(per_tag.items(), key=lambda x: x[1])]
            per_tag_text = "\n".join(tag_lines)
        else:
            per_tag_text = "(not available)"

        # Top 5 successful queries
        ok_queries = [q for q in result.get("per_query", [])
                      if q.get("status") == "ok" and q.get("rr", 0) >= 1.0]
        ok_queries.sort(key=lambda q: -q.get("ndcg", 0))
        success_lines = []
        for sq in ok_queries[:5]:
            success_lines.append('  "{}" [{}] — RR=1.0'.format(
                sq.get("query", ""), ", ".join(sq.get("tags", []))))
        successes_text = "\n".join(success_lines) if success_lines else "(none)"

        prompt = PROPOSER_PROMPT.format(
            mrr=mrr, hit1=hit1, recall=recall, ndcg=ndcg,
            evaluated=result["evaluated"],
            best_mrr=best_mrr, best_iter=best_iter,
            config=json.dumps(config, indent=2),
            n_traces=min(trace_limit, len(result["failed_traces"])),
            traces=traces_text,
            history=history_text,
            per_tag_breakdown=per_tag_text,
            successes=successes_text,
        )

        # Save prompt
        prompt_path = run_dir / "iter_{:03d}_prompt.txt".format(iteration)
        with open(prompt_path, "w") as f:
            f.write(prompt)

        logger.info("Calling proposer...")
        proposed = _call_proposer(prompt)
        if not proposed:
            logger.warning("Proposer returned no valid config, keeping current")
            continue

        # Save proposal
        proposal_path = run_dir / "iter_{:03d}_proposal.json".format(iteration)
        with open(proposal_path, "w") as f:
            json.dump(proposed, f, indent=2)

        # Validate: only accept known keys with numeric values
        valid = True
        for key, value in proposed.items():
            if key not in DEFAULT_CONFIG:
                logger.warning("Ignoring unknown key: %s", key)
                continue
            if not isinstance(value, (int, float, bool)):
                logger.warning("Ignoring non-numeric value for %s: %s", key, value)
                valid = False
                continue
            config[key] = value

        if not valid:
            logger.warning("Proposal had invalid entries, partially applied")

        # Log changes
        changes = {k: v for k, v in config.items() if v != DEFAULT_CONFIG.get(k)}
        if changes:
            logger.info("Config changes: %s", json.dumps(changes))
        else:
            logger.info("No config changes (proposer returned same values)")

    # Save final results
    summary = {
        "run_id": run_id,
        "iterations": len(history),
        "best_mrr": best_mrr,
        "best_iteration": best_iter,
        "best_config": best_config,
        "default_config": DEFAULT_CONFIG,
        "history": history,
    }
    summary_path = run_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)

    logger.info("\n=== Optimization Complete ===")
    logger.info("Best MRR@%d: %.4f (iteration %d)", k, best_mrr, best_iter)
    logger.info("Results saved to: %s", run_dir)

    if best_mrr > DEFAULT_CONFIG.get("_baseline_mrr", 0):
        logger.info("Best config:")
        for key, value in best_config.items():
            if value != DEFAULT_CONFIG.get(key):
                logger.info("  %s: %s → %s", key, DEFAULT_CONFIG.get(key), value)

    return summary


def main():
    parser = argparse.ArgumentParser(description="Meta-Harness search optimizer")
    parser.add_argument("--db", type=Path, required=True, help="Path to decisions.db")
    parser.add_argument("--golden", type=Path,
                        default=REPO_ROOT / "benchmarks" / "search_relevance_golden.json")
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("-k", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true",
                        help="Run one evaluation only, no proposer")
    parser.add_argument("--with-rerank", action="store_true",
                        help="Enable LLM reranking during optimization")
    parser.add_argument("--with-llm-parse", action="store_true",
                        help="Enable LLM query parsing (requires ANTHROPIC_API_KEY)")
    parser.add_argument("--parse-cache", type=Path, default=None,
                        help="Path to pre-cached LLM parse results JSON")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Override config defaults from CLI flags
    config_overrides = {}
    if args.with_rerank:
        config_overrides["llm_rerank_enabled"] = True
    if args.with_llm_parse:
        config_overrides["llm_query_parse_enabled"] = True

    # Load pre-cached LLM parses if provided
    if args.parse_cache and args.parse_cache.exists():
        import mcp_server
        cache_data = json.load(open(args.parse_cache))
        for query_key, parsed in cache_data.get("structured_parses", {}).items():
            mcp_server._STRUCTURED_PARSE_CACHE[query_key] = parsed
        for query_key, terms in cache_data.get("expansions", {}).items():
            mcp_server._LLM_EXPANSION_CACHE[query_key] = terms
        logger.info("Loaded %d cached parses, %d cached expansions",
                     len(cache_data.get("structured_parses", {})),
                     len(cache_data.get("expansions", {})))

    # Apply overrides to DEFAULT_CONFIG for this run
    run_config = {**DEFAULT_CONFIG, **config_overrides}

    summary = optimize(
        db_path=args.db.resolve(),
        golden_path=args.golden.resolve(),
        max_iterations=args.iterations,
        k=args.k,
        dry_run=args.dry_run,
        initial_config=run_config,
    )
    print("\nBest MRR@{}: {:.4f}".format(args.k, summary["best_mrr"]))


if __name__ == "__main__":
    main()
