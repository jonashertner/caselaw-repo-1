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
- FTS5 full-text search with BM25 ranking
- Reciprocal Rank Fusion (RRF) to merge multiple retrieval strategies
- Citation graph signals (citation count, leading case status)
- Optional LLM reranking (disabled in this offline optimization)

Your job: analyze execution traces from failed queries and propose config changes that improve MRR@10.

IMPORTANT CONSTRAINTS:
- Only modify numeric parameters (weights, thresholds, constants)
- Do not add new features or change the pipeline architecture
- LLM features are disabled (offline optimization) — don't enable them
- Vector search is disabled (no vectors.db) — don't enable it
- Small incremental changes are better than large jumps
- Always explain your reasoning based on the trace data"""

PROPOSER_PROMPT = """## Current Performance
MRR@10: {mrr:.4f} | Hit@1: {hit1:.4f} | Recall@10: {recall:.4f} | nDCG@10: {ndcg:.4f}
Evaluated: {evaluated} queries

## Best So Far
MRR@10: {best_mrr:.4f} (iteration {best_iter})

## Current Config
```json
{config}
```

## Failed Query Traces (worst {n_traces} by MRR)

{traces}

## Iteration History
{history}

## Your Task

Analyze the failed query traces. For each failed query, explain:
1. What the query was looking for
2. What was returned instead (look at topk results)
3. Why the relevant decision was missed or ranked too low

Then propose a MODIFIED CONFIG (JSON) that addresses the failures.
Output ONLY the JSON config dict, wrapped in ```json ... ``` markers.
Keep your analysis concise (under 300 words)."""


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
) -> dict:
    """Main optimization loop."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir = RESULTS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    config = copy.deepcopy(DEFAULT_CONFIG)
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

        prompt = PROPOSER_PROMPT.format(
            mrr=mrr, hit1=hit1, recall=recall, ndcg=ndcg,
            evaluated=result["evaluated"],
            best_mrr=best_mrr, best_iter=best_iter,
            config=json.dumps(config, indent=2),
            n_traces=min(trace_limit, len(result["failed_traces"])),
            traces=traces_text,
            history=history_text,
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
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    summary = optimize(
        db_path=args.db.resolve(),
        golden_path=args.golden.resolve(),
        max_iterations=args.iterations,
        k=args.k,
        dry_run=args.dry_run,
    )
    print("\nBest MRR@{}: {:.4f}".format(args.k, summary["best_mrr"]))


if __name__ == "__main__":
    main()
