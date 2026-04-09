#!/usr/bin/env python3
"""
Pre-cache LLM structured parses and expansions for benchmark queries.

Calls _parse_query_structured() and _expand_query_with_llm() for each
benchmark query and saves results to a JSON cache file. This enables the
optimizer to use LLM features without repeated API calls.

Usage:
    python3 -m scripts.search_optimizer.cache_parses \
        --golden benchmarks/search_relevance_golden.json \
        --output scripts/search_optimizer/parse_cache.json

Requires ANTHROPIC_API_KEY environment variable.
Cost: ~100 Haiku calls ≈ $0.05.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logger = logging.getLogger("cache_parses")


def cache_parses(
    golden_path: Path,
    output_path: Path,
    db_path: Path | None = None,
) -> dict:
    """Generate and save LLM parse cache for all benchmark queries."""
    import mcp_server

    # Set up mcp_server with DB path if provided
    if db_path:
        mcp_server.DB_PATH = db_path
        mcp_server.DATA_DIR = db_path.parent

    # Ensure LLM expansion is enabled
    mcp_server.LLM_EXPANSION_ENABLED = True

    with open(golden_path) as f:
        golden = json.load(f)
    queries = golden["queries"]

    structured_parses: dict[str, dict] = {}
    expansions: dict[str, list[str]] = {}

    for i, q in enumerate(queries):
        query_text = q.get("query", "")
        cache_key = query_text.strip().lower()
        logger.info("[%d/%d] %s", i + 1, len(queries), query_text[:80])

        # Structured parse
        try:
            parsed = mcp_server._parse_query_structured(query_text)
            if parsed:
                structured_parses[cache_key] = parsed
                logger.info("  structured: %s", parsed.get("doctrine", "")[:60])
        except Exception as e:
            logger.warning("  structured parse failed: %s", e)

        # LLM expansion
        try:
            terms = mcp_server._expand_query_with_llm(query_text)
            if terms:
                expansions[cache_key] = terms
                logger.info("  expansion: %s", terms[:3])
        except Exception as e:
            logger.warning("  expansion failed: %s", e)

        # Small delay to avoid rate limiting
        time.sleep(0.1)

    cache = {
        "structured_parses": structured_parses,
        "expansions": expansions,
        "total_queries": len(queries),
        "cached_parses": len(structured_parses),
        "cached_expansions": len(expansions),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

    logger.info("Saved %d parses, %d expansions to %s",
                len(structured_parses), len(expansions), output_path)
    return cache


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Pre-cache LLM parses for benchmark queries")
    parser.add_argument("--golden", type=Path,
                        default=REPO_ROOT / "benchmarks" / "search_relevance_golden.json")
    parser.add_argument("--output", type=Path,
                        default=REPO_ROOT / "scripts" / "search_optimizer" / "parse_cache.json")
    parser.add_argument("--db", type=Path, default=None,
                        help="Path to decisions.db (needed for some parse features)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    cache = cache_parses(
        golden_path=args.golden.resolve(),
        output_path=args.output.resolve(),
        db_path=args.db.resolve() if args.db else None,
    )
    print("Done: {} parses, {} expansions".format(
        cache["cached_parses"], cache["cached_expansions"]))


if __name__ == "__main__":
    main()
