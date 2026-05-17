"""Build the lawyer-authored cross-lingual query brief.

The v1 cross-lingual benchmark queries are extracted from each target
case's official regeste — a coupling that makes absolute MRR scores an
upper bound on realistic legal-research performance (see paper §7, §11).

This script samples 30 of the 50 v1 cases, stratified by legal area and
target-language, and emits two artifacts:

1. A Markdown brief (one section per case) that the lawyer fills in.
   Each section gives ONLY: docket, legal area, primary law. It does
   NOT show the regeste or any holding-derived vocabulary — that's the
   whole point of the realism experiment.

2. An empty JSONL template (`lawyer_queries.jsonl`) with the same q_id
   format as `cross_lingual_v1.jsonl`. After the lawyer fills in their
   queries in the Markdown, the user transcribes them into the JSONL
   (or a small follow-up script does it).

Output is fully deterministic given --seed.

Usage:
    python3 -m benchmarks.build_lawyer_query_brief \\
        --v1 benchmarks/swiss_legal_rag_bench/cross_lingual_v1.jsonl \\
        --md benchmarks/swiss_legal_rag_bench/lawyer_query_brief.md \\
        --jsonl benchmarks/swiss_legal_rag_bench/lawyer_queries_template.jsonl \\
        --n 30 --seed 42
"""
from __future__ import annotations

import argparse
import json
import random
from collections import defaultdict
from pathlib import Path
from typing import Optional


HEADER = """# Lawyer-authored query brief (v1.1 realism experiment)

## Why this exists

The v1 cross-lingual retrieval benchmark uses queries extracted directly
from each target case's own multilingual regeste. Because the query and
the target share construction vocabulary, the resulting MRR@10 = 0.630
is an **upper bound** on realistic legal-research performance.

This experiment measures how much that score drops when a lawyer
authors the query **without sight of the target's regeste**. The
resulting Δ-MRR is the realism cost of the v1 methodology.

## How to use this brief

For each case below, write **one search query** (5–15 words, your
preferred language) that you would actually type into a legal-research
system to try to find this case as the top result. Write the query as
if a client had brought you a fact pattern in this legal area and you
were starting research from scratch.

**Do not look up the case.** The whole point is to capture what a
practitioner would *actually* search for, given only the legal area
and primary statute.

Each case shows:

- **Docket** — the official BGE / decision number (this is the *target*
  you are trying to retrieve, not given to you in a real research
  scenario; we show it only so the experiment is reproducible).
- **Legal area** — high-level domain (e.g. "accident_insurance").
- **Primary law** — the dominant statute the case turns on.
- **Your query** — your authored query, in your preferred language.

Once you have authored all queries, save this file and let the
maintainer transcribe them into `lawyer_queries.jsonl` for
evaluation against the retrieval system.

---
"""


CASE_TEMPLATE = """## Case {i:02d} of {total}: `{docket}`

- **Legal area:** {legal_area}
- **Primary law:** {primary_law}
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---
"""


def stratified_sample(
    cases: list[dict], n: int, seed: int,
) -> list[dict]:
    """Sample n cases stratified by (legal_area, target_lang) to roughly
    match the v1 distribution. Falls back to random if strata are too
    small.
    """
    rng = random.Random(seed)
    by_area = defaultdict(list)
    for c in cases:
        by_area[c["legal_area"]].append(c)

    total = len(cases)
    out: list[dict] = []
    # Proportional allocation, round-robin to avoid running out
    quotas = {
        a: max(1, round(n * len(v) / total))
        for a, v in by_area.items()
    }
    for area, items in by_area.items():
        take = min(quotas[area], len(items))
        out.extend(rng.sample(items, take))

    rng.shuffle(out)
    # Trim or pad to exactly n
    if len(out) > n:
        out = out[:n]
    elif len(out) < n:
        remaining = [c for c in cases if c not in out]
        out.extend(rng.sample(remaining, n - len(out)))
    return out


def load_v1_targets(v1_path: Path) -> list[dict]:
    """Deduplicate v1 jsonl down to one row per target_decision_id, with
    the metadata fields we need.
    """
    seen: dict[str, dict] = {}
    with v1_path.open() as f:
        for line in f:
            r = json.loads(line)
            tid = r.get("target_decision_id")
            if not tid or tid in seen:
                continue
            seen[tid] = {
                "target_decision_id": tid,
                "docket": r.get("docket", tid),
                "legal_area": r.get("legal_area", "unknown"),
                "primary_law": r.get("primary_law", "unknown"),
                "target_lang": r.get("target_lang", "?"),
                "in_degree": r.get("in_degree", 0),
            }
    return list(seen.values())


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--v1", required=True, type=Path)
    p.add_argument("--md", required=True, type=Path)
    p.add_argument("--jsonl", required=True, type=Path)
    p.add_argument("--n", type=int, default=30)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args(argv)

    cases = load_v1_targets(args.v1)
    if len(cases) < args.n:
        raise SystemExit(
            f"only {len(cases)} unique cases in v1; cannot sample {args.n}"
        )

    sample = stratified_sample(cases, args.n, args.seed)

    # Markdown brief
    md_parts = [HEADER]
    for i, c in enumerate(sample, 1):
        md_parts.append(
            CASE_TEMPLATE.format(
                i=i, total=args.n,
                docket=c["docket"],
                legal_area=c["legal_area"],
                primary_law=c["primary_law"],
            )
        )
    args.md.parent.mkdir(parents=True, exist_ok=True)
    args.md.write_text("\n".join(md_parts))

    # JSONL template (one row per case, q_text empty)
    with args.jsonl.open("w") as f:
        for c in sample:
            row = {
                "q_id": f"lawyer_{c['target_decision_id']}",
                "q_lang": "",  # lawyer fills in
                "q_text": "",  # lawyer fills in
                "target_decision_id": c["target_decision_id"],
                "target_lang": c["target_lang"],
                "in_degree": c["in_degree"],
                "docket": c["docket"],
                "legal_area": c["legal_area"],
                "primary_law": c["primary_law"],
                "source": "lawyer_authored",
                "v1_paired_target_id": c["target_decision_id"],
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"  wrote {args.md} ({args.n} cases)")
    print(f"  wrote {args.jsonl} (template with {args.n} empty rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
