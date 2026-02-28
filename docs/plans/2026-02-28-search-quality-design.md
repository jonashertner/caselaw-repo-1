# Search Quality Upgrade — Design

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve search quality by enabling the existing cross-encoder re-ranker and expanding the benchmark golden set from 16 to ~80 queries.

**Architecture:** Two independent tracks — (A) cross-encoder activation on VPS and (B) golden set expansion. Track A re-ranks FTS5 top-30 candidates with `mmarco-mMiniLMv2-L12-H384-v1` via the existing `_apply_cross_encoder_boosts` pipeline. Track B grows the benchmark dataset so future improvements can be measured reliably.

**Tech Stack:** sentence-transformers CrossEncoder (already in codebase), Python, SQLite FTS5, existing benchmark runner.

---

## Track A: Cross-Encoder Activation

### What exists
- `_get_cross_encoder()` — lazy loads `cross-encoder/mmarco-mMiniLMv2-L12-H384-v1` via `sentence_transformers.CrossEncoder`
- `_apply_cross_encoder_boosts()` — re-scores top `CROSS_ENCODER_TOP_N=30` candidates, adds `CROSS_ENCODER_WEIGHT=1.4 × normalized_ce_score` to each candidate's combined score
- Called at `mcp_server.py:2290` in `_rank_results()`
- Gated on `CROSS_ENCODER_ENABLED = os.environ.get("SWISS_CASELAW_CROSS_ENCODER", "0").lower() in {"1","true","yes"}`

### Steps
1. Smoke-test on VPS: verify `sentence_transformers` CrossEncoder can load the model
2. If OK, add `SWISS_CASELAW_CROSS_ENCODER=true` to `.env.mcp`
3. Restart workers
4. Run benchmark: `SWISS_CASELAW_DIR=/opt/caselaw/repo/output python3 benchmarks/run_search_benchmark.py --db /opt/caselaw/repo/output/decisions.db`
5. Compare to baseline (MRR@10=0.394, Recall@10=0.625, nDCG@10=0.698)
6. If better: keep. If worse: investigate and tune `CROSS_ENCODER_TOP_N` / `CROSS_ENCODER_WEIGHT`

### Success criteria
- MRR@10 ≥ 0.42 (baseline + 2.5pp)
- Recall@10 ≥ 0.625 (no regression)
- Latency p95 ≤ 4 s (cross-encoder adds ~100–300 ms on VPS CPU)

---

## Track B: Golden Set Expansion

### What exists
- `benchmarks/search_relevance_golden.json` — 16 queries, format: `{id, query, tags, relevant: [{decision_id, grade}]}`
- Benchmark runner: `benchmarks/run_search_benchmark.py`

### Query coverage gaps to fill (~65 new queries)

| Category | Current | Target | Notes |
|----------|---------|--------|-------|
| Concept-match (legal↔colloquial) | 2 | 8 | e.g., "Autounfall" → Haftpflicht OR, "Kündigung" → OR/ArG |
| Statute-based | 0 | 12 | "Art. 97 OR", "Art. 263 StGB", "Art. 42 ZGB" |
| Cantonal decisions | 0 | 8 | ZH, BE, GE, VD, TI cantonal courts |
| BVGer / asylum | 3 | 6 | More BVGE / E-xxx dockets |
| Italian queries | 1 | 5 | IT query → IT or DE decision |
| French queries | 3 | 8 | FR query → FR or DE decision |
| Short/vague | 0 | 6 | "Mietrecht", "Erbrecht Pflichtteil" |
| Citation-style | 0 | 4 | "BGE 133 III 121" direct lookup |
| Date-filtered | 0 | 4 | Queries with year context |
| Multi-relevant | 4 | 10 | Queries with 3+ relevant decisions |

### Process
1. Use the Swiss caselaw MCP tools (`search_decisions`, `find_leading_cases`) to discover candidate decisions for each category
2. Write queries + relevant judgments in `search_relevance_golden.json` format
3. Verify each expected decision_id exists in the DB before adding
4. User reviews the batch and removes incorrect entries
5. Run benchmark with expanded set to get new baseline numbers

### Format reminder
```json
{
  "id": "q017",
  "query": "Autounfall Haftpflicht Mitverschulden",
  "tags": ["de", "liability", "nl"],
  "relevant": [
    {"decision_id": "bge_131_III_12", "grade": 3},
    {"decision_id": "bger_4A.123_2010", "grade": 2}
  ]
}
```

Grades: 3 = highly relevant (primary case), 2 = relevant, 1 = marginally relevant.

---

## Order of execution

1. Track B first — expand golden set so Track A can be measured accurately
2. Track A second — enable cross-encoder, benchmark against the full 80-query set
