# Hybrid Dense Retrieval Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve Swiss caselaw search from MRR 0.456 to MRR 0.60+ by fixing vector search integration, upgrading the cross-encoder, and expanding evaluation infrastructure.

**Architecture:** Incremental upgrade to existing FTS5+RRF pipeline. Phase 1 reactivates BGE-M3 vectors as a reranking signal (not candidate injection). Phase 2 swaps the cross-encoder from 33M to 278M+ params. Both gated by benchmark results. Phases 3-4 (fine-tuning, Haiku reranking) are conditional and planned separately if needed.

**Tech Stack:** Python 3, SQLite FTS5, sqlite-vec, SentenceTransformers, BGE-M3 (BAAI/bge-m3), bge-reranker-base (BAAI/bge-reranker-base), PyTorch (CPU), Anthropic API (Haiku for existing LLM expansion)

**Spec:** `docs/superpowers/specs/2026-03-16-hybrid-dense-retrieval-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `benchmarks/search_relevance_golden.json` | Modify | Add ~50 new queries across weak areas |
| `benchmarks/generate_golden_queries.py` | Create | Semi-automated golden set generation from citation graph |
| `benchmarks/run_citation_eval.py` | Create | Automated citation-pair regression check (~5K pairs) |
| `benchmarks/run_search_benchmark.py` | No change | Existing benchmark runner, used as-is |
| `mcp_server.py` | Modify | Vector signal integration (mode A/B), cross-encoder config |
| `search_stack/build_vectors.py` | No change | Existing vector DB builder, used as-is |
| `.env.mcp` (VPS only) | Modify | Add new env vars for cross-encoder model, vector config |

---

## Chunk 1: Evaluation Infrastructure

### Task 1: Generate Golden Set Expansion Candidates

**Files:**
- Create: `benchmarks/generate_golden_queries.py`

- [ ] **Step 1: Write the golden query generator script**

This script uses the citation graph to find top-cited decisions per legal domain and outputs candidate queries for manual review. See spec section "Golden Set Expansion" for the semi-automated generation process. The script should:
- Accept `--graph-db` and `--fts-db` paths
- Query `decision_statutes` joined with `citation_targets` for top-cited decisions per statute article
- For each, extract regeste text and generate a search-style query from key terms
- Output JSON with candidates, each marked `"status": "candidate"` for manual curation
- Target statute articles: ART.8.EMRK, ART.271.OR, ART.261.OR, ART.269.OR, ART.641.ZGB, ART.127.OR, ART.42.OR, ART.58.SVG, ART.190.DBG

- [ ] **Step 2: Run on VPS to generate candidates**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 benchmarks/generate_golden_queries.py \
  --graph-db /mnt/HC_Volume_104655575/output/reference_graph.db \
  --fts-db /mnt/HC_Volume_104655575/output/decisions.db \
  --output benchmarks/golden_candidates.json'
```

Expected: JSON file with ~27 candidate queries from statute domains.

- [ ] **Step 3: Review candidates and add to golden set**

Manually review `golden_candidates.json`. For each accepted candidate:
1. Verify regeste content matches the query concept
2. Write a natural search query (not just extracted terms)
3. Add 2-4 additional expected decisions using citation graph neighbors
4. Add to `benchmarks/search_relevance_golden.json` with appropriate tags

Target: add ~50 queries total (supplement candidates with manually crafted Italian, French, cross-lingual, tenancy, tax, and practitioner queries).

- [ ] **Step 4: Commit expanded golden set**

```bash
git add benchmarks/search_relevance_golden.json benchmarks/generate_golden_queries.py
git commit -m "feat: expand golden set to ~100 queries for dense retrieval benchmarking"
```

### Task 2: Build Citation-Pair Regression Check

**Files:**
- Create: `benchmarks/run_citation_eval.py`

- [ ] **Step 1: Write the citation-pair script**

Script should:
- Sample N citation pairs from `citation_targets` where both source and target are BGE decisions with regeste >= 50 chars
- Exclude prior-instance citations via `is_prior_instance` flag
- For each pair, use first 100 chars of source regeste as query
- Check if target decision_id appears in top-k search results
- Report hit rate and MRR
- Accept `--graph-db`, `--fts-db`, `-n` (sample size), `-k` (top-k), `--json-output`

Important: this tests retrieval similarity, NOT user-facing search quality. Use only for regression detection.

- [ ] **Step 2: Test locally with small sample**

```bash
python3 benchmarks/run_citation_eval.py \
  --graph-db output/reference_graph.db \
  --fts-db ~/.swiss-caselaw/decisions.db \
  -n 50 -k 10
```

Expected: runs without error, prints hit rate and MRR for 50 pairs.

- [ ] **Step 3: Commit**

```bash
git add benchmarks/run_citation_eval.py
git commit -m "feat: add citation-pair regression check for search quality"
```

### Task 3: Establish Baseline

- [ ] **Step 1: Deploy golden set and citation eval to VPS**

```bash
git push origin main
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && git stash && git pull --rebase origin main && git stash pop'
```

- [ ] **Step 2: Run golden set baseline**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db \
    --json-output benchmarks/baseline_pre_dense.json'
```

Record baseline MRR, Recall, nDCG, Hit@1 per tag. This is the number to beat.

- [ ] **Step 3: Run citation-pair baseline**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  python3 benchmarks/run_citation_eval.py \
    --graph-db /mnt/HC_Volume_104655575/output/reference_graph.db \
    --fts-db /mnt/HC_Volume_104655575/output/decisions.db \
    -n 2000 -k 10 \
    --json-output benchmarks/citation_eval_baseline.json'
```

Record baseline citation-pair hit rate and MRR.

- [ ] **Step 4: Commit baseline results**

```bash
git add benchmarks/baseline_pre_dense.json benchmarks/citation_eval_baseline.json
git commit -m "data: record baseline benchmarks before dense retrieval changes"
```

---

## Chunk 2: Vector Search Integration (Phase 1)

### Task 4: Rebuild vectors.db on VPS

- [ ] **Step 1: Install dependencies on VPS**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'pip3 install sqlite-vec FlagEmbedding'
```

If FlagEmbedding fails, fall back to sentence-transformers:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'pip3 install sqlite-vec sentence-transformers'
```

- [ ] **Step 2: Run vector build in tmux**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40
tmux new -s vecbuild
cd /opt/caselaw/repo
python3 search_stack/build_vectors.py \
  --db /mnt/HC_Volume_104655575/output/decisions.db \
  --output /mnt/HC_Volume_104655575/output/vectors.db \
  --model BAAI/bge-m3 \
  --batch-size 32 \
  --text-field regeste \
  2>&1 | tee logs/build_vectors.log
```

Expected: ~2-3 hours. Monitor with `tail -f logs/build_vectors.log`.

- [ ] **Step 3: Verify vectors.db**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'python3 -c "
import sqlite3
conn = sqlite3.connect(\"/mnt/HC_Volume_104655575/output/vectors.db\")
tables = [r[0] for r in conn.execute(\"SELECT name FROM sqlite_master WHERE type=\\\"table\\\"\").fetchall()]
print(\"Tables:\", tables)
count = conn.execute(\"SELECT COUNT(*) FROM vec_decisions\").fetchone()[0]
print(\"Vectors:\", count)
conn.close()
"'
```

Expected: ~500K-900K vectors (decisions with regeste text).

### Task 5: Implement Vector Signal Mode A (Reranking Only)

**Files:**
- Modify: `mcp_server.py` (vector integration in `_search_fts5_inner`)

- [ ] **Step 1: Modify vector integration to signal-only mode**

In `_search_fts5_inner`, find the existing vector search block (around lines 1100-1140). Change from candidate injection to signal-only:

Current behavior: vector results are injected as new candidates into `candidate_meta`.

New behavior (mode A): only compute vector similarity for candidates already in `candidate_meta`. The key change: encode the query, run KNN on the full vector DB (top 200), then intersect results with existing pool IDs. Store cosine distances in `vector_scores` dict.

The existing code at lines 2833-2836 already applies `vector_signal = VECTOR_SIGNAL_WEIGHT * max(0.0, 1.0 - vec_dist)` in `_rerank_rows`. Just ensure `vector_scores` is populated correctly.

Check `build_vectors.py` for the exact sqlite-vec KNN query syntax before implementing.

- [ ] **Step 2: Test vector signal activates**

Restart workers and run a test search. Verify no errors in logs:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
```

- [ ] **Step 3: Commit mode A**

```bash
git add mcp_server.py
git commit -m "feat: vector search mode A — similarity signal for pool reranking only"
```

### Task 6: Benchmark Mode A and Tune Weights

- [ ] **Step 1: Deploy and benchmark with default VECTOR_SIGNAL_WEIGHT=3.0**

```bash
git push origin main
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && git stash && git pull --rebase origin main && git stash pop && \
  systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
# Wait 30s for model load, then:
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db \
    --json-output benchmarks/benchmark_vector_modeA_w3.json'
```

Compare MRR against baseline. If regression, try lower weight.

- [ ] **Step 2: Sweep weights if needed**

Test VECTOR_SIGNAL_WEIGHT values of 1.0, 2.0, 3.0, 5.0. Deploy the weight that gives highest MRR without regressions.

- [ ] **Step 3: Update default weight and commit if changed**

### Task 7: Implement and Test Mode B (Gated Candidate Injection)

- [ ] **Step 1: Add mode B alongside mode A**

Add env var `SWISS_CASELAW_VECTOR_MODE` with values `signal` (mode A, default) or `inject` (mode B).

For mode B: run open KNN search (top 50), inject up to 10 candidates NOT already in pool with low base RRF weight (0.3). Also add vector signal for all KNN matches that ARE in pool.

- [ ] **Step 2: Benchmark mode B**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  SWISS_CASELAW_VECTOR_MODE=inject \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db'
```

Compare against mode A and baseline. Deploy the winner.

- [ ] **Step 3: Commit and deploy winner**

```bash
git add mcp_server.py
git commit -m "feat: add vector mode B (gated candidate injection, max 10)"
```

Set `SWISS_CASELAW_VECTOR_MODE` in `.env.mcp` on VPS to the winning mode.

---

## Chunk 3: Cross-Encoder Upgrade (Phase 2)

### Task 8: Benchmark Cross-Encoder Candidates

**Files:**
- Modify: `mcp_server.py` (cross-encoder config is already configurable via env vars)

- [ ] **Step 1: Verify CROSS_ENCODER_MODEL is configurable via env var**

Check `mcp_server.py` line 136-138. `SWISS_CASELAW_CROSS_ENCODER_MODEL` env var should already exist. If so, no code change needed.

- [ ] **Step 2: Install and test bge-reranker-base on VPS**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'python3 -c "
from sentence_transformers import CrossEncoder
model = CrossEncoder(\"BAAI/bge-reranker-base\")
print(\"Loaded successfully\")
scores = model.predict([(\"Swiss tort liability\", \"Art. 41 OR Schadenersatz\")])
print(f\"Test score: {scores[0]:.4f}\")
"'
```

Expected: model downloads (~1GB), loads, returns a score.

- [ ] **Step 3: Benchmark bge-reranker-base**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  SWISS_CASELAW_CROSS_ENCODER=1 \
  SWISS_CASELAW_CROSS_ENCODER_MODEL="BAAI/bge-reranker-base" \
  SWISS_CASELAW_CROSS_ENCODER_TOP_N=20 \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db'
```

Compare MRR, Hit@1, and latency against baseline.

- [ ] **Step 4: If quality gain is large, also test bge-reranker-v2-m3**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  SWISS_CASELAW_CROSS_ENCODER=1 \
  SWISS_CASELAW_CROSS_ENCODER_MODEL="BAAI/bge-reranker-v2-m3" \
  SWISS_CASELAW_CROSS_ENCODER_TOP_N=15 \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db'
```

Compare quality vs latency tradeoff against bge-reranker-base.

### Task 9: Tune and Deploy Best Cross-Encoder

- [ ] **Step 1: Sweep CROSS_ENCODER_WEIGHT for winning model**

Test weights 1.0, 1.4, 2.0, 3.0 with the best-performing model from Task 8.

- [ ] **Step 2: Update .env.mcp on VPS with winning config**

Add/update these lines in `/opt/caselaw/repo/.env.mcp`:
```
SWISS_CASELAW_CROSS_ENCODER=1
SWISS_CASELAW_CROSS_ENCODER_MODEL=<winning model>
SWISS_CASELAW_CROSS_ENCODER_TOP_N=<winning top_n>
SWISS_CASELAW_CROSS_ENCODER_WEIGHT=<winning weight>
```

- [ ] **Step 3: Restart workers and verify**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
```

- [ ] **Step 4: Run final benchmark with all Phase 1+2 improvements**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db \
    --json-output benchmarks/benchmark_dense_phase1_2.json'
```

Also run citation-pair eval to check for regressions:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  python3 benchmarks/run_citation_eval.py \
    --graph-db /mnt/HC_Volume_104655575/output/reference_graph.db \
    --fts-db /mnt/HC_Volume_104655575/output/decisions.db \
    -n 2000 -k 10'
```

- [ ] **Step 5: Decision gate**

Compare final MRR against baseline and 0.60 target:
- **MRR >= 0.60**: Done. Update memory, commit results, celebrate.
- **MRR < 0.60**: Proceed to Phase 3 (fine-tuning). Write a separate plan for the training pipeline covering: training data extraction, LoRA fine-tuning script, vector rebuild, and Haiku reranking.

- [ ] **Step 6: Commit results and update memory**

```bash
git add benchmarks/benchmark_dense_phase1_2.json
git commit -m "data: benchmark results after vector search + cross-encoder upgrade"
```

Update `memory/search_improvement_progress.md` with new MRR numbers and deployment state.
