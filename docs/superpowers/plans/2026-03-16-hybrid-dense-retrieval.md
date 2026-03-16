# Hybrid Dense Retrieval Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve Swiss caselaw search from MRR 0.456 to MRR 0.60+ by upgrading the cross-encoder, fixing vector search integration, and expanding evaluation infrastructure.

**Architecture:** Incremental upgrade to existing FTS5+RRF pipeline. Start with the highest-leverage, lowest-risk change (cross-encoder swap), then add vector search if needed. Each change is independently deployable and reversible. Phases 3-4 (fine-tuning, Haiku reranking) are conditional and planned separately if needed.

**Tech Stack:** Python 3, SQLite FTS5, sqlite-vec, SentenceTransformers, BGE-M3 (BAAI/bge-m3), bge-reranker-base (BAAI/bge-reranker-base), PyTorch (CPU), Anthropic API (Haiku for existing LLM expansion)

**Spec:** `docs/superpowers/specs/2026-03-16-hybrid-dense-retrieval-design.md`

---

## Availability Rules

The MCP server must remain available to users throughout all changes. These rules apply to every task:

1. **Never restart all 4 workers simultaneously.** Use rolling restart: `for p in 8770 8771 8772 8773; do systemctl restart mcp-server@$p && sleep 5; done`
2. **Never pip install without checking existing versions first.** Run `pip3 show <package>` before installing. Use `--no-deps` if the package is already installed and only needs an upgrade.
3. **Never run CPU-intensive builds at default priority.** Use `nice -n 19 ionice -c3` for vector builds and model downloads to avoid impacting live search.
4. **Benchmark runs use a separate Python process**, not the live workers. Benchmarks don't affect availability.
5. **All config changes via env vars** — no code changes needed for rollback. Just update `.env.mcp` and rolling-restart.
6. **Verify health after every restart:** `curl -s https://mcp.opencaselaw.ch/health | python3 -m json.tool`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `benchmarks/search_relevance_golden.json` | Modify | Add ~50 new queries across weak areas |
| `benchmarks/generate_golden_queries.py` | Create | Semi-automated golden set generation from citation graph |
| `benchmarks/run_citation_eval.py` | Create | Automated citation-pair regression check (~5K pairs) |
| `benchmarks/run_search_benchmark.py` | No change | Existing benchmark runner, used as-is |
| `mcp_server.py` | Modify | Vector signal integration (mode A/B) |
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

Expected: JSON file with ~27 candidate queries from statute domains. This is a read-only operation — no impact on live service.

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

No restart needed — benchmark scripts don't affect running workers.

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

## Chunk 2: Cross-Encoder Upgrade (Highest Leverage, Lowest Risk)

This goes first because it requires no build step, no new dependencies (sentence-transformers already installed), and is fully configurable via env vars. Zero impact on availability.

### Task 4: Verify Dependencies and Test Cross-Encoder Models

- [ ] **Step 1: Check existing packages on VPS**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'pip3 show sentence-transformers torch | grep -E "^(Name|Version)"'
```

Expected: sentence-transformers and torch already installed (used by existing cross-encoder).

- [ ] **Step 2: Download bge-reranker-base model (background, low priority)**

Download the model without affecting live service. This just downloads files to the HuggingFace cache:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'nice -n 19 python3 -c "
from sentence_transformers import CrossEncoder
model = CrossEncoder(\"BAAI/bge-reranker-base\")
scores = model.predict([(\"Swiss tort liability\", \"Art. 41 OR Schadenersatz\")])
print(f\"bge-reranker-base loaded OK, test score: {scores[0]:.4f}\")
del model
"'
```

Expected: model downloads (~1GB), loads, returns a score. No impact on running workers.

- [ ] **Step 3: Benchmark bge-reranker-base (separate process, no restart)**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  SWISS_CASELAW_CROSS_ENCODER=1 \
  SWISS_CASELAW_CROSS_ENCODER_MODEL="BAAI/bge-reranker-base" \
  SWISS_CASELAW_CROSS_ENCODER_TOP_N=20 \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db'
```

This runs the benchmark in a separate Python process with the new model. Live workers are unaffected. Compare MRR, Hit@1, and latency against baseline.

- [ ] **Step 4: If quality gain is large, also download and test bge-reranker-v2-m3**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'nice -n 19 python3 -c "
from sentence_transformers import CrossEncoder
model = CrossEncoder(\"BAAI/bge-reranker-v2-m3\")
scores = model.predict([(\"Swiss tort liability\", \"Art. 41 OR Schadenersatz\")])
print(f\"bge-reranker-v2-m3 loaded OK, test score: {scores[0]:.4f}\")
del model
"'
```

Then benchmark:

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

### Task 5: Tune and Deploy Best Cross-Encoder

- [ ] **Step 1: Sweep CROSS_ENCODER_WEIGHT for winning model**

Test weights 1.0, 1.4, 2.0, 3.0 with the best model from Task 4. All benchmarks run in separate processes — no worker restart needed.

- [ ] **Step 2: Check memory impact before deploying**

Estimate per-worker memory with new model:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'python3 -c "
import psutil, os
proc = psutil.Process(os.getpid())
from sentence_transformers import CrossEncoder
model = CrossEncoder(\"BAAI/bge-reranker-base\")  # or winning model
mem_mb = proc.memory_info().rss / 1024 / 1024
print(f\"Memory after model load: {mem_mb:.0f} MB\")
# Check total system memory
total = psutil.virtual_memory()
print(f\"System: {total.used/1024/1024/1024:.1f}GB used / {total.total/1024/1024/1024:.1f}GB total\")
del model
"'
```

If the model uses >4GB per worker, consider reducing to 2 workers instead of 4.

- [ ] **Step 3: Update .env.mcp on VPS with winning config**

Add/update these lines in `/opt/caselaw/repo/.env.mcp`:
```
SWISS_CASELAW_CROSS_ENCODER=1
SWISS_CASELAW_CROSS_ENCODER_MODEL=<winning model>
SWISS_CASELAW_CROSS_ENCODER_TOP_N=<winning top_n>
SWISS_CASELAW_CROSS_ENCODER_WEIGHT=<winning weight>
```

- [ ] **Step 4: Rolling restart workers (one at a time)**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'for p in 8770 8771 8772 8773; do
  echo "Restarting worker $p..."
  systemctl restart mcp-server@$p
  sleep 15  # wait for model to load before restarting next
  echo "Worker $p restarted"
done'
```

- [ ] **Step 5: Verify health after restart**

```bash
curl -s https://mcp.opencaselaw.ch/health | python3 -m json.tool
```

Expected: `{"status": "ok", "decisions": 979431}` (or similar count).

- [ ] **Step 6: Run final benchmark to confirm production deployment matches test**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db \
    --json-output benchmarks/benchmark_cross_encoder_upgrade.json'
```

- [ ] **Step 7: Decision gate — is vector search needed?**

Compare MRR against baseline and 0.60 target:
- **MRR >= 0.60**: Skip Chunk 3. Update memory, commit results, done.
- **MRR < 0.60 but significant improvement**: Proceed to Chunk 3 (vector search) for additional gains.
- **MRR regressed**: Rollback by removing `SWISS_CASELAW_CROSS_ENCODER=1` from .env.mcp and rolling-restart.

- [ ] **Step 8: Commit results**

```bash
git add benchmarks/benchmark_cross_encoder_upgrade.json
git commit -m "data: benchmark results after cross-encoder upgrade"
```

---

## Chunk 3: Vector Search Integration (Conditional — Only If MRR < 0.60)

### Task 6: Rebuild vectors.db on VPS

- [ ] **Step 1: Check dependencies**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'pip3 show sqlite-vec FlagEmbedding 2>&1 | grep -E "^(Name|Version)" || echo "MISSING"'
```

If missing, install at low priority:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'nice -n 19 pip3 install sqlite-vec FlagEmbedding --no-deps 2>&1 || nice -n 19 pip3 install sqlite-vec sentence-transformers --no-deps'
```

After installing, verify existing workers still respond:

```bash
curl -s https://mcp.opencaselaw.ch/health | python3 -m json.tool
```

- [ ] **Step 2: Run vector build in tmux (low priority, background)**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40
tmux new -s vecbuild
cd /opt/caselaw/repo
nice -n 19 ionice -c3 python3 search_stack/build_vectors.py \
  --db /mnt/HC_Volume_104655575/output/decisions.db \
  --output /mnt/HC_Volume_104655575/output/vectors.db.tmp \
  --model BAAI/bge-m3 \
  --batch-size 32 \
  --text-field regeste \
  2>&1 | tee logs/build_vectors.log
# After completion, atomic swap:
mv /mnt/HC_Volume_104655575/output/vectors.db /mnt/HC_Volume_104655575/output/vectors.db.prev 2>/dev/null
mv /mnt/HC_Volume_104655575/output/vectors.db.tmp /mnt/HC_Volume_104655575/output/vectors.db
```

Build to `.tmp` file, then atomic rename. Live workers see old (empty) vectors.db until swap. Expected: ~2-3 hours. Monitor with `tail -f logs/build_vectors.log` from another session.

**Note**: `nice -n 19 ionice -c3` ensures the build doesn't compete with live search for CPU/IO.

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

### Task 7: Implement Vector Signal Mode A (Reranking Only)

**Files:**
- Modify: `mcp_server.py` (vector integration in `_search_fts5_inner`)

- [ ] **Step 1: Modify vector integration to signal-only mode**

In `_search_fts5_inner`, find the existing vector search block (around lines 1100-1140). Change from candidate injection to signal-only:

Current behavior: vector results are injected as new candidates into `candidate_meta`.

New behavior (mode A): only compute vector similarity for candidates already in `candidate_meta`. Encode the query, run KNN on the full vector DB (top 200), then intersect results with existing pool IDs. Store cosine distances in `vector_scores` dict.

The existing code at lines 2833-2836 already applies `vector_signal = VECTOR_SIGNAL_WEIGHT * max(0.0, 1.0 - vec_dist)` in `_rerank_rows`. Just ensure `vector_scores` is populated correctly.

Check `build_vectors.py` for the exact sqlite-vec KNN query syntax before implementing.

- [ ] **Step 2: Benchmark mode A (separate process, no restart needed)**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db \
    --json-output benchmarks/benchmark_vector_modeA.json'
```

Compare MRR against cross-encoder-only baseline. If regression, try lower `VECTOR_SIGNAL_WEIGHT`.

- [ ] **Step 3: Sweep vector signal weight if needed**

Test VECTOR_SIGNAL_WEIGHT values of 1.0, 2.0, 3.0, 5.0. All benchmarks run in separate processes.

- [ ] **Step 4: Commit mode A**

```bash
git add mcp_server.py
git commit -m "feat: vector search mode A — similarity signal for pool reranking only"
```

### Task 8: Implement and Test Mode B (Gated Candidate Injection)

- [ ] **Step 1: Add mode B alongside mode A**

Add env var `SWISS_CASELAW_VECTOR_MODE` with values `signal` (mode A, default) or `inject` (mode B).

For mode B: run open KNN search (top 50), inject up to 10 candidates NOT already in pool with low base RRF weight (0.3). Also add vector signal for all KNN matches that ARE in pool.

- [ ] **Step 2: Benchmark mode B (separate process)**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  SWISS_CASELAW_VECTOR_MODE=inject \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db'
```

Compare against mode A and baseline. Deploy the winner.

- [ ] **Step 3: Commit**

```bash
git add mcp_server.py
git commit -m "feat: add vector mode B (gated candidate injection, max 10)"
```

- [ ] **Step 4: Deploy winning vector mode with rolling restart**

Update `.env.mcp` with winning vector mode and weight, then:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'for p in 8770 8771 8772 8773; do
  echo "Restarting worker $p..."
  systemctl restart mcp-server@$p
  sleep 15
  echo "Worker $p restarted"
done'
```

Verify health: `curl -s https://mcp.opencaselaw.ch/health`

### Task 9: Final Benchmark and Decision Gate

- [ ] **Step 1: Run final benchmark with all improvements**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  python3 benchmarks/run_search_benchmark.py \
    --db /mnt/HC_Volume_104655575/output/decisions.db \
    --json-output benchmarks/benchmark_dense_final.json'
```

Also run citation-pair eval:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  export $(grep -v "^#" .env.mcp | xargs) && \
  python3 benchmarks/run_citation_eval.py \
    --graph-db /mnt/HC_Volume_104655575/output/reference_graph.db \
    --fts-db /mnt/HC_Volume_104655575/output/decisions.db \
    -n 2000 -k 10'
```

- [ ] **Step 2: Decision gate**

Compare final MRR against baseline and 0.60 target:
- **MRR >= 0.60**: Done. Update memory, commit results.
- **MRR < 0.60**: Write a separate plan for Phase 3 (LoRA fine-tuning) and Phase 4 (Haiku reranking).

- [ ] **Step 3: Commit results and update memory**

```bash
git add benchmarks/benchmark_dense_final.json
git commit -m "data: final benchmark results after dense retrieval integration"
```

Update `memory/search_improvement_progress.md` with new MRR numbers, deployed models, and config.
