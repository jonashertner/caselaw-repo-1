# Hybrid Dense Retrieval for Swiss Caselaw Search

**Date**: 2026-03-16
**Status**: Approved
**Goal**: Improve search quality from MRR 0.456 to MRR 0.60+ (world-class for automated legal search)

## Context

The current search pipeline uses FTS5 BM25 with RRF fusion, LLM-driven query expansion (Haiku), citation graph signals, and a weak cross-encoder (mmarco-mMiniLMv2, 33M params). MRR improved from 0.320 to 0.456 through strategy tuning, but further gains require architectural changes.

A prior attempt with BGE-M3 off-the-shelf vectors (Mar 13) showed zero improvement. Investigation revealed the failure was in fusion (vector candidates conflicted with FTS5 results), not the embeddings themselves.

## Architecture

Incremental upgrade in 4 phases, each independently deployable and reversible. Stop as soon as MRR target is reached.

### Phase 1: Fix Vector Search Integration

**Problem**: Prior vector integration injected new candidates into the pool, diluting FTS5 results. Vector results competed on RRF score without enough signal to rank well.

**Fix**: Change vectors from "candidate source" to "reranking signal only":
- Vectors do NOT add new candidates to the pool
- For each FTS5-found candidate, compute cosine similarity to query vector
- Add similarity as a weighted signal in the final scoring formula
- This boosts semantically relevant candidates that FTS5 already found

**Steps**:
1. Rebuild `vectors.db` on VPS using existing `build_vectors.py` (BGE-M3, regeste text)
2. Modify `_search_fts5_inner()`: remove vector candidate injection, keep only signal
3. Tune `VECTOR_SIGNAL_WEIGHT` (sweep 1.0-5.0) against golden set
4. Benchmark

**Infrastructure**: `build_vectors.py` already exists (836 lines). BGE-M3 model loads lazily. vectors.db stored on data volume alongside other DBs.

**Expected build time**: ~2-3 hours on VPS (980K decisions, batch_size=32).

**Latency impact**: ~50ms per query for encoding + KNN lookup. Negligible vs current 5s from LLM calls.

### Phase 2: Swap Cross-Encoder

**Problem**: Current cross-encoder (mmarco-mMiniLMv2, 33M params, 12 layers, 384 hidden) is too weak to distinguish legally relevant from tangentially-related results.

**Fix**: Replace with `BAAI/bge-reranker-v2-m3` (568M params). Drop-in replacement via SentenceTransformers `CrossEncoder` class.

**Changes**:
- Model: `cross-encoder/mmarco-mMiniLMv2-L12-H384-v1` → `BAAI/bge-reranker-v2-m3`
- `CROSS_ENCODER_TOP_N`: 30 → 20 (larger model is slower per pair)
- `CROSS_ENCODER_WEIGHT`: retune (currently 1.4)
- `CROSS_ENCODER_ENABLED`: default to `True`

**Latency**: ~200-400ms for 20 pairs on 16-core CPU (vs ~50ms current). Acceptable.

**Memory**: ~2.2GB model weights. VPS has 64GB RAM, ~25% used.

**Fallback**: If quality regresses, try `BAAI/bge-reranker-base` (278M params) or revert to current model.

### Phase 3: Fine-Tune Embeddings (Conditional)

Only if phases 1-2 don't reach MRR 0.60.

**Base model**: BGE-M3 (BAAI/bge-m3), 568M params, 1024-dim.

**Training method**: LoRA (rank 16, alpha 32). Trains ~5-10M params (~1-2% of total).

**Where**: Mac M4 Pro (24GB, MPS backend, ~2-3 hours) or VPS CPU overnight (~8-12 hours). Decision deferred until needed.

**Training data**:

Dataset 1 — Citation pairs (~500K triples):
- Positive: if decision A cites decision B, pair (A.regeste, B.regeste)
- Hard negatives: BM25-similar decisions from same court/domain but not cited
- Filter: only decisions with regeste text, exclude prior-instance citations
- Split: 90% train, 5% val, 5% test

Dataset 2 — Regeste-to-document pairs (~200K triples):
- Positive: (decision.regeste, decision.full_text[:512])
- Hard negatives: BM25-similar decisions from different legal domains
- Filter: decisions with both regeste AND full_text ≥ 200 chars

**Training details**:
- Loss: MultipleNegativesRankingLoss (InfoNCE/contrastive)
- Two phases: citation pairs first (legal relatedness), then regeste-to-doc (relevance matching)
- Optimizer: AdamW, lr=2e-5, warmup 10%, cosine decay
- Epochs: 3 per phase
- Batch size: 8-16 (LoRA, fits in 24GB with MPS or 64GB CPU)

**After training**:
- Rebuild `vectors.db` with fine-tuned model
- Re-tune `VECTOR_SIGNAL_WEIGHT`
- Benchmark

### Phase 4: Claude Haiku Reranking (Conditional)

Only if phases 1-3 don't reach MRR 0.60.

**When it fires**:
- Skip for docket lookups and exact matches (already MRR=1.0)
- Skip when top result score is 2x+ the second result (clear winner)
- Fire for NL, concept-match, and statute queries where top 5 results are close in score

**How it works**:
1. Take top 15 candidates (post cross-encoder)
2. Send to Haiku: decision_id + docket + regeste (first 300 chars) per candidate
3. Prompt: rank by legal relevance to query (doctrine match, statute applicability, factual similarity)
4. Haiku returns ordered list of decision_ids
5. Apply reranking with weight 3.0-5.0

**Cost**: ~$0.0002/query. At 1000 queries/day = $0.20/day.

**Latency**: ~1-2s added for triggered queries. Total ~7-8s. Acceptable for professional research tool.

## Evaluation Infrastructure

### Golden Set Expansion (53 → ~100 queries)

Add ~50 queries before any search changes to establish stable baseline:
- ~15 statute queries with citation-graph verified expectations
- ~10 Italian/French queries
- ~8 tenancy/tax/short queries
- ~5 cross-lingual queries (German query → French/Italian result)
- ~10 practitioner-style queries
- Each query: 3-5 expected decisions verified against citation graph

### Automated Citation-Pair Eval (~5K pairs)

- Hold out 5K citation pairs from decisions with regeste text
- Test: given regeste A as query, does cited decision B appear in top 10?
- Fast regression check (~5 min runtime)
- Not a substitute for golden set, but catches large regressions

### Process Per Change

1. Run golden set benchmark (~100 queries)
2. Run citation-pair eval (~5K pairs)
3. Compare against baseline
4. Deploy only if both improve or are neutral

## Implementation Sequence

```
Step 1: Expand golden set → establish baseline
Step 2: Build automated citation-pair eval
Step 3: Rebuild vectors.db on VPS (BGE-M3 off-the-shelf)
Step 4: Fix vector integration (reranking signal only)
Step 5: Tune vector signal weight → benchmark
Step 6: Swap cross-encoder to bge-reranker-v2-m3
Step 7: Tune cross-encoder weight + top_N → benchmark
    ── Decision gate: MRR ≥ 0.60? ──
    Yes → Done
    No  → Continue to step 8
Step 8: Fine-tune BGE-M3 with LoRA (Mac or VPS)
Step 9: Rebuild vectors.db with fine-tuned model → benchmark
    ── Decision gate: MRR ≥ 0.60? ──
    Yes → Done
    No  → Continue to step 10
Step 10: Add Haiku reranking layer
Step 11: Tune gating + weight → benchmark
```

## Timeline Estimates

- Steps 1-2: ~2-3 hours (eval infrastructure)
- Steps 3-5: ~4-5 hours (vector rebuild + integration fix + tuning)
- Steps 6-7: ~1-2 hours (cross-encoder swap + tuning)
- Steps 8-9: ~1 day if needed (training + rebuild)
- Steps 10-11: ~2-3 hours if needed (Haiku reranking)

## Files Involved

**Search pipeline**: `mcp_server.py` (lines 828-1340 search, 1829-1985 vectors, 2700-2870 reranking, 3480-3517 cross-encoder)

**Build scripts**: `search_stack/build_vectors.py`, `search_stack/build_reference_graph.py`

**Benchmark**: `benchmarks/run_search_benchmark.py`, `benchmarks/search_relevance_golden.json`

**Config**: `VECTOR_SIGNAL_WEIGHT`, `CROSS_ENCODER_ENABLED`, `CROSS_ENCODER_TOP_N`, `CROSS_ENCODER_WEIGHT` in `mcp_server.py`

**Databases**: `output/vectors.db` (to be rebuilt), `output/decisions.db`, `output/reference_graph.db`

## Constraints

- VPS: 16 dedicated CPU, 64GB RAM, 153GB free disk
- No GPU on VPS — all inference is CPU
- Search latency budget: current ~5-6s (LLM-dominated), target ≤8s
- Must degrade gracefully if vectors.db missing
- Lazy model loading (on first query, cached globally)
- Zero-downtime deployment (atomic DB swaps)

## Risks

1. **bge-reranker-v2-m3 too slow on CPU** — mitigate by reducing TOP_N or falling back to bge-reranker-base
2. **BGE-M3 vectors still don't help after integration fix** — proceed to fine-tuning (phase 3)
3. **Fine-tuning on noisy citation pairs** — mitigate by filtering prior-instance citations and using hard negatives
4. **Haiku reranking latency too high** — mitigate by aggressive gating (only ~30% of queries trigger it)
5. **Golden set too small for reliable eval** — mitigate by expanding to 100+ queries before starting
