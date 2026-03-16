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

**Fix — two modes to test**:

Mode A — Reranking signal only (safe, no regression risk):
- Vectors do NOT add new candidates to the pool
- For each FTS5-found candidate, compute cosine similarity to query vector
- Add similarity as a weighted signal in the final scoring formula
- This boosts semantically relevant candidates that FTS5 already found

Mode B — Gated candidate injection (higher ceiling):
- Vector search returns top 50 candidates
- Only inject candidates NOT already in FTS5 pool, capped at 10 new candidates
- Injected candidates get a lower base RRF weight (0.3) to avoid overwhelming FTS5 results
- This recovers semantic matches that keywords miss (the recall gap)

Test mode A first. If MRR improves, also test mode B to see if recall improves further. Deploy whichever scores higher.

**Steps**:
1. Rebuild `vectors.db` on VPS using existing `build_vectors.py` (BGE-M3, regeste text)
2. Implement mode A in `_search_fts5_inner()`: vector similarity as signal only
3. Tune `VECTOR_SIGNAL_WEIGHT` (sweep 1.0-5.0) against golden set
4. Benchmark mode A
5. Implement mode B: gated candidate injection with low base weight
6. Benchmark mode B, deploy the winner

**Infrastructure**: `build_vectors.py` already exists (836 lines). BGE-M3 model loads lazily. vectors.db stored on data volume alongside other DBs.

**Expected build time**: ~2-3 hours on VPS (980K decisions, batch_size=32).

**Latency impact**: ~50ms per query for encoding + KNN lookup. Negligible vs current 5s from LLM calls.

### Phase 2: Swap Cross-Encoder

**Problem**: Current cross-encoder (mmarco-mMiniLMv2, 33M params, 12 layers, 384 hidden) is too weak to distinguish legally relevant from tangentially-related results.

**Fix**: Try progressively larger cross-encoders, benchmark each:

| Model | Params | Est. latency (20 pairs, CPU) | Notes |
|-------|--------|------------------------------|-------|
| mmarco-mMiniLMv2 (current) | 33M | ~50ms | Baseline |
| bge-reranker-base | 278M | ~1-2s | Good quality/speed tradeoff |
| bge-reranker-v2-m3 | 568M | ~3-5s | Best quality, slowest |

Start with `bge-reranker-base` (278M). If quality gain is large, also test `bge-reranker-v2-m3` to see if the extra latency is worth it. The 568M model at 20 pairs on CPU will take **3-5 seconds** (not 200-400ms as originally estimated — each forward pass through 568M params takes ~200-500ms).

**Changes**:
- Model: configurable via `CROSS_ENCODER_MODEL` env var
- `CROSS_ENCODER_TOP_N`: reduce to 15-20 for larger models
- `CROSS_ENCODER_WEIGHT`: retune per model
- `CROSS_ENCODER_ENABLED`: default to `True`

**Memory**: ~1-2.2GB model weights depending on model. VPS has 64GB RAM.

**Cold start**: First query loads model (~10-30s). Consider preloading in worker startup (`on_startup` hook) to avoid first-query penalty.

**Fallback**: Revert to mmarco-mMiniLMv2 via env var if quality or latency regresses.

### Phase 3: Fine-Tune Embeddings (Conditional)

Only if phases 1-2 don't reach MRR 0.60.

**Base model**: BGE-M3 (BAAI/bge-m3), 568M params, 1024-dim.

**Training method**: LoRA (rank 16, alpha 32) applied to all attention Q/K/V projection layers. Trains ~5-10M params (~1-2% of total).

**Where**: Mac M4 Pro (24GB, MPS backend, ~2-3 hours) or VPS CPU overnight (~8-12 hours). Decision deferred until needed. Data transfer: scp training data from VPS (~200MB), scp model weights back (~50MB LoRA adapter).

**Training data**:

Dataset 1 — Citation pairs (~500K triples):
- Positive: if decision A cites decision B, pair (A.regeste, B.regeste)
- Hard negatives: BM25-similar decisions from same court/domain but not cited (pre-mined using FTS5)
- Filter: only decisions with regeste text ≥ 50 chars
- Exclude: prior-instance citations (`is_prior_instance` flag), known negative/distinguishing citations (heuristic: regeste contains "anders als", "im Unterschied zu", "à la différence de" near the citation)
- Split: 90% train, 5% val, 5% test

Dataset 2 — Regeste-to-document pairs (~200K triples):
- Positive: (decision.regeste, decision.full_text[:512])
- Hard negatives: BM25-similar decisions from different legal domains
- Filter: decisions with both regeste AND full_text ≥ 200 chars

**Training details**:
- Loss: MultipleNegativesRankingLoss (InfoNCE/contrastive) with pre-mined hard negatives appended to each batch
- Two phases: citation pairs first (legal relatedness), then regeste-to-doc (relevance matching)
- Optimizer: AdamW, lr=2e-5, warmup 10%, cosine decay
- Epochs: 3 per phase
- Batch size: 8 actual, gradient accumulation 4 steps = effective batch size 32 (gives 31 in-batch negatives + hard negatives per positive)
- LoRA targets: all attention Q/K/V projections across all transformer layers
- Precision: fp16 on MPS (Mac) or fp32 on CPU (VPS)

**After training**:
- Rebuild `vectors.db` with fine-tuned model on VPS
- Re-tune `VECTOR_SIGNAL_WEIGHT`
- Benchmark on golden set + citation-pair eval

### Phase 4: Claude Haiku Reranking (Conditional)

Only if phases 1-3 don't reach MRR 0.60.

**When it fires**:
- Skip for docket lookups and exact matches (already MRR=1.0)
- Skip when top result score is 2x+ the second result (clear winner)
- Fire for NL, concept-match, and statute queries where top 5 results are close in score

**How it works**:
1. Take top 15 candidates (post cross-encoder)
2. Send to Haiku via structured JSON output (tool_use or response_format=json):
   - Query text
   - Per candidate: decision_id, docket_number, regeste[:300]
3. Prompt instructs: rank by legal relevance considering doctrine match, applicable statute provisions, factual pattern similarity, court authority level
4. Haiku returns JSON array of decision_ids in ranked order
5. Apply reranking: position-based score with weight 3.0-5.0

**Structured output**: Use `response_format: {"type": "json_object"}` to ensure reliable parsing. Fallback: regex extraction if JSON parsing fails.

**Cost**: ~$0.0002/query. At 1000 queries/day = $0.20/day.

**Latency**: ~1-2s added for triggered queries. Total ~7-8s for affected queries. Acceptable for professional research tool.

## Evaluation Infrastructure

### Golden Set Expansion (53 → ~100 queries)

Add ~50 queries before any search changes to establish stable baseline.

**Semi-automated generation process**:
1. For each weak tag (statute, tenancy, tax, Italian, short), use citation graph to find top-cited decisions per legal domain
2. Generate candidate queries from decision regeste text (extract key terms)
3. Use Haiku to suggest natural search queries that should find each decision
4. Manually verify and curate: is this a realistic user query? Are expected decisions correct?
5. Each query gets 3-5 expected decisions verified against citation graph

**Target distribution**:
- ~15 statute queries with citation-graph verified expectations
- ~10 Italian/French queries
- ~8 tenancy/tax/short queries
- ~5 cross-lingual queries (German query → French/Italian result)
- ~10 practitioner-style queries

### Automated Citation-Pair Eval (~5K pairs)

- Hold out 5K citation pairs from decisions with regeste text (not used in fine-tuning if phase 3 proceeds)
- Test: given regeste A as query, does cited decision B appear in top 10?
- Fast regression check (~5 min runtime)

**Limitations**: This tests retrieval similarity, not user-facing search quality. A model that memorizes citation patterns scores perfectly but may not help real queries. Use only for regression detection, never as the primary quality signal. The golden set remains the source of truth for quality.

### Process Per Change

1. Run golden set benchmark (~100 queries)
2. Run citation-pair eval (~5K pairs)
3. Compare against baseline
4. Deploy only if golden set improves or is neutral AND citation eval doesn't regress significantly

## Implementation Sequence

```
Step 1: Expand golden set → establish baseline
Step 2: Build automated citation-pair eval
Step 3: Rebuild vectors.db on VPS (BGE-M3 off-the-shelf)
Step 4: Fix vector integration — test mode A (signal only) and mode B (gated injection)
Step 5: Tune vector weights → benchmark, deploy winner
Step 6: Try cross-encoders: bge-reranker-base, then bge-reranker-v2-m3 if warranted
Step 7: Tune cross-encoder weight + top_N → benchmark, deploy best
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

## Operational Details

### Model Storage
- Model weights: `/mnt/HC_Volume_104655575/models/<model-name>/` on VPS
- LoRA adapters (if fine-tuned): `/mnt/HC_Volume_104655575/models/bge-m3-lora-v1/`
- Keep previous version for rollback

### Cold Start Mitigation
- Preload embedding model and cross-encoder in uvicorn worker startup (`on_startup` hook)
- Estimated startup time: 30-60s with both models loaded
- Workers already restart sequentially (rolling restart), so no downtime

### Build Pipeline Integration
- vectors.db rebuild: only when model changes (not on nightly FTS5 rebuild)
- Add `--rebuild-vectors` flag to `publish.py` for manual trigger
- Atomic swap: build to `vectors.db.tmp`, then `os.replace()` (same pattern as FTS5 rebuild)

### Rollback
- Keep previous `vectors.db` as `vectors.db.prev` after each rebuild
- Cross-encoder model selectable via `CROSS_ENCODER_MODEL` env var — rollback = change env var + restart
- Vector search disable: set `VECTOR_SEARCH_ENABLED=false` in .env.mcp

## Timeline Estimates

- Steps 1-2: ~2-3 hours (eval infrastructure)
- Steps 3-5: ~4-5 hours (vector rebuild + integration fix + tuning)
- Steps 6-7: ~2-3 hours (cross-encoder swap + latency testing + tuning)
- Steps 8-9: ~1 day if needed (training + rebuild)
- Steps 10-11: ~2-3 hours if needed (Haiku reranking)

## Files Involved

**Search pipeline**: `mcp_server.py` (lines 828-1340 search, 1829-1985 vectors, 2700-2870 reranking, 3480-3517 cross-encoder)

**Build scripts**: `search_stack/build_vectors.py`, `search_stack/build_reference_graph.py`

**Benchmark**: `benchmarks/run_search_benchmark.py`, `benchmarks/search_relevance_golden.json`

**Config**: `VECTOR_SIGNAL_WEIGHT`, `CROSS_ENCODER_ENABLED`, `CROSS_ENCODER_MODEL`, `CROSS_ENCODER_TOP_N`, `CROSS_ENCODER_WEIGHT` in `mcp_server.py`

**Databases**: `output/vectors.db` (to be rebuilt), `output/decisions.db`, `output/reference_graph.db`

## Constraints

- VPS: 16 dedicated CPU, 64GB RAM, 153GB free disk
- No GPU on VPS — all inference is CPU
- Search latency budget: current ~5-6s (LLM-dominated), target ≤10s total
- Must degrade gracefully if vectors.db or model missing
- Lazy model loading (on first query, cached globally) with optional preload on startup
- Zero-downtime deployment (atomic DB swaps, rolling worker restart)
- 4 uvicorn workers — each loads its own copy of model weights

## Risks

1. **Cross-encoder latency** — 568M params on CPU could add 3-5s per query. Mitigate: start with 278M bge-reranker-base (~1-2s), only upgrade if quality justifies latency.
2. **Memory pressure from dual models** — BGE-M3 (2.2GB) + cross-encoder (1-2.2GB) × 4 workers = 12-17GB for models alone. Monitor: if workers swap, reduce to 2 workers or use smaller models.
3. **BGE-M3 vectors still don't help** — proceed to fine-tuning (phase 3). The off-the-shelf model may lack Swiss legal domain knowledge regardless of fusion strategy.
4. **Noisy citation training data** — mitigate by filtering prior-instance and distinguishing citations, using hard negatives, and validating on golden set (not just citation-pair eval).
5. **Haiku reranking latency** — mitigate by aggressive gating (only ~30% of queries trigger it, skip for docket/exact).
6. **Golden set too small** — expand to 100+ queries with semi-automated generation before starting any changes.
7. **Cold start** — first query after restart loads 2-4GB of models. Mitigate with preload on worker startup.
