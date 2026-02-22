# Semantic Search — Design Document

**Date:** 2026-02-22
**Status:** Approved

## Overview

Add semantic (vector) search to opencaselaw.ch alongside the existing FTS5 lexical search. This solves two gaps: concept matching (searching "Hundebiss" finds "Tierhalterhaftung" cases) and cross-lingual retrieval (German query finds relevant French/Italian decisions).

## Architecture

**Dual retrieval with RRF fusion.** At query time, two paths run in parallel:

```
Query → ┬── FTS5 (existing, ~50ms) ──→ top-K candidates ──┐
        └── Vec search (~200-400ms) ──→ top-K candidates ──┤
                                                           ├── RRF merge → rerank → results
```

FTS5 provides precision (exact terms, docket matches, statute refs). Vector search provides recall (semantic similarity, cross-lingual concepts). RRF merges both candidate sets. The existing 15 reranking signals apply to all candidates, plus a new cosine similarity signal.

Decisions found by vector search but NOT by FTS5 enter the candidate pool — this is the key win over reranking-only approaches.

**Graceful degradation:** If `vectors.db` doesn't exist or the model fails to load, search works exactly as today. No breaking change.

## Components

### 1. Embedding Model: BGE-M3

BAAI/bge-m3 — 568M params, 1024-dim dense embeddings.

- Best multilingual model available (100+ languages, covers DE/FR/IT/RM)
- No query/passage prefix needed — same encoding for both
- Max 8192 tokens, we use max_length=512 (sufficient for regeste/summaries)
- Apache 2.0 license

**Inference:** ONNX int8 quantized for CPU. ~100-200ms per query on 16-core VPS. ~15-45 docs/sec for batch embedding.

**Text selection per decision:**
1. `regeste` if exists and len > 20
2. Else first 2000 chars of `full_text`

### 2. Vector Storage: sqlite-vec

sqlite-vec extension for SQLite — brute-force vector search with metadata filtering and partition keys.

**Table schema:**
```sql
CREATE VIRTUAL TABLE vec_decisions USING vec0(
    decision_id TEXT PRIMARY KEY,
    embedding float[1024] distance_metric=cosine,
    language TEXT partition key
);
```

**Partition strategy:** By `language` (4 partitions: de ~600k, fr ~250k, it ~150k, rm ~1k). Same-language queries hit one partition (~50-100ms). Cross-lingual queries scan all partitions (~200-400ms). No court partition — semantic search must find cases across courts.

**Storage:** ~5-6 GB for 1M decisions (4GB vectors + metadata overhead).

**KNN query:**
```sql
SELECT decision_id, distance
FROM vec_decisions
WHERE embedding MATCH :query_vector AND k = 50
ORDER BY distance;
```

### 3. Embedding Pipeline (`search_stack/build_vectors.py`)

Offline batch job, same pattern as `build_fts5.py` and `build_reference_graph.py`.

**Process:**
1. Load BGE-M3 model (ONNX int8, ~2GB)
2. Stream JSONL files, extract text (regeste or full_text[:2000])
3. Batch encode (batch_size=32, max_length=512)
4. Insert into sqlite-vec with `serialize_float32()`
5. Atomic build: `vectors.db.tmp` → `os.replace()` → `vectors.db`

**Runtime:** ~8-12 hours on VPS for 1M decisions (one-time). Daily incremental (~50-200 new decisions) takes seconds.

**ONNX export:** First run exports int8 quantized model to `~/.swiss-caselaw/bge-m3-onnx-int8/`. Cached for reuse by both builder and MCP workers.

### 4. Query-Time Integration (changes to `mcp_server.py`)

**Model loading:** Lazy singleton — loads BGE-M3 ONNX on first search call. ~2-3GB RAM per worker. 4 workers × 3GB = 12GB, well within 64GB.

**New function `_search_vectors(query, language, k)`:**
1. Embed query → 1024-dim float32 (~100-200ms)
2. KNN against `vectors.db` with optional language partition
3. Return `(decision_id, cosine_distance)` pairs

**Fusion:** Vector results get RRF scores added to `fusion_scores` dict alongside existing FTS5 strategies. Cosine similarity becomes reranking signal #16 (initial weight ~2.0-4.0, tuned via benchmark).

**New env vars:**
- `SWISS_CASELAW_VECTORS_DB` — path to vectors.db
- `SWISS_CASELAW_VECTOR_SEARCH` — enable/disable (default: 1 if vectors.db exists)
- `SWISS_CASELAW_VECTOR_WEIGHT` — RRF weight (default: 1.0)
- `SWISS_CASELAW_VECTOR_K` — vector pool size (default: 50)

### 5. Dependencies

Added to `pyproject.toml[semantic]`:
```
sentence-transformers>=3.0
sqlite-vec>=0.1.6
onnxruntime>=1.17
```

### 6. Tests

**Unit tests (`test_build_vectors.py`):**
- Text selection logic (regeste > full_text fallback)
- Vector DB round-trip (insert + KNN)
- Cosine distance sanity (similar texts closer than dissimilar)

**Integration test:**
- Build small vectors.db from fixtures
- Verify "Hundebiss" finds "Tierhalterhaftung" semantically

**Benchmark extension:**
- Add 3-5 semantic queries to golden set where FTS5 fails
- Compare MRR@10, Recall@10, nDCG@10 before/after
- Weight tuning: vary vector weight from 0.5 to 4.0

## Deployment

1. Install deps on VPS
2. Run `build_vectors.py` overnight → `output/vectors.db`
3. Restart MCP workers → auto-detect vectors.db, enable vector search
4. Add to daily cron after `build_fts5.py`

## Disk & Memory Budget

| Component | Size |
|---|---|
| BGE-M3 model cache | ~2 GB |
| ONNX int8 export | ~0.6 GB |
| vectors.db | ~5-6 GB |
| RAM per MCP worker (model loaded) | +2-3 GB |
| **Total new disk** | **~8 GB** |
| **Total new RAM (4 workers)** | **~12 GB** |

## What This Does NOT Include

- GPU inference — CPU with ONNX int8 is sufficient
- Sparse vectors or ColBERT from BGE-M3 — dense only for now
- Vector search as a standalone MCP tool — integrated into `search_decisions`
- Changes to `publish.py` — vector build added to cron directly
- User-facing "semantic mode" toggle — always-on when vectors.db exists
