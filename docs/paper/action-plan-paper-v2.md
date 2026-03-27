# OpenCaseLaw Paper v2 — Action Plan

**Target venue:** SIGIR 2027 or ECIR 2027 (submission deadline ~Feb 2027)
**Paper type:** Resource + Systems paper (corpus + retrieval system + evaluation)

## Title (working)

"LLM-Augmented Multilingual Legal Retrieval: A Production Study on 963K Swiss Court Decisions"

## Core contribution

First production evaluation of LLM-augmented retrieval (query parsing + reranking) for multilingual legal case law, with citation-graph authority signals and cross-lingual retrieval analysis.

---

## Phase 1: Data Collection (now → Apr 27)
*Fully automated, no manual work*

- [x] Deploy search traces (JSONL per query, no PII)
- [x] Deploy metrics persistence (10-min flush to daily_metrics.jsonl)
- [x] Weekly ablation benchmark (5 configs, Sunday 09:30 UTC)
- [x] Weekly research summary (aggregated stats)
- [ ] Collect 4 weeks of production traces (~1000+ real queries)
- [ ] 4 weekly ablation snapshots showing stability

**What accumulates automatically:**
- `research_logs/search_traces_*.jsonl` — per-query pipeline data
- `research_logs/daily_metrics.jsonl` — tool usage, clients, followup rate
- `benchmarks/ablation_history.jsonl` — MRR/NDCG under 5 configs weekly
- `benchmarks/research_summary_*.json` — aggregated stats weekly

## Phase 2: Analysis (Apr 28 — May 4)
*One session, ~2 hours*

- [ ] Run `scripts/research_summary.py` on full trace corpus
- [ ] Generate paper tables from ablation_history.jsonl:
  - Table 1: Component ablation (MRR, Hit@1, Hit@5, NDCG@10)
  - Table 2: Cross-lingual retrieval effectiveness
  - Table 3: Latency breakdown by component
- [ ] Generate figures:
  - Fig 1: Component contribution waterfall (baseline → +parse → +rerank → +crossling → +citation)
  - Fig 2: Query language vs result language distribution
  - Fig 3: Latency P50/P95 by component
- [ ] Compute production stats:
  - Followup rate (proxy for user satisfaction)
  - Zero-result rate (search gap analysis)
  - Client distribution (Claude vs ChatGPT vs Gemini adoption)

## Phase 3: Writing (May 5 — May 18)
*~1 week focused writing*

### Paper structure (8 pages + references)

1. **Introduction** (0.75p)
   - Swiss legal landscape: 26 cantons, 3 languages, fragmented access
   - Gap: no open multilingual retrieval system with authority ranking
   - Contribution: production system + evaluation

2. **Related Work** (0.75p)
   - Legal IR: CLERC, LeCaRDv2, COLIEE
   - Swiss legal NLP: Swiss-Judgment-Prediction, SCALE
   - LLM-augmented retrieval: query expansion, reranking
   - Open legal data: Caselaw Access Project (US), Open Legal Data (DE)

3. **System Architecture** (1.5p)
   - Corpus: 963K decisions, 102 courts, 1875–2026
   - Citation graph: 8.7M edges, authority ranking
   - Search pipeline: FTS5 → multi-strategy → RRF → LLM rerank
   - LLM query understanding: structured parse → doctrine + statutes + cross-lingual synonyms
   - Cross-lingual: DE query → FR/IT doctrine injection + result interleaving

4. **Evaluation** (2.5p)
   - 4.1 Offline: 100-query golden set, citation-graph verified
     - Ablation table (Table 1): full, -parse, -rerank, -crossling, -citation
     - Baseline comparison: BM25-only MRR vs full pipeline MRR
   - 4.2 Online: 4 weeks production data
     - Followup rate, zero-result rate, latency distribution
     - Client adoption (Table 3)
   - 4.3 Cross-lingual retrieval (novel)
     - Rate of cross-lingual results for monolingual queries
     - Position distribution of first cross-lingual result
     - Qualitative examples
   - 4.4 Component analysis
     - Haiku rerank: fire rate, change rate, cost per query
     - Citation boost: correlation with human relevance

5. **Discussion** (1p)
   - When LLM parsing helps vs hurts (concept vs docket queries)
   - Cross-lingual retrieval: promising but incomplete
   - Authority ≠ relevance: citation count bias toward procedural decisions
   - Cost: Haiku API spend vs quality gain tradeoff
   - Limitations: no user study, golden set size, followup ≠ relevance

6. **Conclusion** (0.5p)

### Appendix
- A: Full court list with decision counts
- B: Golden set query distribution by legal domain
- C: Example search traces showing pipeline stages

## Phase 4: Review + Submit (May 19 — May 25)

- [ ] Internal review (re-read with fresh eyes)
- [ ] External review (ask 1-2 legal informatics colleagues)
- [ ] Camera-ready formatting (LaTeX, ACL/SIGIR template)
- [ ] Submit to arXiv (immediate) + venue (deadline-dependent)

---

## Key numbers to report

| Metric | Source | Status |
|--------|--------|--------|
| Corpus size (963K, 102 courts) | stats.json | Have |
| Citation graph (8.7M edges) | reference_graph.db | Have |
| MRR baseline (BM25-only) | frozen benchmark | Have: 0.320 |
| MRR full pipeline | ablation benchmark | Have: 0.647 |
| MRR per component | ablation benchmark | Collecting weekly |
| NDCG@10 per component | ablation benchmark | Collecting weekly |
| Cross-lingual retrieval rate | search traces | Collecting |
| Followup rate | daily_metrics.jsonl | Collecting |
| Haiku rerank change rate | daily_metrics.jsonl | Collecting |
| Latency P50/P95 | search traces | Collecting |
| Client distribution | daily_metrics.jsonl | Collecting |
| Zero-result rate | daily_metrics.jsonl | Collecting |

## What makes this paper strong

1. **Production system, not toy benchmark** — real users, real queries
2. **Multilingual** — DE/FR/IT, unique to Swiss jurisdiction
3. **Citation graph** — 8.7M edges for authority ranking, novel for legal IR
4. **LLM-in-the-loop** — structured parse + reranking, with cost analysis
5. **Open data** — corpus on HuggingFace, code on GitHub, reproducible
6. **Rigorous ablation** — each component measured independently
