# Canonical numbers — single source of truth

This document fixes the canonical values for every headline statistic that
appears in any externally-visible OpenCaseLaw surface (paper, dashboards,
README, HuggingFace dataset card, methodology page). When you update any
of these, update the source of truth listed under "Where the value lives"
and every consumer should follow.

When values appear in code, prefer to read from the source-of-truth file
at runtime rather than hard-coding. When that's not possible (HTML, .tex),
update all listed consumers in the same commit.

## Snapshot vs. live distinction

| Concept | Use when | Examples |
|---|---|---|
| **Paper snapshot** | Anything in the v3 paper or `docs/paper/v3/tables/*.json` | "971,992 decisions, snapshot 2026-05-13" |
| **Live** | Dashboards / README / dataset card | "972,000+ decisions" (rounded; grows daily) |

## The numbers

### Corpus scale

| Quantity | Snapshot value (paper) | Live value | Source of truth | Consumers |
|---|---:|---|---|---|
| Decisions | **971,992** | **~972k** | `docs/paper/v3/tables/corpus_graph_stats.json::total_decisions` | paper §3 abstract, dataset_card.md, README.md, methodology.html, index.html |
| Courts | 108 | 108 | same JSON | all |
| Cantons | 26 | 26 | constant | all |
| Languages | DE 449,575 (46.3 %), FR 441,158 (45.4 %), IT 80,704 (8.3 %) | same | corpus_overview.tex | paper §3, methodology.html |
| Date range | 1875–2026 | 1875–2026 | constant | all |
| Snapshot date (paper) | **2026-05-13** | — | `corpus_graph_stats.json::snapshot_date` | paper, methodology.html date references |

### Citation graph

| Quantity | Value | Source | Consumers |
|---|---:|---|---|
| Citation edges (raw) | 8,649,879 | `corpus_graph_stats.json::rg_citation_edges` | paper §4, dataset_card |
| Resolved edges | 8,089,112 (93.5 %) | same | all |
| Cited ≥ 100 times | 10,874 | `in_degree_buckets` | paper Table 2 |
| Cited ≥ 1,000 times | 999 | same | paper |
| Cited ≥ 10,000 times | **47** | same | paper §4, citation_graph.tex |
| Top-cited | BGE 125 V 351, 85,108 incoming | `top30_cited[0]` | paper, methodology |
| Cross-lingual share | 34.0 % | computed from cross_lang_matrix | paper §4 |

### Statute graph

| Quantity | Value | Source | Consumers |
|---|---:|---|---|
| Decision–statute edges | 11,261,717 | `corpus_graph_stats.json::rg_statute_edges` | all |
| Distinct provisions | 283,119 | `rg_distinct_statutes` | all |
| Federal SR laws | 5,516 | constant (Fedlex) | all |
| Federal articles | 400,405 (across DE/FR/IT) | constant | all |
| Cantonal laws | 15,722 | `cantonal_laws.db` | all |
| Cantonal articles | 353,437 | same | all |
| Direct portal coverage | **all 26 cantons** (LexWork 18 + SIL 2 + ZH 1 + TI 1) | `cantonal_laws.db.laws.text_source` | paper §3, README, methodology |
| LexFind PDF supplements | **4 cantons** | same | same |
| Top provision | BGG/LTF Art. 100, 205,725 decisions | `top_statutes_canonical.json` | paper §4 |

### Materialien

| Quantity | Value | Source | Consumers |
|---|---:|---|---|
| Botschaft documents | 5,292 | `corpus_graph_stats.json` | paper §4, dataset_card |
| Paragraphs | 381,711 | same | paper |
| Article-anchored links | 8,124 | same | paper |

### Commentaries

| Quantity | Value | Source |
|---|---:|---|
| Total commentaries | 1,058 (OnlineKommentar 362 + OpenLegalCommentary 696) | `ok_commentaries.db` |
| Paper citation | 362 (OnlineKommentar only) | paper §4 simplifies to the CC-BY count |

### MCP tool counts

**These are the most-drifted numbers historically. Pick the right one for context.**

| Mode | Tools | Where used |
|---|---:|---|
| **Public / remote mode** (deployed at `mcp.opencaselaw.ch`) | **38** | public-facing dashboards, live API |
| **Local mode** (running on operator's machine with `update_database` + `check_update_status`) | **40** | README internal docs, local dev guides |
| **Local-only delta** | 2 (`update_database`, `check_update_status`) | technical references |

Standard phrasing: *"40 MCP tools (38 remote in public mode + 2 local-only)"* or simply *"38 tools"* when only the public surface matters.

**Paper note**: paper v3 (snapshot 2026-05-13) cites 33 tools; that figure is frozen for the snapshot. Public surface has since grown by 5 scholarship tools (`search_scholarship`, `get_scholarship`, `find_scholarship_citing_statute`, `list_scholarship_sources`, `get_scholarship_full_text`) — bringing the live count to 38.

### Search-quality numbers

**Distinguish two different benchmarks** — these are easy to confuse.

| Benchmark | Snapshot date | MRR@10 | Hit@10 | n queries | Where reported |
|---|---|---:|---:|---:|---|
| Online golden-set (with Haiku rerank) | 2026-03-19 | 0.647 | 0.570 (Hit@1) | 100 | methodology.html |
| Offline golden-set (no LLM) | 2026-03-19 | 0.470 | 0.330 (Hit@1) | 100 | baseline_by_language.tex |
| **Cross-lingual diagnostic (regeste-derived)** | 2026-05-13 | **0.630** | **0.833** | 150 (50 × 3 langs) | paper §1 §7, abstract |

The paper's headline is the cross-lingual 0.630/0.833 number. The methodology page's 0.647 is the older online golden-set bench (different design).

### Audit / hallucination bench (v1.1)

| Condition | n | Correctness | Groundedness | Retrieval acc. | correct/hallucination/retrieval/reasoning |
|---|---:|---:|---:|---:|---|
| Prior-only | 30 | 86.7 % | — | — | 26 / 4 / — / — |
| Retrieval-augmented | 30 | 63.3 % | 93.3 % | 53.3 % | 17 / 2 / 9 / 2 |

Source: `benchmarks/swiss_legal_rag_bench/results/v1_1_*.json`. Reported in paper §8 and the new `tables/audit_bench_results.tex`.

### Precision proxies (citation graph)

| Stratum | n | Date-sanity pass | Self-cite | Confidence p50 |
|---|---:|---:|---:|---:|
| docket_norm | 3,286,234 | 97.25 % | 0 | 0.99 |
| bge_bare | 2,943,028 | 99.75 % | 0 | 0.85 |
| bge_norm | 1,215,768 | 99.26 % | 0 | 0.75 |
| bge_pincite | 652,508 | 97.35 % | 0 | 0.75 |
| **Overall** | **8,097,538** | **98.47 %** | 0 | — |

Source: `benchmarks/citation_precision_proxies.json`. Reported in paper §4 and `tables/precision_proxies.tex`.

### Literature comparators (citing other people's numbers — be exact)

| Paper | Range we cite | Verified against |
|---|---|---|
| Dahl et al. 2024 | **58–88 %** (ChatGPT-4 floor to Llama-2 ceiling) | arXiv:2401.01301 abstract |
| Magesh et al. 2024/2025 | **17–33 %** (Lexis+ AI, Westlaw AI, Ask Practical Law) | JELS 22 (2025) abstract |

Common error to avoid: "58–82 %" for Dahl is WRONG. Was in v1.0 paper and dataset_card; fixed 2026-05-18.

### LLM cost (30-day, indicative)

| Period | Total | Notes |
|---|---:|---|
| Last 30 days | ~$87 | Haiku 4.5: ~$76 (search_rerank + query_parse + query_expansion); Sonnet 4.6: ~$11 (audit + reflect) |
| Daily typical | $4–8 | Bursts to $30+ on high-traffic days |

Source: `logs/llm_usage.jsonl`. Don't quote these numbers in the paper — they're operational, not scientific.

### Active cohorts (privacy-preserving, weekly)

| Channel | Weekly cohorts (max-daily HLL, last full week) | Notes |
|---|---:|---|
| browser_chrome | ~14,000 | dominated by SEO traffic |
| chatgpt | ~500 | |
| python_script | ~80 | |
| claude_other (CLI + desktop) | ~75 | |
| claude_desktop (explicit) | ~20 | |
| cursor | ~5 | |
| word_addin | ~5 | matches ~10 Pro licenses |

Source: `analytics.db::weekly_reach` (after 2026-05-18 cohort-derivation upgrade). Updated weekly.

## Process: how to update a canonical number

1. Update the source of truth (the JSON file or DB).
2. Re-run `make paper-tables` so `tables/*.tex` regenerate.
3. Grep this doc + every consumer for the old number; update them.
4. Bump the "Last verified" line below.

**Last verified:** 2026-05-18 (this commit).
