---
title: "OpenCaseLaw: An Open Dataset and Search Platform for Swiss Court Decisions"
author: Jonas Hertner
date: March 2026
geometry: margin=1in
fontsize: 11pt
urlcolor: blue
linkcolor: blue
header-includes: |
  \usepackage{booktabs}
  \usepackage{microtype}
  \usepackage{hyperref}
  \setlength{\parskip}{0.4em}
---

## Abstract

We present OpenCaseLaw, an open corpus and search system for Swiss case law.
The March 20, 2026 snapshot contains 962,724 decisions from 102 federal, cantonal, and quasi-judicial sources, covering all 26 cantons and the period 1875--2026.
The corpus includes 448,461 German (46.6%), 434,663 French (45.1%), and 79,600 Italian (8.3%) decisions.
OpenCaseLaw releases a 34-field Parquet export and reproducible tooling to build a local search index and reference database.
The reference database contains 8.76 million extracted case-citation references (6.42 million resolved in-corpus, 73.3% resolution rate) and 11.23 million decision-to-statute links.
We describe the collection, deduplication, and retrieval pipeline, and release a 100-query multilingual benchmark with a release-matched baseline report.
The code is MIT-licensed; each record links back to the decision as published by its originating court.

## 1. Introduction

Swiss case law is published across a fragmented landscape of federal and cantonal court websites.
Each of the 26 cantons runs its own portal, and cross-court search does not exist in the open ecosystem.
Commercial systems such as Swisslex and Weblaw aggregate this data but require paid subscriptions.
Existing open resources cover either a single court, a narrow NLP task, or raw publication access without retrieval tooling.

OpenCaseLaw provides infrastructure rather than a single benchmark.
It combines corpus acquisition, normalization, searchable exports, reference extraction, and interfaces for both programmatic and LLM-mediated access.
The project contributes:

1. **A broad open corpus.** The March 20, 2026 snapshot contains 962,724 decisions from 102 sources---all 26 cantons, federal courts, and several regulatory bodies.
2. **Retrieval artifacts.** The release includes Parquet exports plus tooling for a local FTS5 search database, a citation and statute reference database, REST endpoints, and an MCP server.
3. **Evaluation infrastructure.** A 100-query tagged benchmark and a release-matched offline baseline.

The paper reports what is versioned and inspectable in the repository.
Where the implementation distinguishes between data models, search indexes, and export schemas, we state that explicitly.

## 2. Related Work

**Swiss legal datasets.**
Swiss-Judgment-Prediction (Niklaus et al., 2021) and the Swiss Federal Supreme Court Dataset (Geering and Merane, 2024) provide Federal Supreme Court resources for downstream analysis.
OpenCaseLaw differs in court coverage: it spans 102 sources across all cantons and multiple federal and regulatory bodies.

**Swiss legal benchmarks.**
SCALE (Rasiah et al., 2023) benchmarks citation extraction, court-view generation, and summarization on Swiss legal text.
It is complementary: SCALE focuses on task evaluation, OpenCaseLaw on corpus and retrieval infrastructure.

**Broader legal corpora.**
MultiLegalPile (Niklaus et al., 2023a) provides a 689 GB multilingual legal corpus for pretraining; LexGLUE (Chalkidis et al., 2022) benchmarks legal NLP tasks.
Neither provides Swiss court-wide retrieval infrastructure or citation databases.

**Open case law infrastructure.**
The Caselaw Access Project (Harvard Law School) provides 6.7 million US court decisions.
We are not aware of a comparable Swiss-wide open corpus with nationwide coverage, structured metadata, and citation databases.

**Legal information retrieval.**
BM25 and Reciprocal Rank Fusion remain robust retrieval baselines (Robertson and Zaragoza, 2009; Cormack et al., 2009).
Locke et al. (2024) survey legal text retrieval.
OpenCaseLaw uses BM25 and RRF as part of a practical multilingual search pipeline.

**Re-identification risk.**
Pilan et al. (2024) find that aggregation and structured metadata increase re-identification risk in court decisions even when party names are redacted.
This applies directly to corpora like ours.

## 3. Dataset and Processing Pipeline

### 3.1 Snapshot Statistics

Table 1 reports the frozen release snapshot (March 20, 2026).
All corpus-wide counts in this paper come from this snapshot.
Retrieval metrics in Section 5.3 come from the bundled benchmark artifact.

| Metric | Value |
|:-------|------:|
| Snapshot timestamp | 2026-03-20 |
| Decisions | 962,724 |
| Courts and public bodies | 102 |
| Federal sources | 20 |
| Cantonal sources | 82 |
| Federal decisions | 344,141 |
| Cantonal decisions | 618,583 |
| Earliest decision | 1875-01-01 |
| Latest decision | 2026-03-19 |
| German | 448,461 (46.6%) |
| French | 434,663 (45.1%) |
| Italian | 79,600 (8.3%) |

The four largest sources are the Federal Supreme Court (174,270), Geneva (167,003), the Federal Administrative Court (91,613), and Vaud (74,819).

### 3.2 Collection

54 scrapers run nightly, each targeting one court website or publication portal.
Each decision is normalized into a 28-field data model covering court identity, docket number, dates, language, legal area, regeste, full text, and source URLs.

### 3.3 Deduplication

Deduplication uses a canonical key that normalizes court code, docket number, and date to collapse formatting variants.
Within-court deduplication keeps the version with the richest content.
Cross-court deduplication operates within hand-maintained overlap groups for cantons where decisions appear on multiple portals (e.g., Zürich: 17 court codes).

A BGE leading case and its underlying Federal Supreme Court ruling are *not* deduplicated---they have different courts, different docket numbers, and different content scope.
Similarly, a cantonal decision and its federal appeal remain as distinct records.

This is an engineering heuristic, not a curated legal identity model.
The repository does not ship a manual audit of false merges or missed duplicates.

### 3.4 Schemas

The repository uses three related schemas:

1. **Core model** (`models.py`): 28 fields, used by scrapers.
2. **Search database** (`db_schema.py`): 24-column SQLite table with FTS5 index.
3. **Parquet export** (`export_parquet.py`): 34-field Arrow schema with provenance fields.

The project does not have a single monolithic schema; it has a layered data contract for scraping, retrieval, and export.

## 4. Reference Database

A second SQLite database stores case citations and statute references extracted from decision text.

### 4.1 Extraction

Regular expressions extract BGE references (`BGE 131 III 115`), docket numbers (`4A_372/2019`), and statute provisions (`Art. 41 OR`).
Each reference is resolved against the corpus using normalized docket matching with confidence scoring.

### 4.2 Scale

Table 2 summarizes the reference database from the March 20, 2026 build.

| Metric | Value |
|:-------|------:|
| Case-citation references | 8.76 million |
| Resolved in-corpus links | 6.42 million |
| Resolution rate | 73.3% |
| Decision-to-statute links | 11.23 million |

These are distinct quantities: 8.76 million refers to case-citation references; 11.23 million refers to statute links.
They should not be added.
The repository does not ship a manual precision/recall study; Table 2 reports artifact scale, not validated extraction quality.

### 4.3 Uses

The reference database supports citation lookup, leading-case discovery, appeal-chain tracing, trend analysis, and statute-aware search enrichment.

## 5. Retrieval and Interfaces

### 5.1 Search Pipeline

The search pipeline has five stages:

1. **Query parsing.** Multiple lexical query variants, legal synonym expansion, umlaut normalization, and optional LLM-based structured parsing.
2. **Candidate retrieval.** Several FTS5 strategies fused with Reciprocal Rank Fusion.
3. **Signal scoring.** Lexical features, metadata, citation counts, and statute signals.
4. **Optional reranking.** Confidence-gated LLM reranking for ambiguous cases.
5. **Result enrichment.** Court metadata, citation counts, and statute references.

### 5.2 Distribution

The corpus is available as Parquet files, a local SQLite FTS5 index, a REST API, and an MCP server supporting Claude, ChatGPT, and Gemini.
The MCP tool surface is deployment-dependent (up to 21 tools; remote mode omits update tools).

### 5.3 Evaluation

The repository includes a 100-query benchmark spanning 74 German, 16 French, 7 Italian, and 3 untagged queries across 15 legal domains.
Each query has 1--6 graded relevant decisions (mean 3.04).
The judgments were created by the author; no multi-annotator agreement is reported.

The release bundle includes a benchmark run matched to the 962,724-decision snapshot:

| Metric | Value |
|:-------|------:|
| MRR\@10 | 0.6042 |
| Recall\@10 | 0.5835 |
| nDCG\@10 | 0.6062 |
| Hit\@1 | 0.52 |

This result uses the local search database with the reference graph but without vector search, statute databases, or LLM-based expansion and reranking.
It is dominated by German queries; concept-match and statute-oriented retrieval remain the hardest slices.

## 6. Ethics, Legal Basis, and Limitations

### 6.1 Legal Basis

Published Swiss court decisions are excluded from copyright under Art. 5 para. 1 lit. c URG.
Federal publication duties are governed by Art. 27 BGG; cantonal duties vary.
Code is MIT-licensed; dataset packaging is CC0-1.0.

OpenCaseLaw preserves decisions as published by the originating courts and links back to source URLs.
It does not perform anonymization.
Cantonal anonymization practices vary.

Large-scale aggregation changes the privacy risk profile.
Structured metadata combined with full text may enable re-identification even when names are redacted (Pilan et al., 2024).
The repository includes a governance and removal policy.

### 6.2 Limitations

- **Coverage is broad, not audited.** Publication depth varies by court and era.
- **Historical quality varies.** Older material may contain OCR artifacts.
- **Reference extraction is rule-based** and misses non-standard references.
- **Identity is operational, not jurisprudential.** Identifiers are engineering heuristics, not curated case identities.
- **No manual evaluation.** Neither citation extraction nor deduplication is evaluated against human annotations in this paper.
- **The benchmark is author-judged.** No inter-annotator agreement or held-out test split is provided.

## 7. Availability

All release artifacts are in the GitHub release tagged `opencaselaw-paper-2026-03-20`.

| Resource | Location |
|:---------|:---------|
| Dataset (Parquet) | huggingface.co/datasets/voilaj/swiss-caselaw |
| Source code | github.com/jonashertner/caselaw-repo-1 |
| Release bundle | GitHub release `opencaselaw-paper-2026-03-20` |
| Governance policy | `docs/governance-and-removal-policy.md` |
| MCP server | mcp.opencaselaw.ch |
| REST API docs | mcp.opencaselaw.ch/api/docs |
| Dashboard | opencaselaw.ch |

## References

- Caselaw Access Project. Harvard Law School. https://case.law
- Chalkidis, I. et al. (2022). LexGLUE: A Benchmark Dataset for Legal Language Understanding in English. *ACL 2022*.
- Cormack, G., Clarke, C., and Buettcher, S. (2009). Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods. *SIGIR 2009*.
- Geering, F. and Merane, J. (2024). Swiss Federal Supreme Court Dataset. Zenodo. doi:10.5281/zenodo.11092977.
- Kano, Y. et al. (2024). COLIEE 2024. *JSAI 2024*.
- Locke, S., Zhai, Z., and Kohlmeier, J. (2024). A Survey on Legal Text Retrieval. *ACL 2024*.
- Model Context Protocol. Anthropic, 2024. https://modelcontextprotocol.io
- Niklaus, J. et al. (2021). Swiss-Judgment-Prediction. *NLP4PositiveImpact, EMNLP 2021*.
- Niklaus, J. et al. (2023a). MultiLegalPile: A 689 GB Multilingual Legal Corpus. *arXiv:2306.02069*.
- Pilan, I. et al. (2024). Anonymity at Risk? Re-Identification in Court Decisions. *Findings of NAACL 2024*.
- Rasiah, V. et al. (2023). SCALE: Scaling up Evaluation of Swiss Court Rulings. *arXiv:2306.09237*.
- Robertson, S. and Zaragoza, H. (2009). The Probabilistic Relevance Framework: BM25 and Beyond. *FTIR*.
