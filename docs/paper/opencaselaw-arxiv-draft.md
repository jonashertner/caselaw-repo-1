# OpenCaseLaw: An Open Dataset and Search Platform for Swiss Court Decisions

**Jonas Hertner**

March 2026

---

## Abstract

We present OpenCaseLaw, an open corpus and retrieval stack for Swiss case law. In the repository snapshot generated on March 18, 2026, the dataset contains 962,272 decisions from 101 federal, cantonal, and regulatory courts or public bodies, covering all 26 cantons and the period 1875-2026. The current snapshot contains 448,215 German decisions (46.6%), 434,470 French decisions (45.2%), and 79,587 Italian decisions (8.3%); the export schema also reserves a Romansh language code. OpenCaseLaw releases a 34-field Parquet export, a local SQLite FTS5 index, a citation/reference database with 8.77 million extracted case-citation references, 6.46 million resolved in-corpus decision links, and 11.3 million decision-statute links, plus REST and Model Context Protocol (MCP) interfaces for retrieval from conventional clients and LLM tools. We describe the collection, normalization, deduplication, export, and retrieval pipeline, and we release a multilingual benchmark harness with 100 tagged evaluation queries. The code is MIT-licensed and all records link back to the decisions as published by the originating courts.

## 1. Introduction

Swiss case law is published across a fragmented landscape of federal and cantonal court websites, publication portals, and administrative repositories. The result is a difficult retrieval environment: coverage varies by court, interfaces are heterogeneous, and cross-court search is poor. Commercial systems such as Swisslex and Weblaw partially solve this problem, but they are closed, subscription-based products. Open Swiss resources exist, but they typically emphasize either a subset of courts, a narrow NLP task, or raw publication access without a reusable retrieval stack.

OpenCaseLaw is intended as infrastructure rather than a single benchmark dataset. It combines corpus acquisition, normalization, searchable exports, reference extraction, and interfaces for both programmatic and LLM-mediated access. The project makes three main contributions:

1. **A broad open Swiss case-law corpus.** The March 18, 2026 snapshot contains 962,272 decisions from 101 courts or public bodies, including all 26 cantons, federal courts, and several regulatory or quasi-judicial bodies.
2. **Reusable retrieval artifacts.** The release includes Parquet exports, a local SQLite FTS5 database, a citation/reference database, REST endpoints, and an MCP server.
3. **Evaluation infrastructure.** The repository includes a multilingual search benchmark harness and a 100-query tagged gold set designed for retrieval regression testing and system comparison.

The paper focuses on what is versioned and inspectable in the repository. Where the implementation distinguishes between core data models, search indexes, and export schemas, we state that explicitly instead of collapsing them into a single "dataset" abstraction.

## 2. Related Work

**Swiss legal datasets.** Swiss-Judgment-Prediction (Niklaus et al., 2021) and later work by Rasiah et al. (2023) provide labeled Federal Supreme Court datasets for downstream NLP tasks. Their focus is task supervision and explanation rather than nationwide corpus coverage or operational retrieval.

**Broader legal corpora.** MultiLegalPile (Niklaus et al., 2023) is a large multilingual legal corpus for language-model pretraining, and LexGLUE (Chalkidis et al., 2022) is a benchmark suite for legal NLP tasks. These resources are valuable for representation learning and evaluation, but they do not provide Swiss court-wide retrieval infrastructure, court-level normalization, or citation/statute reference databases for Swiss jurisprudence.

**Open and commercial Swiss retrieval systems.** Swisslex and Weblaw provide mature commercial access to Swiss case law, annotations, and editorial tooling. Open resources such as entscheidsuche.ch are important for public access, but they do not expose the same combination of downloadable structured exports, local full-text search, citation/reference databases, and LLM-facing tool interfaces.

**Legal information retrieval.** BM25 and Reciprocal Rank Fusion remain robust retrieval baselines (Robertson and Zaragoza, 2009; Cormack et al., 2009). OpenCaseLaw uses them as part of a practical search pipeline optimized for multilingual Swiss legal text rather than as the sole research contribution.

## 3. Dataset and Processing Pipeline

### 3.1 Snapshot Statistics

Table 1 reports the repository snapshot reflected in `docs/stats.json`, generated on March 18, 2026.

| Metric | Value |
|--------|-------|
| Snapshot timestamp | 2026-03-18T05:21:09Z |
| Decisions | 962,272 |
| Courts / public bodies | 101 |
| Federal sources | 20 |
| Cantonal sources | 81 |
| Federal decisions | 344,031 |
| Cantonal decisions | 618,241 |
| Earliest decision date | 1875-01-01 |
| Latest decision date | 2026-03-17 |
| German | 448,215 (46.58%) |
| French | 434,470 (45.15%) |
| Italian | 79,587 (8.27%) |

The largest single sources in the current snapshot are the Federal Supreme Court (`bger`, 174,213 decisions), Geneva (`ge_gerichte`, 166,912), the Federal Administrative Court (`bvger`, 91,560), and Vaud across three publication pipelines (`vd_findinfo`, `vd_gerichte`, `vd_omni`, together 155,399).

### 3.2 Collection

The canonical scraper registry in `run_scraper.py` currently contains 54 scraper or ingest jobs. These jobs target official court websites, cantonal publication portals, and auxiliary public repositories. The codebase includes direct scrapers for many courts as well as ingestion paths for bulk sources such as entscheidsuche.ch and Fedlex-derived statute material.

Each decision is normalized into a shared `Decision` model (`models.py`). The model captures 28 core fields, including court identity, docket information, dates, language, title, legal area, regeste, full text, selected metadata, and source URLs.

### 3.3 Normalization and Deduplication

OpenCaseLaw uses deterministic identifiers and a more aggressive canonical key for deduplication:

- `decision_id` is typically `{court}_{normalized_docket}`.
- `canonical_key` normalizes court, docket, and date more aggressively to collapse formatting variants.
- `build_fts5.py` applies within-court deduplication first, keeping the version with the richest content, then performs explicit cross-court deduplication within hand-maintained overlap groups such as Zurich, Vaud, Basel-Stadt, Bern, and Aargau.

This is an engineering compromise rather than a perfect legal identity model. It is strong enough for operational search and export, but it should not be confused with a fully curated jurisprudential ontology of proceedings, appeals, and republications.

### 3.4 Schemas and Access Artifacts

The repository intentionally uses three related but distinct schemas:

1. **Core model.** `models.py` defines a 28-field `Decision` object used by scrapers.
2. **Search database.** `db_schema.py` defines a 24-column SQLite table optimized for local search plus a JSON blob for full record preservation.
3. **Parquet export.** `export_parquet.py` defines a 34-field Arrow schema, adding export-oriented provenance and computed fields such as `has_full_text` and `text_length`.

For a paper, this distinction matters. The project does not have a single monolithic "34-field model"; it has a layered data contract designed for scraping, retrieval, and export.

## 4. Reference Databases

OpenCaseLaw builds a second SQLite artifact, `reference_graph.db`, from decision text. The current implementation stores case citations and statute references in related but separate tables, rather than as one homogeneous graph.

### 4.1 Extraction

The reference builder (`search_stack/build_reference_graph.py`) extracts:

- BGE references such as `BGE 131 III 115`
- federal docket references such as `4A_372/2019`
- BVGer and BStGer docket formats
- statute references such as `Art. 41 OR`

Case-reference resolution is then attempted against the in-corpus decision database using normalized dockets and confidence scoring based on court compatibility, canton compatibility, temporal plausibility, and ambiguity among candidate matches.

### 4.2 Scale

Table 2 summarizes the currently documented reference-database scale.

| Metric | Value |
|--------|-------|
| Extracted case-citation references | 8.77 million |
| Resolved source-reference pairs | 6.46 million |
| Resolution rate | 73.7% |
| Decision-statute links | 11.3 million |

The important distinction is that `8.77 million` refers to extracted case-citation references, whereas `11.3 million` refers to decision-statute mention links. These should not be merged into one undifferentiated edge count.

### 4.3 Uses

The reference database supports several retrieval and analysis tasks implemented in `mcp_server.py`:

- incoming and outgoing citation lookup for a decision
- leading-case discovery by topic or statute article
- appeal-chain tracing through prior-instance references
- year-by-year topic trend analysis
- statute-aware enrichment of search results

## 5. Retrieval Stack and Interfaces

### 5.1 Search Pipeline

The main search implementation lives in `mcp_server.py`. Its retrieval pipeline is staged:

1. **Query parsing and expansion.** The system builds multiple lexical query variants, applies hand-maintained legal synonym expansions, handles umlaut normalization, and can optionally call a small LLM for structured parsing and multilingual expansion.
2. **Candidate retrieval.** Several FTS5 strategies are executed and fused with Reciprocal Rank Fusion.
3. **Signal scoring.** Candidates are reweighted using lexical match features, metadata, docket cues, court priors, and citation/statute-reference signals when the graph database is available.
4. **Optional reranking.** The implementation can invoke confidence-gated LLM reranking for ambiguous cases and can also incorporate optional vector or cross-encoder signals depending on deployment settings.
5. **Result enrichment.** Returned hits are enriched with court metadata, citation counts, statute mentions, and related research signals.

This design is pragmatic rather than theoretically pure: it prioritizes recoverable legal search behavior over a single learned ranker.

### 5.2 Distribution Interfaces

OpenCaseLaw is available in several forms:

- **Parquet dataset** for bulk analysis and offline ML workflows
- **local SQLite FTS5 index** for offline search
- **REST API** for conventional HTTP clients
- **MCP server** for tool use from Claude, ChatGPT, Gemini, and similar systems

The tool surface is slightly deployment-dependent. The remote MCP deployment exposes 19 tools; local mode adds update-management tools, bringing the total to 21.

### 5.3 Evaluation Assets

The repository includes:

- `benchmarks/run_search_benchmark.py`
- `benchmarks/search_relevance_golden.json`

The current gold set contains 100 tagged queries. Language tags cover 74 German queries, 16 French queries, 7 Italian queries, and 3 unlabeled queries. Query types include docket lookup, statute lookup, natural-language topic search, concept-match retrieval, and cross-lingual retrieval.

This benchmark infrastructure is one of the more important research artifacts in the repository because it makes retrieval changes testable on fixed inputs instead of anecdotal examples. A submission-ready benchmark section should report results for one frozen configuration on one frozen gold set.

## 6. Ethics, Legal Basis, and Limitations

### 6.1 Ethics and Legal Access

OpenCaseLaw indexes decisions that courts or public bodies have already published. The project does not itself perform anonymization; it preserves the published form of the source material and links back to the original URLs. That makes publication policy a first-order dependency of the dataset, especially for cantonal courts whose anonymization practices vary.

### 6.2 Limitations

- **Coverage is broad, not perfect.** The corpus spans all cantons and federal courts, but publication depth still varies by court and era.
- **Historical quality varies.** Older BGE material and scanned PDFs can contain OCR artifacts or short extracted text.
- **Reference extraction is rule-based.** Citation and statute extraction are regex-driven and therefore miss non-standard, implicit, or stylistically unusual references.
- **Identity is operational, not jurisprudential.** `decision_id` and `canonical_key` are strong engineering identifiers, but they are not the same thing as a fully curated canonical case identity across republications and appeal stages.
- **Schema layering increases complexity.** The distinction between the core model, search schema, and export schema is useful in code but easy to misstate in documentation or papers.
- **Search evaluation should be frozen more rigorously.** The benchmark harness exists and is useful, but archival claims should always be tied to a versioned run on a fixed configuration.

## 7. Availability

| Resource | URL |
|----------|-----|
| Dataset (Parquet) | [huggingface.co/datasets/voilaj/swiss-caselaw](https://huggingface.co/datasets/voilaj/swiss-caselaw) |
| Source code | [github.com/jonashertner/caselaw-repo-1](https://github.com/jonashertner/caselaw-repo-1) |
| MCP server | `https://mcp.opencaselaw.ch` |
| REST API docs | [mcp.opencaselaw.ch/api/docs](https://mcp.opencaselaw.ch/api/docs) |
| Public stats snapshot | [opencaselaw.ch](https://opencaselaw.ch) |

## References

- Chalkidis, I., et al. (2022). LexGLUE: A Benchmark Dataset for Legal Language Understanding in English. ACL 2022.
- Cormack, G., Clarke, C., and Buettcher, S. (2009). Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods. SIGIR 2009.
- Kano, Y., et al. (2024). COLIEE 2024: Competition on Legal Information Extraction/Entailment. JSAI.
- Model Context Protocol Specification. Anthropic, 2024. https://modelcontextprotocol.io
- Niklaus, J., et al. (2021). Swiss-Judgment-Prediction: A Multilingual Legal Judgment Prediction Benchmark. NLP4PositiveImpact Workshop, EMNLP 2021.
- Niklaus, J., et al. (2023). MultiLegalPile: A 689GB Multilingual Legal Corpus. arXiv:2306.02069.
- Rasiah, V., et al. (2023). Explanation-based Dataset and Swiss Court View Generation. ACL 2023 Findings.
- Robertson, S., and Zaragoza, H. (2009). The Probabilistic Relevance Framework: BM25 and Beyond. Foundations and Trends in Information Retrieval.
