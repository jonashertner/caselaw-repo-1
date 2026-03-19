# OpenCaseLaw: An Open Dataset and Search Platform for Swiss Court Decisions

**Jonas Hertner**

March 2026

---

## Abstract

We present OpenCaseLaw, an open corpus and retrieval stack for Swiss case law. In the repository snapshot generated on March 18, 2026, the dataset contains 962,272 decisions from 101 federal, cantonal, and regulatory courts or public bodies, covering all 26 cantons and the period 1875-2026. The current snapshot contains 448,215 German decisions (46.6%), 434,470 French decisions (45.2%), and 79,587 Italian decisions (8.3%); the export schema also reserves a Romansh language code. OpenCaseLaw releases a 34-field Parquet export, a local SQLite FTS5 index, a citation/reference database with 8.75 million extracted case-citation references, 6.46 million resolved in-corpus decision links, and 11.3 million decision-statute links, plus REST and Model Context Protocol (MCP) interfaces for retrieval from conventional clients and LLM tools. We describe the collection, normalization, deduplication, export, and retrieval pipeline, and we release a multilingual benchmark harness with 100 tagged evaluation queries. The code is MIT-licensed and all records link back to the decisions as published by the originating courts.

## 1. Introduction

Swiss case law is published across a fragmented landscape of federal and cantonal court websites, publication portals, and administrative repositories. The result is a difficult retrieval environment: coverage varies by court, interfaces are heterogeneous, and cross-court search is poor. Commercial systems such as Swisslex and Weblaw partially solve this problem, but they are closed, subscription-based products. Open Swiss resources exist, but they typically emphasize either a subset of courts, a narrow NLP task, or raw publication access without a reusable retrieval stack.

OpenCaseLaw is intended as infrastructure rather than a single benchmark dataset. It combines corpus acquisition, normalization, searchable exports, reference extraction, and interfaces for both programmatic and LLM-mediated access. The project makes three main contributions:

1. **A broad open Swiss case-law corpus.** The March 18, 2026 snapshot contains 962,272 decisions from 101 courts or public bodies, including all 26 cantons, federal courts, and several regulatory or quasi-judicial bodies.
2. **Reusable retrieval artifacts.** The release includes Parquet exports, a local SQLite FTS5 database, a citation/reference database, REST endpoints, and an MCP server.
3. **Evaluation infrastructure.** The repository includes a multilingual search benchmark harness and a 100-query tagged gold set designed for retrieval regression testing and system comparison.

The paper focuses on what is versioned and inspectable in the repository. Where the implementation distinguishes between core data models, search indexes, and export schemas, we state that explicitly instead of collapsing them into a single "dataset" abstraction.

## 2. Related Work

**Swiss legal datasets.** Swiss-Judgment-Prediction (Niklaus et al., 2021) and later work by Rasiah et al. (2023) provide labeled Federal Supreme Court datasets for downstream NLP tasks. SwissRulings (RCDS) covers 637,000 Federal Supreme Court cases. Stürmer et al. (2024) released 122,000 cases with 31 structured variables. SCALE (Niklaus et al., 2023b) benchmarks citation extraction, summarization, and other tasks on Swiss multilingual legal text. All of these focus on the Federal Supreme Court. OpenCaseLaw covers 101 sources across all court levels and cantons, with a cross-corpus reference database.

**Broader legal corpora.** MultiLegalPile (Niklaus et al., 2023a) is a large multilingual legal corpus for language-model pretraining, and LexGLUE (Chalkidis et al., 2022) is a benchmark suite for legal NLP tasks. These resources are valuable for representation learning and evaluation, but they do not provide Swiss court-wide retrieval infrastructure, court-level normalization, or citation/statute reference databases for Swiss jurisprudence.

**Open case law infrastructure.** The Caselaw Access Project (Harvard Law School) provides 6.7 million US court decisions. Open Legal Data covers German court decisions. Entscheidsuche.ch aggregates Swiss cantonal decisions but provides limited search, no structured metadata beyond basic fields, and no citation analysis.

**Legal information retrieval.** BM25 and Reciprocal Rank Fusion remain robust retrieval baselines (Robertson and Zaragoza, 2009; Cormack et al., 2009). Locke et al. (2024) survey legal text retrieval approaches. OpenCaseLaw uses BM25 and RRF as part of a practical search pipeline optimized for multilingual Swiss legal text rather than as the sole research contribution.

**Re-identification risk.** Pilan et al. (2024) assess re-identification capabilities of LLMs in court decisions, finding that aggregation and structured metadata increase privacy risk even when party names are redacted. This is directly relevant to large-scale case law corpora like ours.

## 3. Dataset and Processing Pipeline

### 3.1 Snapshot Statistics

Table 1 reports the repository snapshot reflected in `docs/stats.json`, generated on March 18, 2026.

| Metric | Value |
|--------|-------|
| Snapshot timestamp | 2026-03-18T21:29:11Z |
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

Unless otherwise noted, corpus-wide counts in this paper come from this snapshot file. Retrieval metrics in Section 5.3 come from the frozen benchmark artifact named there.

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
| Extracted case-citation references | 8.75 million |
| Resolved source-reference pairs | 6.46 million |
| Resolution rate | 73.7% |
| Decision-statute links | 11.3 million |

The important distinction is that `8.75 million` refers to extracted case-citation references, whereas `11.3 million` refers to decision-statute mention links. These should not be merged into one undifferentiated edge count.

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

The tool surface is deployment-dependent. The repository defines up to 21 MCP tools. Remote mode omits local update-management tools, and legislation-search tools depend on optional LexFind-backed deployment configuration.

### 5.3 Evaluation Assets

The repository includes:

- `benchmarks/run_search_benchmark.py`
- `benchmarks/search_relevance_golden.json`

The current gold set contains 100 tagged queries. Language tags mark 74 German-tagged queries, 16 French-tagged queries, 7 Italian-tagged queries, and 3 queries without a language tag. By query type, the set includes 46 natural-language queries, 11 statute-oriented queries, 8 concept-match queries, and 2 explicitly cross-lingual queries, plus smaller slices for docket lookup, short queries, and other robustness cases.

This benchmark infrastructure is one of the more important research artifacts in the repository because it makes retrieval changes testable on fixed inputs instead of anecdotal examples. To anchor the current paper to a versioned result, the repository now includes `benchmarks/search_benchmark_2026-03-19_offline_full.json`, a frozen run on the 100-query set against a 1,078,177-row local `decisions.db`. That offline baseline achieved MRR@10 = 0.4697, Recall@10 = 0.4958, nDCG@10 = 0.5250, and Hit@1 = 0.33.

This number should be interpreted carefully. It is a deterministic local baseline, not a full hosted-system score: the evaluation environment for that run did not have a local reference-graph database, local statutes/commentary databases, vector search, or Anthropic-backed query expansion and reranking available. The benchmark remains useful for publication because it is frozen and inspectable, but it is not yet a balanced benchmark for strong multilingual claims: it is still dominated by German natural-language queries, and its hardest slices remain concept-match and statute-oriented retrieval.

## 6. Ethics, Legal Basis, and Limitations

### 6.1 Legal Basis and Governance

Published Swiss court decisions are excluded from copyright protection under Art. 5 para. 1 lit. c URG (Federal Act on Copyright), which exempts official works including judicial decisions. The duty to publish Federal Supreme Court decisions is established by Art. 27 BGG. Cantonal publication duties vary by jurisdiction.

OpenCaseLaw indexes decisions in the form published by the originating courts. The project does not itself perform anonymization; it preserves the published form of the source material and links back to the original URLs. That makes court publication policy a first-order dependency of the dataset, especially for cantonal courts whose anonymization practices vary.

Large-scale aggregation changes the privacy risk profile compared to individual court-website publication. Structured metadata combined with full text may enable re-identification even when party names are redacted (Pilan et al., 2024). The repository documents a process for courts or affected parties to request removal of specific decisions.

### 6.2 Limitations

- **Coverage is broad, not perfect.** The corpus spans all cantons and federal courts, but publication depth still varies by court and era.
- **Historical quality varies.** Older BGE material and scanned PDFs can contain OCR artifacts or short extracted text.
- **Reference extraction is rule-based.** Citation and statute extraction are regex-driven and therefore miss non-standard, implicit, or stylistically unusual references.
- **Identity is operational, not jurisprudential.** `decision_id` and `canonical_key` are strong engineering identifiers, but they are not the same thing as a fully curated canonical case identity across republications and appeal stages.
- **Schema layering increases complexity.** The distinction between the core model, search schema, and export schema is useful in code but easy to misstate in documentation or papers.
- **Published artifacts are generated by multiple pipelines.** The corpus, search index, reference database, and dashboard statistics are closely related but operationally distinct build products; papers should state clearly which artifact a reported number comes from.
- **The archived benchmark is still a baseline, not a final system result.** The March 19, 2026 frozen report is reproducible and useful, but it reflects the offline local configuration available in that environment rather than a fully provisioned hosted deployment.

## 7. Availability

| Resource | URL |
|----------|-----|
| Dataset (Parquet) | [huggingface.co/datasets/voilaj/swiss-caselaw](https://huggingface.co/datasets/voilaj/swiss-caselaw) |
| Source code | [github.com/jonashertner/caselaw-repo-1](https://github.com/jonashertner/caselaw-repo-1) |
| Frozen benchmark report | `benchmarks/search_benchmark_2026-03-19_offline_full.json` |
| MCP server | `https://mcp.opencaselaw.ch` |
| REST API docs | [mcp.opencaselaw.ch/api/docs](https://mcp.opencaselaw.ch/api/docs) |
| Public stats snapshot | [opencaselaw.ch](https://opencaselaw.ch) |

## References

- Caselaw Access Project. Harvard Law School Library Innovation Lab. https://case.law
- Chalkidis, I., Jana, A., Hartung, D., Bommarito, M., Androutsopoulos, I., Katz, D., and Aletras, N. (2022). LexGLUE: A Benchmark Dataset for Legal Language Understanding in English. In *Proceedings of ACL 2022*.
- Cormack, G., Clarke, C., and Buettcher, S. (2009). Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods. In *Proceedings of SIGIR 2009*.
- Kano, Y., Soh, J., Ngo, L., Rabelo, J., and Satoh, K. (2024). COLIEE 2024: Competition on Legal Information Extraction/Entailment. In *JSAI 2024*.
- Locke, S., Zhai, Z., and Kohlmeier, J. (2024). A Survey on Legal Text Retrieval. In *Proceedings of ACL 2024*.
- Model Context Protocol Specification. Anthropic, 2024. https://modelcontextprotocol.io
- Niklaus, J., Chalkidis, I., and Stürmer, M. (2021). Swiss-Judgment-Prediction: A Multilingual Legal Judgment Prediction Benchmark. In *NLP4PositiveImpact Workshop, EMNLP 2021*.
- Niklaus, J., Matoshi, V., Stürmer, M., Chalkidis, I., and Ho, D. (2023a). MultiLegalPile: A 689GB Multilingual Legal Corpus. *arXiv:2306.02069*.
- Niklaus, J., Matoshi, V., Sturm, F., Chalkidis, I., and Ho, D. (2023b). SCALE: Scaling up the Annotation, Curation, and Evaluation of Swiss Court Rulings. *arXiv:2306.09237*.
- Pilan, I., Lognoul, T., Niklaus, J., Stürmer, M., and Chalkidis, I. (2024). Anonymity at Risk? Assessing Re-Identification Capabilities of Large Language Models in Court Decisions. In *Findings of NAACL 2024*.
- Rasiah, V., Niklaus, J., Chalkidis, I., Ho, D., and Stürmer, M. (2023). SCALE: Explanation-based Dataset for Swiss Court View Generation. In *Findings of ACL 2023*.
- Robertson, S. and Zaragoza, H. (2009). The Probabilistic Relevance Framework: BM25 and Beyond. *Foundations and Trends in Information Retrieval*.
- Stürmer, S., Niklaus, J., and Chalkidis, I. (2024). Swiss Federal Supreme Court Dataset. Zenodo. doi:10.5281/zenodo.11092977.
