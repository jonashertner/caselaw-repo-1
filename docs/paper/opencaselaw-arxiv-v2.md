# OpenCaseLaw: A Versioned Open Corpus and Citation Graph for Published Swiss Case Law

**Jonas Hertner**

March 2026

*Draft v2 — addresses reviewer feedback. Requires: frozen snapshot, manual relevance judgments, citation extraction evaluation, number reconciliation across all public surfaces before submission.*

---

## TODO before submission

- [ ] Freeze release snapshot (OpenCaseLaw-2026-04-01 or similar)
- [ ] Pin every number in paper to frozen snapshot
- [ ] Reconcile counts across HuggingFace card, README, dashboard, health endpoint, paper
- [ ] Create dev/test split for benchmark (80/20 from golden set)
- [ ] Obtain manual relevance judgments from 2-3 legal experts
- [ ] Compute inter-annotator agreement
- [ ] Evaluate citation extraction precision/recall on 200 manually annotated decisions
- [ ] Evaluate deduplication on 500 manually reviewed decision pairs
- [ ] Evaluate text extraction quality on 200 manually reviewed decisions
- [ ] Create GitHub release with tag
- [ ] Create Zenodo DOI for code and data snapshot
- [ ] Pin HuggingFace dataset revision
- [ ] Fix HuggingFace license field (MIT → CC0 for data, MIT for code)
- [ ] Update all public surfaces to match paper numbers
- [ ] Add formal definitions for all citation graph quantities
- [ ] Add bootstrap confidence intervals to retrieval results

---

## Abstract

We introduce OpenCaseLaw, a versioned open corpus of published Swiss case law collected from official federal and cantonal publication channels. The [DATE] release contains [N] decisions spanning 1875–2026 in German, French, Italian, and Romansh, together with structured metadata across 34 fields and an extracted citation graph linking decisions to each other and to federal statute provisions. We describe the collection pipeline, deduplication methodology, and citation extraction approach, and report extraction quality on manually annotated subsets. We release a multilingual retrieval benchmark with expert relevance judgments covering [M] queries across [K] legal domains and three languages, together with an open baseline retrieval system combining lexical search, citation features, and optional LLM reranking. We discuss publication bias, anonymization variance across courts, and governance considerations for large-scale republication of judicial decisions.

## 1. Introduction

Published Swiss case law is distributed across a fragmented landscape of federal and cantonal publication channels. The Federal Supreme Court publishes decisions on bger.ch; the Federal Administrative Court uses a Weblaw-hosted platform; each of the 26 cantons maintains its own publication portal with distinct formats, search interfaces, and publication schedules. This fragmentation creates practical barriers for legal research, computational legal studies, and access to justice.

Existing open Swiss legal datasets address parts of this landscape. Swiss-Judgment-Prediction (Niklaus et al., 2021) provides 85,000 Federal Supreme Court decisions for outcome prediction. SwissRulings (RCDS) extends to 637,000 Federal Supreme Court cases. The Swiss Federal Supreme Court Dataset (Stürmer et al., 2024) offers 122,000 cases with 31 structured variables. However, none of these covers cantonal courts, and none includes citation graphs extracted from decision text. Commercial aggregators (Swisslex, Weblaw) provide comprehensive coverage but restrict access through paid subscriptions.

OpenCaseLaw contributes:

1. **A versioned corpus** of [N] published Swiss judicial and quasi-judicial decisions from [C] federal and cantonal publication channels, with full text, structured metadata, and daily updates.

2. **An extracted citation graph** of [X] million decision-to-decision edges and [Y] million statute-to-decision edges, with extraction quality evaluated on manually annotated samples.

3. **A multilingual retrieval benchmark** of [M] queries across [K] legal domains in German, French, and Italian, with expert relevance judgments and an open baseline system.

## 2. Related Work

**Swiss legal NLP datasets.** Niklaus et al. (2021) published Swiss-Judgment-Prediction, containing 85,000 Federal Supreme Court decisions for binary outcome prediction. Rasiah et al. (2023) extended this with natural language explanations derived from court reasoning. SwissRulings (RCDS) provides 637,000 Federal Supreme Court cases on HuggingFace. Stürmer et al. (2024) released a 122,000-case dataset with 31 structured variables from the Federal Supreme Court. All of these cover a single court. OpenCaseLaw covers [C] publication channels across all court levels and cantons.

**Multilingual legal benchmarks.** Niklaus et al. (2023) introduced SCALE (Swiss Court rulings Assessment for Legal Evaluation), benchmarking citation extraction, summarization, and other tasks on Swiss multilingual legal text. MultiLegalPile (Niklaus et al., 2023) provides a 689GB multilingual legal corpus for pretraining. LexGLUE (Chalkidis et al., 2022) benchmarks legal NLP on EU and US law. Our retrieval benchmark differs in evaluating cross-lingual case law retrieval with citation-based relevance signals.

**Open case law infrastructure.** The Caselaw Access Project (Harvard Law School) provides 6.7 million US court decisions. Open Legal Data covers German court decisions. Find Case Law provides UK case law. None include Swiss decisions. Entscheidsuche.ch aggregates Swiss cantonal decisions but provides no citation analysis, no structured metadata beyond basic fields, and no API.

**Legal information retrieval.** COLIEE (Kano et al., 2024) evaluates case law retrieval on Japanese and Canadian law. Locke et al. (2024) survey legal text retrieval approaches including dense and sparse methods. Our work applies Reciprocal Rank Fusion (Cormack et al., 2009) over multiple BM25 strategies with citation graph features.

## 3. Corpus

### 3.1 Source Taxonomy

We distinguish four source types:

| Type | Examples | Count | Decisions |
|------|----------|-------|-----------|
| Federal courts | BGer, BVGer, BStGer, BPatGer | [n] | [n] |
| Cantonal courts | ZH Obergericht, GE Cour de justice, TI Tribunale d'appello, ... | [n] | [n] |
| Federal quasi-judicial bodies | FINMA, WEKO, EDÖB, ElCom, PostCom, ComCom, UBI | [n] | [n] |
| Supranational (Swiss subset) | ECHR Swiss cases (via HUDOC) | 1 | ~475 |
| Historical collections | BGE volumes 1–79 (1875–1953), VPB, EMARK | [n] | [n] |

The corpus comprises published decisions from official publication channels. It does not include unpublished decisions, decisions behind paywalls, or decisions removed by courts after initial publication. Publication practices vary by court and canton, creating inherent coverage bias: some courts publish all decisions, others only selected ones.

### 3.2 Collection

[N] automated scrapers run nightly, each targeting a specific publication channel. Scrapers are idempotent and checkpoint-resumable. Text extraction from PDFs uses fitz (PyMuPDF) and pdfplumber; HTML pages use BeautifulSoup; JavaScript-rendered portals use Playwright.

### 3.3 Deduplication

The corpus is deduplicated from [RAW] raw entries to [DEDUP] unique decisions using two passes:

1. **Within-source:** canonical key (court code + normalized docket + date) collapses formatting variants. The version with the longest full text is kept, preferring entries with a regeste.

2. **Cross-source:** explicit overlap groups handle decisions published on multiple portals (e.g., Zürich decisions appearing under 17 court codes). Only decisions within the same defined overlap group are compared.

Importantly, a BGE leading case and its underlying BGer decision are *not* deduplicated — they have different courts, different docket numbers, and different content scope. Similarly, a cantonal decision and its federal appeal are retained as distinct proceedings.

**Deduplication evaluation.** We manually reviewed [N] decision pairs flagged as duplicates and [N] pairs not flagged, evaluating precision and recall of the deduplication. Results: [TODO].

### 3.4 Text Extraction Quality

We manually reviewed [N] randomly sampled decisions, assessing:
- Completeness: is the full text present?
- Encoding: are characters correctly decoded (no mojibake)?
- Structure: are regeste, Sachverhalt, Erwägungen identifiable?

Results: [TODO — need manual evaluation before submission]

### 3.5 Schema

Each decision has 34 fields. Key fields: `decision_id` (unique identifier), `court` (publication channel code), `canton`, `docket_number`, `decision_date`, `language` (de/fr/it/rm), `regeste` (official headnote, present in ~[X]% of decisions), `full_text`, `legal_area`, `source_url`, `pdf_url`. Full schema: `models.py` in the repository.

### 3.6 Decision Length Distribution

| Statistic | Characters |
|-----------|-----------|
| Mean | [N] |
| Median | [N] |
| 10th percentile | [N] |
| 90th percentile | [N] |
| Maximum | [N] |

[N] decisions ([X]%) have fewer than 500 characters due to scanned PDFs without text layers.

## 4. Citation Graph

### 4.1 Definitions

We extract four types of references from decision full text:

- **Decision citation mention:** a textual reference to another court decision (e.g., "BGE 131 III 115", "4A_372/2019"). A single decision may contain multiple mentions of the same target.
- **Decision citation edge:** a unique directed link from source decision to target decision, derived by deduplicating mentions. Each edge has a confidence score (high: exact docket match; medium: partial match).
- **Resolved edge:** a citation edge where the target reference is matched to a specific `decision_id` in the corpus.
- **Statute mention:** a reference to a specific provision of a federal statute (e.g., "Art. 41 OR"). Extracted via regex, linked to statute identifiers.

### 4.2 Extraction

References are extracted using regular expressions targeting BGE references, docket numbers, BVGE references, and statute provisions. Each extracted reference is resolved against the corpus using normalized docket matching.

### 4.3 Quantities

| Quantity | Definition | Count |
|----------|-----------|-------|
| Decision citation mentions | Total textual references to other decisions | [N] |
| Unique decision citation edges | Deduplicated source→target pairs | [N] |
| Resolved decision edges | Edges matched to a decision_id in corpus | [N] ([X]%) |
| Unresolved decision references | References not matched (unpublished decisions, non-standard formats) | [N] |
| Statute mentions | Total references to statute provisions | [N] |
| Unique statute-decision edges | Deduplicated (decision, statute_article) pairs | [N] |

### 4.4 Extraction Evaluation

We manually annotated citations in [N] randomly sampled decisions (covering [N] total citation mentions), evaluating extraction precision and recall.

| | Precision | Recall | F1 |
|---|-----------|--------|-----|
| Decision citations | [TODO] | [TODO] | [TODO] |
| Statute references | [TODO] | [TODO] | [TODO] |

Common error types: [TODO — need manual evaluation]

## 5. Retrieval Benchmark

### 5.1 Query Set

We constructed a retrieval benchmark of [M] queries in three languages:

| Language | Queries |
|----------|---------|
| German | [N] |
| French | [N] |
| Italian | [N] |

Queries cover [K] legal domains including [list]. Query types include docket lookups, statute-article queries, natural language questions, and concept-match queries (where user vocabulary differs from legal doctrine terms).

### 5.2 Relevance Judgments

Each query was assessed by [N] legal experts (law students / practitioners / professors) who identified relevant decisions and assigned graded relevance (3 = highly relevant, 2 = relevant, 1 = marginally relevant). Each query has [mean] relevant decisions (range: [min]–[max]).

Inter-annotator agreement: [TODO — need multiple annotators]

### 5.3 Dev/Test Split

The query set is split into [N] development queries (used for tuning) and [N] test queries (used only for final evaluation). All results reported in the main paper use the test split only.

### 5.4 Baseline System

Our baseline combines:

1. **Lexical retrieval:** SQLite FTS5 with BM25 scoring, unicode61 tokenizer, and multiple query strategies (AND, OR, field-focused) fused via Reciprocal Rank Fusion.

2. **Citation features:** incoming citation count (authority signal), in-pool citation signal (how many other candidates cite this decision), statute mention count.

3. **LLM query parsing (optional):** structured decomposition of the query into statute references, doctrine terms, and synonyms via Claude Haiku. Cost: ~$0.0001/query.

4. **LLM reranking (optional):** confidence-gated reranking of top 15 candidates by Claude Haiku. Cost: ~$0.0002/query.

5. **German compound decomposition:** morpheme-boundary splitting of long compound words.

### 5.5 Results

All results on the held-out test set of [N] queries.

| Configuration | MRR@10 | Hit@1 | Recall@10 |
|---------------|--------|-------|-----------|
| BM25 baseline | [TODO] | [TODO] | [TODO] |
| + Citation features | [TODO] | [TODO] | [TODO] |
| + LLM query parsing | [TODO] | [TODO] | [TODO] |
| + LLM reranking | [TODO] | [TODO] | [TODO] |
| + Compound decomposition | [TODO] | [TODO] | [TODO] |

95% bootstrap confidence intervals computed over [N] bootstrap samples.

Per-language breakdown:

| Language | n | MRR@10 | Hit@1 |
|----------|---|--------|-------|
| German | [N] | [TODO] | [TODO] |
| French | [N] | [TODO] | [TODO] |
| Italian | [N] | [TODO] | [TODO] |

We also evaluated bge-reranker-base (278M parameters, trained on MS MARCO) as an alternative to LLM reranking. It reduced MRR at all weight settings, consistent with prior findings that general-purpose rerankers underperform on domain-specific multilingual legal text.

## 6. Governance and Ethics

### 6.1 Legal Framework

Published Swiss court decisions are excluded from copyright protection under Art. 5 para. 1 lit. c URG (Federal Act on Copyright), which exempts official works including judicial decisions. Publication of federal court decisions is governed by Art. 27 BGG (Federal Supreme Court Act). Cantonal publication duties vary by jurisdiction.

### 6.2 Anonymization

Anonymization is performed by publishing courts, not by us. Federal courts consistently anonymize parties using initials or pseudonyms. Cantonal courts vary: some redact names, others publish as rendered. We do not add or remove anonymization.

Large-scale aggregation changes the privacy risk profile compared to decisions published individually on court websites. Automated linking across decisions, combined with structured metadata (court, date, legal area, canton), may enable re-identification even when party names are redacted (cf. Pilan et al., 2024). Users should be aware of this risk, particularly for cantonal decisions with less stringent anonymization.

### 6.3 Takedown and Correction

Courts occasionally retract or re-anonymize published decisions. We monitor source URLs for removals during nightly scraping. A formal takedown process is documented in the repository for courts or affected parties to request removal.

### 6.4 Intended Use

The corpus is intended for legal research, computational legal studies, and tool development. It is not intended as a substitute for professional legal counsel. Tools built on this corpus should clearly disclose their data source and limitations.

## 7. Limitations

- **Publication bias.** The corpus contains only decisions that courts chose to publish. Publication practices vary by court and canton. The corpus should not be interpreted as a complete record of Swiss judicial activity.
- **Temporal coverage varies.** Federal courts from 1996, most cantonal courts from 2000+. Historical BGE from 1875 may contain OCR artifacts.
- **Citation extraction is regex-based.** Resolution rate is [X]%. Informal references, footnote citations with unusual formatting, and negative citations (distinguishing rather than following) are not reliably captured.
- **No editorial annotation.** Legal area classification and statute references are automatically extracted, not curated by legal experts.
- **Evaluation set size.** [M] queries across [K] domains provides directional signal but limits per-domain statistical power.
- **LLM dependency.** Query parsing and reranking require Claude Haiku API access. The system degrades gracefully to BM25-only retrieval.

## 8. Release

| Artifact | Location | Persistent ID |
|----------|----------|---------------|
| Corpus (Parquet) | HuggingFace | [revision hash] |
| Citation graph (SQLite) | [TBD] | [DOI] |
| Benchmark queries + judgments | GitHub | [release tag] |
| Evaluation code | GitHub | [release tag] |
| Source code | GitHub | [DOI via Zenodo] |

The corpus is updated daily. Versioned snapshots are tagged for reproducibility.

## References

- Chalkidis, I., et al. (2022). LexGLUE: A Benchmark Dataset for Legal Language Understanding in English. ACL 2022.
- Cormack, G., Clarke, C., & Buettcher, S. (2009). Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods. SIGIR 2009.
- Kano, Y., et al. (2024). COLIEE 2024: Competition on Legal Information Extraction/Entailment. JSAI.
- Locke, S., et al. (2024). Legal Text Retrieval: A Survey. ACL 2024.
- Niklaus, J., et al. (2021). Swiss-Judgment-Prediction: A Multilingual Legal Judgment Prediction Benchmark. NLP4PositiveImpact, EMNLP 2021.
- Niklaus, J., et al. (2023). MultiLegalPile: A 689GB Multilingual Legal Corpus. arXiv:2306.02069.
- Niklaus, J., et al. (2023). SCALE: Scaling up the Swiss Court rulings Assessment for Legal Evaluation. arXiv:2306.09237.
- Pilan, I., et al. (2024). Anonymity at Risk? Assessing Re-Identification Capabilities of Large Language Models in Court Decisions. Findings of NAACL 2024.
- Rasiah, V., et al. (2023). SCALE: Explanation-based Dataset for Swiss Court View Generation. ACL 2023 Findings.
- Robertson, S., & Zaragoza, H. (2009). The Probabilistic Relevance Framework: BM25 and Beyond. Foundations and Trends in Information Retrieval.
- Stürmer, S., et al. (2024). Swiss Federal Supreme Court Dataset. Zenodo. doi:10.5281/zenodo.11092977.
