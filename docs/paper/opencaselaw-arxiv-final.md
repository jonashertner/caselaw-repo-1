# OpenCaseLaw: A Versioned Open Corpus and Citation Graph for Published Swiss Case Law

**Jonas Hertner**

March 2026

---

## Abstract

We introduce OpenCaseLaw, a versioned open corpus of published Swiss case law collected from official federal and cantonal publication channels. The March 2026 snapshot contains 962,272 decisions from 101 federal, cantonal, and quasi-judicial sources, covering all 26 cantons and the period 1875–2026, in German (448,215), French (434,470), and Italian (79,587). The release includes a 34-field Parquet export, a local SQLite FTS5 search index, and a reference database containing 8.75 million extracted case-citation references (6.46 million resolved to in-corpus decisions, 73.8% resolution rate) and 11.2 million decision-to-statute links across 281,391 distinct provisions. We describe the collection, deduplication, and reference extraction pipeline, and release a 100-query multilingual retrieval benchmark with an open baseline system combining BM25, citation graph features, and optional LLM reranking. The corpus is updated daily; all code is MIT-licensed; decisions are official publications excluded from copyright under Art. 5 URG.

## 1. Introduction

Published Swiss case law is distributed across a fragmented landscape of federal and cantonal publication channels. The Federal Supreme Court publishes on bger.ch; the Federal Administrative Court uses a Weblaw-hosted platform; each of the 26 cantons maintains its own portal with distinct formats, search interfaces, and publication schedules. This fragmentation creates practical barriers for legal research, computational legal studies, and access to justice.

Existing open Swiss legal datasets address parts of this landscape. Swiss-Judgment-Prediction (Niklaus et al., 2021) provides 85,000 Federal Supreme Court decisions for outcome prediction. SwissRulings (RCDS) covers 637,000 Federal Supreme Court cases. The Swiss Federal Supreme Court Dataset (Stürmer et al., 2024) offers 122,000 cases with 31 structured variables. SCALE (Niklaus et al., 2023b) benchmarks citation extraction, summarization, and other tasks on Swiss multilingual legal text. However, none of these covers cantonal courts, and none includes a citation graph extracted across the full corpus. Commercial aggregators (Swisslex, Weblaw) provide comprehensive coverage but restrict access through paid subscriptions.

OpenCaseLaw is intended as infrastructure rather than a single benchmark dataset. It combines corpus acquisition, normalization, searchable exports, reference extraction, and interfaces for both programmatic and LLM-mediated access. The project makes three contributions:

1. **A broad open Swiss case-law corpus.** The March 2026 snapshot contains 962,272 decisions from 101 sources — 20 federal courts and quasi-judicial bodies, 81 cantonal courts — covering all 26 cantons and the period 1875–2026.

2. **An extracted reference database.** 8.75 million case-citation references (6.46 million resolved in-corpus) and 11.2 million decision-to-statute links, with formal quantity definitions and a report of resolution methodology.

3. **Retrieval infrastructure.** A 100-query multilingual benchmark, an open baseline retrieval system, and distribution via Parquet, REST API, and Model Context Protocol (MCP) server.

## 2. Related Work

**Swiss legal NLP datasets.** Swiss-Judgment-Prediction (Niklaus et al., 2021) provides 85,000 Federal Supreme Court decisions labeled for binary outcome prediction. Rasiah et al. (2023) extended this with natural language explanations. SwissRulings provides 637,000 Federal Supreme Court cases. Stürmer et al. (2024) released 122,000 cases with 31 structured variables. All of these cover a single court. OpenCaseLaw covers 101 sources across all court levels and cantons, with a cross-corpus citation graph.

**Multilingual legal benchmarks.** SCALE (Niklaus et al., 2023b) benchmarks citation extraction, summarization, and other tasks on Swiss multilingual legal text and is the most directly relevant benchmark work. MultiLegalPile (Niklaus et al., 2023a) provides a 689GB multilingual legal corpus for pretraining. LexGLUE (Chalkidis et al., 2022) benchmarks legal NLP on EU and US law. None include cross-court Swiss citation graphs.

**Open case law infrastructure.** The Caselaw Access Project (Harvard Law School) provides 6.7 million US court decisions. Open Legal Data covers German court decisions. Entscheidsuche.ch aggregates Swiss cantonal decisions but provides limited search, no structured metadata beyond basic fields, and no citation analysis. We verified that the OpenCaseLaw corpus covers all decisions available through entscheidsuche.ch, plus additional sources they do not index (regulatory bodies, historical collections, ECHR Swiss cases).

**Legal information retrieval.** Locke et al. (2024) survey legal text retrieval approaches. COLIEE (Kano et al., 2024) evaluates retrieval and entailment on Japanese and Canadian law. Our retrieval baseline uses BM25 and Reciprocal Rank Fusion (Cormack et al., 2009; Robertson and Zaragoza, 2009) with citation graph features.

**Re-identification risk in legal data.** Pilan et al. (2024) assess re-identification capabilities of LLMs in court decisions, finding that aggregation and structured metadata increase privacy risk even when party names are redacted. This is directly relevant to large-scale case law corpora.

## 3. Corpus

### 3.1 Source Taxonomy

We distinguish four source types. This distinction matters because the corpus is not exclusively composed of court decisions in the strict judicial sense.

| Type | Count | Decisions | Examples |
|------|-------|-----------|----------|
| Federal courts | 7 | 298,731 | BGer (174,213), BVGer (91,560), BStGer (11,406), BPatGer (189), BGE published (21,228), BGE historical (14,578) |
| Cantonal courts | 81 | 618,241 | GE (166,912), VD (155,399 across 3 portals), ZH (81,000 across 21 sub-courts), TI (59,247) |
| Federal quasi-judicial bodies | 11 | 29,953 | FINMA (2,988), EDÖB (1,797), WEKO (256), VPB (22,884), ElCom (422), ComCom (64), PostCom (213), UBI (641) |
| Supranational (Swiss subset) | 2 | 1,285 | ECHR Swiss cases (475), EMARK asylum commission (810) |
| **Total** | **101** | **962,272** | |

All 26 cantons are represented. The smallest (AI: 79, NW: 992, SH: 695) reflect genuinely smaller court systems, not incomplete scraping.

### 3.2 Languages

| Language | Decisions | Share |
|----------|-----------|-------|
| German | 448,215 | 46.6% |
| French | 434,470 | 45.1% |
| Italian | 79,587 | 8.3% |

The near-parity between German and French reflects the large volume of Geneva (166,912) and Vaud (155,399) decisions. Italian decisions come primarily from Ticino (59,247) and the Federal Supreme Court.

### 3.3 Collection

54 automated scrapers run nightly at 01:00 UTC. Each targets a specific publication channel — court websites, Weblaw APIs, FindInfo portals, Omnis platforms, or direct court APIs. Scrapers are idempotent and checkpoint-resumable: they track previously seen decisions by docket number and fetch only new content.

Text extraction uses fitz (PyMuPDF) and pdfplumber for PDFs, BeautifulSoup for HTML, and Playwright with stealth plugins for JavaScript-rendered portals.

### 3.4 Deduplication

The corpus is deduplicated from approximately 1.24 million raw entries to 962,272 unique decisions using two passes:

1. **Within-source deduplication.** A canonical key derived from court code, normalized docket number, and decision date collapses formatting variants (dots, underscores, slashes, case). The version with the longest full text is kept, preferring entries with a regeste.

2. **Cross-source deduplication.** Hand-maintained overlap groups handle decisions published on multiple portals. For example, Zürich decisions appear under 17 court codes; Aargau under 18. Only decisions within the same defined overlap group are compared.

Two important non-deduplication cases: a BGE leading case (official excerpt published in the BGE collection) and its underlying BGer decision (full ruling) are retained as distinct records — they have different courts, different docket numbers, and different content scope. Similarly, a cantonal decision and its federal appeal are both retained as distinct proceedings.

**Validation status.** The deduplication logic has not been evaluated on a manually annotated sample. This is a limitation; precision and recall of the deduplication should be assessed in future work.

### 3.5 Schema

Each decision has 34 fields in the Parquet export. The repository uses three related schemas by design:

1. **Core model** (`models.py`): 28 fields used by scrapers.
2. **Search index** (`db_schema.py`): 24-column SQLite FTS5 table.
3. **Parquet export** (`export_parquet.py`): 34-field Arrow schema with provenance and computed fields.

Key fields: `decision_id`, `court`, `canton`, `docket_number`, `decision_date`, `language`, `regeste` (official headnote, present in 503,557 decisions = 52.3%), `full_text`, `legal_area`, `source_url`, `pdf_url`.

### 3.6 Text Statistics

| Statistic | Value |
|-----------|-------|
| Mean full text length | 22,039 characters |
| Decisions with full text ≥ 500 chars | 936,273 (97.3%) |
| Decisions with full text < 500 chars | 25,999 (2.7%) |
| Decisions with regeste ≥ 20 chars | 503,557 (52.3%) |

The 2.7% short-text decisions are primarily scanned PDFs without text layers; full text may be available at the source URL.

## 4. Reference Database

OpenCaseLaw builds a second artifact, `reference_graph.db`, from decision full text. The database stores case citations and statute references in separate tables.

### 4.1 Definitions

We define the following quantities precisely to avoid conflation:

- **Case-citation reference** (`decision_citations` table): an extracted textual reference from a source decision to a target decision or case identifier. One row per unique (source, target_ref) pair. Multiple mentions of the same target in one decision produce one row with a `mention_count`.

- **Resolved citation link** (`citation_targets` table): a case-citation reference successfully matched to a `decision_id` in the corpus. Each row maps a specific reference to a resolved target with a confidence score.

- **Statute-decision link** (`decision_statutes` table): a reference from a decision to a specific statute provision (e.g., Art. 41 OR). One row per unique (decision, statute_article) pair with a `mention_count`.

### 4.2 Scale

Table: Reference database quantities, March 2026 snapshot.

| Quantity | Table | Count |
|----------|-------|-------|
| Case-citation references (unique source-target pairs) | `decision_citations` | 8,751,616 |
| Resolved citation links (matched to in-corpus decision_id) | `citation_targets` | 6,463,313 |
| Resolution rate | | 73.8% |
| Statute-decision links (unique decision-article pairs) | `decision_statutes` | 11,220,293 |
| Distinct statute provisions referenced | | 281,391 |

These quantities are not additive. Case-citation references and statute-decision links are distinct relation types stored in separate tables.

Unresolved references (26.2%) include citations to unpublished lower court decisions, decisions not yet in the corpus, and references with non-standard formatting that the regex extractor does not capture.

### 4.3 Extraction Methodology

References are extracted using regular expressions targeting four formats:

1. BGE references: `BGE 131 III 115`, `ATF 140 III 264`, `DTF 142 IV 245`
2. Docket numbers: `4A_372/2019`, `6B_1234/2025`, `E-5483/2016`
3. BVGE references: `BVGE 2013/10`
4. Statute provisions: `Art. 41 OR`, `Art. 8 BV`, `§ 261bis StGB`

Resolution uses normalized docket matching with confidence scoring based on court compatibility and format specificity.

**Validation status.** The extraction has not been evaluated for precision and recall on a manually annotated sample. This is a significant limitation. Informal inspection suggests high precision for BGE and docket references (distinctive formats) and lower recall for informal citations ("the cited judgment," implicit references). Negative citations (distinguishing rather than following a precedent) are not distinguished from positive citations.

### 4.4 Applications

The reference database supports: incoming/outgoing citation lookup, leading-case identification by statute article, appeal chain reconstruction via prior-instance flags, year-by-year topic trend analysis, and in-pool citation signal for search ranking.

## 5. Retrieval

### 5.1 Search Pipeline

The retrieval implementation (`mcp_server.py`) uses a five-stage pipeline:

1. **Query parsing.** Multiple FTS5 query variants are constructed. A hand-maintained 120-entry legal synonym dictionary expands terms across languages with automatic umlaut normalization. German compound words are decomposed at morpheme boundaries. Optionally, an LLM (Claude Haiku) parses the query into structured facets: statute references, doctrine name, and multilingual synonyms (~$0.0001/query).

2. **Candidate retrieval.** 6–8 FTS5 strategies run in parallel (AND, OR, field-focused, language-focused, LLM-expanded). Results are fused via Reciprocal Rank Fusion with per-strategy weights.

3. **Signal scoring.** Candidates are scored on 15+ features: BM25, docket match, term coverage, statute graph mentions, incoming citation count, in-pool citation signal, language match, and court-type priors.

4. **Optional LLM reranking.** Top 15 candidates are sent to Claude Haiku for legal relevance reranking. Confidence-gated: fires only when top results are close in score. Skipped for docket lookups. ~$0.0002/query.

5. **Result enrichment.** Each result is enriched with court name (human-readable), court level, legal area (derived from statute references, excluding procedural statutes), top statute articles discussed, incoming citation count, and a leading-case flag.

### 5.2 Benchmark

The repository includes a 100-query benchmark (`benchmarks/search_relevance_golden.json`) covering 74 German, 16 French, 7 Italian, and 3 cross-lingual queries across 15 legal domains. Each query has 3–5 expected decisions identified using citation graph authority and manual verification.

Table: Retrieval results on the full 100-query set, March 2026 snapshot. All configurations use the same query set.

| Configuration | MRR@10 | Hit@1 |
|---------------|--------|-------|
| BM25 baseline (no LLM, no citation features) | 0.363 | 0.264 |
| + LLM query parsing + synonym expansion | 0.456 | 0.377 |
| + LLM reranking (Haiku, w=3.0, top 15) | 0.501 | 0.434 |
| + Compound decomposition + BVGE normalization | 0.510 | 0.443 |
| Full system (above + expanded 100-query set) | 0.647 | 0.570 |

**Important caveat.** The jump from 0.510 to 0.647 in the last row reflects the expansion of the query set from 53 to 100 queries. The new queries were selected to cover underrepresented legal domains (tenancy, tax, employment, criminal) and happened to have higher baseline performance. On the original 53-query subset, the final system scores MRR 0.510. The 0.647 figure is the result on the complete 100-query set and should not be directly compared with the 53-query ablation rows.

**Validation status.** Relevance judgments were produced by a single annotator (the author) using citation graph authority as a guide. Inter-annotator agreement has not been computed. A submission to a peer-reviewed venue should obtain judgments from multiple legal experts and report agreement.

We also evaluated bge-reranker-base (278M parameters, trained on English MS MARCO). It reduced MRR at all weight settings, consistent with the observation that general-purpose rerankers underperform on domain-specific multilingual text. This is not a strong baseline comparison; a multilingual legal-domain reranker would be more informative.

### 5.3 Distribution

The corpus is available via:

- **Parquet** on HuggingFace for bulk analysis (~7 GB, 100 files)
- **Local SQLite FTS5 index** for offline search (~65 GB)
- **REST API** with OpenAPI documentation
- **MCP server** supporting SSE and Streamable HTTP transports, with 19 read-only tools covering search, citation analysis, statute lookup, legislation search, and scholarly commentary. Compatible with Claude, ChatGPT (Developer Mode, recommended with GPT-5.3), and Gemini CLI.

## 6. Legal Framework

Published Swiss court decisions are excluded from copyright protection under Art. 5 para. 1 lit. c of the Federal Act on Copyright and Related Rights (URG), which exempts official works including judicial decisions. The duty to publish Federal Supreme Court decisions is established by Art. 27 of the Federal Supreme Court Act (BGG). Cantonal publication duties vary by jurisdiction.

OpenCaseLaw indexes decisions in the form published by the originating courts. Anonymization is performed by the courts; we do not add or remove anonymization. Federal courts consistently anonymize parties. Cantonal anonymization practices vary.

Large-scale aggregation changes the privacy risk profile compared to individual court-website publication. Structured metadata (court, date, legal area, canton) combined with full text may enable re-identification even when party names are redacted (Pilan et al., 2024). Users of the corpus should be aware of this risk, particularly for cantonal decisions with less stringent anonymization.

The corpus is intended for legal research, computational legal studies, and tool development. It is not intended as a substitute for qualified legal counsel.

## 7. Limitations

- **Coverage is broad, not audited.** The corpus spans all cantons and federal courts, but we have not conducted a court-by-court recall audit against official publication counts. Publication depth varies by court and era.
- **Publication bias.** The corpus contains only decisions that courts chose to publish. It should not be interpreted as a complete record of Swiss judicial activity.
- **Reference extraction is rule-based.** The 73.8% resolution rate is a system-level number without per-type precision/recall evaluation on manually annotated data.
- **Deduplication is untested.** The canonical key approach has not been evaluated against manual judgments. Edge cases (e.g., corrected republications, multi-language versions of the same decision) may produce false negatives or false positives.
- **Retrieval evaluation is preliminary.** Single annotator, no inter-annotator agreement, no dev/test split, and the evaluation set changed during system development. Results should be interpreted as indicative, not definitive.
- **Entity taxonomy is not purely judicial.** The corpus includes quasi-judicial bodies (FINMA, WEKO, EDÖB) and a supranational court (ECHR). The title and description reflect this but could be clearer.
- **LLM dependency.** Query parsing and reranking depend on Claude Haiku API availability. The system degrades gracefully to BM25-only search.

## 8. Future Work

Three areas would strengthen the corpus as a research artifact:

1. **Manual evaluation.** Citation extraction precision/recall on annotated samples. Deduplication accuracy on reviewed pairs. Multi-annotator relevance judgments with agreement statistics for the retrieval benchmark.

2. **Versioned benchmark.** A frozen dev/test split of the query set with all results reported on the held-out test partition only. This is standard practice and should be done before peer-reviewed submission.

3. **Coverage audit.** Court-by-court comparison against official publication counts where available, documenting known gaps.

## 9. Availability

| Artifact | URL |
|----------|-----|
| Corpus (Parquet, updated daily) | [huggingface.co/datasets/voilaj/swiss-caselaw](https://huggingface.co/datasets/voilaj/swiss-caselaw) |
| Source code (MIT) | [github.com/jonashertner/caselaw-repo-1](https://github.com/jonashertner/caselaw-repo-1) |
| MCP server (19 tools, no auth) | `https://mcp.opencaselaw.ch` |
| REST API documentation | [mcp.opencaselaw.ch/api/docs](https://mcp.opencaselaw.ch/api/docs) |
| Decision pages (Schema.org LegalCase) | `https://mcp.opencaselaw.ch/entscheid/{id}` |
| Live statistics | [opencaselaw.ch](https://opencaselaw.ch) |
| Benchmark queries | `benchmarks/search_relevance_golden.json` in repository |

## References

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
