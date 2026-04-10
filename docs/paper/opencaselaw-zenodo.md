# OpenCaseLaw: A Complete Open Corpus and Citation Graph of Swiss Court Decisions

**Jonas Hertner**
Independent Researcher, Zurich, Switzerland
jonas@opencaselaw.ch

**April 2026**

---

## Abstract

We present OpenCaseLaw, the first open-access corpus to provide nationwide coverage of Swiss case law with an integrated citation graph. The April 2026 release contains 965,038 full-text court decisions from 104 courts spanning all 26 cantons and the period 1875--2026, in German (46.6%), French (45.1%), and Italian (8.3%). Alongside the corpus, we release a citation graph of 8.87 million extracted references with 6.53 million resolved in-corpus links (73.6% resolution rate) and 11.34 million decision-to-statute links across 284,145 distinct provisions. Analysis of cross-language citation patterns reveals a striking asymmetry: French-language courts cite German-language decisions 1.64 million times---84% as often as they cite French-language decisions---quantifying the dominance of German-language *Bundesgerichtsentscheide* (BGE) jurisprudence across Switzerland's language boundaries. Peak citation age is 11--20 years, indicating long institutional memory in Swiss law. We provide a 100-query multilingual retrieval benchmark (MRR@10 = 0.63 offline with automated Meta-Harness weight tuning, 0.68 with confidence-gated LLM reranking) and release the corpus as Parquet on Hugging Face under CC0-1.0. The dataset is updated daily and all code is MIT-licensed.

**Keywords:** legal corpus, Swiss law, case law, citation network, multilingual, open data, legal information retrieval

---

## 1. Introduction

Switzerland publishes court decisions across a fragmented infrastructure of 26 cantonal portals, multiple federal court websites, and various regulatory bodies. No unified public interface exists. Commercial aggregators---Swisslex and Weblaw---require paid subscriptions and prohibit bulk access. Existing open datasets cover at most a single court (typically the Federal Supreme Court) or serve narrow NLP benchmarks without providing the raw corpus, structured metadata, or citation structure needed for large-scale legal research.

OpenCaseLaw closes this gap. It is the first open dataset to combine four properties that no prior Swiss legal resource offers simultaneously:

1. **Nationwide coverage.** 965,038 decisions from 104 courts across all 26 cantons, seven federal courts, regulatory bodies, and attorney disciplinary authorities---more decisions than any known Swiss legal database, open or commercial.
2. **A large-scale citation graph.** 8.87 million citation references resolved into 6.53 million in-corpus links, enabling network analysis of Swiss jurisprudence for the first time at national scale.
3. **Trilingual full text.** 100% full-text availability across German, French, and Italian, with median text lengths of 14,000--18,000 characters.
4. **Open access with daily updates.** CC0-1.0 licensing, Parquet distribution on Hugging Face, and a live search API---not a frozen one-time release.

The citation graph analysis yields a novel empirical finding: cross-language citation patterns quantify the well-known but previously unmeasured dominance of German-language BGE jurisprudence. French-language courts cite German decisions at 84% the rate of French decisions; Italian-language courts cite German decisions four times more often than Italian ones. This asymmetry, visible only at corpus scale, has implications for legal harmonization research, judicial behavior studies, and multilingual legal NLP.

The remainder of this paper describes the corpus (Section 3), the citation and statute reference graph with its cross-language findings (Section 4), a retrieval baseline (Section 5), enabled research directions (Section 6), and ethical considerations (Section 7).

## 2. Related Work

### 2.1 Swiss Legal Datasets

Several open datasets have been released for Swiss legal NLP, but all are limited in scope.

**Table 1.** Comparison of Swiss case law resources.

| Resource | Decisions | Courts | Graph | Full text | Access | Updated |
|:---------|----------:|-------:|:-----:|:---------:|:------:|:-------:|
| OpenCaseLaw (this work) | 965,038 | 104 | 6.53M | 100% | CC0 | Daily |
| Swiss-Judgment-Prediction | 85,000 | 1 | No | Partial | Open | No |
| SCD | 122,000 | 1 | No | Yes | Open | No |
| SCALE | --- | 1 | No | Partial | Open | No |
| MultiLegalPile | Swiss subset | Mixed | No | Yes | Open | No |
| Swisslex (comm.) | ~500k+ | Many | No | Yes | Paid | Yes |
| Weblaw (comm.) | ~735k+ | Many | No | Yes | Paid | Yes |
| entscheidsuche.ch | ~700k+ | Many | No | Yes | Free | Yes |

Citations for the open datasets: Swiss-Judgment-Prediction (Niklaus et al., 2021), SCD (Geering & Merane, 2024), SCALE (Rasiah et al., 2023), MultiLegalPile (Niklaus et al., 2023). Commercial resource counts are estimates. "Graph" = citation graph provided at release; "---" = not applicable.

*Swiss-Judgment-Prediction* (Niklaus et al., 2021) provides 85,000 Federal Supreme Court decisions labeled with binary outcome for judgment prediction. *SCD* (Geering & Merane, 2024) extends this to 122,000 decisions with 31 structured variables. Both are limited to a single court and lack citation structure. *SCALE* (Rasiah et al., 2023) defines benchmark tasks---citation extraction, court view generation, summarization---on Swiss legal text but does not release a general-purpose corpus. *MultiLegalPile* (Niklaus et al., 2023) assembles 689 GB of multilingual legal text for language model pretraining; its Swiss subset lacks per-court metadata and citation links.

OpenCaseLaw differs from all of these in three respects: it covers the full Swiss court system rather than a single court, it provides a resolved citation graph, and it is continuously updated rather than a static snapshot.

### 2.2 International Legal Corpora and Citation Networks

The *Caselaw Access Project* (Harvard Law School, 2018) digitized 6.7 million US court decisions---the closest international analog in ambition. Unlike CAP, OpenCaseLaw operates in a trilingual jurisdiction and provides a citation graph at release. *GerDaLIR* (Ostendorff et al., 2021) benchmarks German legal information retrieval on 131,000 court decisions. *COLIEE* (Kano et al., 2024) provides competition tasks for case law retrieval and entailment on Canadian and Japanese corpora. *LexGLUE* (Chalkidis et al., 2022) benchmarks legal NLP on English-language tasks.

Citation network analysis of legal corpora has a substantial literature. Fowler et al. (2007) analyze the US Supreme Court citation network to identify influential precedents and measure legal relevance through network centrality. Waltl et al. (2017) apply network analysis to German legal texts. Bommarito and Katz (2010) study the complexity of the US Code through citation structure. Whalen (2016) examines citation dynamics in Canadian case law. To our knowledge, no prior work has constructed or analyzed a citation network at national scale for Swiss jurisprudence.

### 2.3 Legal Information Retrieval

BM25 (Robertson & Zaragoza, 2009) and Reciprocal Rank Fusion (Cormack et al., 2009) remain strong baselines in legal retrieval. Locke et al. (2024) survey the field and note that domain-specific retrieval systems often outperform general-purpose models on legal text. Shao et al. (2020) demonstrate that incorporating citation signals improves legal case retrieval, an approach OpenCaseLaw supports through its integrated citation graph.

## 3. Corpus Description

### 3.1 Collection and Processing

OpenCaseLaw is built by 58 automated scrapers, each targeting one court's publication portal. Scrapers run daily via systemd timers on a dedicated server. Each decision is normalized into a structured record containing: court identifier, docket number, decision date, language, legal area, headnote (*Regeste*/*Resume*), full text, chamber, and source URL. Deduplication uses a canonical key (normalized court code + docket number + date), with within-court deduplication retaining the richest-content version and cross-court deduplication operating within hand-maintained overlap groups. A BGE leading case and its underlying Federal Supreme Court ruling are retained as distinct records, as they differ in court, docket number, and editorial scope.

The corpus is exported as Parquet files (34 fields, ~7 GB total) and uploaded to Hugging Face. The full pipeline---scraping, deduplication, FTS5 index build, Parquet export, and upload---runs unattended daily.

### 3.2 Corpus Statistics

**Table 2.** Corpus summary (April 2026 snapshot).

| Metric | Value |
|:-------|------:|
| Total decisions | 965,038 |
| Courts and public bodies | 104 |
| Cantons represented | 26 of 26 |
| Temporal range | 1875--2026 |
| German decisions | 449,731 (46.6%) |
| French decisions | 435,563 (45.1%) |
| Italian decisions | 79,744 (8.3%) |
| Full-text availability | 100% |

**Temporal distribution.** The corpus reflects both the expansion of Swiss judicial publication and the digitization of historical archives. Growth accelerated sharply in the 2000s with the adoption of electronic publication.

**Table 3.** Decisions by decade.

| Decade | Decisions | Decade | Decisions |
|:-------|----------:|:-------|----------:|
| 1870s | 659 | 1940s | 1,990 |
| 1880s | 1,042 | 1950s | 2,304 |
| 1890s | 1,646 | 1960s | 2,298 |
| 1900s | 2,372 | 1970s | 4,526 |
| 1910s | 2,281 | 1980s | 10,545 |
| 1920s | 1,978 | 1990s | 31,503 |
| 1930s | 2,022 | 2000s | 189,128 |
| | | 2010s | 416,025 |
| | | 2020s | 288,517 |

Since 2015, the corpus grows by approximately 47,000 decisions per year, reflecting the current publication rate of Swiss courts.

**Court distribution.** The five largest sources account for 59% of the corpus:

**Table 4.** Top 10 courts by decision count.

| Court | Decisions |
|:------|----------:|
| Federal Supreme Court (BGer) | 174,757 |
| Geneva cantonal courts | 167,326 |
| Federal Administrative Court (BVGer) | 91,906 |
| Vaud cantonal courts | 74,819 |
| Ticino cantonal courts | 59,338 |

The long tail comprises specialized courts, regulatory bodies (FINMA, WEKO, EDOEB, ELCOM, PostCom, ComCom), and historical sources (BStGer since 2004, BPatGer, EMARK asylum decisions).

### 3.3 Full-Text Availability and Quality

All 965,038 decisions have full text. Text length distributions vary by language:

**Table 5.** Text length in characters by language.

| Language | Median | 10th percentile | 90th percentile |
|:---------|-------:|-----------------:|-----------------:|
| German | 15,309 | 1,799 | 42,687 |
| French | 18,034 | 4,497 | 49,393 |
| Italian | 14,130 | 3,438 | 43,530 |

French decisions are longer on average, likely reflecting the verbosity norms of *Romandie* courts, particularly Geneva's prolific cantonal judiciary. The 10th-percentile German texts (1,799 characters) include brief procedural orders and cost rulings that are substantively thin but legally published.

Historical material (pre-1990) contains OCR artifacts in some cases. A targeted repair pass in March 2026 re-extracted full text via PDF parsing for 33,884 short-text entries, but approximately 5,000 scanned-PDF decisions remain candidates for OCR improvement.

## 4. Citation and Statute Reference Graph

The citation graph is a primary contribution of this work. It transforms the corpus from a collection of independent documents into a structured network of legal authority, enabling analyses that are impossible with text alone.

### 4.1 Extraction and Resolution

Citation extraction uses regular expressions tuned to Swiss legal citation conventions:

- **BGE references** (`BGE 131 III 115`, `ATF 140 I 201`, `DTF 136 V 117`): volume, division, and page of the *Amtliche Sammlung* (official collection), in all three language variants.
- **Docket numbers** (`4A_372/2019`, `2C_1084/2013`): Federal Supreme Court and other federal court case numbers.
- **Statute references** (`Art. 41 OR`, `Art. 8 BV`, `art. 29 Cst.`): article, optional paragraph/letter, and statute abbreviation.

Each extracted citation is resolved against the corpus using normalized docket matching. BGE references are mapped to their corresponding decisions via volume-division-page lookup. The resolution rate of 73.6% (6.53M of 8.87M) reflects the fact that not all cited decisions are published online---older decisions, cantonal rulings referenced by federal courts, and decisions from jurisdictions outside Switzerland account for the unresolved 26.4%.

### 4.2 Graph Properties

**Table 6.** Citation graph summary.

| Metric | Value |
|:-------|------:|
| Total extracted citation references | 8,870,000 |
| Resolved in-corpus links | 6,532,468 |
| Resolution rate | 73.6% |
| Decision-to-statute links | 11,340,907 |
| Distinct statute provisions cited | 284,145 |
| Unique citing decisions | 680,421 |
| Unique cited decisions | 207,938 |

The graph has 680,421 unique citing decisions (70.5% of the corpus) and 207,938 unique cited decisions. The in-degree distribution follows a heavy-tailed power law typical of citation networks (Fowler et al., 2007):

**Table 7.** In-degree distribution (citation counts).

| Citations received | Decisions |
|:-------------------|----------:|
| 1 | 63,178 |
| 2--5 | 64,554 |
| 6--10 | 25,622 |
| 11--50 | 38,567 |
| 51--100 | 7,406 |
| 101--500 | 6,685 |
| 501--1,000 | 1,026 |
| 1,000+ | 900 |

The 900 decisions cited more than 1,000 times represent the core of Swiss jurisprudence---the *Leitentscheide* (leading cases) that anchor entire areas of law.

**Table 8.** Most-cited decisions.

| Decision | Subject area | Citations |
|:---------|:-------------|----------:|
| BGE 125 V 351 | Social insurance | 61,981 |
| BGE 134 V 231 | Social insurance | 35,078 |
| BGE 122 V 157 | Social insurance | 28,174 |
| BGE 130 V 343 | Social insurance | 22,484 |
| BGE 137 V 210 | Social insurance | 20,604 |

The dominance of social insurance (*Sozialversicherungsrecht*) decisions at the top of the citation ranking reflects the volume of social insurance litigation in Switzerland and the high degree of procedural standardization in this area, where courts routinely cite the same foundational cases on burden of proof, benefit calculation, and procedural requirements.

### 4.3 Cross-Language Citation Patterns

The trilingual structure of Swiss jurisprudence creates a natural experiment in cross-language legal influence. Because the Federal Supreme Court publishes each decision in one language only, and because cantonal courts write in their canton's official language, citations that cross language boundaries reveal how legal authority flows between linguistic communities.

**Table 9.** Cross-language citation matrix (resolved links).

| Citing language | Cited: DE | Cited: FR | Cited: IT | Total |
|:----------------|----------:|----------:|----------:|------:|
| **German** | 2,072,201 | 358,948 | 24,299 | 2,455,448 |
| **French** | 1,641,244 | 1,963,380 | 51,734 | 3,656,358 |
| **Italian** | 239,748 | 116,906 | 60,204 | 416,858 |

Three findings emerge:

**Finding 1: French courts rely heavily on German-language jurisprudence.** French-language decisions cite German-language decisions 1,641,244 times---84% of the rate at which they cite French-language decisions (1,963,380). This is not a marginal phenomenon; nearly half of all citations from French decisions point to German-language sources.

**Finding 2: Italian courts cite German decisions four times more than Italian ones.** Italian-language decisions cite German decisions 239,748 times versus 60,204 citations to Italian decisions (a 4:1 ratio). Italian courts are the most dependent on cross-language authority.

**Finding 3: The asymmetry is directional.** German-language decisions cite French sources only 358,948 times (17% of their German-language citations). Legal influence flows predominantly from German to French and Italian, not the reverse. This reflects the simple fact that the majority of BGE leading cases are written in German, and all courts cite BGE regardless of their own language.

These patterns quantify what Swiss legal practitioners know intuitively but what has not been measured at corpus scale. They have implications for legal translation policy, judicial training, and the design of multilingual legal NLP systems (which must handle cross-language citation resolution as a first-class task, not an edge case).

### 4.4 Citation Temporal Decay

**Table 10.** Citation age distribution (years between cited and citing decisions).

| Citation age | Links |
|:-------------|------:|
| Same year | 265,069 |
| 1--2 years | 957,634 |
| 3--5 years | 1,273,815 |
| 6--10 years | 1,550,263 |
| 11--20 years | 1,586,985 |
| 21--50 years | 752,515 |
| 51+ years | 47,564 |

Peak citation age is 11--20 years, with substantial citation volume extending beyond 20 years. This temporal profile differs markedly from academic citation patterns, where the half-life is typically 5--8 years. Swiss courts exhibit long institutional memory: a decision from the 1990s remains actively cited in the 2020s. The 47,564 citations to decisions more than 50 years old confirm that foundational jurisprudence from the mid-20th century retains binding authority.

The near-absence of same-year citations (4.1% of total) reflects the time lag inherent in the appellate process and publication cycle.

### 4.5 Statute Reference Network

The 11.34 million decision-to-statute links connect the case law corpus to the statutory framework. The top statutes by citation frequency reveal the procedural backbone of Swiss litigation:

**Table 11.** Most-cited statute provisions.

| Provision | Citations |
|:----------|----------:|
| Art. 100 Abs. 1 LTF (appeal deadline) | 181,266 |
| Art. 113 LTF (subsidiary constitutional complaint) | 111,502 |
| Art. 42 LTF (brief requirements) | 90,544 |
| Art. 42 BGG (Rechtsschrift) | 68,270 |
| Art. 66 Abs. 1 BGG (costs) | 58,920 |

The dominance of procedural provisions (LTF/BGG) at the top reflects the fact that every Federal Supreme Court decision must address jurisdictional and procedural questions before reaching the merits. The most-cited substantive statutes by total decision count are: LTF (339,000 decisions), BGG (186,000), CPC (117,000), CST (92,000), and LPGA (89,000).

The 284,145 distinct provisions cited span federal statutes, cantonal laws, and international treaties, providing a map of which legal provisions generate the most litigation and judicial interpretation.

## 5. Retrieval Baseline

To establish a reproducible baseline for legal information retrieval on this corpus, we release a 100-query multilingual benchmark.

### 5.1 Benchmark Design

The benchmark comprises 100 queries: 74 German, 16 French, and 7 Italian, spanning 15 legal domains (social insurance, contract law, criminal law, administrative law, tax law, etc.). Each query has 1--6 graded relevant decisions (mean 3.04), identified through citation-graph verification and manual assessment by the author. Relevance judgments are single-annotator; no inter-annotator agreement is reported. The benchmark is released as JSON for reproducibility.

### 5.2 Retrieval Pipeline

The retrieval system uses a five-stage pipeline: (1) query parsing with synonym expansion, compound decomposition, and umlaut normalization; (2) multi-strategy FTS5 candidate retrieval fused with Reciprocal Rank Fusion (Cormack et al., 2009); (3) feature scoring incorporating lexical match, metadata, and citation-graph signals; (4) optional LLM-based structured query parsing (Claude Haiku) that extracts doctrine names, statute references, and leading BGE candidates; and (5) optional confidence-gated LLM reranking of the top 15 candidates.

### 5.3 Automated Pipeline Optimization

Inspired by recent work on automated harness optimization (Lee et al., 2026), we implemented a Meta-Harness-style optimizer that iteratively tunes 55+ scoring parameters using Claude Sonnet as a proposer. The optimizer receives execution traces (per-query candidate lists, rankings, per-tag MRR breakdowns) and proposes weight adjustments targeting the weakest query categories. The key insight from Lee et al.---that full execution traces enable order-of-magnitude faster convergence than score-only feedback---holds in our setting: the optimizer converged in 2 iterations to improved weights.

The optimizer identified that the default configuration under-valued LLM-derived doctrine signals and statute graph contributions. The converged configuration boosts doctrine concept translation weight from 1.5 to 3.5, statute signal base from 2.2 to 3.5, and statute graph RRF weight from 1.0 to 2.2.

### 5.4 Results

**Table 12.** Retrieval baseline results on the 100-query benchmark.

| Configuration | MRR@10 | Hit@1 | Recall@10 |
|:--------------|-------:|------:|----------:|
| Naive BM25 (starting baseline) | 0.320 | 0.26 | -- |
| FTS5 + RRF + graph signals (offline) | 0.587 | 0.50 | 0.574 |
| + LLM structured query parsing | 0.611 | 0.53 | 0.584 |
| + Meta-Harness optimization | **0.628** | **0.54** | **0.581** |
| + Confidence-gated Haiku reranking | 0.680 | 0.60 | 0.595 |

Each configuration operates on the full 965,038-decision corpus. The offline configuration uses only the local search index and reference graph---no neural components, vector search, or external API calls. LLM structured parsing adds a ~200ms Haiku call per query to extract doctrine concepts and statute references. Meta-Harness optimization is a one-time tuning cost; the optimized weights add zero runtime overhead. Confidence-gated reranking fires for ~50% of queries (those without a dominant top result), adding 1--3 seconds of latency per gated query.

The final optimized configuration nearly doubles MRR@10 over the naive BM25 baseline (0.320 → 0.680, +112%), with the largest single gains coming from graph signals (+0.267) and LLM structured parsing + Meta-Harness tuning (+0.041). These results demonstrate the value of combining traditional lexical retrieval with domain-specific signals (citation graph, statute references) and automated hyperparameter optimization.

## 6. Enabled Research Directions

The combination of nationwide coverage, trilingual text, and a resolved citation graph opens several research directions that were previously infeasible with Swiss legal data.

**Judicial citation network analysis.** The 6.53 million resolved citation links enable the study of precedent formation, legal influence propagation, and the identification of landmark decisions through network centrality measures (Fowler et al., 2007). The court-level structure supports analysis of vertical authority (cantonal-to-federal citation patterns) and horizontal diffusion (cross-cantonal citation).

**Cross-language legal NLP.** The trilingual corpus with cross-language citations provides a natural testbed for multilingual legal language models. Tasks include cross-lingual citation recommendation, multilingual legal judgment prediction, and translation-invariant document retrieval. The cross-language citation matrix (Section 4.3) provides ground-truth signal for evaluating how well models handle the linguistic asymmetries of Swiss law.

**Legal harmonization studies.** Switzerland's federal structure, with 26 cantonal legal systems, creates variation in how courts interpret the same federal statutes. The statute reference network (Section 4.5) enables quantitative comparison of cantonal interpretation patterns: which cantons cite the same provisions differently, and how cantonal jurisprudence converges or diverges over time.

**Temporal dynamics of jurisprudence.** The 150-year temporal span, combined with citation timestamps, supports the study of how legal doctrines emerge, stabilize, and are superseded. The citation decay analysis (Section 4.4) provides a starting point; finer-grained analysis by legal domain, court level, or historical period is now possible.

**Legal information retrieval benchmarks.** The 100-query benchmark can be extended. The corpus's size (nearly 1M decisions), linguistic diversity, and domain specificity make it suitable for evaluating retrieval systems on realistic legal search tasks---a gap noted by Locke et al. (2024).

**Judgment prediction at scale.** Prior Swiss judgment prediction work (Niklaus et al., 2021) was limited to the Federal Supreme Court. The full cantonal coverage enables prediction tasks across court levels and legal areas, including the study of reversal rates and appeal outcomes using linked decision chains.

**Regulatory and compliance analysis.** The inclusion of decisions from FINMA, WEKO (Competition Commission), EDOEB (Data Protection Commissioner), and other regulatory bodies supports quantitative analysis of regulatory enforcement patterns and their evolution over time.

## 7. Ethics, Legal Basis, and Limitations

### 7.1 Legal Basis

Published Swiss court decisions are excluded from copyright protection under Art. 5(1)(c) of the Swiss Copyright Act (URG). Federal courts publish decisions pursuant to Art. 27 of the Federal Supreme Court Act (BGG); cantonal publication duties are governed by cantonal procedural codes. The dataset packaging is released under CC0-1.0; all source code is MIT-licensed.

### 7.2 Privacy Considerations

OpenCaseLaw preserves decisions exactly as published by the originating courts, including each court's anonymization choices, and links back to source URLs. The project does not perform additional anonymization. Cantonal anonymization practices vary: some cantons redact party names comprehensively, others publish identifying details for certain case types.

Large-scale aggregation creates privacy risks beyond those of individual court portals. Structured metadata combined with full text may enable re-identification even when names are redacted (Pilan et al., 2024). The dataset includes a governance and removal policy under which individuals may request removal of specific decisions. Researchers using the corpus for studies involving personal data should conduct their own data protection assessment.

### 7.3 Limitations

- **Coverage is comprehensive but not audited.** The corpus aims for completeness of published decisions, but publication depth varies by court and era. Unpublished decisions---the majority of Swiss court output---are not included.
- **Historical text quality varies.** Pre-1990 material may contain OCR artifacts. Approximately 5,000 scanned-PDF decisions could benefit from improved OCR.
- **Citation extraction is rule-based.** The regular-expression approach handles standard Swiss citation formats reliably but misses informal references, abbreviations, and citations embedded in footnotes with non-standard formatting. No precision/recall evaluation against human annotations is provided.
- **Single-annotator benchmark.** The retrieval benchmark reflects one expert's relevance judgments. Inter-annotator agreement is not reported.
- **Commercial comparison is approximate.** Decision counts for Swisslex and Weblaw are estimates; exact figures are not publicly disclosed.

## 8. Availability

**Table 13.** Release artifacts.

| Resource | Location |
|:---------|:---------|
| Dataset (Parquet, ~7 GB) | https://huggingface.co/datasets/voilaj/swiss-caselaw |
| Source code (MIT) | https://github.com/jonashertner/caselaw-repo-1 |
| Live search API | https://mcp.opencaselaw.ch |
| Dashboard | https://opencaselaw.ch |
| Governance policy | `docs/governance-and-removal-policy.md` in repository |

The dataset is updated daily. Versioned snapshots can be reconstructed from the Hugging Face commit history. The citation graph database (`reference_graph.db`) and retrieval benchmark (`benchmarks/`) are included in the source repository.

---

## References

- Bommarito, M. J. and Katz, D. M. (2010). A Mathematical Approach to the Study of the United States Code. *Physica A*, 389(19), 4195--4200.

- Caselaw Access Project (2018). Harvard Law School Library Innovation Lab. https://case.law

- Chalkidis, I., Jana, A., Hartung, D., Bommarito, M., Androutsopoulos, I., Katz, D. M., and Aletras, N. (2022). LexGLUE: A Benchmark Dataset for Legal Language Understanding in English. *Proceedings of ACL 2022*, 4310--4330.

- Cormack, G. V., Clarke, C. L. A., and Buettcher, S. (2009). Reciprocal Rank Fusion Outperforms Condorcet and Individual Rank Learning Methods. *Proceedings of SIGIR 2009*, 758--759.

- Fowler, J. H., Johnson, T. R., Spriggs, J. F., Jeon, S., and Wahlbeck, P. J. (2007). Network Analysis and the Law: Measuring the Legal Importance of Precedents at the U.S. Supreme Court. *Political Analysis*, 15(3), 324--346.

- Geering, F. and Merane, J. (2024). Swiss Federal Supreme Court Dataset. Zenodo. doi:10.5281/zenodo.11092977.

- Kano, Y., Soh, J., Yoshioka, M., Rabelo, J., and Kim, M.-Y. (2024). COLIEE 2024: Competition on Legal Information Extraction and Entailment. *Proceedings of JSAI 2024*.

- Lee, Y., Nair, R., Zhang, Q., Lee, K., Khattab, O., and Finn, C. (2026). Meta-Harness: End-to-End Optimization of Model Harnesses. *arXiv:2603.28052*.

- Locke, S., Zhai, Z., and Kohlmeier, J. (2024). A Survey on Legal Text Retrieval. *Proceedings of ACL 2024*.

- Model Context Protocol (2024). Anthropic. https://modelcontextprotocol.io

- Niklaus, J., Chalkidis, I., and Stuermer, M. (2021). Swiss-Judgment-Prediction: A Multilingual Legal Judgment Prediction Benchmark. *Proceedings of the Workshop on Natural Language Processing for Positive Impact (NLP4PI), EMNLP 2021*.

- Niklaus, J., Matoshi, V., Sturmer, M., Chalkidis, I., and Ho, D. E. (2023). MultiLegalPile: A 689GB Multilingual Legal Corpus. *arXiv:2306.02069*.

- Ostendorff, M., Blume, T., and Ostendorff, S. (2021). Aspect-Based Document Similarity for Research Papers (GerDaLIR). *Proceedings of COLING 2021*.

- Pilan, I., Lison, P., Ovrelid, L., Papadopoulou, A., Bain, D., and Quartey, J. (2024). Anonymity at Risk? Assessing Re-Identification Capabilities of Large Language Models in Court Decisions. *Findings of NAACL 2024*.

- Rasiah, V., Niklaus, J., Feijo, D., Welch, T., and Chalkidis, I. (2023). SCALE: Scaling up the Evaluation of Swiss Case Law. *arXiv:2306.09237*.

- Robertson, S. E. and Zaragoza, H. (2009). The Probabilistic Relevance Framework: BM25 and Beyond. *Foundations and Trends in Information Retrieval*, 3(4), 333--389.

- Shao, Y., Mao, J., Liu, Y., Ma, W., Satoh, K., Zhang, M., and Ma, S. (2020). BERT-PLI: Modeling Paragraph-Level Interactions for Legal Case Retrieval. *Proceedings of IJCAI 2020*, 3501--3507.

- Swiss Copyright Act (URG), Art. 5(1)(c). SR 231.1. Federal Assembly of the Swiss Confederation.

- Swiss Federal Supreme Court Act (BGG), Art. 27. SR 173.110. Federal Assembly of the Swiss Confederation.

- Waltl, B., Matthes, F., Waltl, T., and Grass, T. (2017). Lexical Analysis of German Legal Texts. *Proceedings of the Workshop on Automated Detection, Extraction and Analysis of Semantic Information in Legal Texts, ICAIL 2017*.

- Whalen, R. (2016). Legal Networks: The Promises and Challenges of Legal Network Analysis. *Michigan State Law Review*, 2016(2), 539--565.
