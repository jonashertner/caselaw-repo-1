# OpenCaseLaw: A Large-Scale Open Corpus and Citation Graph of Published Swiss Court Decisions

**Jonas Hertner**
Independent Researcher, Zurich, Switzerland
jh@jonashertner.com

**April 2026** — snapshot version 2026-04-10

---

## Abstract

Switzerland publishes court decisions across a fragmented infrastructure of 26 cantonal portals, multiple federal court websites, and regulatory agencies. No unified open resource exists. We release *OpenCaseLaw*, a large-scale open corpus of published Swiss court decisions together with a resolved citation and statute-reference graph. The April 10, 2026 snapshot contains 965,141 full-text decisions from 104 courts and regulatory bodies across all 26 cantons and the period 1875--2026, in German (46.6%), French (45.1%), and Italian (8.3%). The accompanying reference graph contains 6.53 million resolved decision-to-decision citation links and 11.34 million decision-to-statute references. To our knowledge, this is the first open dataset to couple comprehensive cantonal and federal coverage with a resolved citation graph for Swiss case law.

We report one empirical finding: cross-language citations from French- and Italian-speaking courts are dominated by references to the official collection (*Bundesgerichtsentscheide* / BGE), which is published primarily in German. Once BGE citations are removed, French-language courts cite French-language sources 4.6 times more often than German-language ones, and Italian-language courts cite Italian and German sources at roughly similar rates. The apparent "German dominance" of Swiss jurisprudence is therefore largely a BGE effect, not a general cross-language asymmetry.

The dataset is released as Parquet on Hugging Face under CC0-1.0; all code is MIT-licensed. A reproducible snapshot, a 100-query multilingual retrieval benchmark, and the reference-graph database are pinned to a specific commit for the version accompanying this paper.

**Keywords:** legal corpus, Swiss law, case law, citation network, multilingual, open data, legal information retrieval

---

## 1. Introduction

Switzerland publishes court decisions through a fragmented infrastructure: 26 cantonal portals, multiple federal court websites, and various regulatory agencies. No unified public interface exists. Commercial aggregators such as Swisslex and Weblaw provide broader coverage but require paid subscriptions and prohibit bulk access. Existing open datasets are limited to a single court (typically the Federal Supreme Court) or to narrow NLP benchmarks.

This paper describes *OpenCaseLaw*, an open corpus of published Swiss court decisions with a resolved citation graph. We make three concrete contributions:

1. **A large-scale open corpus** of 965,141 published decisions from 104 courts and regulatory bodies, with full text in German, French, and Italian. To our knowledge, this is the largest open collection of published Swiss court decisions and the only one that combines comprehensive cantonal and federal coverage with trilingual full text.

2. **A resolved citation and statute-reference graph.** We extract and resolve 8.87 million citation references (73.6% resolved to in-corpus targets) and 11.34 million decision-to-statute links across 284,145 distinct provisions. We release the graph as a SQLite database alongside the corpus.

3. **A reproducible retrieval baseline.** We release a 100-query multilingual benchmark with graded relevance judgments and report results for a BM25 + citation-graph pipeline. All artifacts are pinned to specific commit hashes for reproducibility.

*Scope and terminology.* Throughout this paper, "published" means decisions made publicly available by the originating court through an online publication channel. Unpublished decisions---the majority of Swiss court output---are out of scope. The corpus reflects what courts choose to publish; it does not claim to be complete with respect to all decisions ever rendered. We are careful to state empirical claims relatively ("to our knowledge") rather than absolutely, given that commercial database sizes are not publicly audited.

The remainder of the paper describes the corpus (Section 3), the citation and statute reference graph with a deliberately conservative cross-language analysis (Section 4), a data-quality and validation section (Section 5), a reproducible retrieval baseline (Section 6), availability and reproducibility (Section 7), and ethical considerations and limitations (Section 8).

## 2. Related Work

### 2.1 Swiss Legal Datasets

Several open datasets have been released for Swiss legal NLP, but all are limited in scope.

**Table 1.** Swiss case law resources. Dataset citations: Swiss-Judgment-Prediction (Niklaus et al., 2021), SCD (Geering & Merane, 2024), SCALE (Rasiah et al., 2023), MultiLegalPile (Niklaus et al., 2023). Commercial sizes are self-reported estimates; exact figures are not publicly audited. "Graph" indicates whether a citation graph is provided at release.

| Resource | Decisions | Courts | Graph | Full text | Access | Updated |
|:---------|----------:|-------:|:-----:|:---------:|:------:|:-------:|
| OpenCaseLaw (this work) | 965,141 | 104 | 6.53M | 100% | CC0 | Daily |
| Swiss-Judgment-Prediction | 85,000 | 1 | No | Partial | Open | No |
| SCD | 122,000 | 1 | No | Yes | Open | No |
| SCALE | --- | 1 | No | Partial | Open | No |
| MultiLegalPile (Swiss subset) | n/a | Mixed | No | Yes | Open | No |
| Swisslex (commercial, est.) | ~500k+ | Many | No | Yes | Paid | Yes |
| Weblaw (commercial, est.) | ~735k+ | Many | No | Yes | Paid | Yes |
| entscheidsuche.ch | ~700k+ | Many | No | Yes | Free | Yes |

*Swiss-Judgment-Prediction* (Niklaus et al., 2021) provides 85,000 Federal Supreme Court decisions labeled with binary outcome for judgment prediction. *SCD* (Geering & Merane, 2024) extends this to 122,000 decisions with 31 structured variables. Both are limited to a single court and lack citation structure. *SCALE* (Rasiah et al., 2023) defines benchmark tasks on Swiss legal text without releasing a general-purpose corpus. *MultiLegalPile* (Niklaus et al., 2023) assembles 689 GB of multilingual legal text for language model pretraining; its Swiss subset lacks per-court metadata and citation links. OpenCaseLaw differs in three respects: it covers the full Swiss published court system rather than a single court, it provides a resolved citation graph, and it is continuously updated rather than a static snapshot.

### 2.2 International Legal Corpora and Citation Networks

The *Caselaw Access Project* (Harvard Law School, 2018) digitized 6.7 million US court decisions---the closest international analogue in ambition. *GerDaLIR* (Althammer et al., 2021) is a German legal information retrieval benchmark with 131,000 court decisions. *COLIEE* (Kano et al., 2024) provides competition tasks for case law retrieval on Canadian and Japanese corpora. *LexGLUE* (Chalkidis et al., 2022) benchmarks legal NLP on English-language tasks.

Citation network analysis of legal corpora has a substantial literature. Fowler et al. (2007) analyse the US Supreme Court citation network to identify influential precedents through network centrality. Bommarito and Katz (2010) study the complexity of the US Code through citation structure. Whalen (2016) examines citation dynamics in Canadian case law. Waltl et al. (2017) apply network analysis to German legal texts. To our knowledge, no prior work has constructed or analysed a citation network at national scale for Swiss jurisprudence.

### 2.3 Legal Information Retrieval

BM25 (Robertson & Zaragoza, 2009) and Reciprocal Rank Fusion (Cormack et al., 2009) remain strong baselines in legal retrieval. Locke et al. (2024) survey the field and note that domain-specific retrieval systems often outperform general-purpose models on legal text. Shao et al. (2020) demonstrate that incorporating citation signals improves legal case retrieval.

## 3. Corpus Description

### 3.1 Collection and Processing

OpenCaseLaw is assembled by 58 court-specific scrapers that download decisions from each court's publication portal and normalise them into a unified schema. Scrapers run daily via systemd timers on a dedicated server. Each decision is stored as a structured record with 34 fields including court identifier, docket number, decision date, language, legal area, headnote (*Regeste*/*Résumé*), full text, chamber, and source URL.

Deduplication uses a canonical key derived from normalised court code, docket number, and decision date. Within-court deduplication retains the version with the richest content. Cross-court deduplication operates only within hand-maintained overlap groups (e.g., between a *Bundesgerichtsentscheide* (BGE) leading case and its underlying Federal Supreme Court ruling). BGE leading cases are retained as distinct records because they differ from the underlying BGer ruling in court code, docket number, editorial scope, and regeste; Section 4.1 discusses how this affects graph analysis.

The corpus is exported as 104 Parquet files (one per court, ~7 GB total) and uploaded to Hugging Face. The full pipeline---scraping, deduplication, FTS5 index build, Parquet export, and upload---runs unattended daily.

### 3.2 Corpus Statistics

**Table 2.** Corpus summary (April 10, 2026 snapshot).

| Metric | Value |
|:-------|------:|
| Total decisions | 965,141 |
| Courts and public bodies | 104 |
| Cantons represented | 26 of 26 |
| Temporal range | 1875--2026 |
| German decisions | 449,780 (46.6%) |
| French decisions | 435,611 (45.1%) |
| Italian decisions | 79,750 (8.3%) |

Romansh-language decisions are not represented because the four courts operating in Romansh (in Graubünden) do not publish electronically in Romansh.

**Temporal distribution.** The corpus reflects both the expansion of Swiss judicial publication and the digitization of historical archives. Table 3 buckets every decision in the snapshot, including pre-1870 early federal decisions (796), 2026-year-to-date decisions (6,233), and decisions where a reliable date could not be extracted (4,932). The buckets sum exactly to 965,141.

**Table 3.** Decisions by decade. Buckets sum to the corpus total.

| Period | Decisions | Period | Decisions |
|:-------|----------:|:-------|----------:|
| pre-1870 | 796 | 1950s | 2,304 |
| 1870s | 659 | 1960s | 2,298 |
| 1880s | 1,042 | 1970s | 4,526 |
| 1890s | 1,646 | 1980s | 10,545 |
| 1900s | 2,372 | 1990s | 31,503 |
| 1910s | 2,281 | 2000s | 189,134 |
| 1920s | 1,978 | 2010s | 416,026 |
| 1930s | 2,022 | 2020s | 282,854 |
| 1940s | 1,990 | 2026 YTD | 6,233 |
| | | Date unknown | 4,932 |

Since 2015, the corpus grows by approximately 47,000 decisions per year, reflecting the current publication rate of Swiss courts. The 4,932 decisions with unknown dates are concentrated in older historical sources and scanned-PDF collections where date extraction failed.

**Court distribution.** Table 4 lists the ten largest sources by decision count. The Vaud cantonal courts appear in three entries because the scraper tracks three distinct publication snapshots (`vd_gerichte` current, `vd_findinfo` 2008+ archive, `vd_omni` pre-2008 archive) that are retained as separate records because the corresponding source URLs differ; combined they contribute 155,639 decisions, which would make Vaud the third-largest source after the Federal Supreme Court and Geneva.

**Table 4.** Top 10 sources by decision count.

| Source | Decisions |
|:-------|----------:|
| Federal Supreme Court (bger) | 174,779 |
| Geneva cantonal courts (ge_gerichte) | 167,346 |
| Federal Administrative Court (bvger) | 91,919 |
| Vaud FindInfo archive (vd_findinfo, 2008+) | 74,819 |
| Ticino cantonal courts (ti_gerichte) | 59,341 |
| Vaud current (vd_gerichte) | 52,788 |
| BGE leading cases (bge) | 35,815 |
| Zurich Social Insurance Court | 33,761 |
| Vaud Jurisweb archive (vd_omni, pre-2008) | 28,032 |
| Zurich Upper Court (zh_obergericht) | 27,499 |

The long tail comprises specialised courts, regulatory bodies (FINMA, WEKO, EDOEB, ELCOM, PostCom, ComCom), historical sources (BStGer since 2004, BPatGer, EMARK asylum decisions), and attorney disciplinary authorities.

### 3.3 Field Completeness and Missingness

Swiss courts publish decisions with varying levels of structured metadata. Table 5 reports the share of records with a non-empty value for each of the most commonly-used fields. Field completeness should be interpreted alongside the knowledge that some courts (notably BGer and BVGer) produce rich structured metadata while many cantonal courts publish only full text.

**Table 5.** Field completeness by optional field.

| Field | Non-empty | Share |
|:------|----------:|------:|
| `docket_number` | 965,115 | 100.0% |
| `decision_date` | 960,209 | 99.5% |
| `regeste` (headnote) | 562,165 | 58.2% |
| `title` | 351,641 | 36.4% |
| `chamber` | 300,144 | 31.1% |
| `decision_type` | 140,389 | 14.5% |
| `outcome` | 90,735 | 9.4% |
| `full_text` | 965,141 | 100.0% |

All decisions have full text. Docket number and decision date are near-universal. Headnote, title, chamber, decision type, and outcome are systematically available only for a subset of courts (primarily federal and larger cantonal courts).

### 3.4 Full-Text Length

Text length distributions vary by language. French decisions tend to be longer on average, likely reflecting the drafting norms of courts in Romandie, particularly the prolific Geneva cantonal judiciary. The 10th-percentile German texts include brief procedural orders and cost rulings that are substantively thin but legally published.

Historical material (pre-1990) contains OCR artifacts in some cases. A targeted repair pass in March 2026 re-extracted full text via PDF parsing for 33,884 short-text entries, but approximately 5,000 scanned-PDF decisions remain candidates for improved OCR.

## 4. Citation and Statute Reference Graph

The citation graph is the main structural contribution of this work. It transforms the corpus from a collection of independent documents into a network of legal authority that can be queried, visualised, and analysed.

### 4.1 Extraction and Resolution

Citation extraction uses regular expressions tuned to Swiss legal citation conventions. We extract three reference types:

- **BGE references** (`BGE 131 III 115`, `ATF 140 I 201`, `DTF 136 V 117`): volume, division, and page of the *Amtliche Sammlung / Recueil officiel / Raccolta ufficiale*, in all three language variants.
- **Docket numbers** (`4A_372/2019`, `2C_1084/2013`): Federal Supreme Court and other federal court case numbers.
- **Statute references** (`Art. 41 OR`, `Art. 8 BV`, `art. 29 Cst.`): article, optional paragraph and letter, and statute abbreviation.

Each extracted decision-to-decision citation is resolved against the corpus using normalised docket matching; BGE references are mapped to their corresponding decisions via volume-division-page lookup. Table 6 summarises the resulting graph.

**Table 6.** Citation graph summary.

| Metric | Value |
|:-------|------:|
| Extracted decision citation references | 8,870,000 |
| Resolved in-corpus links | 6,533,534 |
| Resolution rate | 73.7% |
| Decision-to-statute links | 11,340,907 |
| Distinct statute provisions cited | 284,145 |
| Unique citing decisions (source ends) | 680,421 |
| Unique cited decisions (target ends) | 207,938 |

The 26.3% of references that do not resolve to an in-corpus target are concentrated in (a) older decisions not covered by online publication, (b) cantonal rulings cited by federal courts but not published by the originating canton, and (c) decisions from foreign jurisdictions (ECtHR, CJEU, German *Bundesgerichtshof*, etc.). We do not treat unresolved references as errors; they reflect the coverage gap between the published and citing corpora rather than extraction failures.

*BGE/BGer duplication.* BGE leading cases are published separately from the underlying Federal Supreme Court ruling, and both are indexed in the corpus. When a decision cites "BGE 140 III 264" we resolve it to the BGE record, not to the BGer record. In Section 4.3 we analyse BGE-citation patterns separately to avoid double-counting.

### 4.2 In-Degree Distribution

The in-degree distribution follows the heavy-tailed shape typical of citation networks (Fowler et al., 2007). A small number of "landmark" decisions concentrate a disproportionate share of all incoming citations.

**Table 7.** In-degree distribution (incoming citation counts per decision).

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

The 900 decisions cited more than 1,000 times correspond to the core of Swiss leading-case jurisprudence. Table 8 lists the five most cited decisions; all five are BGE rulings on social insurance law, reflecting both the volume of social insurance litigation in Switzerland and the high procedural standardisation in this area.

**Table 8.** Most-cited decisions.

| Decision | Subject area | Citations |
|:---------|:-------------|----------:|
| BGE 125 V 351 | Social insurance | 61,981 |
| BGE 134 V 231 | Social insurance | 35,078 |
| BGE 122 V 157 | Social insurance | 28,174 |
| BGE 130 V 343 | Social insurance | 22,484 |
| BGE 137 V 210 | Social insurance | 20,604 |

### 4.3 Cross-Language Citation Patterns

Swiss courts write in the official language of their canton or in the language of the underlying proceedings; the Federal Supreme Court produces each decision in one language only. Citations that cross language boundaries are therefore observable and can be used to ask whether legal authority flows asymmetrically between linguistic communities.

We report two views of the cross-language citation matrix. Table 9a counts every resolved citation; Table 9b counts only citations whose target is *not* a BGE leading case, which removes the effect of BGE citations being predominantly German.

**Table 9a.** Cross-language citation matrix (all 6,533,534 resolved links). Rows = citing-court language; columns = cited-decision language.

| Citing language | Cited: DE | Cited: FR | Cited: IT | Total |
|:----------------|----------:|----------:|----------:|------:|
| **German** | 2,074,160 | 359,280 | 24,328 | 2,457,768 |
| **French** | 1,642,206 | 1,964,785 | 51,766 | 3,658,757 |
| **Italian** | 239,843 | 116,939 | 60,227 | 417,009 |

**Table 9b.** Cross-language citation matrix, **BGE targets excluded** (2,328,745 resolved links, = 6,533,534 total − 4,204,789 BGE-target).

| Citing language | Cited: DE | Cited: FR | Cited: IT | Total |
|:----------------|----------:|----------:|----------:|------:|
| **German** | 760,264 | 89,846 | 6,151 | 856,261 |
| **French** | 234,136 | 1,082,541 | 12,209 | 1,328,886 |
| **Italian** | 54,065 | 40,681 | 48,852 | 143,598 |

Three observations follow.

**Observation 1: French courts cite German sources heavily, but mostly via BGE.** French-language decisions make 1,642,206 citations to German-language sources. Of these, 1,408,070 (86%) are citations to BGE leading cases, which are published predominantly in German. When BGE targets are removed, French-language courts cite French-language sources 4.6 times more often than German-language ones (1,082,541 vs 234,136). The headline "German dominance" of French jurisprudence is therefore largely a BGE effect, not a general cross-language asymmetry.

**Observation 2: Italian courts use cross-language citation more evenly.** Italian-language decisions cite German sources 239,843 times and Italian sources only 60,227 times overall. Once BGE targets are excluded, the counts become 54,065 (to German) and 48,852 (to Italian), which is close to parity. Italian courts appear to operate in a more genuinely multilingual citation environment than French courts, possibly because the Italian-speaking Swiss corpus is smaller and Italian practitioners more routinely consult German and French sources.

**Observation 3: The apparent directional asymmetry is confounded.** The raw matrix suggests that legal authority flows from German to French and Italian but not the reverse. This is at least partly explained by the BGE publication mix and by differences in court size and output per language region. We do not attempt a causal interpretation. A full analysis would need to control for publication volume, court type, and the BGE share of the citing court's authority base.

These observations describe what the citation matrix shows; they should not be read as claims about the functional authority of courts in different language regions. We invite researchers interested in legal harmonisation to use the raw graph for more rigorous analyses.

### 4.4 Citation Temporal Decay

Table 10 buckets the 6,494,553 resolved citation links for which both source and target decision dates are known. A further 38,981 resolved citations (0.6%) are excluded from the age analysis because either the source or the target lacks a parseable date. A small number of links have negative age (citing decision dated before the cited decision); these arise primarily from inter-court appeals where our date extraction pulls the appeal-instance date rather than the original ruling date.

**Table 10.** Citation age (years between cited and citing decisions), resolved links with dates on both ends. Buckets sum to 6,494,553.

| Citation age | Links |
|:-------------|------:|
| Negative (source before target) | 56,040 |
| Same year (0) | 265,185 |
| 1--2 years | 958,305 |
| 3--5 years | 1,274,589 |
| 6--10 years | 1,551,441 |
| 11--20 years | 1,588,264 |
| 21--50 years | 753,133 |
| 51+ years | 47,596 |
| **Total** | **6,494,553** |

Peak citation age is 11--20 years. Citation volume remains substantial for citations aged 21-50 years, consistent with Swiss courts exhibiting long institutional memory. *Important caveat:* the age distribution is affected by growth in corpus volume over time (very old decisions have had more time to be cited, but the citing base was much smaller in earlier decades) and by changes in publication practice. A proper analysis would need to normalise by the size of the citing base per year. We report the raw distribution and leave the controlled analysis to future work.

### 4.5 Statute Reference Network

The 11.34 million decision-to-statute links connect the case law corpus to the statutory framework. Swiss statutes are known by multiple abbreviations depending on language (for example, the Federal Court Act appears as *Bundesgerichtsgesetz* / BGG in German, *Loi sur le Tribunal fédéral* / LTF in French, and *Legge sul Tribunale federale* / LTF also in Italian). Our extraction preserves the surface form used in each decision; Table 11 presents both the raw surface-form totals and the canonicalised totals after merging aliases.

**Table 11.** Most-cited statutes after merging language aliases. Mentions are counted across all article-level provision identifiers sharing the same canonical statute.

| Canonical statute | Surface forms merged | Mentions |
|:------------------|:---------------------|---------:|
| Federal Court Act | BGG + LTF | 1,715,464 |
| Civil Procedure Code | ZPO + CPC | 991,614 |
| Criminal Procedure Code | StPO + CPP | 905,448 |
| Penal Code | StGB + CP | 472,355 |
| Civil Code | ZGB + CC | 426,270 |
| Code of Obligations | OR + CO | 327,071 |
| Federal Constitution | BV + Cst. | 299,540 |
| Foreign Nationals Act | AIG + LEtr | 124,894 |

The dominance of procedural provisions (Federal Court Act, Civil Procedure Code, Criminal Procedure Code) at the top of the list reflects that every Federal Supreme Court decision must address jurisdictional and procedural questions before reaching the merits. The raw surface-form top 10 (before merging) is dominated by aliases of the Federal Court Act, as expected: `ART.100.ABS.1.LTF` (181,294 mentions), `ART.113.LTF` (111,512), `ART.42.LTF` (90,546), `ART.42.BGG` (68,295), and `ART.66.ABS.1.BGG` (58,926). Counting surface forms separately would double-count the same provision and inflate rank positions; the dataset release includes the alias map as a JSON file so that downstream analyses can canonicalise consistently.

The 284,145 distinct provision identifiers (before alias canonicalisation) span federal statutes, cantonal laws, and international treaties.

## 5. Data Quality and Validation

Scale is insufficient to make a dataset useful; coverage, extraction quality, and resolution accuracy all matter. This section reports what we have validated, what we have not, and where the weaknesses of the current release lie.

### 5.1 Citation Extraction

Our citation extractor uses regular expressions tuned to Swiss citation conventions. We have not yet performed a formal precision/recall evaluation against a human-annotated gold set. What we can report:

- **Structural checks.** 100% of extracted BGE references parse to valid (volume, division, page) triples. 100% of extracted docket numbers parse to valid federal court case-number patterns. Invalid candidates (e.g., overlapping year ranges) are rejected at extraction time.
- **Resolution rate.** 73.7% of extracted decision-citation references resolve to a target in the corpus. The residual 26.3% is dominated by citations to decisions not published online, foreign jurisdictions, and pre-publication Federal Supreme Court rulings.
- **Known failure modes.** The regular-expression approach misses citations embedded in footnotes with unusual formatting, citations using informal abbreviations ("the *Genf* case"), and citations where the docket number spans a line break in the source PDF. A conservative estimate based on manual spot-checks of 20 decisions is that an additional 2--5% of reference-like strings are missed.

A human-annotated gold set of 100 decisions would be sufficient to report precision/recall and is planned for the next release. We publish the extractor code so that others can evaluate it independently.

### 5.2 Resolution Accuracy

We spot-checked 50 randomly-sampled resolved citations against the cited decisions. 48/50 (96%) were correctly resolved; 2 were BGE alias ambiguities where the same BGE volume/division/page triple was associated with two candidate decisions and our resolver picked the one with more context matches. We do not report a confidence interval on this small sample; a larger evaluation is planned.

### 5.3 Deduplication

Our canonical key (normalised court code + docket number + date) is tight by construction, but language variants of the same decision (e.g., a BGer decision published in German and French) can produce two records with different canonical keys. We do not merge language variants; downstream analyses that need to treat them as one should use the `decision_group_id` field (when available) or perform document-level deduplication.

### 5.4 Field Completeness

See Section 3.3 and Table 5. No field is 100% complete except `docket_number` and `full_text`. Users querying optional fields (regeste, chamber, outcome) should handle missingness explicitly.

### 5.5 Limitations of This Section

This validation section is intentionally modest. A full data-quality audit would require (a) a human-annotated citation gold set, (b) a manual accuracy check on a larger resolution sample, (c) a deduplication error-rate estimate with independent human review, and (d) a statute-normalisation gold set. We consider these essential next steps and invite contributions.

## 6. Retrieval Baseline

We release a 100-query multilingual retrieval benchmark to enable reproducible comparisons on Swiss case-law retrieval.

### 6.1 Benchmark Design

The benchmark comprises 100 queries. Queries were originally tagged with language labels; 74 carry a German tag, 16 a French tag, 7 an Italian tag, and 3 are untagged (their query text is mixed-language or language-ambiguous). Queries span 15 legal domains (social insurance, contract law, criminal law, administrative law, tax law, tenancy, migration, and others). Each query has 1--6 graded relevant decisions (mean 3.04, grade scale 1--3), identified through citation-graph inspection and manual assessment by the author. Relevance judgments are single-annotator; no inter-annotator agreement is reported. The benchmark is released as a JSON file. We consider inter-annotator evaluation a necessary next step and will release an updated benchmark with multi-annotator judgments in a subsequent version.

### 6.2 Pipeline

The retrieval pipeline uses FTS5 full-text search with BM25 scoring, fused with the citation graph through a handful of domain-specific signals. Pipeline details and scoring weights are available in the source repository. The paper version of the benchmark corresponds to commit `<SHA>` of the repository.

### 6.3 Results

**Table 12.** Retrieval baseline on the 100-query benchmark (MRR@10, Hit@1, Recall@10, nDCG@10). All configurations operate on the full 965,141-decision corpus.

| Configuration | MRR@10 | Hit@1 | Recall@10 | nDCG@10 |
|:--------------|-------:|------:|----------:|--------:|
| Naive BM25 | 0.320 | 0.26 | 0.42 | 0.34 |
| + citation-graph signals | 0.587 | 0.50 | 0.574 | 0.50 |
| + LLM-based query parsing | 0.611 | 0.53 | 0.584 | 0.51 |
| + automated weight tuning | **0.628** | **0.54** | **0.581** | **0.52** |
| + LLM-based reranking | 0.680 | 0.60 | 0.595 | -- |

The dominant source of improvement over the naive BM25 baseline is the addition of citation-graph signals (+0.267 MRR). LLM-based query parsing adds +0.024 MRR (the LLM extracts doctrine names and statute references that are used to augment the lexical query). Automated weight tuning, described briefly in Appendix A, adds a further +0.017. Optional LLM-based reranking on the top 15 results adds another +0.052. We do not report bootstrap confidence intervals in this version; a rigorous comparison with significance testing is planned.

*Per-language results and pipeline sensitivity analysis (excluding and including each signal group separately) are reported in the supplementary material released with the benchmark.*

## 7. Availability and Reproducibility

The corpus is updated daily and the main repository advances continuously. To support reproducibility, the version of the corpus, graph, benchmark, and code corresponding to this paper is pinned to fixed identifiers.

**Table 13.** Pinned artifacts for the April 2026 snapshot.

| Artifact | Identifier |
|:---------|:-----------|
| Corpus snapshot date | 2026-04-10 |
| Decisions in snapshot | 965,141 |
| Hugging Face dataset | `voilaj/swiss-caselaw` |
| Hugging Face commit | (pinned at publication; see `docs/paper/REPRODUCIBILITY.md`) |
| Source code repo | `github.com/jonashertner/caselaw-repo-1` |
| Code commit | (pinned at publication) |
| Benchmark file | `benchmarks/search_relevance_golden.json` |
| Reference graph | `output/reference_graph.db` (re-buildable from corpus + code) |
| Statute alias map | `output/statute_aliases.json` (derived from Fedlex) |

Users who need the exact numbers in this paper should check out the pinned code commit and use the pinned Hugging Face commit. The live dataset continues to grow; a direct comparison of future snapshot statistics with the numbers in this paper requires rebuilding against the pinned snapshot.

**Removal policy.** Once the corpus is mirrored by third parties, removal is not fully reversible. Our governance policy (in the repository) commits to (a) removing specific decisions from the next published Hugging Face snapshot on justified request, (b) honouring court-originated anonymisation requests, and (c) not republishing decisions that have been withdrawn by the originating court. We cannot force third-party mirrors to remove data, and we say so plainly.

## 8. Ethics and Limitations

### 8.1 Legal Basis

Published Swiss court decisions are excluded from copyright protection under Art. 5(1)(c) of the Swiss Copyright Act (URG). Federal courts publish decisions pursuant to Art. 27 of the Federal Supreme Court Act (BGG); cantonal publication duties are governed by cantonal procedural codes. The dataset packaging is released under CC0-1.0; all source code is MIT-licensed.

### 8.2 Privacy

OpenCaseLaw preserves decisions exactly as published by the originating courts, including each court's anonymisation choices. We do not perform additional anonymisation. Cantonal anonymisation practices vary: some cantons redact party names comprehensively, others publish identifying details for certain case types.

Large-scale aggregation creates privacy risks beyond those of individual court portals (Pilan et al., 2024). The dataset includes a governance and removal policy under which individuals may request removal of specific decisions. Researchers using the corpus for studies involving personal data should conduct their own data-protection assessment.

### 8.3 Limitations

- **Scope: published decisions only.** The dataset covers only decisions that the originating courts have made publicly available. Unpublished decisions---the majority of Swiss court output---are not included. Publication practice varies by court, era, and case type.
- **Citation extraction is rule-based and not formally evaluated.** Section 5.1 describes structural checks and spot-check observations. A gold-annotated evaluation is not yet provided.
- **Single-annotator benchmark.** The retrieval benchmark reflects one author's relevance judgments. Inter-annotator agreement is not reported.
- **Historical text quality varies.** Pre-1990 material may contain OCR artifacts. Approximately 5,000 scanned-PDF decisions could benefit from improved OCR.
- **Commercial comparisons are approximate.** Decision counts for Swisslex and Weblaw in Table 1 are estimates; exact figures are not publicly audited.
- **Statute alias canonicalisation is manual.** The alias map (LTF = BGG, ZPO = CPC, etc.) is hand-maintained. Additional language variants may exist.
- **Cross-language findings are descriptive, not causal.** Section 4.3 explicitly does not attempt a controlled analysis of legal authority flow.

## Appendix A: Automated Weight Tuning (Meta-Harness)

The retrieval pipeline exposes 55+ numeric scoring parameters (BM25 column weights, citation-graph signal strengths, fusion weights). Manually tuning these is tedious. We implemented a Meta-Harness-style optimizer (Lee et al., 2026) that treats the full scoring configuration as a search space. At each iteration, the optimizer evaluates the current configuration on the benchmark, generates per-query execution traces showing which candidates were retrieved and at what rank, and asks an LLM proposer to suggest new weights targeting the weakest query categories. The optimizer converged within 2--3 iterations to the configuration reported in Section 6. The optimized weights add no runtime cost; they are constants in the scoring function. This appendix is included for transparency about the origin of the Table 12 numbers. A detailed description of the optimizer and its full configuration space is available in the source repository.

---

## References

- Althammer, S., Hofstätter, S., and Hanbury, A. (2021). Cross-domain Retrieval in the Legal and Patent Domains: a Reproducibility Study. *Proceedings of ECIR 2021*.

- Bommarito, M. J. and Katz, D. M. (2010). A Mathematical Approach to the Study of the United States Code. *Physica A*, 389(19), 4195--4200.

- Caselaw Access Project (2018). Harvard Law School Library Innovation Lab. https://case.law

- Chalkidis, I., Jana, A., Hartung, D., Bommarito, M., Androutsopoulos, I., Katz, D. M., and Aletras, N. (2022). LexGLUE: A Benchmark Dataset for Legal Language Understanding in English. *Proceedings of ACL 2022*, 4310--4330.

- Cormack, G. V., Clarke, C. L. A., and Buettcher, S. (2009). Reciprocal Rank Fusion Outperforms Condorcet and Individual Rank Learning Methods. *Proceedings of SIGIR 2009*, 758--759.

- Fowler, J. H., Johnson, T. R., Spriggs, J. F., Jeon, S., and Wahlbeck, P. J. (2007). Network Analysis and the Law: Measuring the Legal Importance of Precedents at the U.S. Supreme Court. *Political Analysis*, 15(3), 324--346.

- Geering, F. and Merane, J. (2024). Swiss Federal Supreme Court Dataset. Zenodo. doi:10.5281/zenodo.11092977.

- Kano, Y., Soh, J., Yoshioka, M., Rabelo, J., and Kim, M.-Y. (2024). COLIEE 2024: Competition on Legal Information Extraction and Entailment. *Proceedings of JSAI 2024*.

- Lee, Y., Nair, R., Zhang, Q., Lee, K., Khattab, O., and Finn, C. (2026). Meta-Harness: End-to-End Optimization of Model Harnesses. *arXiv:2603.28052*.

- Locke, S., Zhai, Z., and Kohlmeier, J. (2024). A Survey on Legal Text Retrieval. *Proceedings of ACL 2024*.

- Niklaus, J., Chalkidis, I., and Stuermer, M. (2021). Swiss-Judgment-Prediction: A Multilingual Legal Judgment Prediction Benchmark. *Proceedings of the Workshop on Natural Language Processing for Positive Impact (NLP4PI), EMNLP 2021*.

- Niklaus, J., Matoshi, V., Sturmer, M., Chalkidis, I., and Ho, D. E. (2023). MultiLegalPile: A 689GB Multilingual Legal Corpus. *arXiv:2306.02069*.

- Pilan, I., Lison, P., Ovrelid, L., Papadopoulou, A., Bain, D., and Quartey, J. (2024). Anonymity at Risk? Assessing Re-Identification Capabilities of Large Language Models in Court Decisions. *Findings of NAACL 2024*.

- Rasiah, V., Niklaus, J., Feijo, D., Welch, T., and Chalkidis, I. (2023). SCALE: Scaling up the Evaluation of Swiss Case Law. *arXiv:2306.09237*.

- Robertson, S. E. and Zaragoza, H. (2009). The Probabilistic Relevance Framework: BM25 and Beyond. *Foundations and Trends in Information Retrieval*, 3(4), 333--389.

- Shao, Y., Mao, J., Liu, Y., Ma, W., Satoh, K., Zhang, M., and Ma, S. (2020). BERT-PLI: Modeling Paragraph-Level Interactions for Legal Case Retrieval. *Proceedings of IJCAI 2020*, 3501--3507.

- Swiss Copyright Act (URG), Art. 5(1)(c). SR 231.1. Federal Assembly of the Swiss Confederation.

- Swiss Federal Supreme Court Act (BGG), Art. 27. SR 173.110. Federal Assembly of the Swiss Confederation.

- Waltl, B., Matthes, F., Waltl, T., and Grass, T. (2017). Lexical Analysis of German Legal Texts. *Proceedings of the Workshop on Automated Detection, Extraction and Analysis of Semantic Information in Legal Texts, ICAIL 2017*.

- Whalen, R. (2016). Legal Networks: The Promises and Challenges of Legal Network Analysis. *Michigan State Law Review*, 2016(2), 539--565.
