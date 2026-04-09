# Zenodo Submission Guide — OpenCaseLaw

## Upload type
Publication — Journal article (or: Dataset)

Note: Zenodo supports both. If submitting as a dataset paper, use "Publication > Journal article". If submitting the dataset itself with the paper as documentation, use "Dataset".

## Metadata

### Title
OpenCaseLaw: A Complete Open Corpus and Citation Graph of Swiss Court Decisions

### Authors
1. **Jonas Hertner** — Independent Researcher, Zurich, Switzerland — ORCID: (add if available)

### Description (abstract)
We present OpenCaseLaw, the first open-access corpus to provide nationwide coverage of Swiss case law with an integrated citation graph. The April 2026 release contains 965,038 full-text court decisions from 104 courts spanning all 26 cantons and the period 1875–2026, in German (46.6%), French (45.1%), and Italian (8.3%). Alongside the corpus, we release a citation graph of 8.87 million extracted references with 6.53 million resolved in-corpus links (73.6% resolution rate) and 11.34 million decision-to-statute links across 284,145 distinct provisions. Analysis of cross-language citation patterns reveals a striking asymmetry: French-language courts cite German-language decisions 1.64 million times—84% as often as they cite French-language decisions—quantifying the dominance of German-language Bundesgerichtsentscheide (BGE) jurisprudence across Switzerland's language boundaries. Peak citation age is 11–20 years, indicating long institutional memory in Swiss law. We provide a 100-query multilingual retrieval benchmark (MRR@10 = 0.60 offline, 0.65 with LLM reranking) and release the corpus as Parquet on Hugging Face under CC0-1.0. The dataset is updated daily and all code is MIT-licensed.

### Publication date
2026-04-09

### Publisher
Zenodo

### Keywords
- legal corpus
- Swiss law
- case law
- citation network
- multilingual
- open data
- legal information retrieval
- court decisions
- NLP
- CC0

### Language
English

### License
CC-BY-4.0 (for the paper itself)

Note: The *dataset* is CC0-1.0, but the *paper* describing it is CC-BY-4.0. This is standard practice — the paper has creative authorship, the data does not.

### Access right
Open Access

### Related identifiers

| Identifier | Relation | Resource type |
|---|---|---|
| https://huggingface.co/datasets/voilaj/swiss-caselaw | Is supplement to | Dataset |
| https://github.com/jonashertner/caselaw-repo-1 | Is supplement to | Software |
| https://opencaselaw.ch | Is supplement to | Other |
| https://mcp.opencaselaw.ch | Is supplement to | Other |

### Subjects
- Law
- Computer Science
- Natural Language Processing
- Information Retrieval
- Open Data

### Communities
Consider adding to these Zenodo communities (if they accept):
- Open Access
- Legal Informatics
- Multilingual NLP
- Open Data Switzerland

### Grants
(None — independent research, self-funded)

### Notes
The dataset described in this paper is continuously updated. The statistics reported correspond to the April 9, 2026 snapshot. The latest version is always available at https://huggingface.co/datasets/voilaj/swiss-caselaw.

---

## Files to upload

1. **opencaselaw-zenodo.pdf** — the paper (convert from .md)
2. Optionally: **benchmark_golden.json** — the 100-query retrieval benchmark

## Pre-submission checklist

- [ ] Convert opencaselaw-zenodo.md to PDF (clean formatting, no line-number artifacts)
- [ ] Verify all table numbers are correct against latest data
- [ ] Check all URLs are live and accessible
- [ ] Verify HuggingFace dataset is public and up to date
- [ ] Verify GitHub repo is public
- [ ] Add ORCID if available
- [ ] Review Zenodo preview before publishing
- [ ] After publish: update README.md and HuggingFace dataset card with DOI

## After publication

1. Add the Zenodo DOI badge to:
   - GitHub README.md
   - HuggingFace dataset card
   - opencaselaw.ch dashboard
2. Cross-reference on arXiv if also submitting there
3. Share on relevant channels (legal informatics mailing lists, Swiss NLP community)
