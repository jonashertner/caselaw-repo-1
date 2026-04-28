# Zenodo deposit 1 — Swiss Caselaw corpus snapshot (April 2026)

This file is the paste-ready Zenodo form. Mint the DOI and put it in
the paper's "Data Availability" section. The corpus itself is already
on Hugging Face (CC0); the Zenodo deposit gives it a citable DOI for
academic literature.

----------------------------------------------------------------------

## Upload type

**Dataset**

## Title

```
Swiss Caselaw — Open Corpus of Published Swiss Federal and Cantonal
Court Decisions (snapshot April 2026)
```

## Authors

| # | Name | Affiliation | ORCID | Email |
|---|------|-------------|-------|-------|
| 1 | Jonas Hertner | Independent Researcher, Zurich | [add if you have one] | team@jonashertner.com |

## Description (abstract — paste verbatim)

```
A complete open-access corpus of published Swiss court decisions from
all federal and cantonal courts. The April 2026 snapshot contains
969,000+ decisions spanning the years 1875 to 2026, covering all
26 cantons and 102 individual courts and quasi-judicial bodies.

Decisions are released as Apache Parquet files with a 34-field schema
covering court identity, docket number, decision date, language,
title, legal area, regeste (head-note), full text, citation
information, and provenance. The corpus is multilingual: 46.6 %
German, 45.1 % French, 8.3 % Italian.

Alongside the decisions, the deposit includes:

- A reference graph with 8.8M extracted case-citation references
  (6.5M resolved in-corpus, 73 % resolution rate) and 11.3M
  decision-to-statute links across 284,000 distinct statute
  provisions.
- A 100-query multilingual retrieval benchmark with golden labels
  (golden_set_2026.json), released under CC0.
- Daily-refreshed companion service at
  https://huggingface.co/datasets/voilaj/swiss-caselaw and live
  search at https://opencaselaw.ch.

The dataset is released under CC0-1.0 (Public Domain Dedication).
All code that produced the dataset is published under the MIT
licence at https://github.com/jonashertner/caselaw-repo-1.
```

## Publication date

`2026-04-28` (or the date you actually click publish)

## Publisher

`Zenodo`

## Keywords

```
swiss law
case law
court decisions
multilingual
german
french
italian
romansh
legal informatics
information retrieval
citation graph
open data
public domain
CC0
```

## Language

`English` (description) — note that the dataset itself contains DE/FR/IT content

## Licence

**Creative Commons Zero v1.0 Universal (CC0-1.0)**

## Access right

`Open Access`

## Subjects (Zenodo subject hierarchy)

- Law (FOR 18)
- Computer Sciences (FOR 08)
- Information and Computing Sciences (FOR 08)
- Computer Software (FOR 0803)

## Related identifiers

| Identifier | Relation | Resource type |
|---|---|---|
| https://huggingface.co/datasets/voilaj/swiss-caselaw | Is identical to | Dataset |
| https://github.com/jonashertner/caselaw-repo-1 | Is supplemented by | Software |
| https://opencaselaw.ch | Is supplemented by | Other |
| https://mcp.opencaselaw.ch | Is supplemented by | Other |
| 10.5281/zenodo.[arXiv-paper-DOI-once-minted] | Is described by | Publication |

## Funding

`None — independent research, self-funded`

## Communities

Submit to the following Zenodo communities:

- `legal-informatics` (if exists)
- `multilingual-nlp` (if exists)
- `open-data-switzerland` (if exists)
- `swissubase` (if exists, Swiss research data)

## Files to upload

```
artifacts/paper_release_2026-04-XX/
├── decisions.parquet           (~7 GB — split into shards if Zenodo
│                                 50 GB limit applies; or upload only
│                                 a sample + reference HF for full)
├── manifest.json               (snapshot metadata + checksums)
├── checksums.sha256            (per-file SHA-256)
├── golden_set_2026.json        (100-query benchmark)
├── reference_graph.parquet     (citation edges, sample)
├── stats_snapshot.json         (corpus statistics)
└── README.md                   (deposit landing page)
```

**Practical note on size:** Zenodo free tier limits a single record to
50 GB. The corpus Parquet files are ~7 GB total, fits comfortably.
If we want to include reference_graph.db (~1.2 GB) we're still well
under.

## Pre-publish checklist

- [ ] Pull the latest paper-release artifact:
      `artifacts/paper_release_2026-04-XX/`
      (run `python3 scripts/build_paper_release.py --tag 2026-04`)
- [ ] Verify all checksums match
- [ ] Verify HF dataset is up-to-date (so HF and Zenodo agree)
- [ ] Add ORCID if you have one (we'll mint a DOI either way)
- [ ] Use the Zenodo "Reserve DOI" button BEFORE filling the rest —
      this gives you the DOI to embed in the README.md and the
      Hugging Face dataset card before you publish

## After publication

1. Add Zenodo DOI badge to:
   - `README.md` (top, under the title)
   - `dataset_card.md` (HuggingFace landing)
   - `opencaselaw.ch` footer
2. Reference the DOI in the arXiv paper's "Data Availability" §

## Zenodo URL

After publishing: `https://doi.org/10.5281/zenodo.<NUMBER>`
