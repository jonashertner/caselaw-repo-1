# arXiv submission metadata — Paper 1 (Resource)

**Status**: ready for arXiv submission as v1.0 of the resource paper.
This is the split from the v3 monolithic draft (2026-05-21). Companion
evaluation paper (`p2-eval/`) is in preparation.

## Title

```
OpenCaseLaw: An Open Multilingual Citation Graph for Swiss Jurisprudence
```

## Authors

```
Jonas Hertner
```

Single author. Affiliation: OpenCaseLaw. Contact: jonashertner@protonmail.ch.

## Abstract

```
34.0% of resolved decision-to-decision citations in Swiss jurisprudence span two different official languages. This is a structural property of multilingual civil-law jurisdictions that no English-language legal-NLP resource captures, and it has not been measured before because no resolved cross-court Swiss citation graph existed. We measure it, and release the substrate. OpenCaseLaw is the first open multilingual citation graph for Swiss jurisprudence: 972,882 court decisions across 109 courts in 28 jurisdictional layers (DE/FR/IT), paired with 8.05M resolved cross-court citation edges (92.9% coverage), 11.28M decision-to-statute edges spanning 283,330 provisions, bridges to 5,516 federal and 15,722 cantonal laws, 362 CC-BY commentaries, and 8,124 article-level links from federal statutes to the Federal Council messages (Botschaften) that explain them — the first open programmatic bridge from Swiss case law to its legislative-history substrate. Every decision carries a Swiss-native cli:ch identifier with a Council-of-EU ECLI projection in Schema.org/LegalCase JSON-LD. Every daily publish commits an RFC-6962 Merkle root anchored on Bitcoin via OpenTimestamps, with a per-decision inclusion-proof API. Live at mcp.opencaselaw.ch; corpus CC0 on Hugging Face; code MIT. Snapshot 2026-05-21.
```

## Comments line

```
First of a two-paper series. Resource paper: corpus + multilingual citation graph + Materialien bridge + cli:ch identifier + cryptographic provenance. Companion paper (in preparation) covers evaluation diagnostics with multi-annotator inter-annotator agreement. Live infrastructure at https://mcp.opencaselaw.ch and https://opencaselaw.ch. Code at https://github.com/jonashertner/caselaw-repo-1 (MIT). Corpus at https://huggingface.co/datasets/voilaj/swiss-caselaw (CC0).
```

## Primary category

```
cs.CL  (Computation and Language)
```

## Cross-list categories

```
cs.IR  (Information Retrieval)
cs.DL  (Digital Libraries)
cs.DB  (Databases)
cs.CR  (Cryptography and Security)  — for the Merkle/OpenTimestamps layer
```

## What this paper is

A focused resource release. The killer empirical contribution is the
**34.0% cross-lingual citation share** — a structural property of Swiss
jurisprudence measured here for the first time. The supporting structure
is the cross-court resolved citation graph that made the measurement
possible, the statute graph and Materialien bridge that anchor the
citations to their legislative source, the cli:ch + ECLI identifier
layer, and the cryptographic provenance layer.

## What this paper is NOT

Evaluation. The cross-lingual retrieval diagnostic, the five-rail
closing audit, the prior-only-vs-RAG-augmented bench — all of those
were in the v3 monolithic draft and have been moved to the companion
paper (`docs/paper/p2-eval/`), which will ship once the v2.0 evaluation
components are complete: multi-annotator inter-annotator agreement on
the question sets, lawyer-authored held-out queries, human calibration
of the LLM judge, and n ≥ 200 for the audit bench.

## Pre-submission checklist

- [x] Numbers are from the 2026-05-21 snapshot (single source of truth:
  `tables/corpus_graph_stats.json`)
- [x] All cited works present in `bib/refs.bib`
- [x] Cross-references resolve (\ref, \cite all defined)
- [x] Honest scope: no eval claims, no audit claims, no benchmark
  claims that don't fit the resource frame
- [ ] **User-side**: Zenodo DOI minted for corpus + integrity snapshot
- [ ] **User-side**: Git tag `paper-p1-resource-v1.0` at submission commit
- [ ] **User-side**: TeX Live rebuild to regenerate paper.bbl
