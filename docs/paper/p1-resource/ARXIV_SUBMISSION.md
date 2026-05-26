# arXiv submission metadata — Paper 1 (Resource)

**Status**: pre-submission ready; final arXiv upload should wait for the
Zenodo DOI, submission git tag, and final TeX Live rebuild.
This is the split from the v3 monolithic draft (2026-05-21). Companion
evaluation paper (`p2-eval/`) is in preparation.

## Title

```
OpenCaseLaw: A Verifiable Multilingual Citation Graph for Swiss Jurisprudence
```

## Authors

```
Jonas Hertner
```

Single author. Affiliation: OpenCaseLaw. Contact: jh@jonashertner.com.

## Abstract

```
OpenCaseLaw is an open, verifiable Swiss legal graph that joins court decisions, resolved case citations, statute references, legislative-history materials, scholarly commentaries, stable identifiers, and cryptographic provenance. The release contains 972,882 court decisions across 109 courts in 28 jurisdictional layers (DE/FR/IT), paired with 8.05M resolved citation tokens (92.9% coverage of 8.66M extracted tokens) expanded to 8.10M decision-to-decision link edges, 11.28M decision-to-statute edges spanning 283,330 provisions, bridges to 5,516 federal and 15,722 cantonal laws, 362 CC-BY/CC-BY-SA commentaries, and 8,124 article-level Materialien anchors from federal statutes to Federal Council messages (Botschaften). Every decision carries a Swiss-native cli:ch identifier with a Council-of-EU ECLI projection in Schema.org/LegalCase JSON-LD, and every daily publish commits an RFC-6962 Merkle root anchored on Bitcoin via OpenTimestamps, with a per-decision inclusion-proof API. As descriptive characterization of the released graph, we report row-normalised cross-language citation flow: Italian-language decisions cite outside Italian 84.6% of the time, French-language decisions 44.3%, and German-language decisions 15.0%; the aggregate cross-language share is 34.0%. The paper reports quality-control precision proxies, a 400-sample rule-based mechanical consistency check (400/400 pass for the checked resolver property), and explicitly treats per-language resolution-rate bias as the main measurement confounder. Live at mcp.opencaselaw.ch; corpus CC0 on Hugging Face; code MIT. Snapshot 2026-05-21.
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

A focused resource release. The contribution is the cross-court
resolved citation graph, the statute graph and Materialien bridge, the
cli:ch + ECLI identifier layer, and the cryptographic provenance layer.
The 34.0% cross-lingual citation share and row-normalised
source-language asymmetry are descriptive corpus characterizations,
not the primary contribution.

## What this paper is NOT

Evaluation. The cross-lingual retrieval diagnostic, the five-rail
closing audit, the prior-only-vs-RAG-augmented bench — all of those
were in the v3 monolithic draft and have been moved to the companion
paper (`docs/paper/p2-eval/`), which will ship once the v2.0 evaluation
components are complete: multi-annotator inter-annotator agreement on
the question sets, lawyer-authored held-out queries, human calibration
of the LLM judge, and n ≥ 200 for the audit bench.

## Pre-submission checklist

- [x] Main corpus/graph numbers are from the 2026-05-21 snapshot
  (`tables/corpus_graph_stats.json`); citation-precision proxies were
  re-run against the same 2026-05-21 frozen graph and now match the
  released E_link denominator (8,102,236 rows)
- [x] All cited works present in `bib/refs.bib`
- [x] Cross-references resolve (\ref, \cite all defined)
- [x] Honest scope: no evaluation claims, no human-precision claims, and
  the 400-sample audit result is labelled as rule-based mechanical
  consistency rather than semantic precision
- [x] Manuscript now includes citation-flow figure generated from the
  frozen matrix and a reproducibility capsule with the 2026-05-21
  integrity root
- [x] Git tag `paper-p1-resource-v1.0` advanced to the final-pass commit
  `3940a54` (Figure-1 edge fix + em-dashes + [H] table positioning); both
  local and `origin` updated 2026-05-26
- [x] paper.bbl committed (built via tectonic; 13 entries cover all
  13 unique \cite{} keys in paper.tex)
- [x] Zenodo deposit bundle refreshed (paper.pdf SHA-256
  `cadae8260e2f352f21702c4bf4508567f66e2c11cb806f29286de145f79b1d5f`,
  matches root paper.pdf and `zenodo_deposit/checksums.sha256`)
- [ ] **User-side**: Zenodo DOI minted for corpus + integrity snapshot
- [ ] **User-side (optional)**: TeX Live rebuild to regenerate paper.bbl
  for ultra-portable arXiv submission (tectonic-built `.bbl` already
  works; only redo if arXiv complains)

## Submission handoff — 3 steps

1. **Mint Zenodo DOI** — upload the six files in
   `docs/paper/p1-resource/zenodo_deposit/` (paper.pdf + 4 integrity
   files + corpus_graph_stats + precision_proxies) per the recipe in
   `zenodo_deposit/README.md`. Capture both the version-specific DOI
   and the concept DOI.

2. **Add DOI to arXiv comments line** (optional, can also leave for v2):
   append `; archived at https://doi.org/10.5281/zenodo.<N>` to the
   Comments line in the section above.

3. **Upload to arXiv**: use `paper.pdf` (or zip of source if arXiv
   prefers — `paper.tex`, `paper.bbl`, `bib/refs.bib`,
   `tables/*.tex` are all in `docs/paper/p1-resource/`). Set primary
   `cs.CL`, cross-list `cs.IR cs.DL cs.DB cs.CR`. Paste the title,
   abstract, and Comments line from this document.
