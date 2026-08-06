# arXiv submission metadata — Paper 1 (Resource)

**Status**: MAJOR REVISION in progress (external review 2026-08-07).
Wave 0 applied 2026-08-07: title, terminology (decision records /
court and source collections), PublG correction, unofficial-ECLI
wording, telemetry demoted to operational lessons, abstract rewritten
under the 1,920-char metadata limit (currently 1,369 plain chars),
`adjudication` → `rule_consistent` (schema v2), primary standards
references added. DO NOT SUBMIT before Wave 1 (post-ATF/DTF re-freeze:
all graph counts, snapshot date, Merkle root, DOI) and Wave 2 (human
semantic audits) are complete. Prior GPT-5.6-Sol review reconciled in
`REVIEW_GPT56SOL.md`.

## Title

```
OpenCaseLaw: An Open Multilingual Corpus and Citation Graph for Swiss Jurisprudence
```

## Authors

```
Jonas Hertner
```

Single author. Affiliation: OpenCaseLaw. Contact: jh@jonashertner.com.

## Abstract (plain-text projection of the paper.tex abstract; counts are Wave-1 placeholders from the 2026-07-30 snapshot)

```
OpenCaseLaw is an open multilingual corpus and citation graph for Swiss jurisprudence. The frozen [SNAPSHOT] release contains [N-RECORDS] decision records from 118 court and source collections across federal, cantonal, regulatory, and ECtHR layers in German, French, and Italian (1875-2026). A resolved citation graph links [N-RESOLVED] of [N-EXTRACTED] extracted citation tokens ([COVERAGE] coverage), represented as [N-LINKS] source-target link rows, alongside [N-STATUTE-EDGES] decision-to-statute edges over [N-PROVISIONS] distinct provisions. Interpretive bridges connect provisions and decisions to Federal Council dispatches (Botschaften) at article granularity, to open-licensed commentaries, to open-access scholarship with a bidirectional citation bridge, and to federal administrative practice. Every record carries a project-defined cli:ch identifier with an unofficial ECLI-compatible projection, a content hash, and a retrievable inclusion proof against a daily RFC 6962 Merkle root anchored via OpenTimestamps. We describe data acquisition, normalisation, citation and statute resolution, automated error diagnostics, licensing, coverage limits, and access through downloadable tables and REST/MCP interfaces under a citation-integrity serving contract. Project-created database packaging and metadata are dedicated to CC0; ECtHR-origin texts are distributed under their source terms; code is MIT.
```

Placeholders map to the LaTeX macros at the top of paper.tex
(\SnapDate, \NRecords, \NExtracted, \NResolved, \NCoverage, \NLinks,
\NStatuteEdges, \NProvisions); Wave 1 swaps the macro block against the
post-ATF/DTF freeze and this plain projection follows. After Wave 2,
add one sentence with the weighted semantic citation precision and CI.

## Comments line

```
Resource paper, first of a two-paper series; companion evaluation paper in preparation. Live infrastructure at https://mcp.opencaselaw.ch and https://opencaselaw.ch. Code at https://github.com/jonashertner/caselaw-repo-1 (MIT). Corpus at https://huggingface.co/datasets/voilaj/swiss-caselaw (CC0, ECtHR carve-out documented). All headline numbers regenerate from the frozen 2026-07-30 snapshot (tables/corpus_graph_stats.json) via scripts/paper_snapshot_stats.py.
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

## License

arXiv license selection: CC BY 4.0 (recommended for maximal reuse of the
paper text; the corpus licence — CC0 with ECtHR carve-out — is stated in
the paper's data card and is independent of the arXiv text licence).

## What this paper is

A focused resource release. The contribution is the cross-court resolved
citation graph (93.8% coverage), the statute graph and four interpretive
bridges (Botschaften, commentaries, scholarship, administrative
practice), the cli:ch + ECLI identifier layer, the cryptographic
provenance layer, and the deployed agent-consumable serving surface with
measured-with-caveats adoption telemetry. The cross-lingual citation
matrix is a descriptive corpus characterisation, not the primary
contribution.

## What this paper is NOT

Evaluation. The cross-lingual retrieval diagnostic, audit-rail
adversarial probes, human calibration of the LLM grounding judge, and
the manual semantic adjudication of the 400-sample audit set are
companion-paper scope (`docs/paper/p2-eval/`).

## Pre-submission checklist

- [x] All corpus/graph numbers from the frozen 2026-07-30 snapshot
  (`tables/corpus_graph_stats.json`), regenerated via
  `scripts/paper_snapshot_stats.py`; operational figures (tools, tests,
  checks) explicitly scoped as repository/deployment facts as of the
  snapshot date
- [x] External review (GPT-5.6-Sol xhigh, 2026-07-31): 4 major findings
  all resolved — OLD comparison-table correction, "sessions" relabelled
  to transport opens/requests, six prose counts added to the snapshot
  generator or rescoped, QA table corrected to 8 probes / 1,568 tests
- [x] All cited works present in `bib/refs.bib`; PDF builds with zero
  unresolved \cite/\ref (verified via pdftotext scan, no "[?]")
- [x] paper.bbl regenerated by tectonic 2026-08-06 (9,689 bytes, current
  bibliography incl. 2025-2026 additions)
- [x] Standalone build verified: tarball extracted to a clean directory
  compiles with tectonic
- [ ] **User-side**: Zenodo DOI refresh for the 2026-07-30 snapshot
  (optional — the 2026-05 concept DOI remains valid; can also land in v2)
- [ ] **User-side**: arXiv account upload

## Submission handoff — 2 steps

1. **Upload to arXiv**: use
   `opencaselaw-p1-arxiv-2026-08-06.tar.gz` (source; arXiv will build
   it) or `paper.pdf` as fallback. Primary `cs.CL`, cross-list
   `cs.IR cs.DL cs.DB cs.CR`. Paste title, abstract, and Comments line
   from this document.

2. **Optional**: mint/refresh the Zenodo DOI per
   `zenodo_deposit/README.md` and append
   `; archived at https://doi.org/10.5281/zenodo.<N>` to Comments
   (can wait for v2).
