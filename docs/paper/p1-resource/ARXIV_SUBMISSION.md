# arXiv submission metadata — Paper 1 (Resource)

**Status**: submission-ready on the 2026-07-30 snapshot. Manuscript
fundamentally revised 2026-07-31; externally reviewed the same day
(GPT-5.6-Sol xhigh via Codex, all four major findings resolved —
`REVIEW_GPT56SOL.md`). Tarball verified to build standalone with
tectonic: `opencaselaw-p1-arxiv-2026-08-06.tar.gz` (paper.tex +
paper.bbl + bib/refs.bib + tables/*.tex; PDF 17 pp, zero unresolved
references). Companion evaluation paper (`p2-eval/`) in preparation.

## Title

```
OpenCaseLaw: A Verifiable Multilingual Research Substrate for Swiss Jurisprudence
```

## Authors

```
Jonas Hertner
```

Single author. Affiliation: OpenCaseLaw. Contact: jh@jonashertner.com.

## Abstract (plain-text projection of the paper.tex abstract, 2026-07-30 snapshot)

```
We release OpenCaseLaw, an open, verifiable research substrate for Swiss law: a resolved cross-court citation graph joined to several components of Swiss legal interpretation (statute articles, legislative-history Materialien, administrative practice, scholarly commentary, and open-access scholarship), with a Swiss-native identifier scheme, cryptographic provenance, and a deployed, agent-consumable serving layer. The release contains 1,050,981 court decisions across 118 courts in 28 jurisdictional layers (DE/FR/IT, 1875-2026), 9.06M resolved citation tokens (93.8% coverage of 9.66M extracted) expanded to 9.99M link edges, 12.42M decision-to-statute edges over 298,922 distinct provisions, and four interpretive bridges: 19,809 article-level links to 6,154 Federal Council Botschaften (421,489 full-text paragraphs), 1,146 open-licensed commentaries, 24,363 open-access scholarship records with a bidirectional citation bridge, and 1,892 federal administrative-practice documents. Every decision has a Swiss-native cli:ch identifier with an ECLI projection and a retrievable cryptographic membership proof against a daily RFC-6962/OpenTimestamps integrity root. The substrate is served as deployed infrastructure at mcp.opencaselaw.ch: 42 public Model Context Protocol tools under a citation-integrity serving contract, REST, and daily Parquet. Operator-side telemetry over the 30 days to the snapshot shows use by AI-assistant clients on three platforms and traffic patterns consistent with automated monitoring; the instrumentation recorded ~1.5M MCP transport opens or requests. We report these observations as adoption evidence with stated caveats, not as an evaluation. Quality controls include date-chronology violations on 2.0% of dated link edges (a conditional date-sanity ceiling, not a precision estimate) and a 400-sample rule-consistency audit (400/400 pass) frozen from the 2026-05-21 release. Corpus CC0 on Hugging Face with a documented carve-out for ECtHR-origin texts (© ECHR-CEDH, redistributed under reuse terms, excluded from the CC0 export); code MIT. Snapshot 2026-07-30.
```

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
