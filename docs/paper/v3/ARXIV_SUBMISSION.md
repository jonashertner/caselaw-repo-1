# arXiv submission metadata — paper v1.1

**Submission tarball**: `opencaselaw-arxiv-2026-05-19.tar.gz` (99 KB, 27 files)
**PDF preview**: `paper.pdf` (27 pages, 445 KB)
**Status**: substantively ready for arXiv submission

## How to submit

1. Log in at https://arxiv.org/submit
2. Start a new submission
3. Upload `opencaselaw-arxiv-2026-05-19.tar.gz`
4. arXiv will compile via TeX Live; verify the rendered PDF matches `paper.pdf`
5. Fill in the metadata below (copy-paste)
6. Preview, agree to licensing, submit

## Metadata fields (copy-paste targets)

### Title

```
OpenCaseLaw: An Open Multilingual Legal Knowledge Graph for Switzerland
```

### Authors

```
Jonas Hertner
```

(Single author. Affiliation: OpenCaseLaw. Email: jonashertner@protonmail.ch)

### Abstract

```
We present OpenCaseLaw, an open, continuously-refreshed corpus of 972,882 Swiss court decisions across 109 courts spanning 26 Swiss cantons, the federal jurisdiction, and the Swiss-relevant subset of the European Court of Human Rights (28 jurisdictional layers), in German, French, and Italian, paired with a citation graph of 8.66M distinct outgoing references (8.05M resolved, 92.9% coverage) and a statute graph of 11.28M edges over 283,330 distinct provisions. The corpus is bridged to 5,516 federal and 15,722 cantonal laws (753,842 articles in total), 362 CC-BY scholarly commentaries, and 5,292 Federal Council Botschaften (Materialien, DE/FR/IT-parallel, with 381,711 full-text paragraphs and 8,124 article-anchored links). The infrastructure is served through a deployed MCP/REST endpoint at mcp.opencaselaw.ch (33 public MCP tools, OpenAPI 3.0.3 REST, daily Parquet on Hugging Face), a Word add-in client, and a public dashboard. Each decision carries a Swiss-native cli:ch identifier with an ECLI projection in Schema.org/LegalCase JSON-LD; every daily publish is committed against an RFC-6962 Merkle root anchored on the Bitcoin blockchain via OpenTimestamps, with a per-decision inclusion-proof API endpoint. Snapshot: 2026-05-21. We use the resource to introduce three contributions: (i) a released corpus and knowledge graph with documented provenance, licensing, and a four-layer dataset-health framework (63 codified checks, drift detection, and a publish-gate that blocks production updates on critical regression); (ii) a regeste-derived cross-lingual retrieval diagnostic with parallel keyword queries in DE/FR/IT against 50 highly-cited decisions. Because each query is extracted from its target case's own multilingual regeste, the absolute scores are an upper bound on realistic legal-research retrieval; we therefore report per-direction asymmetries as the interpretable signal; (iii) a deployed five-rail closing audit that checks named legal-RAG error classes deterministically (case-citation existence, statute resolution, verbatim-quote source matching, decision-date sanity) plus an opt-in proposition-grounding judge, with per-class metrics and explicit calibration limits. We do not claim a general hallucination-rate reduction. Released artifacts (corpus packaging, citation/statute graphs, benchmark) follow record-level upstream terms: code is MIT; original-source decisions are official acts of Swiss authorities and inherit their public-domain status under URG Art. 5; commentaries retain CC-BY-4.0 / CC-BY-SA-4.0. A public dashboard tracks live dataset health.
```

### Comments line

```
27 pages, 12 sections, 5 appendices, 12 tables, 1 figure. Live infrastructure at https://mcp.opencaselaw.ch and https://opencaselaw.ch. Code at https://github.com/jonashertner/caselaw-repo-1 (MIT). Corpus at https://huggingface.co/datasets/voilaj/swiss-caselaw (CC0). Benchmark at https://huggingface.co/datasets/voilaj/swiss-legal-rag-bench (CC0).
```

### Primary category

```
cs.CL  (Computation and Language)
```

### Cross-list categories

```
cs.IR  (Information Retrieval)
cs.DL  (Digital Libraries)
cs.DB  (Databases)
```

Optional secondary (for NeurIPS D&B 2026 alignment):
```
cs.AI  (Artificial Intelligence)
```

### MSC class (optional)

```
68T50 (Natural language processing)
```

### ACM class (optional)

```
I.2.7  (Natural Language Processing)
H.3.3  (Information Storage and Retrieval / Information Search and Retrieval)
```

### License (arXiv form)

```
arXiv perpetual non-exclusive license (default)
```

### Report number

```
(leave blank)
```

### Journal reference

```
(leave blank — pre-print)
```

### DOI

```
(leave blank for now; Zenodo upload separately if desired)
```

## Pre-submission checklist

- [x] PDF compiles clean (27 pages, no undefined refs)
- [x] All 12 tables render
- [x] Bibliography compiles (33 references, plainnat style)
- [x] All numbers consistent with `docs/canonical_numbers.md`
- [x] Cross-references resolve (`\ref`, `\cite` all defined)
- [x] Live URLs in §12 verified reachable (mcp.opencaselaw.ch, opencaselaw.ch, HF datasets, GitHub)
- [x] Appendix E legal posture present
- [ ] **User-side**: Zenodo DOI minted for corpus + benchmark snapshot (optional, can update arXiv submission later)
- [ ] **User-side**: Git tag `paper-resource-2026-05` at commit pinned by paper
- [ ] **User-side**: Confirm `team@jonashertner.com` reflects current contact preference

## Post-submission tasks

1. **Tag the pinned commit**:
   ```bash
   git tag -a paper-resource-2026-05 -m "OpenCaseLaw paper v1.1 — corpus snapshot 2026-05-13"
   git push origin paper-resource-2026-05
   ```

2. **Update README and dashboard** with the arXiv URL once issued (typically `arXiv:2605.NNNNN`).

3. **Add bib entry** to `docs/paper/v3/bib/refs.bib` for self-citation in future work:
   ```bibtex
   @article{hertner2026opencaselaw,
     title  = {OpenCaseLaw: An Open Multilingual Legal Knowledge Graph for Switzerland},
     author = {Hertner, Jonas},
     journal= {arXiv preprint arXiv:2605.NNNNN},
     year   = {2026}
   }
   ```

4. **Announce** on the dashboard `/paper/` anchor page.

## Open follow-ups (not blockers for v1.1)

- v2.0: lawyer-authored cross-lingual queries (multi-annotator IAA)
- v2.0: Italian-target cell in the cross-lingual table (candidate identifier already in repo)
- v2.0: statute temporal validity (OG → BGG/LTF historical mapping)
- v2.0: formal Swiss data-protection lawyer review of Appendix E
- v2.0: per-record license-tag field in Parquet export
- Tracked in `docs/paper/v3/v1_1_roadmap.md`
