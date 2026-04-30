# arXiv submission package — 2026-04-30

This directory contains everything needed to submit the paper to arXiv.

## Files

- `paper.tex` — self-contained LaTeX source
- `paper.pdf` — compiled PDF (reference; arXiv will rebuild from .tex)
- `/tmp/arxiv_submission/paper-arxiv-2026-04-30.tar.gz` — tarball ready to upload

## Submission flow at https://arxiv.org/submit

### Step 1 — Start submission

After login at arxiv.org/user, click "Start a new submission".

### Step 2 — Article metadata

| Field | Value |
|---|---|
| **License** | `CC-BY-4.0` (recommended for max reuse) |
| **Primary archive** | `cs` (Computer Science) |
| **Primary category** | `cs.CL` (Computation and Language — legal NLP fits here) |
| **Secondary categories** | `cs.IR` (Information Retrieval — RAG focus) and optionally `cs.AI` |

### Step 3 — File upload

Upload `paper-arxiv-2026-04-30.tar.gz` from `/tmp/arxiv_submission/`.

arXiv will compile from source. Verify the resulting PDF matches `paper.pdf` (14 pages, lmodern-rendered ligatures correct).

If arXiv rejects the source upload (occasional issues with single-file
tarballs), upload `paper.tex` directly — arXiv accepts both bare `.tex`
and tarball.

### Step 4 — Title, authors, abstract

**Title:**

```
Verification-First Legal RAG: A Five-Rail Closing Audit and Calibration Stress Test on Swiss Federal Law
```

**Authors:**

```
Jonas Hertner (OpenCaseLaw — Independent researcher, Zurich, Switzerland)
```

If the user is endorsed under a different identifier (institutional affiliation, ORCID, etc.), use that.

**Abstract** (~340 words, copy verbatim from paper.tex):

```
We propose a five-rail closing audit pipeline for legal-RAG outputs (case-citation existence, statute-reference resolution, verbatim-quote source-matching, decision-date sanity, and proposition grounding by a separate-call LLM judge) and stress-test it against Claude Sonnet 4.6 on 30 Swiss-federal-law questions in a prior-only condition (no retrieval; the audit pipeline is intended for production RAG outputs but the present experiment exercises it on outputs without retrieval, by design). Four rails are deterministic (~3 ms per draft); the fifth is judge-mediated (~2 s, ≈$0.005 per call). We report two runs against the same questions and benchmark artefact: a v1 run, and a v2 run after deploying mechanical fixes (citation-leading claim extraction, judge-call retry) that the v1 run identified as audit-pipeline gaps. Three findings. (i) Citation-existence is not the bottleneck: in v1 the model emits 33 case citations and 1 fails to resolve in our corpus (3.0%); in v2 it emits 37 and 1 fails to resolve (2.7%). In both runs the same docket fails to resolve (BGE/DTF 117 II 198 on q-017); we cannot assert without external verification whether the docket is fabricated or simply absent from our corpus snapshot. Hallucination rates reported by Dahl et al. 2024 (58-82%) and Magesh et al. 2025 (17-33%) were measured under different model and condition combinations and we do not claim a like-for-like comparison. (ii) Calibration of the grounding judge is the open problem: with the mechanical fixes in place, the v2 grounding rail evaluates at least one citation on 27 of 30 drafts and fires on 13 — 3 of 6 wrong drafts (50% wrong-draft flag rate) plus 10 of 24 correct (FPR 41.7%). Inspection shows the wrong-draft flags are clean citation-claim mismatch flags, but the judge is opinionated enough on correct drafts that running it as a binary gate would over-flag. (iii) End-to-end run variance is substantial: at temperature=0, the generator produces a different draft for all 30 questions across our two runs, and 4 of 30 receive different correctness verdicts. We release the audit pipeline (MIT) and Swiss Legal RAG Bench v0.2 (30 author-curated questions, DE/FR/IT; benchmark packaging CC0).
```

### Step 5 — Comments field (optional but useful)

```
Pilot stress-test paper. 14 pages. Code (MIT) + benchmark (CC0) released; production deployment at mcp.opencaselaw.ch. Two runs reported (v1 commit 4553379, v2 commit 12d32d3). v1.0 forthcoming with named expert co-authors and a multi-annotator protocol.
```

### Step 6 — Submit

After review, click "Submit". arXiv will assign an ID like `arXiv:2604.NNNNN` and email a moderation timeline (typically 1-3 business days).

## Pre-submission sanity checklist

- [x] PDF compiles (14 pages, no missing references)
- [x] Title, authors, abstract match between paper and metadata
- [x] License chosen (CC-BY-4.0 recommended)
- [x] Categories chosen (cs.CL primary, cs.IR + cs.AI secondary)
- [x] Reproducibility section present with commit-pinned URLs (commit `4553379` for v1, `12d32d3` for v2)
- [x] Hugging Face dataset URL valid (https://huggingface.co/datasets/voilaj/swiss-legal-rag-bench)
- [x] All references resolve (no `?? ` in PDF text)

## After submission

1. Save the arXiv ID (e.g., `2604.12345`).
2. Update `MEMORY.md` with the arXiv URL.
3. Optionally announce on social media / mailing lists.
4. Plan v1.0 follow-up (150 questions, named co-authors).
