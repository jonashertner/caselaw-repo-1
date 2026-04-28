# Abstract — final (anchored on real Step-4 numbers)

This is the abstract that goes into `paper.tex`. Replaces Drafts A/B/C
in `01_title_and_abstract.md`. Numbers are from
`experiments/ablation_table.json`.

----------------------------------------------------------------------

## Title

**Verification-First Legal RAG: A Defense-in-Depth Audit Pipeline and a Multilingual Swiss-Law Benchmark**

----------------------------------------------------------------------

## Abstract (~200 words)

Recent measurement (Dahl, Magesh, Suzgun & Ho 2024) found that
58–82 % of legal queries to general-purpose LLMs and (Magesh et al.
2024) that 17–33 % of queries to commercial legal-RAG tools produce
a fabricated authority. We present a five-rail closing audit pipeline
for retrieval-augmented legal LLMs that addresses these failures
per error class: case-citation existence, statute-reference
resolution, verbatim-quote source-matching, decision-date sanity, and
proposition grounding by an independent LLM judge. Four rails are
deterministic and add 3 ms per draft; the fifth is judge-mediated
at 2 s and ≈$0.005 per call. We evaluate the pipeline on **Swiss
Legal RAG Bench v0.2**, a public 30-question multilingual benchmark
(DE/FR/IT) for Swiss federal law that we also release. In a
prior-only condition (no retrieval), Claude Sonnet 4.6 emits 33
case citations across 30 drafts with **3.0 %** at the citation level
fabricated — well below Magesh's 17–33 %, attributable to corpus
presence in pre-training. The deterministic rails catch fabrications
with **perfect precision (0 % FPR) and 16.7 % recall**; the
grounding rail extends recall to 50 % at 25 % FPR. The residual
error class — *real citation, misrepresented holding* — is caught
by the grounding judge at only 2/3, and we identify it as the open
problem for verification-first legal RAG. We release the audit
pipeline (MIT) and the benchmark (CC0).

----------------------------------------------------------------------

## Why this abstract works for arXiv moderation

1. **Opens with a recognised problem** (Magesh/Dahl numbers cited by Butler & Butler 2026).
2. **Names the contribution** in sentence 2: a 5-rail audit pipeline.
3. **Specifies the cost budget** — 3 ms / 2 s / $0.005 — concrete, defensible.
4. **Cites the benchmark** as the second contribution, not buried.
5. **Reports a falsifiable measurement** (3.0 %, 16.7 %, 50 %) — researcher can re-run our public artifact and verify.
6. **Distinguishes us from Magesh** by naming the model and condition explicitly.
7. **Identifies an open problem** in sentence 9 — strongest signal that this is research, not a release.
8. **Closes with reproducibility** (CC0 + MIT) — moderator-friendly.

## What changed vs Draft A

- **"prevent 79 % of fabricated-authority outputs"** removed (was a placeholder; real number is more nuanced).
- Replaced with three honest measured numbers: 3.0 % citation-level, 16.7 % deterministic recall at 0 % FPR, 50 % full-audit recall at 25 % FPR.
- Added the **residual-error-class identification** as the paper's intellectual contribution beyond the rails.
- Stayed under 200 words.
