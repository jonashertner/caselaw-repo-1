# Step 1 brief — paper concept lock-in

This file is the agreement we work from. We confirm title, abstract,
contributions, target length and category here, then everything
downstream (experiments, sections, figures) is derived from it.

----------------------------------------------------------------------

## Working title

> **Verification-First Legal RAG: A Defense-in-Depth Audit Pipeline and
> a Multilingual Swiss-Law Benchmark**

Why this title:
- "Verification-First" plants the conceptual flag in the first word
- "Defense-in-Depth" is the architectural framing that distinguishes
  this from "verification" as a single LLM-judge call
- "Audit Pipeline" is the artifact (5 rails) that the paper *is about*
- "Multilingual Swiss-Law Benchmark" makes the second contribution
  visible in the title (matters for citation searches)

Alternative shorter title (back-up): **"Five Rails Against Legal-RAG
Hallucination, with a Swiss Multilingual Benchmark"**

## One-sentence pitch

> A 5-rail closing audit (case-citation existence + statute existence +
> verbatim-quote match + decision-date sanity + opt-in proposition
> grounding) reduces fabricated-authority output of a frontier LLM by
> X percentage points on a new Swiss multilingual benchmark, with
> per-rail ablation isolating each error class' contribution.

## Two contributions (each isolable, each replicable)

**C1 — Methodology.** A defense-in-depth audit pipeline for legal RAG.
Five rails, four deterministic and one judge-mediated, each addressing
one named hallucination class. Code MIT-licensed; rails can be lifted
into any retrieval-augmented LLM stack serving a closed legal corpus.

**C2 — Resource.** Swiss Legal RAG Bench v0.2 — first multilingual
(DE/FR/IT) benchmark for Swiss legal RAG, modelled on Butler & Butler
2026 (Isaacus, *Legal RAG Bench*) with cross-lingual retrieval added
as a fourth dimension. 30+ expert-annotated questions with reference
answers and supporting evidence. CC0 on Hugging Face.

## Target length and category

- 8 pages + references (= ~12 pages total) using the standard ACL/EMNLP
  one-column LaTeX style
- Primary arXiv category: **cs.CL** (Computation and Language)
- Secondary cross-list: **cs.IR** (Information Retrieval)
- Optional second cross-list: **cs.CY** (Computers and Society —
  legal/public-interest application)

## Diagnostic questions for v1 rejection (please answer)

To shape v2 correctly:

1. **Rejection text.** What did the moderators actually say? Even one
   sentence of the rejection email would let me target the exact
   objection. If you don't have it, we proceed on the most-likely-cause
   hypothesis: "no novel methodology beyond resource description".

2. **Endorsement.** First-time arXiv submitters in cs.CL / cs.IR need
   an endorsement. Have you been endorsed before in either category?
   (If yes: nothing to do. If no: we either need an endorser before
   submission, or we route through a category that doesn't gate
   first-time submissions.)

3. **Author list.** Solo (Jonas Hertner)? Or do you want to bring in
   a co-author from a Swiss law faculty / informatics chair? Adding an
   institutional affiliation line is the single biggest credibility
   signal a moderator sees.

4. **Reclassification check.** Was v1 actually rejected, or was it
   reclassified to a category you didn't want? These have very
   different fixes — please confirm.

## What I'm doing while you answer

These four artifacts will exist by the time you reply, none depending
on your answers:

- `01_title_and_abstract.md` — three abstract drafts to pick from
- `02_contribution_table.md` — explicit per-contribution claim, evidence,
  reproducibility manifest
- `03_related_work_map.md` — the 12-paper related-work landscape
  with positioning of ours vs each
- LaTeX skeleton in `paper.tex` with placeholder sections, ready to
  fill in Step 5

## The 7-step plan, locked

| Step | What | Effort | Dependency |
|------|------|--------|------------|
| 1 | Concept lock-in (this brief)              | 30 min together | YOUR answers to the 4 questions above |
| 2 | Mint Zenodo DOIs (corpus + benchmark)     | 30 min together | nothing |
| 3 | Grow Swiss Legal RAG Bench 10 → 30 questions | 6 hrs your time | I draft 20 candidates; you vet |
| 4 | Per-rail ablation experiment + tables     | 30 min autonomous | Step 3 done |
| 5 | Draft paper §§1–7 in LaTeX                | days 3–5 autonomous | Steps 1, 4 done |
| 6 | Internal review                           | 2 hrs together | Step 5 done |
| 7 | Compile + submit to arXiv                 | 30 min together | Step 6 done |

Total: ~1 week elapsed. Most of the time is autonomous; your input
clusters in steps 1, 3, 6, 7.
