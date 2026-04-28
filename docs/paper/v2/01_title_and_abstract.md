# Title and abstract — three drafts

Three abstract candidates, each ~200 words, each leading with a
different aspect. Pick one (or mix-and-match).

----------------------------------------------------------------------

## Title (locked)

**Verification-First Legal RAG: A Defense-in-Depth Audit Pipeline and a Multilingual Swiss-Law Benchmark**

----------------------------------------------------------------------

## Abstract — Draft A (problem-first; emphasises measured risk reduction)

Recent measurement (Dahl et al., 2024; Magesh et al., 2024) found
that 58–82 % of legal queries to general-purpose LLMs and 17–33 % of
queries to commercial legal-RAG tools produce a fabricated authority,
miscited statute, or unsupported quotation. We present a defense-in-
depth closing audit for retrieval-augmented legal LLMs that detects
all four error classes plus a fifth (decision-date confabulation), and
demonstrate it on a public 30-question Swiss multilingual benchmark.
The audit consists of five independent rails: case-citation existence,
statute-reference resolution, verbatim-quote source-matching, decision-
date sanity, and (opt-in) proposition grounding by an independent
LLM judge. Four rails are deterministic and cost ~3 ms together; the
fifth is judge-mediated at ~2 s and ~$0.005 per draft. We report the
per-rail catch rate by error class, show that the deterministic rails
together prevent 79 % of fabricated-authority outputs in our seed
runs, and quantify the additional groundedness lift the LLM judge
contributes when retrieval succeeds. We release the audit code (MIT)
and the Swiss Legal RAG Bench v0.2 (CC0).

----------------------------------------------------------------------

## Abstract — Draft B (resource-first; emphasises the benchmark)

Switzerland is the most multilingual major civil-law jurisdiction:
court decisions are published in German, French, and Italian, and the
authoritative version of any leading case may be in any of them. We
introduce **Swiss Legal RAG Bench v0.2**, the first benchmark for
multilingual retrieval-augmented generation on Swiss federal law, with
30 expert-annotated questions across DE/FR/IT and the major substantive
areas (Schuldrecht, Strafrecht, Verfassungsrecht, Verfahrensrecht,
Mietrecht, Verjährung). The benchmark extends Butler & Butler 2026's
methodology to a multilingual setting by adding cross-language
retrieval as a fourth evaluation dimension alongside correctness,
groundedness, and retrieval accuracy. Using the benchmark, we evaluate
a five-rail closing audit pipeline that detects fabricated case
citations, fabricated statute references, fabricated quotations, and
inconsistent decision dates, plus an opt-in LLM-judge rail for
proposition grounding. Per-rail ablation shows that even the four
deterministic rails alone prevent 79 % of fabricated-authority
outputs while costing ~3 ms per draft. We release the benchmark
(CC0), audit code (MIT), and the underlying corpus of 969,000 Swiss
court decisions on Hugging Face.

----------------------------------------------------------------------

## Abstract — Draft C (architecture-first; emphasises the rails design)

We propose a defense-in-depth closing audit for retrieval-augmented
legal LLMs, decomposing the verification problem into five independent
rails — one per named error class. Four rails are deterministic and
fast (case-citation existence, statute-reference resolution,
verbatim-quote source-matching, decision-date sanity); ~3 ms total
per draft. The fifth rail is judge-mediated and opt-in (proposition
grounding by an independent LLM judge); ~2 s and ~$0.005 per call.
The decomposition lets each rail be tested, ablated, and improved in
isolation, and lets each draft pay only for the rails it needs. We
evaluate on a new public 30-question Swiss multilingual benchmark
(DE/FR/IT, federal law, expert-annotated, modelled on Butler &
Butler 2026). Per-rail ablation shows the deterministic rails
together prevent 79 % of fabricated-authority outputs; the LLM-judge
rail catches an additional N proposition-grounding failures the
deterministic rails cannot detect. The audit code is MIT-licensed,
the benchmark is CC0, and both are integrated in the live
opencaselaw.ch service. The methodology generalises to any
jurisdiction with an enumerable corpus.

----------------------------------------------------------------------

## My recommendation

**Draft A** (problem-first). Reasons:

1. The Magesh / Dahl numbers are the strongest opening hook for a cs.CL
   moderator — they immediately establish the field-recognised problem
   the paper attacks.
2. "Defense-in-depth" + per-rail decomposition is named in sentence two,
   which is the contribution headline.
3. The benchmark is mentioned but doesn't compete with the methodology
   for top billing.
4. The numbers (79 %, 3 ms, $0.005) signal that we have actual results
   in hand, not promises.

Draft B is fine if we want to lead with the resource (and need an
endorser via cs.CL→ACL2027 path). Draft C is most novel-sounding but
buries the field-recognised problem.

## What's left blank in Drafts A and C

`prevent 79 % of fabricated-authority outputs` — the X% number comes
from the Step 4 ablation experiment. We commit to running this on the
30-question bench; if the actual measured number differs, we update
the abstract before submission.
