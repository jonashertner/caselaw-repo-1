# Paper 2 (Evaluation) — in preparation

**Working title**: *Cross-Lingual Retrieval and Verification Diagnostics
for Swiss Legal RAG*

This is the companion to `p1-resource/` (the corpus + graph + identifier
+ provenance release). Paper 2 covers what the resource enables:
measurement of cross-lingual retrieval, audit-rail behaviour, and
hallucination characterisation on top of the published substrate.

## Planned scope

1. **Cross-lingual retrieval diagnostic at n ≥ 200**
   - Lawyer-authored held-out queries (NOT regeste-derived)
   - Italian-original target cases included
   - Multi-annotator gold (3–5 reviewers, Krippendorff α reported)
   - Bootstrap 95% CIs on per-cell MRR@10 and Hit@10
   - The IT→DE vs DE→FR asymmetry test (already significant at p=0.0011
     in the n=150 single-curator v1, to be re-confirmed at n≥200)

2. **Five-rail closing audit with adversarial probes**
   - R1 (case-citation existence): ~100 ground-truth probes, precision/recall
   - R2 (statute resolution): ~100 ground-truth probes
   - R3 (verbatim-quote source matching): ~100 quoted-substring probes
     including paraphrase-in-quotes
   - R4 (decision-date sanity): date-mismatch probes
   - R5 (proposition grounding): human-calibrated 100-pair set with
     LLM-vs-human precision/recall

3. **Prior-only vs retrieval-augmented bench at n ≥ 200**
   - Expand the n=30 set to n≥200 to reach statistical power
   - Re-run the McNemar paired test (n=30 produced p=0.065, NOT
     significant; n≥200 will resolve direction)
   - Multi-judge ensemble (different model families) to remove
     self-confirmation bias

4. **Citation-precision human audit**
   - Annotate the 400-sample stratified set already shipped at
     `benchmarks/citation_precision_sample_400.jsonl`
   - Replace the date-sanity proxies with true precision intervals

## Blockers

1. Multi-annotator panel: 3–5 paid Swiss lawyers (DE/FR/IT native)
   covering both annotation and inter-annotator agreement
2. Lawyer-authored query rewrite (no regeste peeking)
3. Human calibration of R5 grounding judge
4. Annotation passes on the 400-sample citation-precision set
5. API budget for multi-judge runs (DeepSeek/GPT-5.2 alongside Sonnet)

## Framework code already shipped

- `benchmarks/swiss_legal_rag_bench/statistical_analysis.py` — Wilson
  CIs, McNemar exact, bootstrap CIs, permutation tests on existing data
- `benchmarks/swiss_legal_rag_bench/results/statistical_analysis.json`
  — current numbers with CIs against n=150 + n=30
- `benchmarks/audit_rails/R1_adversarial_probes_v1.jsonl` — 17-probe
  ground-truth seed set for R1
- `benchmarks/audit_rails/README.md` — schema + runner pattern
- `benchmarks/citation_precision_audit.py` + sample set — ready for
  annotation passes

## Timeline

Paper 2 ships after the multi-annotator work is funded and executed.
We have not committed a public timeline. The work is gated on operator
decisions about budget and lawyer-annotator availability.

## Target venues

EMNLP main (evaluation paper). ACL main as alternative. JURIX for the
legal-AI-specialist audience.
