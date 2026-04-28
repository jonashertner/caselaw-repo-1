# Contribution table — what we claim, what proves it, what's reproducible

This is the explicit "for each contribution: claim / evidence /
reproducibility manifest" table that Section 1 of the paper makes
verbally. The arXiv moderator looks at this implicitly when deciding
whether the paper is a research contribution or a pure release.

----------------------------------------------------------------------

## C1 — Defense-in-depth audit pipeline for legal RAG

| Aspect | Specification |
|--------|---------------|
| **Claim** | A 5-rail closing audit, each rail addressing one named error class, achieves measurable hallucination-rate reduction on legal-RAG output with bounded cost (≤3 ms for the 4 deterministic rails, ≤2 s for the LLM-judge rail). |
| **Novelty** | Prior work treats legal hallucination either as a single black-box output to judge (Magesh 2024 uses a holistic judge) or as a per-citation retrieval check (commercial legal-RAG tools). We decompose by error class and show that **the decomposition itself reveals which class is most expensive to fix and which can be solved deterministically**. |
| **Evidence** | Per-rail ablation table from Step 4 experiment: 6 audit configurations × 30 questions = 180 trials, measuring (a) catch rate per rail per error class, (b) total false-positive rate, (c) latency budget. |
| **Reproducibility** | `mcp_server.py:_handle_attest_response` (live in production); `tests/web/test_attest_audits.py` (37 unit tests); `submissions/mcp-registries/02-mcp-registry.md` (the manifest). MIT-licensed. Anyone can run the same ablation against their corpus by configuring our audit harness. |
| **Generalisation** | Each rail is corpus-agnostic. The case-existence rail needs an enumerable decision corpus. The statute rail needs a statute mirror. The quote rail needs the cited decisions' verbatim text. None are Swiss-specific. We discuss generalisation in §5.1. |

## C2 — Swiss Legal RAG Bench v0.2 (multilingual extension)

| Aspect | Specification |
|--------|---------------|
| **Claim** | A 30-question expert-annotated benchmark for multilingual legal RAG, extending Butler & Butler 2026's methodology with cross-language retrieval as a fourth evaluation dimension. |
| **Novelty** | Switzerland is the only major civil-law jurisdiction where the *authoritative version of a leading case may be in a language other than the question's*. The cross-language dimension is genuinely missing from existing legal-RAG benchmarks (Butler & Butler 2026 is Australian/English; LegalBench-RAG is US/English; HousingQA is US/English). |
| **Evidence** | Benchmark file: `benchmarks/swiss_legal_rag_bench/questions.jsonl` (30 questions in v0.2). Methodology: extension of the c/g/r 4-leaf taxonomy with a 5th leaf for cross-language retrieval failures. Baseline: live OpenCaseLaw stack + Claude Sonnet 4.6 results in §4.3. |
| **Reproducibility** | Benchmark on Hugging Face (`voilaj/swiss-legal-rag-bench`, CC0). Evaluation harness `benchmarks/swiss_legal_rag_bench/evaluate.py` (MIT). Anyone can swap the retriever/generator/judge in 3 lines and re-run. |
| **Limitations** | 30 questions is below the Butler & Butler 100-question target. We're explicit about this in §6. v0.3 (planned) grows to 100. |

## What we are NOT claiming (defensive)

| Non-claim | Why we're explicit |
|-----------|--------------------|
| "Best-in-class retrieval" | We don't out-perform Kanon 2 Embedder. Our retrieval is BM25 + RRF + Haiku rerank — solid but not novel. We say so in §3. |
| "First Swiss legal corpus" | Swiss-Judgment-Prediction (Niklaus 2021), SCALE (Rasiah 2023) preceded us with narrower scope. We position as **broadest-coverage** open Swiss corpus, not first. |
| "Eliminates hallucination" | We **reduce** measurable hallucination rate by N%; we don't eliminate. The LLM-judge rail itself can fail. We say so in §6.1. |
| "Better than commercial legal-RAG" | We compare against Magesh 2024's published numbers. We don't claim to outperform Lexis+ AI / Westlaw AI directly because we lack access to run a controlled head-to-head. |

## Reproducibility manifest (Appendix A material)

Everything needed to reproduce the paper's quantitative claims:

| Artifact | URL | Licence |
|----------|-----|---------|
| Audit pipeline source | `github.com/jonashertner/caselaw-repo-1/blob/main/mcp_server.py#_handle_attest_response` | MIT |
| Unit tests | `github.com/jonashertner/caselaw-repo-1/tree/main/tests/web/test_attest_audits.py` | MIT |
| Benchmark questions + harness | HF `voilaj/swiss-legal-rag-bench` v0.2 | CC0 |
| Underlying corpus | HF `voilaj/swiss-caselaw` (969k decisions) | CC0 |
| Live MCP endpoint (for reproduction) | `mcp.opencaselaw.ch` | — |
| Frozen paper-release snapshot | `artifacts/paper_release_2026-04-XX/` | CC0 |
| Per-rail ablation results | `docs/paper/v2/tables/ablation.json` (Step 4 output) | CC0 |
| Cost telemetry methodology | `scripts/llm_usage_report.py` | MIT |
