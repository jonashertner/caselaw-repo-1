# Swiss Legal RAG Bench

An end-to-end benchmark for retrieval-augmented generation systems operating
on Swiss federal and cantonal law.

Modelled on Butler & Butler, *Legal RAG Bench* (Isaacus, March 2026 — arXiv
2603.01710), this benchmark evaluates a RAG pipeline along three orthogonal
dimensions and decomposes errors into the failure-mode taxonomy the paper
established.

## Why

The OpenCaseLaw search-quality benchmark (`benchmarks/search_benchmark_*.json`)
measures **retrieval accuracy only** (Hit@1, MRR). It cannot tell us whether
a generative model deployed on top of our retrieval actually produces a
**grounded** and **correct** answer, nor can it decompose end-to-end errors
into hallucination / retrieval / reasoning components — the very decomposition
that Butler & Butler 2026 showed is necessary to know what to fix.

Swiss Legal RAG Bench fills that gap. It is the first end-to-end benchmark
specifically for Swiss legal RAG, covering:

- **All three official languages** (DE, FR, IT) — Swiss law is multilingual
  and a useful Swiss benchmark must measure cross-language retrieval.
- **Federal statute lookups** — Fedlex articles in their canonical form.
- **Federal-court doctrine** — leading-case knowledge from the BGE corpus.
- **Procedural mechanics** — thresholds, deadlines, and procedural rules
  that can be answered exactly from the statute.

## Methodology

For each question `i`, embedder `e`, and generator LLM `l`, three binary
signals are computed:

| Signal | Definition |
|---|---|
| `c_eli` (correctness) | The model's answer entails the reference answer. |
| `g_eli` (groundedness) | The model's answer is supported by the retrieved passages. |
| `r_eli` (retrieval) | At least one of the annotated supporting passages was retrieved. |

Errors decompose into three classes (Butler & Butler 2026, Figure 1):

```
                            answer grounded?
                       ┌───────────┴───────────┐
                       no                     yes
                       │                       │
                  HALLUCINATION           answer correct?
                                       ┌──────┴──────┐
                                       no           yes
                                       │             │
                                relevant         CORRECT
                                retrieved?
                                ┌──────┴──────┐
                                no           yes
                                │             │
                          RETRIEVAL ERR   REASONING ERR
```

Concretely:

- **Hallucination**: `g_eli = 0`. Model invented facts not in retrieved context.
- **Retrieval error**: `g_eli = 1`, `c_eli = 0`, `r_eli = 0`. Grounded answer
  but wrong, because the supporting passage wasn't retrieved.
- **Reasoning error**: `g_eli = 1`, `c_eli = 0`, `r_eli = 1`. Supporting
  passage was retrieved but the model still got the answer wrong.

This decomposition triangulates *which component* (embedder vs LLM) drives
end-to-end error, which is necessary to know what to invest in.

## Dataset

`questions.jsonl` — one JSON object per line. Schema:

```json
{
  "id":          "q-001",
  "language":   "de | fr | it",
  "legal_area": "Schuldrecht / Haftpflichtrecht",
  "difficulty": "basic | intermediate | advanced",
  "question":   "Welche vier Voraussetzungen ...",
  "reference_answer": "Schaden, Widerrechtlichkeit, ...",
  "evidence": {
    "statutes":  [{"law_code": "OR", "sr_number": "220",
                   "article": "41", "language": "de"}],
    "decisions": ["bge_BGE_132_III_122"]
  },
  "claim_type": "elements_of_norm | statute_text | statute_thresholds |
                 statute_procedure | statute_change | doctrine_from_bge |
                 norm_overview"
}
```

`evidence.decisions` lists canonical OpenCaseLaw `decision_id`s (matches
the IDs in `mcp.opencaselaw.ch/entscheid/<id>`). `evidence.statutes` lists
the SR-number + article that authoritatively answers the question.

### Versioning

The benchmark is anchored to a corpus snapshot. The first release is
**v0.1**, anchored to corpus state on **2026-04-28** (969,738 decisions).
Adding questions or fixing annotations bumps the patch version; changing
the evaluation methodology bumps minor.

### License

CC0 — same licence as the underlying OpenCaseLaw dataset. Use, redistribute,
modify, and re-publish freely.

## Evaluation harness

`evaluate.py` runs a configured RAG pipeline against the questions and
emits per-question + per-dimension scores plus the error decomposition.

```bash
python3 -m benchmarks.swiss_legal_rag_bench.evaluate \
    --mcp-url https://mcp.opencaselaw.ch \
    --generator claude-sonnet-4-6 \
    --top-k 5 \
    --output benchmarks/swiss_legal_rag_bench/results/run_2026-04-28.json
```

The harness expects:

- A retrieval function: `(query, language, top_k) -> list[passage_id]`.
  The default uses MCP `search_decisions` against the live corpus; swap
  in any retriever to compare.
- A generator function: `(query, retrieved_passages_text) -> answer`.
  The default uses Claude via the Anthropic API; swap in any LLM.
- A judge function: `(claim, evidence_text) -> {supports, ...}`. The
  default uses Claude Sonnet 4.6 in high-reasoning mode (the paper's
  judge baseline used GPT-5.2 in high-reasoning mode; we use Sonnet
  4.6 because that is the model we use elsewhere in the stack and we
  want apples-to-apples).

## Results (v0.1, 2026-04-28)

To be populated by the first run. Each cell reports
`(c%, g%, r%)` averaged across the question set.

| Embedder / Retriever | Generator | Correctness | Groundedness | Retrieval acc. |
|---|---|---|---|---|
| OpenCaseLaw (BM25 + RRF + Haiku rerank) | Claude Sonnet 4.6 | TBD | TBD | TBD |

## Scope and limitations

- **Initial size: 10 seed questions.** This is small. The paper used 100
  hand-crafted questions; we will grow toward that target via expert
  annotation. The framework + methodology are stable from v0.1.
- **Coverage: federal law only.** Cantonal procedural and material law is
  out of scope for v0.1; the next version will add cantonal questions.
- **Reference answers are concise** (1–4 sentences). This is by design —
  longer reference answers make the entailment judge less reliable.
- **Annotated evidence is not exhaustive.** A question's `evidence.decisions`
  lists *one* leading case where multiple are equally authoritative; the
  retrieval-accuracy metric `r_eli` is satisfied if *any* listed evidence
  passage is retrieved.

## Citation

If you use Swiss Legal RAG Bench in published work, please cite:

> OpenCaseLaw contributors. *Swiss Legal RAG Bench: an end-to-end RAG
> benchmark for the Swiss federal corpus.* v0.1, 28 April 2026.
> https://github.com/jonashertner/caselaw-repo-1/tree/main/benchmarks/swiss_legal_rag_bench

And the methodological reference:

> Butler, A. R. & Butler, U. *Legal RAG Bench: an end-to-end benchmark for
> legal RAG.* Isaacus, 2 March 2026. arXiv:2603.01710.
