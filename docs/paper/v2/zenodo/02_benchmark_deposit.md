# Zenodo deposit 2 — Swiss Legal RAG Bench v0.2

Separate from the corpus deposit so that the benchmark gets its own
DOI (academic-citable independently of the underlying data). The
benchmark + audit-pipeline source code together form the artefact that
the paper's experimental section relies on.

----------------------------------------------------------------------

## Upload type

**Software** (with bundled dataset)

## Title

```
Swiss Legal RAG Bench v0.2 — A Multilingual Benchmark for Legal
Retrieval-Augmented Generation, with a Defense-in-Depth Audit
Pipeline Reference Implementation
```

## Authors

| # | Name | Affiliation | ORCID | Email |
|---|------|-------------|-------|-------|
| 1 | Jonas Hertner | Independent Researcher, Zurich | [add if you have one] | team@jonashertner.com |

## Description (abstract — paste verbatim)

```
Swiss Legal RAG Bench is a public benchmark for evaluating
retrieval-augmented generation (RAG) systems on Swiss federal law,
extending the methodology of Butler & Butler (2026, Isaacus, Legal
RAG Bench, arXiv:2603.01710) to a multilingual setting (German,
French, Italian).

Version 0.2 contains 30 expert-annotated questions across the major
substantive areas of Swiss federal law: Schuldrecht (obligations),
Strafrecht (criminal), Verfassungsrecht (constitutional),
Sachenrecht (property), Mietrecht (tenancy), Verfahrensrecht
(procedural), Verjährung (limitation periods), and
Wirtschaftsfreiheit (economic freedom). Each question is paired
with a concise reference answer (1–4 sentences) and annotated
supporting evidence (statute SR-numbers + leading-case
decision_ids).

The deposit also includes the reference implementation of a
five-rail closing audit pipeline for legal-RAG output, addressing
the four hallucination classes documented in Magesh et al. (2024,
Hallucination-Free?) and Dahl et al. (2024, Large Legal Fictions):

  Rail 1 — case-citation existence + pinpoint resolution
  Rail 2 — statute-reference resolution against statute mirror
  Rail 3 — verbatim-quote source-matching
  Rail 4 — decision-date sanity
  Rail 5 — opt-in proposition grounding via independent LLM judge

Rails 1–4 are deterministic and add ~3 ms per draft; rail 5 is
judge-mediated and adds ~2 s and ~$0.005 per call.

Released under CC0-1.0 (questions + harness) and MIT (audit code).
```

## Publication date

`2026-04-28` (or the date you click publish)

## Publisher

`Zenodo`

## Keywords

```
legal informatics
retrieval-augmented generation
RAG
benchmark
multilingual
swiss law
hallucination detection
verification
LLM
evaluation
DE FR IT
open access
```

## Language

`English` (with multilingual content)

## Licence

For the benchmark questions: **Creative Commons Zero v1.0 (CC0-1.0)**
For the audit-pipeline source: **MIT License**

The Zenodo record sets the **default** licence to CC0; per-file MIT
metadata is in `LICENSE-MIT` inside the deposit.

## Access right

`Open Access`

## Subjects

- Law (FOR 18)
- Computer Sciences (FOR 08)
- Artificial Intelligence (FOR 0801)

## Related identifiers

| Identifier | Relation | Resource type |
|---|---|---|
| 10.5281/zenodo.[corpus-DOI-from-deposit-1] | Builds on | Dataset |
| 10.48550/arXiv.2603.01710 | Is methodologically based on | Publication (Butler & Butler 2026) |
| 10.48550/arXiv.2401.01301 | Cites | Publication (Dahl et al. 2024) |
| 10.48550/arXiv.2405.20362 | Cites | Publication (Magesh et al. 2024) |
| https://huggingface.co/datasets/voilaj/swiss-legal-rag-bench | Is identical to | Dataset |
| https://github.com/jonashertner/caselaw-repo-1 | Is part of | Software |

## Funding

`None — independent research, self-funded`

## Files to upload

```
swiss-legal-rag-bench-v0.2.zip
├── README.md                                    deposit landing page
├── LICENSE-CC0                                   for questions + docs
├── LICENSE-MIT                                   for code
├── questions.jsonl                               30 questions
├── evaluate.py                                   evaluation harness
├── audit/                                        rail implementations
│   ├── __init__.py
│   ├── rail1_case_citation.py
│   ├── rail2_statute.py
│   ├── rail3_quote.py
│   ├── rail4_date.py
│   └── rail5_grounding.py
├── results/
│   ├── ablation_2026-04.json                    per-rail ablation
│   └── baseline_butler_butler_comparison.json   table-5 source
├── tests/
│   └── test_audit_rails.py
└── docs/
    ├── methodology.md                            Butler & Butler extension
    ├── claim_types.md                            7 claim categories
    └── language_distribution.md                  DE/FR/IT balance
```

**Size:** ~50 KB total. Trivial to upload; zip is enough.

## Pre-publish checklist

- [ ] Confirm questions.jsonl has v0.2 = 30 questions (Step 3 output)
- [ ] Confirm ablation results from Step 4 are in `results/`
- [ ] Confirm `audit/` rail implementations match the live MCP code
- [ ] "Reserve DOI" button BEFORE filling the rest, embed in README

## After publication

- Update HF dataset card (`voilaj/swiss-legal-rag-bench`) with the
  Zenodo DOI badge
- Update arXiv paper's `\section{Data Availability}` with the DOI
- Update opencaselaw.ch verification section to cite the DOI when
  pointing readers at the benchmark

## Zenodo URL after publishing

`https://doi.org/10.5281/zenodo.<NUMBER>`
