# Per-rail ablation — v0.2 (n=30, prior-only condition)

**Setup:** Claude Sonnet 4.6 generates an answer for each of the 30 questions in v0.2 *without retrieval* (prior-only condition). The closing audit (`attest_response`, `audit_grounding=True`) runs all 5 rails. Per-configuration TPR / FPR computed in post-processing by intersecting the rail-fire flags with each configuration's enabled rail set.

**Ground truth:** an independent Sonnet judge scores each draft for `c_eli` (does the answer entail the v0.2 reference answer?). 6 of 30 drafts judged wrong (= ground truth fabrications); 24 correct.

## Table 4 — per-configuration catch rates

| Config | Rails | TPR (catch wrong) | FPR (flag correct) | Net (TPR – FPR) |
|---|---|---:|---:|---:|
| C0 | ∅ (no rails) | 0/6 = **0.0 %** | 0/24 = 0.0 % | **+0.0 pp** |
| C1 | + case | 1/6 = **16.7 %** | 0/24 = 0.0 % | **+16.7 pp** |
| C2 | + statute | 1/6 = **16.7 %** | 1/24 = 4.2 % | **+12.5 pp** |
| C3 | + quote | 1/6 = **16.7 %** | 2/24 = 8.3 % | **+8.3 pp** |
| C4 | + date  (all 4 deterministic) | 1/6 = **16.7 %** | 2/24 = 8.3 % | **+8.3 pp** |
| C5 | + grounding  (full 5-rail audit) | 3/6 = **50.0 %** | 6/24 = 25.0 % | **+25.0 pp** |

## Table 5 — per-rail solo activation

(How many drafts each rail fires on, regardless of other rails. Reveals each rail's independent contribution.)

| Rail | Fires on N drafts | Fires on wrong | Fires on correct | Solo TPR | Solo FPR |
|---|---:|---:|---:|---:|---:|
| case | 1/30 | 1 | 0 | 16.7 % | 0.0 % |
| statute | 1/30 | 0 | 1 | 0.0 % | 4.2 % |
| quote | 1/30 | 0 | 1 | 0.0 % | 4.2 % |
| date | 0/30 | 0 | 0 | 0.0 % | 0.0 % |
| grounding | 8/30 | 2 | 6 | 33.3 % | 25.0 % |

## Citation accounting

- **Citations emitted by Sonnet across all 30 prior-only drafts:** 33
- **Citations that resolve in the corpus:** 32
- **Validity rate (= 1 − fabrication rate at the citation level):** 97.0 %

## By language

| Lang | n | wrong | rail fired (any) | rail-fire rate |
|---|---:|---:|---:|---:|
| de | 18 | 5 | 3 | 16.7 % |
| fr | 8 | 0 | 3 | 37.5 % |
| it | 4 | 1 | 3 | 75.0 % |

## Issue type breakdown

| Category | Total issues raised across all 30 drafts |
|---|---:|
| case | 1 |
| statute | 2 |
| quote | 1 |
| date | 0 |
| grounding | 9 |

## Notes for paper §5

- The prior-only condition (no retrieval) is the cleanest test of the rails: it guarantees Sonnet has to invent some Swiss legal references, providing a substrate the rails can detect. Adding retrieval would lower the prior to detect.
- Per-rail TPR is monotone non-decreasing as rails are added (each rail can only catch more, never fewer). FPR is also monotone non-decreasing — a known tradeoff. The Net column shows the marginal contribution.
- The cite-citation validity rate (Sonnet's citation-level fabrication rate without retrieval) is directly comparable to Magesh et al. 2024's 17–33% measurements on commercial legal-RAG tools.