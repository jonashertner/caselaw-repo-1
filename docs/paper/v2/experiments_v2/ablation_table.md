# Per-rail ablation — v0.2 (n=30, prior-only condition)

**Setup:** Claude Sonnet 4.6 generates an answer for each of the 30 questions in v0.2 *without retrieval* (prior-only condition). The closing audit (`attest_response`, `audit_grounding=True`) runs all 5 rails. Per-configuration WFR (wrong-draft flag rate) and FPR computed in post-processing by intersecting the rail-fire flags with each configuration's enabled rail set.

**Ground-truth labels:** a second-pass Sonnet call (same model family as the generator) scores each draft for `c_eli` (does the answer entail the v0.2 reference answer?). 6 of 30 drafts labelled wrong by this judge; 24 correct. WFR counts *any* enabled rail firing on a wrong-labelled draft and is NOT a `catch rate' / `TPR' (firing on a wrong draft does not certify that the flag identifies the draft's primary error; see paper §5).

## Table 4 — per-configuration flag rates

| Config | Rails | WFR (flagged wrong) | FPR (flagged correct) | Net (WFR – FPR) |
|---|---|---:|---:|---:|
| C0 | ∅ (no rails) | 0/6 = **0.0 %** | 0/24 = 0.0 % | **+0.0 pp** |
| C1 | + case | 0/6 = **0.0 %** | 1/24 = 4.2 % | **-4.2 pp** |
| C2 | + statute | 0/6 = **0.0 %** | 3/24 = 12.5 % | **-12.5 pp** |
| C3 | + quote | 1/6 = **16.7 %** | 3/24 = 12.5 % | **+4.2 pp** |
| C4 | + date  (all 4 deterministic) | 1/6 = **16.7 %** | 3/24 = 12.5 % | **+4.2 pp** |
| C5 | + grounding  (full 5-rail audit) | 4/6 = **66.7 %** | 12/24 = 50.0 % | **+16.7 pp** |

## Table 5 — per-rail solo activation

(How many drafts each rail fires on, regardless of other rails. Reveals each rail's independent contribution.)

| Rail | Fires on N drafts | Fires on wrong | Fires on correct | Solo WFR | Solo FPR |
|---|---:|---:|---:|---:|---:|
| case | 1/30 | 0 | 1 | 0.0 % | 4.2 % |
| statute | 2/30 | 0 | 2 | 0.0 % | 8.3 % |
| quote | 1/30 | 1 | 0 | 16.7 % | 0.0 % |
| date | 0/30 | 0 | 0 | 0.0 % | 0.0 % |
| grounding | 13/30 | 3 | 10 | 50.0 % | 41.7 % |

## Citation accounting

- **Citations emitted by Sonnet across all 30 prior-only drafts:** 37
- **Citations that resolve in the corpus:** 36
- **Validity rate (= 1 − fabrication rate at the citation level):** 97.3 %

## By language

| Lang | n | wrong | rail fired (any) | rail-fire rate |
|---|---:|---:|---:|---:|
| de | 18 | 4 | 8 | 44.4 % |
| fr | 8 | 1 | 7 | 87.5 % |
| it | 4 | 1 | 1 | 25.0 % |

## Issue type breakdown

| Category | Total issues raised across all 30 drafts |
|---|---:|
| case | 1 |
| statute | 2 |
| quote | 1 |
| date | 0 |
| grounding | 13 |

## Notes for paper §5

- The prior-only condition (no retrieval) is chosen as a stress test of the audit rails: it maximises the density of recoverable errors by forcing the model to rely on training knowledge alone. It does NOT guarantee invented citations (in this run the model produced very few). See paper §5 for the qualified framing.
- Per-rail WFR is monotone non-decreasing as rails are added (each rail can only flag more, never fewer). FPR is also monotone non-decreasing — a known tradeoff. The Net column shows the marginal contribution.
- The cite-citation validity rate (Sonnet's citation-level fabrication rate without retrieval) is reported alongside Magesh et al. 2025's 17–33% measurements on commercial legal-RAG tools, but the two are NOT a like-for-like comparison: different model families, query distributions, and tool conditions. See paper §5 for the qualified framing.