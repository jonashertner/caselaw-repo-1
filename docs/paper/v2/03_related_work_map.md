# Related work map

The 12 papers that frame our work, plus how we position vs each.
This is the §2 source material; we'll prose-ify in Step 5.

----------------------------------------------------------------------

## Tier 1 — direct methodological lineage (cite + extend)

### 1. Butler & Butler 2026 — *Legal RAG Bench: an end-to-end benchmark for legal RAG* (Isaacus, arXiv:2603.01710, March 2026)

**Their move:** First end-to-end RAG benchmark for legal queries with
4-leaf error decomposition (correctness × groundedness × retrieval
accuracy → correct / hallucination / retrieval-error / reasoning-error).
4,876 passages from Victorian Criminal Charge Book + 100 hand-crafted
questions. Full factorial over 3 embedders × 2 LLMs.

**How we extend:** Add cross-language retrieval as a fifth leaf.
Replicate methodology on Swiss multilingual law (DE/FR/IT). Their
methodology is the gold standard; we apply it to a setting they
explicitly didn't cover.

**Cite as:** the methodological foundation. Their abstract is on our
verification page already.

### 2. Dahl, Magesh, Suzgun, Ho 2024 — *Large Legal Fictions: Profiling Legal Hallucinations in Large Language Models* (Stanford RegLab, J. Legal Analysis 16(1), arXiv:2401.01301)

**Their move:** Measure legal-LLM hallucination rate on general LLMs
across 6 task types. Found 58–82 % hallucination rate.

**How we use:** the **problem statement** for our paper. Section 1
opens with their numbers. They measure the disease; we propose the
treatment.

### 3. Magesh, Surani, Dahl, Suzgun, Manning, Ho 2024 — *Hallucination-Free? Assessing the Reliability of Leading AI Legal Research Tools* (Stanford RegLab, arXiv:2405.20362)

**Their move:** Tested commercial legal-RAG tools (Lexis+ AI, Westlaw
AI, Ask Practical Law). Found 17–33 % hallucination rate even with
RAG.

**How we use:** the bridge between Dahl 2024 and us. Shows that RAG
mitigates but doesn't solve. Justifies the audit-rail decomposition:
even with retrieval, commercial systems hallucinate at meaningful
rates because they don't audit per error class.

----------------------------------------------------------------------

## Tier 2 — Swiss legal NLP context (cite, distinguish ourselves)

### 4. Niklaus et al. 2021 — *Swiss-Judgment-Prediction*

Federal-Supreme-Court-only dataset for outcome prediction. Narrower
scope than ours (one court vs. 102), different task.

### 5. Geering & Merane 2024 — *Swiss Federal Supreme Court Dataset*

Also BGer-only, focused on document analysis.

### 6. Rasiah et al. 2023 — *SCALE: Swiss Legal Benchmark Suite*

Multi-task benchmark including citation extraction, court-view
generation, summarisation. Complementary to ours (they benchmark
NLP tasks; we benchmark RAG).

### 7. Niklaus et al. 2023 — *MultiLegalPile*

Pretraining corpus across legal jurisdictions. Different goal
(language-model pretraining vs. retrieval evaluation).

----------------------------------------------------------------------

## Tier 3 — legal-RAG benchmark landscape (cite, position)

### 8. Pipitone & Houir Alami 2024 — *LegalBench-RAG*

US-focused legal-RAG benchmark. Closed-ended QA. Butler & Butler 2026
critique it for using closed-ended questions that don't simulate
real-world conditions where LLMs can hallucinate. Our benchmark is
open-ended (matching Butler & Butler 2026's design).

### 9. Zheng, Guha, Arifov, Zhang, Skreta, Manning, Henderson, Ho 2025 — *HousingQA / BarExamQA*

US-focused, multiple-choice, narrow-domain. Same critique as #8 —
multiple-choice doesn't reveal hallucination because the LLM never
needs to invent an answer.

### 10. Guha et al. 2023 — *LegalBench*

Multi-task benchmark across 162 legal tasks. Important reference for
the field but tasks are simple yes/no classification, not RAG
evaluation.

----------------------------------------------------------------------

## Tier 4 — open legal infrastructure context

### 11. Caselaw Access Project (Harvard, 2018–present)

6.7M US decisions, open. The model we follow philosophically.
Different jurisdiction, no active retrieval system.

### 12. RAG / verification methods literature

- Asai et al. 2023 — *Self-RAG* (general-domain self-verification)
- Press et al. 2023 — *Measuring and Narrowing the Compositionality Gap*
- Gao et al. 2023 — *Retrieval-Augmented Generation Survey*

We cite these for context but our work is more practical/applied.
The closest verification-method work is **CRAG** (Yan et al. 2024),
which corrects RAG output by judging passage quality. We differ by
auditing the *generated text* against the *retrieved corpus*, not
the other way around.

----------------------------------------------------------------------

## Positioning paragraph (for §2 closing)

> While Dahl et al. (2024) and Magesh et al. (2024) characterise the
> hallucination problem in legal LLMs and Butler & Butler (2026)
> introduce a rigorous evaluation methodology, no prior work proposes
> a deployed mechanism that systematically reduces hallucination
> rates by **decomposing the verification problem into independent
> error-class detectors**. Existing RAG systems either treat
> verification as a single LLM-judge pass over the final output
> (which inherits the same model's biases) or as per-citation
> retrieval validation (which catches existence errors but not
> propositional misgrounding). The five-rail closing audit we propose
> in §3 is the first to address each error class with the cheapest
> mechanism that can detect it — deterministic regex+lookup for
> existence, statute resolution and verbatim-quote matching;
> independent-judge LLM only for the irreducibly semantic
> proposition-grounding rail.

This paragraph is the **reviewer-friendly summary** of why this paper
exists. We'll iterate on it but the structure is right.
