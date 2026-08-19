# R1 Novelty audit — working memo (started 2026-08-11)

Question: has anyone measured citation accuracy IN judicial opinions
empirically? The paper's "first" claim survives only in the precise form
this memo establishes.

## Found so far (first sweep)

1. **READ 2026-08-11 — does NOT preempt.** Liebler & Liebert, "Something
   Rotten in the State of Legal Citation: The Life Span of a United States
   Supreme Court Citation Containing an Internet Link (1996–2010)",
   15 Yale J.L. & Tech. 273 (2013). Measures LINK ROT: 29% of internet
   links cited in SCOTUS opinions no longer work. Unit = availability of
   cited WEB sources over time; not errors in citations to legal authority
   at publication. Adjacent prior art (citation permanence in judicial
   opinions) — cite in related work as the nearest empirical study of
   citation infrastructure in opinions, explicitly distinguished: rot is
   post-publication decay of the referent; our object is
   publication-time nonexistence of the cited authority. Its footnotes
   (Coombs 1990; Peoples 2010, blogs in judicial opinions) are leads for
   the remaining sweep.
2. **Princeton CITP (2026-05): "Can AI reduce burdens on courts by
   automatically verifying citations?"** —
   https://blog.citp.princeton.edu/2026/05/27/ — verification-burden
   framing; cite in intro; check whether it reports any human-error rate.
3. APA amicus-brief citation-accuracy study (briefs, not opinions —
   supports the "briefs are studied, opinions are not" line).
4. Longitudinal ChatGPT-model hallucination study (late-2023→2025, rates
   not declining) — strengthens the "the machine side is measured
   repeatedly, the human side never" contrast. Locate the exact paper.
5. Already in bib: Dahl 2024 (JLA), Magesh 2025 (JELS), Charlotin
   registry, arXiv 2606.21155 / 2607.22693 / 2606.00898, Mogull 2017
   (medical quotation errors).

## Primary-source extractions (2026-08-12/13, from the PDFs)

- **Dahl et al. 2024, J. Legal Analysis 16:64** (read in full). Unit =
  RESPONSE to a QA query about a real federal case. 14 tasks in 3
  complexity tiers; reference-based tasks use known metadata,
  reference-free use self-contradiction. Stratified samples, n=5,000 per
  court level (SCOTUS/USCOA/USDC); overruling task n=279. Pooled
  reference-based hallucination rates by model (their Fig. 6): GPT-4
  0.58, GPT-3.5 0.69, PaLM 2 0.72, Llama 2 0.88. "LLMs hallucinate at
  least 58% of the time." Temperature 0. No human baseline measured.
- **Magesh et al. 2025, JELS 22 (arXiv 2405.20362)** (read in full).
  Unit = response. 202 preregistered queries; tools Lexis+ AI, Westlaw
  AI-AR, Ask Practical Law AI, GPT-4. Hallucinated = response contains a
  false statement OR falsely asserts a source supports a statement
  (misgrounded). Rates: Lexis 17%, Westlaw 33%, Practical Law 17%,
  GPT-4 43%; incomplete answers reported separately. Groundedness "may
  exist on a spectrum"; overruled-case citations coded misgrounded.
- **LegalCiteBench (arXiv 2606.21155)**: hallucination = citation does
  not correspond to a real case, or name-reporter mismatch per Westlaw.
  On pre-LLM briefs used as source material: "over 10 human-made
  citation typos present in the original pre-LLM briefs" — anecdotal,
  no rate, no denominator. Their taxonomy: non-existent citation / case
  name mismatch / incorrect pincite / verbatim misquote / content
  misrepresentation. THE sharpest novelty support: the human side is
  noticed and explicitly unquantified.

## Still to sweep (next session)

- "Bluebook error" empirical studies (law-review literature; mostly about
  BRIEFS and law reviews — confirm none covers opinions).
- Shepard's/KeyCite accuracy studies (citator error rates ≠ court error
  rates, but adjacent).
- German/Austrian: Amtliche Sammlung Zitierfehler; jurisprudence
  informatique française; any European empirical legal studies.
- Posner's & empirical-judicial-behaviour literature on opinion-drafting
  errors.
- Fetch Dahl 2024 + Magesh 2025 PDFs; extract exact task definitions,
  units, denominators → feeds R2 comparability table.

## Working conclusion (provisional — do not write into the paper yet)

No exhaustive, decidability-based measurement of nonexistent citations in
judicial decisions found so far. Closest work is rot/decay (YJoLT) and
brief-side accuracy. The defensible claim is likely: "the first
exhaustive, provable measurement of nonexistent-authority citations in a
national jurisdiction's published decisions" — pending the YJoLT read.
