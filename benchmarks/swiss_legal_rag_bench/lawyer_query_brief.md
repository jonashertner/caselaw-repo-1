# Lawyer-authored query brief (v1.1 realism experiment)

## Why this exists

The v1 cross-lingual retrieval benchmark uses queries extracted directly
from each target case's own multilingual regeste. Because the query and
the target share construction vocabulary, the resulting MRR@10 = 0.630
is an **upper bound** on realistic legal-research performance.

This experiment measures how much that score drops when a lawyer
authors the query **without sight of the target's regeste**. The
resulting Δ-MRR is the realism cost of the v1 methodology.

## How to use this brief

For each case below, write **one search query** (5–15 words, your
preferred language) that you would actually type into a legal-research
system to try to find this case as the top result. Write the query as
if a client had brought you a fact pattern in this legal area and you
were starting research from scratch.

**Do not look up the case.** The whole point is to capture what a
practitioner would *actually* search for, given only the legal area
and primary statute.

Each case shows:

- **Docket** — the official BGE / decision number (this is the *target*
  you are trying to retrieve, not given to you in a real research
  scenario; we show it only so the experiment is reproducible).
- **Legal area** — high-level domain (e.g. "accident_insurance").
- **Primary law** — the dominant statute the case turns on.
- **Your query** — your authored query, in your preferred language.

Once you have authored all queries, save this file and let the
maintainer transcribe them into `lawyer_queries.jsonl` for
evaluation against the retrieval system.

---

## Case 01 of 30: `BGE 142 I 135`

- **Legal area:** foreigners_law
- **Primary law:** AuG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 02 of 30: `BGE 129 V 1`

- **Legal area:** old_age_insurance
- **Primary law:** AHVG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 03 of 30: `BGE 137 III 617`

- **Legal area:** civil_procedure
- **Primary law:** ZPO
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 04 of 30: `BGE 132 V 393`

- **Legal area:** old_court_law
- **Primary law:** OG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 05 of 30: `BGE 123 V 150`

- **Legal area:** unemployment_insurance
- **Primary law:** AVIG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 06 of 30: `BGE 125 V 351`

- **Legal area:** accident_insurance
- **Primary law:** UVG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 07 of 30: `BGE 136 IV 55`

- **Legal area:** criminal_code
- **Primary law:** StGB
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 08 of 30: `BGE 128 III 411`

- **Legal area:** civil_code
- **Primary law:** ZGB
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 09 of 30: `BGE 130 V 343`

- **Legal area:** general_insurance_law
- **Primary law:** ATSG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 10 of 30: `BGE 134 II 244`

- **Legal area:** federal_court_procedure
- **Primary law:** BGG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 11 of 30: `BGE 141 V 281`

- **Legal area:** general_insurance_law
- **Primary law:** ATSG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 12 of 30: `BGE 144 IV 345`

- **Legal area:** criminal_procedure
- **Primary law:** StPO
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 13 of 30: `BGE 143 IV 241`

- **Legal area:** criminal_procedure
- **Primary law:** StPO
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 14 of 30: `BGE 138 I 232`

- **Legal area:** contract_tort
- **Primary law:** OR
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 15 of 30: `BGE 134 I 140`

- **Legal area:** echr
- **Primary law:** EMRK
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 16 of 30: `BGE 126 V 319`

- **Legal area:** health_insurance
- **Primary law:** KVG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 17 of 30: `BGE 130 III 321`

- **Legal area:** civil_code
- **Primary law:** ZGB
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 18 of 30: `BGE 115 V 133`

- **Legal area:** accident_insurance
- **Primary law:** UVG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 19 of 30: `BGE 133 II 249`

- **Legal area:** federal_court_procedure
- **Primary law:** BGG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 20 of 30: `BGE 140 III 115`

- **Legal area:** international_private_law
- **Primary law:** IPRG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 21 of 30: `BGE 134 IV 1`

- **Legal area:** criminal_code
- **Primary law:** StGB
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 22 of 30: `BGE 125 V 256`

- **Legal area:** disability_insurance
- **Primary law:** IVG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 23 of 30: `BGE 110 V 48`

- **Legal area:** misc
- **Primary law:** misc
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 24 of 30: `BGE 125 V 193`

- **Legal area:** unemployment_insurance
- **Primary law:** AVIG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 25 of 30: `BGE 129 I 8`

- **Legal area:** constitution
- **Primary law:** BV
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 26 of 30: `BGE 122 V 157`

- **Legal area:** constitution
- **Primary law:** BV
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 27 of 30: `BGE 125 V 413`

- **Legal area:** disability_insurance
- **Primary law:** IVG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 28 of 30: `BGE 144 V 210`

- **Legal area:** misc
- **Primary law:** misc
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 29 of 30: `BGE 138 III 374`

- **Legal area:** civil_procedure
- **Primary law:** ZPO
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---

## Case 30 of 30: `BGE 140 III 86`

- **Legal area:** federal_court_procedure
- **Primary law:** BGG
- **Your query language:** _(de / fr / it — your choice)_
- **Your query:**

```
(write your search query here)
```

- **Notes (optional):**

```
(why you chose these terms, alternatives you considered)
```

---
