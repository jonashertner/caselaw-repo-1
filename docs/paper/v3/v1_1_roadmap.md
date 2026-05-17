# OpenCaseLaw paper — v1.1 roadmap (pre-submission backlog)

The v1.0 reframing (commit `5a1ee1d`) demoted the cross-lingual benchmark
to a "regeste-derived diagnostic" and the audit pipeline to per-rail
error-class checks, removing the worst over-claims from a hostile-reviewer
read. This document tracks the empirical work that closes the remaining
gaps before submission. Target submission window: **2026-06-01**.

Each item lists what's needed, who can do it, the estimated effort, and
the exit criterion that lets the paper claim the gap closed.

---

## 1. Citation-resolution precision audit

**Why it's needed (Fatal #3).** The paper reports 93.5% resolution as a
*coverage* metric. A reviewer will (correctly) ask for *precision*. The
pin-cite fallback (30-page heuristic) is the riskiest stratum.

**Tooling shipped:** `benchmarks/citation_precision_audit.py` produces a
400-row stratified sample as JSONL, ready for adjudication. Reproducible
via `--seed 42`.

**Run command** (on VPS):
```bash
python3 -m benchmarks.citation_precision_audit \
  --graph     /opt/caselaw/repo/output/reference_graph.db \
  --decisions /opt/caselaw/repo/output/decisions.db \
  --out       benchmarks/citation_precision_sample_400.jsonl \
  --seed      42
```

**Strata** (over-samples the riskiest pin-cite class):

| match_type    | population share | sample | rationale |
|---------------|-----------------:|-------:|-----------|
| `docket_norm` | 40.6%            | 120    | mature path; under-sampled |
| `bge_bare`    | 36.3%            | 100    | proportional |
| `bge_norm`    | 15.0%            |  80    | proportional |
| `bge_pincite` |  8.1%            | 100    | over-sampled — 30-page heuristic |
| **total**     |                  | **400**|

### Adjudication protocol

Each row carries the 80-char source context before/after the citation
plus the first ~250 chars of the target's regeste. The adjudicator fills
two fields:

- **`adjudication`** ∈ `{correct, wrong, uncertain}`.
- **`notes`**: free text. Required when `adjudication != "correct"`.

**Decision rules:**

1. **`correct`** — target's regeste plausibly matches the topic the source
   citation is discussing AND the docket/page in `target_ref` resolves to
   the target case under standard Swiss citation conventions (BGE / ATF /
   DTF prefix forms, BGer chamber+number+year, pin-cite into body).
2. **`wrong`** — target is plausibly the *wrong* case (e.g. a pin-cite
   that landed on an unrelated decision in the same volume, or a bare-BGE
   match that collapsed two distinct cases sharing a docket prefix).
3. **`uncertain`** — adjudicator cannot determine without going outside
   the row (e.g. needs to read the full source decision). Used sparingly.

**Reporting after adjudication:**

- Overall precision (correct / (correct + wrong)) with bootstrap 95% CI.
- Per-stratum precision (especially pin-cite — the main risk).
- Per-stratum confusion vignettes (1–2 per stratum) in the paper appendix.

**Adjudicator profile:** lawyer or law student with Swiss federal procedure
familiarity. Budget: ~8 hours at ~CHF 80-100/hour ≈ **CHF 600-800**.

**Exit criterion:** 400/400 adjudicated; overall precision and per-stratum
precision (with CIs) reported in paper §4. If pincite precision < 85%,
either remove the pincite fallback from the deployed graph or downgrade
its confidence weight; revise §4 prose accordingly.

---

## 2. Italian-original target cases for the cross-lingual bench

**Why it's needed (Fatal #2).** v1 has 38 DE-original + 12 FR-original
cases, zero IT-original — so Table 3's IT-target column is empty. The
paper now (post-reframe) calls this a sampling limitation, but
"limitation acknowledged" is weaker than "limitation closed".

**Methodology challenge.** A naive `WHERE language = 'it'` filter on the
decisions table returns Italian *translation* rows, not Italian-*original*
cases. Need either:

- a `regeste` language-detection pass (Italian regestes start with
  "Regesto"; German with "Regeste"; French with "Régeste" / "Regeste"),
  scored on confidence;
- or the explicit original-language metadata that the bench-construction
  code presumably already had (re-derive from the source code).

**Target:** 15 IT-original BGEs at the highest in-degree available
(probably 200–2,000 range, well below the 4,412 floor of the v1 set).
Acknowledge the in-degree band difference explicitly in §7.

**Method:**

1. Audit the v1 bench-construction code path for the original-language
   determination. Document it.
2. Apply that determination to all BGE rows; rank by in-degree; take top
   15 IT-original.
3. Author one query per language (DE/FR/IT) per case → 45 new trials.
4. Re-run the bench harness; produce a v1.1 cross-lingual table with a
   real IT-target column.

**Effort:** ~1 day code + ~1 day query authoring (the construction
methodology mirrors v1, so the labor is per-case).

**Exit criterion:** 45 new trials in `cross_lingual_v1_1.jsonl`; §7 table
gains a non-empty IT column; the prose addresses how the lower in-degree
band affects comparability with the DE/FR cells.

---

## 3. Lawyer-authored query pilot (the realism check)

**Why it's needed (Fatal #1).** The v1 queries are regeste-derived; the
critique says this is "lexical self-retrieval", not realistic legal
research. The paper post-reframe owns the upper-bound caveat, but the
gap remains: *we don't know* how MRR drops on real research questions.

**Target:** 30 fact-pattern-style queries authored by a practicing Swiss
lawyer without sight of the target case's regeste. Stratify by legal
area to match the v1 stratification (so per-area comparisons are
meaningful).

**Protocol:**

1. Lawyer is told the legal area (e.g. accident insurance, federal
   court procedure) and the fact pattern (1-2 sentences of hypothetical
   facts). Lawyer formulates the search query they would actually run.
2. We then evaluate retrieval against the same 50 targets used in v1.
3. Report Δ-MRR (lawyer-authored vs regeste-derived) per cell. The
   delta is the realism cost of the v1 methodology.

**Budget:** ~6-8 hours of lawyer time at typical legal-research billing
≈ **CHF 600-1,200**. Or: collaborator donation (e.g. a partner at a Swiss
firm willing to donate the time in exchange for paper acknowledgement).

**Exit criterion:** 30 lawyer-authored queries; §7 reports the realism
gap and the resulting MRR floor. Paper claim moves from "upper bound" to
"upper bound, with measured realism gap of Δ".

---

## 4. Retrieval-augmented hallucination condition

**Why it's needed (Fatal #4).** The audit-pipeline §8 reports only the
prior-only condition. The "fabrication rate" framing was demoted in
v1.0 to per-rail error-class metrics, but the retrieval-augmented
condition is the one that matters operationally (since deployed users
will use retrieval, not bare prior).

**Method:**

1. Re-run the 30-question bench with retrieval-augmented prompts (the
   generator gets to query the MCP search before drafting).
2. Re-measure R1–R5 per the same per-class decomposition.
3. Compare prior-only vs RAG-aug on each rail.

**Expected directional result:** R1 unresolvable-citation rate ↓
substantially (because the model retrieves real citations); R5 judge
false-positive rate may move either direction.

**Effort:** ½ day (re-use existing bench harness; just change the
prompt stage).

**Exit criterion:** §8 table has two columns (prior-only, RAG-aug) per
rail; the difference is reported and discussed.

---

## 5. Statute graph: LTF/BGG canonicalization + temporal validity

**Why it's needed (review).** Top-statutes table lists LTF Art. 42 and
BGG Art. 42 as separate rows even though the caption says they refer to
the same statute. Statute resolution is also currently snapshot-only —
a 1998 decision citing "OG Art. X" gets resolved against the current SR
mirror, which may not have OG (now superseded by BGG/LTF).

**Method:**

1. Build an alias table: `{LTF→BGG, CO→OR, CC→ZGB, ...}`. Resolve all
   statute references through aliases before counting.
2. For temporal validity: for each statute reference, look up the SR
   version active at the source decision's `decision_date`. If the
   provision was renumbered or repealed between decision and snapshot,
   record the temporal-link metadata.
3. Update top-statutes table to use canonical (post-alias) provisions.

**Effort:** ~1 day code (alias table) + ~2-3 days for proper temporal
handling (requires SR version archive — may need partial Fedlex re-scrape).

**Exit criterion:** Top-statutes table has no duplicate provisions across
abbreviation languages. §4 statute-graph prose explicitly states whether
resolution is snapshot-only or temporal.

---

## 6. Frozen reproducibility artifacts

**Why it's needed (review).** `make verify` currently hits the live MCP
endpoint. Live infrastructure drifts; reviewers need a frozen state.

**Required:**

- Git tag `paper-resource-2026-05` at the commit pinned by the paper.
- Zenodo upload: corpus snapshot tarball (decision counts + manifest +
  SHA-256), citation-graph snapshot, statute-graph snapshot, benchmark
  files. Zenodo issues a DOI.
- Dockerfile that pins the Python+SQLite environment.
- Offline-mode `make verify` that reads only from committed JSONs +
  the Zenodo tarball, with the live-MCP probe as a separate
  `make verify-live` target.

**Effort:** 1-2 days. Zenodo upload requires the user's account
(out-of-band).

**Exit criterion:** Reviewer can `git checkout paper-resource-2026-05
&& make verify-offline` and see all headline claims reproduce without
network access.

---

## 7. Legal/licensing appendix

**Why it's needed (Fatal #6).** Current §3 licensing paragraph is too
thin. Court decisions in Switzerland are excluded from copyright
(URG/CopA Art. 5), but the corpus combines official acts, scraped portal
material, upstream mirrors (entscheidsuche.ch under CC-BY), annotations,
commentaries (CC-BY / CC-BY-SA), and derived metadata. Need record-level
license provenance + a real FADP (Swiss Federal Act on Data Protection)
analysis.

**Required sections:**

1. Record-level license categories per source.
2. CC0 scope limited to our own metadata.
3. CC-BY / CC-BY-SA propagation rules (commentaries, entscheidsuche).
4. Official-act exclusion under URG Art. 5.
5. Personal-data treatment under FADP:
   - inherited source pseudonymisation;
   - takedown procedure (contact, SLA, public log);
   - logging/query privacy on MCP endpoint;
   - Word add-in PII redaction threat model.
6. Liability / contact information.

**Effort:** ~1 day drafting. Optional but recommended: 2-4 hours from
a Swiss data-protection lawyer for review.

**Exit criterion:** Appendix E exists; cited from §3 and §11 (limitations).

---

## Order of operations

If submission target is **2026-06-01** (15 days from 2026-05-17):

| Day | Item | Owner |
|----:|------|-------|
| 1   | Run citation-precision-audit script on VPS → produce sample JSONL | Claude |
| 1-2 | Recruit lawyer / adjudicator (1 person can do both items 1 and 3) | User |
| 1   | Frozen-artifact code: Dockerfile + offline `make verify` | Claude |
| 2-4 | Retrieval-augmented bench re-run | Claude |
| 2-4 | Italian-original BGE identification + 45-trial v1.1 extension | Claude |
| 2-4 | Statute alias table + temporal-validity proof of concept | Claude |
| 3-8 | Adjudicator works on 400-sample audit (~8h) + 30 lawyer queries (~6h) | Adjudicator |
| 5-7 | Legal/licensing appendix drafting | User (+ Claude assist) |
| 8-9 | Integrate audit results + lawyer-query Δ-MRR into paper sections | Claude |
| 9-10| Zenodo upload (corpus snapshot + manifests) | User |
| 10-12 | Internal cold-read pass; pre-submission revision | User |
| 13  | Git tag `paper-resource-2026-05` | User |
| 14  | arXiv submission | User |
| 15  | (buffer) | — |

Items not on the critical path (Docker, legal-review polish, IT-original
in-degree band justification) can slip without blocking submission.

---

## What the v1.1 paper looks like vs v1.0

Once items 1-4 land:

| Section | v1.0 (today) | v1.1 (post-backlog) |
|---|---|---|
| §4 citation graph | 93.5% coverage | + per-stratum precision with CIs (item 1) |
| §7 cross-lingual table | no IT target column | + IT column with 15 IT-original cases (item 2) |
| §7 prose | "upper bound" caveat only | + measured realism gap from lawyer queries (item 3) |
| §8 audit | prior-only per-rail | + retrieval-augmented column per-rail (item 4) |
| Repro | live MCP + commit-pinned JSON | + offline `make verify` + Zenodo DOI (item 6) |
| Legal | half-page | full legal appendix (item 7) |
| Statute graph | snapshot-only, dual-counted | canonicalized, temporal-aware (item 5) |

That is the version submitted to arXiv (and subsequently to NeurIPS D&B
2026, deadline late May / early June).
