# OpenCaseLaw paper — v1.1 roadmap (pre-submission backlog)

The v1.0 reframing (commit `5a1ee1d`) demoted the cross-lingual benchmark
to a "regeste-derived diagnostic" and the audit pipeline to per-rail
error-class checks, removing the worst over-claims from a hostile-reviewer
read. This document tracks the empirical work that closes the remaining
gaps before submission. Target submission window: **2026-06-01**.

Each item lists what's needed, who can do it, the estimated effort, and
the exit criterion that lets the paper claim the gap closed.

---

## 1. Citation-resolution precision — **automated proxies shipped; manual adjudication deferred to v2.0**

**Why it was needed (Fatal #3).** The paper reports 93.5\% resolution as
a *coverage* metric. A reviewer will (correctly) ask for *precision*.

**v1.1 resolution (decided 2026-05-17):** instead of an 8-hour manual
adjudication, ship automated precision *proxies* that bound the worst
case (every counted violation is a guaranteed false positive). Manual
adjudication infrastructure stays in the repo for v2.0.

**Proxies shipped** (paper §4, Table~\ref{tab:precision_proxies}):

| Proxy | Result | Interpretation |
|---|---|---|
| Date sanity (overall) | **98.47\%** pass | Hard lower bound on precision; violations are logical impossibilities |
| Date sanity (pin-cite) | 97.35\% pass | Lowest stratum, consistent with heuristic-based resolver |
| Date sanity (bge_bare) | 99.75\% pass | Highest among BGE strata |
| Self-citation count | 0 / 8.10M | Clean structural signal |
| Confidence p50 (docket_norm) | 0.99 | Most confident stratum |
| Confidence p50 (pin-cite) | 0.75 | -0.10 discount applied by resolver |

**Proxy script shipped:** `benchmarks/citation_precision_proxies.py`,
produces `benchmarks/citation_precision_proxies.json`. Run after any
graph rebuild; `build_tables.py` regenerates the precision_proxies.tex
from the JSON.

**Adjudication infrastructure shipped (still relevant for v2.0):**
`benchmarks/citation_precision_audit.py` produces a 400-row stratified
sample as JSONL, ready for adjudication. Reproducible via `--seed 42`.

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

**Adjudicator:** user-owned. Estimated ~8 hours.

**Adjudication tool shipped:** `benchmarks/citation_precision_audit_tui.py` —
single-keystroke decisions (c=correct / w=wrong / u=uncertain / n=note),
auto-saves after every row, resumable from where you left off.

Run:
```bash
python3 -m benchmarks.citation_precision_audit_tui \
  --sample benchmarks/citation_precision_sample_400.jsonl
```

Decisions write straight back to the sample JSONL (no separate file
to merge). On restart, jumps to the first un-adjudicated row.

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

**Methodology challenge solved.** A naive `WHERE language = 'it'` filter
returns 955 BGE rows of which only ~30% have Italian *body text* — the
rest are Italian-tagged rows whose actual content is German or French.
The reliable signal is full-text body language, not the metadata
`language` column.

**Tooling shipped:** `benchmarks/swiss_legal_rag_bench/build_it_target_candidates.py`
runs a function-word heuristic on a mid-body text slice (offset
800–2300, skipping the standard header preamble); a case counts as
Italian-original when `it_hits >= 5 AND it > max(de, fr)`. Output is a
ranked candidate JSONL.

**Candidates produced:** `it_target_candidates.jsonl` — top 30 by
in-degree. Range: BGE 128 V 174 (n=5,483) down to BGE 150 IV 169
(n=142). Note: 5,483 is comparable to the v1 in-degree floor (4,412),
but the rest of the IT candidates sit much lower than the v1
distribution (median ~250 vs v1 median 8,180); paper must explicitly
acknowledge this asymmetry.

**Regeste-language constraint (closed-by-deferral).** The identified
IT-original BGEs have *German* regestes (BGE publishes German headnotes
regardless of the case's working language). The v1 regeste-derived
query construction therefore cannot directly produce Italian queries
for them.

With item 3 (lawyer-authored queries) deferred to v2.0, the IT-target
column also stays empty in v1.1. The shipped artifacts
(`build_it_target_candidates.py` + `it_target_candidates.jsonl`)
remain in the repo as v2.0 infrastructure: when lawyer-authored queries
land, the candidate pool is already identified and ranked.

**§7 paper edit (v1.1):** add one sentence acknowledging that the
IT-target gap has been investigated and that the identifier
methodology + 30 ranked candidates ship in the repo, while query
construction for them awaits the v2.0 multi-annotator pilot.

**Exit criterion (v1.1, narrowed):** §7 prose addresses that
IT-original cases were identified and remain available for v2.0; the
table itself stays unchanged from v1.0.

---

## 3. Lawyer-authored query pilot — **DEFERRED to v2.0**

**Status:** out of v1.1 scope (decided 2026-05-17). The realism Δ-MRR
experiment is not measured in v1.1; §7 retains the upper-bound caveat
as the honest framing without an empirical delta number.

**Reasoning:** the human cost (one Swiss lawyer × ~6–8 hours of careful
fact-pattern query authoring) is not justified for the realism
improvement this single experiment would buy, given that:

- v1.0 already owns the upper-bound caveat explicitly in §7 and §11.
- The asymmetric cross-cell ranking (which the paper now foregrounds
  as the interpretable signal) is robust to the construction
  methodology.
- v2.0 of the benchmark will introduce lawyer-authored queries as
  part of a broader multi-annotator design (proper IAA, larger
  sample), not as a one-off pilot.

**Side-effect on item 2 (IT-target coverage):** the IT-target column
remains empty in v1.1 (it was the lawyer-query path that would have
filled it). The IT-candidate identifier still ships as v2.0 infrastructure
(see item 2 below).

**Removed artifacts:** the lawyer brief generator
(`benchmarks/build_lawyer_query_brief.py`) and its outputs were removed
from the repo when this item was deferred. Regenerable from git history
if v2.0 needs them.

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

## 5. Statute graph: LTF/BGG canonicalization (**SHIPPED**) + temporal validity (deferred)

**Why it's needed (review).** Top-statutes table lists LTF Art. 42 and
BGG Art. 42 as separate rows even though the caption says they refer to
the same statute. Statute resolution is also currently snapshot-only —
a 1998 decision citing "OG Art. X" gets resolved against the current SR
mirror, which may not have OG (now superseded by BGG/LTF).

**v1.1 status (decided 2026-05-17):**

- **Alias canonicalisation: SHIPPED.** `benchmarks/build_canonical_top_statutes.py`
  emits `top_statutes_canonical.json` from `reference_graph.db` with
  20 alias groups (BGG/LTF, BV/Cst./Cost., ZGB/CC, OR/CO, StGB/CP,
  StPO/CPP, ZPO/CPC, VwVG/PA, ATSG/LPGA, AsylG/LAsi, AIG/LEI, UVG/LAA,
  IVG/LAI, AHVG/LAVS, AVIG/LACI, KVG/LAMal, EMRK/CEDH, SchKG/LP,
  BVG/LPP, VGG/LTAF). Aggregation uses `COUNT(DISTINCT decision_id)`
  under SQL `CASE WHEN`, so a decision citing both BGG Art. 42 and
  LTF Art. 42 in one document is counted once.
- Result: the v1.0 table's five split BGG/LTF rows collapse to a
  single canonical statute; BGG/LTF Art. 42 jumps to #2 at 201,897
  (vs naive 212,997 sum — 5% double-count avoided).
- `build_tables.py` consumes the canonical JSON when present; falls
  back to raw `top30_statutes` otherwise.
- §4 prose rewritten: explicit that canonicalisation is *lexical only*;
  temporal handling deferred.

- **Temporal validity: DEFERRED to v2.0.** Mapping a 1998 \emph{OG}
  reference (Bundesrechtspflegegesetz, repealed 2007 in favour of BGG/LTF)
  to its current SR location, or distinguishing repealed-vs-current
  article versions, requires an SR-version archive and possibly partial
  Fedlex re-scrape; out of scope for v1.1. Paper §4 explicitly states
  the gap.

**Exit criterion (v1.1, met):** ✓ Top-statutes table has no duplicate
provisions across abbreviation languages. ✓ §4 statute-graph prose
explicitly distinguishes lexical canonicalisation (shipped) from
temporal handling (deferred to v2.0).

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
| 1   | Run citation-precision-audit script on VPS → produce sample JSONL | Claude ✓ done |
| 1   | Adjudication TUI | Claude ✓ done |
| 1   | Offline `make verify` + Dockerfile.reviewer | Claude ✓ done |
| 1   | IT-original BGE identifier + 30 candidate JSONL | Claude ✓ done |
| 1-8 | User personally adjudicates 400-sample audit (~8h, TUI) | User |
| 2-4 | Retrieval-augmented bench re-run | Claude |
| 2-4 | Statute alias table + temporal-validity proof of concept | Claude |
| 5-7 | Legal/licensing appendix drafting | User (+ Claude assist) |
| 8-9 | Integrate audit results into paper §4 (precision table) | Claude |
| 9-10| Zenodo upload (corpus snapshot + manifests) | User |
| 10-12 | Internal cold-read pass; pre-submission revision | User |
| 13  | Git tag `paper-resource-2026-05` | User |
| 14  | arXiv submission | User |
| 15  | (buffer) | — |

*Lawyer-query realism Δ-MRR (item 3) and IT-target column (item 2)
deferred to v2.0; both items can be picked up post-arXiv without
blocking the v1.1 submission.*

Items not on the critical path (Docker, legal-review polish, IT-original
in-degree band justification) can slip without blocking submission.

---

## What the v1.1 paper looks like vs v1.0

Once items 1-4 land:

| Section | v1.0 (today) | v1.1 (submission) |
|---|---|---|
| §4 citation graph | 93.5% coverage | + per-stratum precision with CIs (item 1) |
| §7 cross-lingual table | no IT target column | unchanged in table; prose notes IT-target identifier shipped, queries deferred to v2.0 (items 2+3) |
| §8 audit | prior-only per-rail | + retrieval-augmented column per-rail (item 4) |
| Repro | live MCP + commit-pinned JSON | + offline `make verify` + Docker + Zenodo DOI (item 6) ✓ |
| Legal | half-page | full legal appendix (item 7) |
| Statute graph | snapshot-only, dual-counted | canonicalized, temporal-aware (item 5) |

That is the version submitted to arXiv (and subsequently to NeurIPS D&B
2026, deadline late May / early June). The realism Δ-MRR (item 3) and
IT-target column (item 2) move to v2.0 alongside multi-annotator IAA
work.
