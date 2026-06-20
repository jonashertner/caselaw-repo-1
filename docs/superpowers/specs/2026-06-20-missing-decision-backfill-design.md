# Missing-Decision Backfill Subsystem — design

Status: spec, 2026-06-20. Executes Pillar 5 of the Completeness Assurance Plan
(`2026-06-20-completeness-assurance-plan.md`) against the gap measured in Step 1.

## Goal

Recover the **~74,500 genuinely-missing decisions** — cited by corpus decisions
(~190k citations) but not collected — by fetching them from their official
source, prioritized by citation importance, without ever removing coverage.

## What we know (established Step 1, triple-checked)

- **~74,500 genuine gaps**: oracle classified 79,331 "missing"; exact docket/ID
  re-verification put ~94% truly absent; the precise header-scan refinement
  independently found only ~639 present-under-alternate-ID (mostly prior-instance
  noise). Three methods converge.
- **Distribution** (from `citation_gaps.db`, normalized_key → court/year):
  - By era: pre-2000 ~27k refs (heaviest), 2007–2015 ~17k, 2016+ ~20k — both a
    historical gap AND real recent holes.
  - By court: BGer all chambers ~31k, BVGer ~12k, BGE ~4.5k, BStGer + cantonal
    the remainder. (The earlier "34k BStGer" was a classifier over-match; treat
    that bucket as mixed federal-criminal + cantonal.)
- **Not in the frozen es archive** — verified they appear there only as
  citations, not as collected decisions → recovery is source-side, not re-ingest.
- **The crux: fetching needs the decision DATE.** The cheap BGer fetch is
  `relevancy.bger.ch/...aza://DD-MM-YYYY-DOCKET` (no PoW), but for a missing
  docket we don't have the date → a docket→date resolution step is required.

## Architecture

A six-phase pipeline, prioritized by citation count, resumable, gated at ingest.

```
citation_gaps.db (missing, ranked)
   │  Phase 0  triage → per-court worklists
   ▼
per-court worklist {court, docket, citation_count, year}
   │  Phase 1  recoverability probe (per court, top-N) → go/no-go + rate
   ▼
   │  Phase 2  fetch (per-court adapter: docket → date → decision)
   ▼
backfill candidate {Decision | not_found | error}
   │  Phase 3  verify (docket match, dedup, dispose)
   ▼
output/backfill/{court}.jsonl   +  backfill_state.db (per-docket outcome)
   │  Phase 4  GATED ingest (pipeline) → corpus
   ▼
   │  Phase 5  measure: recovery rate, residual gap → completeness KPIs
```

### Phase 0 — Triage
Read `citation_gaps.db` `classification='missing'`, rank by `citation_count`,
bucket by court from `normalized_key`. Emit per-court worklists with year (from
the docket). Highest-cited first.

### Phase 1 — Recoverability probe (de-risk before scale)
For each court, take the top ~100 worklist items, run the adapter, and measure
`{fetched, not_found, error}`. This answers the unknown that blocks everything:
**what fraction is actually recoverable?** Some "missing" are genuinely
unpublished / withdrawn / re-anonymized (the governance-removal set) and will
never be fetchable — Phase 1 quantifies that ceiling per court before committing
to a multi-day run. No code past Phase 1 runs until its rate is known.

### Phase 2 — Per-court fetch adapters
Each court already has a scraper; the backfill adds a **fetch-one-by-docket**
entry point. Adapter contract: `fetch(docket, year) -> Decision | None`.

- **BGer** (~31k, biggest): the date is the blocker. Resolution options, in order:
  1. AZA full-text search for the docket string → parse the result's date + docid
     → fetch full text via `relevancy.bger.ch` (the scraper's `fetch_decision`
     already does the relevancy fetch given a date).
  2. The bger.ch "Rechtsprechung" search accepts an Aktenzeichen (docket) field.
  3. If the docket became a BGE, the search returns the BGE — capture it (and add
     the BGer↔BGE cross-reference, closing that loop).
  AZA search needs PoW (the scraper's `_get_with_pow` handles it); relevancy does
  not. Pre-2007 dockets use the old AZA archive ("Weitere Urteile ab 2000" + the
  pre-2000 EVG path already exercised by the EVG recovery).
- **BVGer** (~12k): bvger.ch search by docket (letter-num/year); the bvger scraper
  template + the post-Weblaw `getDocumentContent` path.
- **BGE** (~4.5k): leading cases — fetch from the official BGE collection
  (relevancy CLIR / the `bge` scraper) by BGE citation. Many missing BGE are
  historical (low volumes) → the `bge_historical` path.
- **BStGer**: bstger.ch via the migrated `getDocumentContent` endpoint.
- **Cantonal** (Phase 2-late): per-canton portals — high variance, many dead
  portals. Lower priority; some only recoverable via Wayback.

### Phase 3 — Verify
For each fetched candidate: (a) the returned decision's docket normalizes to the
requested key (reject mismatches), (b) full_text present + non-trivial, (c) not
already in the corpus under any identity (reuse the oracle's `corpus_keys_for`),
(d) record the outcome in `backfill_state.db`.

### Phase 4 — Gated ingest
Write verified Decisions to `output/backfill/{court}.jsonl`; the standard
`build_fts5` ingest picks them up on the next build. **Pipeline-critical
(invariant #5): explicit user approval + test against a copy before the live
build.** Completeness-first: this only ADDS rows.

### Phase 5 — Measure
Recompute the gap oracle; report recovery rate per court, residual genuine gap,
and the unrecoverable ceiling. Feed the completeness KPIs (Pillar 6). Re-run is
cheap and shows the gap trending down.

## State + resumability

`backfill_state.db(docket PRIMARY KEY, court, citation_count, outcome, attempted_at,
source_url)` — outcome ∈ {fetched, not_found, error, skipped, ingested}. The
fetch loop skips any docket with a terminal outcome, so a multi-day run resumes
and never re-hammers a 404. `not_found` is itself a finding (the recoverable
ceiling); log it, don't retry endlessly.

## Workflow vs scripts (hybrid)

- **Workflow** (parallel agents) for: per-court adapter development + the Phase 1
  recoverability probes (each court is independent) + Phase 3 verification triage.
- **Rate-limited scripts** for the Phase 2 bulk fetch — 74k polite source fetches
  is not an agent job, and per-source politeness requires serialization anyway.

## Hard problems / honest risks

1. **Date resolution for BGer** — the technical crux. Phase 1 must prove the
   AZA-docket-search→date→fetch chain end-to-end before scaling.
2. **Unrecoverable fraction** — unknown until Phase 1. Some are unpublished /
   withdrawn / re-anonymized; recovery rate will be <100%. The subsystem must
   treat `not_found` as a first-class, recorded outcome.
3. **Became-BGE** — a missing BGer docket may resolve to a BGE; capture it and
   write the cross-reference rather than discarding.
4. **Politeness + scale** — ~74k fetches across federal portals; conservative
   `REQUEST_DELAY`, identified UA, multi-day, prioritized top-cited first. PoW /
   tunnel infra lives on the VPS.
5. **Cantonal long tail** — high variance, dead portals; defer to a later phase,
   recover what's live + Wayback the rest.

## Phased rollout + effort

1. **BGer Phase 1 probe** (~1 day): build `fetch(docket, year)` for BGer
   (AZA-search→date→relevancy), probe the top ~200 most-cited missing BGer,
   report the recovery rate. This single step de-risks the whole subsystem.
2. If viable → **BGer Phase 2** full fetch (~31k, multi-day background) + verify +
   gated ingest. Measure the gap drop.
3. **BVGer, BGE, BStGer adapters** (each ~0.5–1 day) → fetch → ingest.
4. **Cantonal** as a later phase (per-canton + Wayback).

## Open questions to settle in Phase 1
- Does AZA full-text search reliably return a single decision for a bare docket,
  with its date? (the date-resolution chain.)
- What is the BGer recovery rate (fetched / not_found / error) on the top 200?
- How many top-missing are actually became-BGE (already present, cross-ref gap)
  vs genuinely uncollected?

The answers turn this spec from a plan into a sized program.

## Phase 1 results — BGer probe (2026-06-20)

Ran the BGer recoverability probe on the top‑cited missing dockets. Decisive,
and it **revises the strategy**:

- **The AZA‑search date‑resolution mechanism WORKS.** Controls (known‑present
  dockets 5A_26/2018, 5A_7/2018, 5A_51/2018) were **3/3 found** by AZA full‑text
  search `query_words={docket}` + a year..year+1 window (the date filter is the
  *decision* date, often the filing year + 1 — bound accordingly). So ordinary
  missing decisions ARE resolvable docket→date→fetch.
- **But the top‑cited missing are NOT in the AZA archive: 0/10.** 4A_101/2014
  (826×), 5A_843/2018 (458×), 6B_494/2022 (253×) … return near‑misses or nothing
  despite the mechanism working. Their citation signature (200–800×) is the
  fingerprint of **leading cases (BGE)** — a different collection — and the
  precise header scan confirmed they are not present in our corpus as a BGE
  either. So the highest‑value gaps are the **hardest**, not the easiest.
- **~half the top sample is pre‑2007** (4C_310/1996, 2A_255/1994, 1P_477/1993…),
  which bger.ch's online AZA archive does not cover at all.

**Strategy correction (important):**
1. **Do NOT prioritize top‑cited‑first** — those are BGE leading cases / pre‑2007,
   the hardest to recover. The naive "most‑cited first" ordering would yield ~0%.
2. **Target the mid‑tier** — ordinary, post‑2007, non‑leading‑case missing
   decisions, where the AZA mechanism works. The next probe must measure the
   recoverable rate on a *mid‑tier* sample (e.g. missing cited 5–50× from
   2010–2023) to size the genuinely‑recoverable fraction of the ~74,500.
3. **BGE‑bound gaps** need the BGE/CLIR source (CLIR query format still TBD) and
   a corpus cross‑check — some may already be present and merely unresolvable by
   BGer docket.
4. **Pre‑2007 is a separate hard track** (print BGE volumes / offline archives /
   Wayback) — likely the floor on recoverability.

Net: the subsystem is viable for the mid‑tier but the top‑cited and pre‑2007
bulk are hard. The recoverable fraction of ~74,500 is **not yet known** and the
mid‑tier probe is the next step that sizes it.

