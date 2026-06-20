# Completeness Assurance Plan — collecting all published Swiss decisions

Status: plan adopted 2026-06-20. Pillar 1a (unresolved-citation oracle) is the
first instrument to build.

## Framing: completeness is a measured property, not a binary

There is no master list of all published Swiss court + authority decisions. They
are spread across ~100+ courts and authorities, each with its own portal; no
body enumerates the whole. So "complete" cannot be *proven* true. The realistic,
honest goal is to make completeness an **instrumented, continuously-measured,
continuously-improving property**:

- explicit per-source coverage ratios,
- a *shrinking* set of **known** gaps,
- automated detection of **new** gaps (so coverage can't silently regress),
- headline KPIs that trend toward complete.

This plan turns "be complete" into something we can measure, alert on, and close.

## Baseline (measured 2026-06-20)

- **991,298 decisions**; ~42% still sourced from the frozen entscheidsuche
  archive (direct-scraper migration ongoing — never drop coverage; retire an
  es_* feed only after direct parity).
- Citation graph: **9,218,628 citations extracted, 8,636,746 resolved (93.7%)**.
  The **~582,000 unresolved citations** — references to decisions that are cited
  but absent — are currently **discarded at graph-build time**. This is the best
  "what are we missing, weighted by importance" signal we have, and we throw it
  away. Recovering it is Pillar 1a.
- `state/coverage.db` already has infrastructure to build on: `coverage_targets`
  (59 sources), `source_snapshots` (2,327), `source_discoveries` (226,964),
  `source_fetch_attempts` (227,100), `gap_queue` (908).

## Six pillars

### 1. Know what you're missing (gap oracles) — highest leverage
- **(1a) Unresolved-citation oracle [BUILD FIRST].** Stop discarding unresolved
  refs; persist + rank by citation count → prioritized missing-decisions list.
  Classify each unresolved ref to separate signal from noise (below).
- **(1b) Docket-sequence gaps.** Courts number sequentially; holes per
  court/year in `docket_number` = missing decisions.
- **(1c) Date-coverage gaps.** Per-source month histograms; zero/anomalously-low
  months vs neighbours = paused source or broken scraper (auto-catches the
  ti/ow/tg/BE cases).
- **(1d) External cross-index.** Diff entscheidsuche vs corpus per court — the
  best independent yardstick even as we migrate off it operationally.

### 2. Know the universe (source registry)
Formalize `coverage_targets` into a complete registry of every Swiss publishing
body — federal courts, federal regulators/authorities, 26 cantons × each court
(Kantons-/Ober-/Verwaltungs-/Sozialversicherungs-/Steuerrekurs-/Handelsgericht +
specialized), administrative practice, ECtHR-CH — and audit it for **source-level
gaps**: publishing bodies with public portals we have never scraped.

### 3. Measure per-source coverage
Capture a denominator per source (its exposed hit-count / sitemap / archive
index / annual Geschäftsbericht caseload) → `coverage_ratio = ours / source_total`.
Flag any source < 95%.

### 4. Continuous completeness audit
A scheduled job recomputing coverage ratios + all gap oracles and flagging
**regressions** — extending `scraper_health` ("is it running") with "is it
*complete*." Writes to a completeness panel + alerts.

### 5. Close gaps (prioritized backfill)
es→direct migration; unresolved-citation-driven recovery (top missing first);
known blocked gaps (BL ~5k outreach, dead portals via Wayback, ju/ne tunnel);
onboard any new source from Pillar 2.

### 6. Prove it (KPIs + dashboard)
Headline KPIs — citation-resolution rate (decision-completeness proxy), median
per-source coverage, # known gaps (trend), # sources < 95% — wired into
`make verify` and a public completeness panel.

## Prioritized sequence
1. Unresolved-citation oracle (1a) — small, uses existing data.
2. Continuous completeness audit (1b + 1c over coverage.db) — medium.
3. Source-registry audit (2) — medium.
4. es→direct migration + citation-driven backfill (5) — ongoing.

---

## Pillar 1a design: the unresolved-citation oracle

**Goal.** Produce a ranked, *classified* list of references that are cited by
corpus decisions but do not resolve to a corpus decision — the prioritized
"missing decisions" list.

**Inputs (read-only on production DBs).**
- `reference_graph.db`: `decision_citations(source_decision_id, target_ref,
  target_type, mention_count, is_prior_instance)` (9.22M raw) and
  `citation_targets(... target_ref, target_decision_id ...)` (8.64M resolved).
- `decisions.db`: the docket index, to re-check apparent gaps against the corpus.

**Method.**
1. Build the set of *resolved* `target_ref`s (distinct refs that resolved at
   least once in `citation_targets`).
2. Stream `decision_citations`; for each `target_ref` NOT in the resolved set,
   aggregate `citation_count` (rows / Σ mention_count) and `distinct_sources`.
3. **Classify** each unresolved ref to separate signal from noise:
   - `resolution_bug` — a normalized form of the ref DOES match a corpus
     decision (docket_norm). The decision exists; the resolver missed it → fix
     the resolver, not a true gap.
   - `noise` — the ref is malformed / not a parseable decision reference
     (fails a format check), or `target_type` is not a decision.
   - `missing` — a well-formed decision ref with no corpus match → a genuine
     gap. Rank these by `citation_count`.
4. Write `citation_gaps(target_ref, target_type, citation_count,
   distinct_sources, classification, normalized_ref)` to `output/citation_gaps.db`
   and emit a ranked report (top-N `missing`).

**Why classification matters.** A raw unresolved list is noisy: high-count
unresolved refs are often a *resolver bug* on a common citation form (one fix
recovers thousands of edges) rather than a missing decision. Splitting the three
classes makes the output actionable: `resolution_bug` → graph fix;
`missing` → backfill target; `noise` → ignore / improve extraction.

**Risk: low.** A new read-only audit script + a new sidecar DB; no change to the
pipeline-critical `build_reference_graph.py` (a later iteration may fold the
capture into the builder once the classifier is trusted).

**Tests (TDD).** Pure classifier + aggregator over synthetic in-memory tables:
- a ref whose normalized form matches the corpus → `resolution_bug`;
- a clean, unmatched BGer/BGE-style ref → `missing`, ranked by count;
- a garbage ref / statute-type → `noise`;
- aggregation sums citation counts and distinct sources correctly.

**Output use.** The ranked `missing` list drives Pillar 5 backfill (fetch the
most-cited missing decisions first) and feeds the Pillar 6 KPIs (resolution rate,
# genuine gaps trend).
