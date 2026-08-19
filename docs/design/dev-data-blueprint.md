# Dev-data blueprint — collect what the purposes actually need

Four stated purposes: (a) assess product utility, (b) improve
functionality, (c) research, (d) train models and build skills. This
maps each to the signals that serve it, marks what already flows
(✓ = live since 2026-08-19), and names the gaps in value order.

The design constraint throughout: the /datenschutz/ no-linkage property.
No IP, no user id, no session reference *on disk*, no attribution
between two queries. In-memory, short-TTL correlation with only a
derived, unlinked record emitted is the established pattern (the page
itself blesses it for the HLL unique-user sketch: "hashed in memory
only … only sketch registers and totals reach disk").

## (a) Assess utility — "does anyone get what they came for?"

| signal | status |
|---|---|
| Outcome labels + empty reasons per tool | ✓ |
| Answered-rate provenance (declared vs status) | ✓ |
| **Result-fetch events (the API "click")** | **gap 1** |
| **Search abandonment** (no fetch followed) | gap 1b |
| **Demand-driven miss ledger** | **gap 2** |
| Latency percentiles per tool | ✓ (metrics) |
| Client-class mix | ✓ (aggregates) |

**Gap 1 — the API click.** `search_followups` today is a boolean
counter: "a fetch happened after a search", globally. It does not know
which result, at what rank, from which query shape. The fix: when
`get_decision`/`get_erwaegung`/`get_case_brief` fetches an id, look it
up in an in-memory map of the session's last search results (the
`_session_clients` tools list already exists), and emit ONE standalone
record: `{type: "result_fetch", rank, decision_id, result_count,
query_len, query_language, gap_s}`. No session id, no query text on
disk; the correlation lives and dies in memory. This is the single
highest-value signal in IR (implicit relevance feedback) and it also
completes dataset 1: (candidates, haiku_order, ce_scores, **fetched**).

**Gap 2 — the miss ledger.** `id_not_found` events say a lookup failed;
they do not say *what was asked for*. For non-free-text identifiers
(dockets, SR numbers, court+date shapes — never query text) record the
requested identifier: `{type: "miss", kind: docket|sr|law_name,
identifier, court?}`. This converts failed demand into a ranked
scraper/corpus to-do list — which court's missing decisions users
actually ask for. Directly prices the Vaud/ch_vb/BGer backfills.

## (b) Improve functionality

| signal | status |
|---|---|
| Rerank labels (candidates + Haiku order) | ✓ |
| Cross-encoder raw scores | ✓ |
| Per-signal contribution vectors (top-10) | ✓ |
| Full query parse + parse_outcome | ✓ |
| Added expansion terms | ✓ |
| **Judge metadata on labels** | **gap 3** |
| **Candidates-not-shown** (pool beyond top-N) | gap 4 |
| Strategy-level attribution (which retrieval strategy contributed the fetched result) | gap 5 |
| Sanitizer interventions (what `_sanitize_fts5` rewrote, shape-only) | gap 6 |

**Gap 3 — labels without provenance are unusable later.** The rerank
record must carry `model`, `prompt_version` (hash of
LLM_RERANK_PROMPT), and `weights_version` (hash of SCORING_CONFIG).
Six months of labels spanning three silent prompt tweaks are three
datasets, not one — and nothing today records which is which. Trivial
to add; ruinous to omit.

**Gap 4.** Haiku sees top-N; the pool holds hundreds. Sampling a few
random *unshown* candidate ids per search (ids only) gives the
hard-negative pool every reranker-training recipe needs.

## (c) Research

| signal | status |
|---|---|
| Pipeline run records | ✓ |
| Per-day scraper health (trend) | ✓ |
| Quality history per run | ✓ |
| Benchmarks (incl. weekly ablations — verified on prod) | ✓ |
| Corpus deltas: what dedup/normalisation removed | task #36 |
| **Citation extraction→resolution pairs** | **gap 7** |
| Per-build corpus census (court × year counts) | gap 8 |
| Version history of decisions | ✓ (versioning/) |

**Gap 7.** The reference-graph build resolves 9.65M of 10M extracted
citations and discards the pairs. Emitting `(raw_citation_string,
context_window_hash, resolved_id | failure_reason)` at build time is
the Paper-2 substrate AND a citation-parser training set. Same
pipeline-gated pattern as #36; do them together.

## (d) Train models and build skills

Model targets, each with its data source:

1. **Reranker** — dataset 1 + gap 1 (fetch labels) + gap 4 (hard
   negatives) + gap 3 (provenance). The complete recipe.
2. **Query parser** (replace the Haiku call; it dominates search p50)
   — full parse ✓ + query text (0c-gated) + parse_outcome ✓.
3. **Citation parser/resolver** — gap 7.
4. **Redaction/anonymisation model** — corpus-derived: label the
   court redaction conventions (`A.________`, `aaaaaaa bbbbbbb`
   poursuite placeholders). Extraction job, no collection needed.
5. **Headnote/Regeste generator** — corpus-derived: (Erwägungen →
   Regeste) aligned pairs already in the corpus. Extraction job.
6. **DE/FR/IT legal translation** — corpus-derived: parallel BGE
   regesten + trilingual federal statutes. Extraction job.
7. **Skills / agent playbooks** — aggregate tool-transition matrix
   (which tool follows which, counts only, no sessions on disk):
   documents how legal research is actually done through the API,
   grounding skill design in observed workflows. Cheap: derive from
   the in-memory session map at flush time, aggregated.

Corpus-derived datasets (4–6) need no telemetry and no notice change:
they are transformations of public court text. They belong in the
private repo as *versioned builds* with cards, generated by extraction
scripts, so training runs can pin exact versions.

## What this changes about the /datenschutz/ amendment (parked)

The amendment must cover, in one pass: the R&D dataset incl. model
training (drafted ✓), **result-fetch events** (unlinked, id+rank only),
**the miss ledger** (identifiers, never query text), and **aggregate
tool-transition counts**. One amendment, complete — not a second edit
in a month. The parked draft covers only the first; extend before
publishing.

## Field-research corrections (2026-08-19, see dev-data-research-report.md)

The external sweep confirms the design and sharpens four points:

1. **`result_set_id`** — echo a stable id in every search response and log
   the full impression (ordered ids + ranks + scores + policy version).
   A later fetch then joins deterministically as the "click" with its
   rank. Strictly better than the in-memory session-map join sketched
   under gap 1; supersedes it. (Joachims WSDM'17; the impression log is
   the half of every training pair that cannot be reconstructed later.)
2. **The full-capture period is the textbook move**, not a detour: "log
   raw events, derive labels offline — derived definitions change, raw
   impressions do not." Capture first, prune by measured use, exactly as
   directed.
3. **Grades and formats**: UMBRELA 0–3 scale for any judged relevance;
   TREC qrels + BEIR layout for exports; temporal splits by decision
   year and COLIEE-style citation masking (our citation graph leaks
   labels into decision text otherwise). Compatibility target:
   `rcds/swiss_doc2doc_ir` (closest existing Swiss schema).
4. **Demand queue without user data**: unresolved citation edges ranked
   by citing frequency (the ~0.35M unresolved of 10M extracted) — the
   Wikipedia-GapFinder pattern; complements the miss ledger and needs
   no notice change at all.

Version-stamping (its cross-cutting #2) shipped today: prompt hash,
scoring hash, judge model on every label.

## Order of implementation

1. Gap 3 (label provenance) — trivial, and every day without it taints
   the labels already accumulating.
2. Gap 1 (result-fetch events) — the flagship signal.
3. Gap 2 (miss ledger) — feeds corpus priorities immediately.
4. Gap 4 (hard negatives) + gap 5 (strategy attribution) — same code
   area as 1.
5. #36 + gap 7 together (both pipeline-gated build-time audits).
6. Extraction jobs 4–6 as separate dataset builds.
7. Amend /datenschutz/ once, covering everything; marker; backfill.
