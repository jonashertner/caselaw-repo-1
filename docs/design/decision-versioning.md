# Decision versioning and change detection — design

**Status**: proposed, awaiting approval. Nothing implemented.
**Author decisions (2026-08-18)**: monthly re-fetch cadence; full text for
the current version plus diffs for superseded ones; a detected change
raises an internal alert so the court can be contacted for context —
version history is not public at launch.

## Why

Today a decision is fetched once and never re-fetched
(`base_scraper.py:113`, `is_known()` skips permanently). If a court
replaces or corrects a published decision we keep serving the superseded
text and never notice. Entscheidsuche.ch re-scrapes into Elasticsearch
and overwrites, so their copy self-heals but carries no history.

Versioning gives us the property neither approach has: **what the
official record said, on which date, provable**. The infrastructure is
already 80 % built — `content_hash` (SHA-256 of `regeste‖full_text`) is
per-decision and dirty-tracked, and the daily RFC 6962 Merkle root
anchored via OpenTimestamps already timestamps it. Each version
therefore arrives with third-party-verifiable dating for free.

It also closes the Paper-2 loop: we report a wrong citation, the court
corrects it, and we hold both versions with anchored timestamps —
turning a private correspondence into a verifiable public record of the
judiciary correcting itself.

## A. Detection — monthly re-fetch

`ScraperState` gains a verification timestamp per decision and a
`needs_refresh(decision_id, max_age_days=30)` predicate. `is_known()`
keeps its meaning for discovery; the refresh pass is a separate
traversal so a stalled refresh can never suppress new-decision capture.

**Load, stated honestly.** A literal monthly cycle over the whole corpus
is 1,053,305 / 30 ≈ **35,100 re-fetches per night**. Today's entire
scrape run makes on the order of a few thousand requests and takes
1.6 h. 35k/night against 26 cantonal portals is very likely to trip
rate limits or look like an attack, and several portals are already
fragile (NE/JU need the SOCKS tunnel; BL has an outstanding gap).

Three ways to honour "monthly", in order of my preference:

1. **Tiered (recommended).** Decisions from the last 24 months monthly
   (~119k in scope → ~4,000/night); everything older annually
   (~930k → ~2,600/night). Total ≈ 6,600/night, roughly 2× today's
   traffic. Corrections cluster near publication, so this catches
   almost everything with a fifth of the load.
2. **Literal monthly, rate-limited.** All 1.05 M on a 30-day rotation
   at ~35k/night, per-portal concurrency caps, and a nightly budget that
   defers rather than hammers. Feasible only if portals tolerate it;
   needs a measured pilot on two courts first.
3. **Monthly for federal, tiered for cantonal.** Federal courts are the
   reputational core, are directly scraped, and tolerate load best.

Whichever is chosen, the refresh pass runs as its **own timer**, not
inside the nightly publish window, so it can be throttled or paused
without touching the rebuild.

## B. Storage — full current, reverse diffs for history

New sidecar database `output/decision_versions.db`, built incrementally
and never rewritten by the nightly rebuild (so the atomic-swap invariant
is untouched and `decisions.db` stays the same size and shape).

```sql
CREATE TABLE decision_versions (
  decision_id   TEXT NOT NULL,
  version_no    INTEGER NOT NULL,      -- 1 = oldest observed
  content_hash  TEXT NOT NULL,         -- SHA-256, same recipe as decisions
  observed_at   TEXT NOT NULL,         -- first time we saw this content
  superseded_at TEXT,                  -- NULL for the current version
  reverse_diff  BLOB,                  -- unified diff: current -> this version
  char_delta    INTEGER,               -- signed length change
  source_url    TEXT,
  merkle_leaf   TEXT,                  -- anchor for observed_at
  PRIMARY KEY (decision_id, version_no)
);
CREATE INDEX idx_dv_hash ON decision_versions(content_hash);
CREATE INDEX idx_dv_observed ON decision_versions(observed_at);
```

The **current** version's text stays in `decisions.full_text` exactly as
today — no reader changes, no migration risk. Superseded versions are
stored as reverse unified diffs against the current text, so
reconstruction walks backwards from what we already serve. This keeps
the common case (serving current text) free and the rare case
(reconstructing history) cheap, at roughly a tenth of the storage of
full copies. Disk peaks at 80 % during rebuild today, so this matters.

## C. Alerting — a change is a case to investigate

New quality-check module `quality/checks/decision_changes.py`, following
the `citation_anomalies.py` pattern exactly: `MODULE_NEVER_CRITICAL`,
alert-only, artifact for human review, never blocks a publish.

Artifact `logs/decision_changes_latest.json`, one record per detected
change: decision_id, court, docket, decision date, both hashes, char
delta, the diff itself, and classification flags:

- `citation_affecting` — the diff touches a citation token. **This is
  the Paper-2 signal**: a court acting on a reported error.
- `text_substantive` — body text changed beyond whitespace/markup.
- `metadata_only` — dates, regeste formatting, boilerplate.
- `anonymisation` — names or identifiers removed (privacy action, needs
  different handling from a correction).

Routing follows the existing daily-alert path. The workflow is
editorial, not automated: a change raises a case, we ask the court for
context, and the answer is recorded with the version.

## D. Governance

`docs/governance-and-removal-policy.md` gains a versions section:

- Version history is **internal at launch**; nothing published until the
  policy text is settled and courts have had notice.
- A court may request removal of any version. Removal deletes the
  content but leaves a **tombstone** — decision_id, version_no,
  removal date, requesting authority — so the audit trail records that a
  removal occurred even when the text is gone.
- Anonymisation-driven changes are honoured immediately and never
  surfaced as "corrections".

## Verification

- Pilot on two courts (one federal, one cantonal) for one cycle before
  any general rollout; report observed change rate and portal impact.
- Reconstruction test: for every stored version, applying the reverse
  diff to the current text must reproduce the recorded `content_hash`
  exactly. This runs in CI over a fixture and nightly over a sample.
- `make test` stays offline; the refresh pass is live-network and lives
  in its own timer with `make smoke`-style probes.
- No change to `decisions.db` schema, so the atomic swap and
  `immutable=1` invariants are untouched.

## Open question for the author

The load numbers in section A are the one place where "monthly" as
stated meets a physical constraint. Option 1 (tiered) honours the intent
at 2× current traffic; option 2 (literal) is 10× and needs a pilot to
show the portals tolerate it. My recommendation is option 1, with the
recent-window cadence set to monthly exactly as requested.
