# Cross-Identifier Decision Representations — Design

Date: 2026-07-15
Status: Design (pending gold-set audit; pipeline changes are invariant-#5-gated)
Origin: External review by Jörn Erbguth (entscheidsuche.ch), verified against production 2026-07-15.
Conferred: gpt-5.6 (xhigh).

## 1. Problem

Several cantonal portals publish each decision under **two identifiers** — a
procedure number (Verfahrensnummer) and a decision/publication number
(Entscheidnummer). OpenCaseLaw's per-source scrapers ingest each as a separate
`decisions` row, so the same legal act is stored twice under different dockets.
The current dedup passes key on `court|docket_number|date` (`_dedup_decisions`,
`_cross_court_dedup`) and the QC check keys on `(court, docket_number,
content_hash)`, so **different-identifier pairs are structurally invisible** to
all of them. `content_hash` is `SHA-256(regeste || full_text)`, not a body-only
fingerprint, so it also differs between the two representations.

### Verified findings (against production, 2026-07-15)

- **Geneva (`ge_gerichte`, 168,950):** each decision appears under a procedure
  number (`A/…` admin, `P/…` penal, `C/…` civil) *and* a decision number
  (`ATA/ATAS/AARP/ACJC/ACPR/DCSO/…`). Proof of identity: the two rows share the
  **same `source_url` and `pdf_url`**; the procedure copy declares the decision
  number in its header; **88.9% of 57,011 procedure rows cross-reference a
  decision-number**. **Exact count by distinct `source_url`: 92,314 unique
  decisions, 76,636 duplicate representations** — and 92,314 lands within **1.5%
  of entscheidsuche's independent GE count of 90,976** (two different methods
  agreeing). (The apparent "45% Geneva gap" from a naive count is not real.)

  **Doubling is 1.83×, not 2×, and it accrues over time.** 76,636 decisions
  (83%) have both representations; 15,678 (17%) have only one, of which 98% are
  decision-number-only. The singletons are a **lifecycle effect**: the judgment
  page appears first, and the procedure/publication page (which carries the
  appeal chain) is created/scraped *later*. 2026 decisions are ~100% singletons
  (2,240/2,251), falling to ~12% for 2021–2025. Implication: linkage must be
  **re-evaluated each build** (dynamic), never a one-shot collapse — otherwise
  a recent decision is counted "unique" until its twin lands and then double-counted.
- **Vaud:** `vd_findinfo` (74,825) + `vd_gerichte` (53,635) are two scrapes of
  the same Tribunal cantonal (`prestations.vd.ch`) under different numbering;
  `vd_findinfo` is additionally doubled internally. `vd_omni` (28,033, CDAP) is
  a **separate source, not duplicated**. The direct VD API already exposes both
  identifiers; the scraper keeps only `affaire.numero` and stores the other as
  `docket_number_2`. ~53–56k duplicate rows.
- **Schaffhausen:** `sh_gerichte` (705) ↔ `sh_obergericht` (718) are the same
  Obergericht, plus a **date-field bug** (`custom_publication_date_date` →
  `decision_date` in `sh_gerichte.py`) that puts the publication date in the
  judgment-date field (47-day error on OGE 60/2024/13). ~700 rows.

**Corrected scale (Phase-2 manifest + 2.5 refinement,
`scripts/build_representation_manifest.py`, 2026-07-15):** duplicate
representations **137,264 → unique decisions 911,689**, vs 1,048,953 records
(~13% overcount). GE **76,636** (exact, `source_url`); VD **59,939**
(`procedure_cross_reference`, restricted to the rubrum/first-700-chars); SH
**689**. Phase-2.5 validated the VD linkage: rubrum-position matches are twins;
the deep-body citation-false-positive tail is only **~90 links (~0.15%)** — the
rubrum restriction excludes it. The manifest is written to
`output/representation_manifest.db` (`decision_representations`) —
read-only/additive, no rows deleted. Residual for the merge step: date
reconciliation (~15% of links carry the judgment-date-vs-communication-date
semantics seen in GE — same decision, pick the judgment-header date).

## 2. Guiding principle — retain the representations, de-duplicate the *count*

The two variants are **not redundant copies** — they are different documents
with different roles and different update lifecycles:

- The **judgment copy** (decision number) is the frozen authoritative text.
- The **publication page** (procedure number) is the portal's *living record*:
  descriptors, norms, résumé, lower-instance reference, and the **appeal chain**.
  Verified: **11.3% of GE procedure pages (6,430) carry a Federal-Court-appeal
  reference the judgment cannot contain** (the appeal is filed *after* the
  judgment). Either representation can change independently over time (appeals
  and TF outcomes on the publication page; corrections / re-anonymization on the
  judgment).

Therefore we do **not** delete either row. We model each decision as one
canonical decision with multiple **representations**, so that:

1. **Historical/archival completeness is preserved** — including appeal-lifecycle
   data that entscheidsuche discards when it merges. This makes OpenCaseLaw
   *more* complete than the reference aggregator, not less.
2. **Counting and search become honest** — the decision is counted once and
   returns once in search, removing the ~13% overcount and the doubled results.
3. **Both identifiers stay resolvable** — a lookup by either the procedure number
   or the decision number resolves to the canonical decision (the #41 pattern).

This is orthogonal to the 2026-07-13 content-aware dedup change: that fixed false
merges of *genuinely distinct* rulings sharing a docket (same docket, different
bodies → keep both). This concerns the reverse — the *same* act under *different*
identifiers → link, don't multiply. The mission ("dedup must never remove
legitimate distinct decisions") is respected: we remove nothing; we relabel.

## 3. Data model (additive — no row deletions)

- **`decision_representations`** — links members to a canonical decision:
  `canonical_decision_id`, `member_decision_id`, `relation_type`
  (`judgment` | `publication_page` | `alt_scrape`), `evidence_method`
  (`shared_source_url` | `declared_cross_reference` | `body_hash` |
  `docket_number_2`), `confidence`, `algorithm_version`, `review_status`.
- **`decision_identifier_aliases`** — every procedure number, decision number and
  legacy id → canonical row. **Separate** from the BGer joined-docket alias table
  (`decision_docket_aliases`, #41) to keep semantics distinct.
- **Body-only dedup fingerprint** — a new `full_text_hash_normalized_v1`
  (Unicode-NFC, soft-hyphen/whitespace/line-ending normalized, order preserved).
  `content_hash` stays unchanged as the integrity hash. Do **not** reuse the
  4,000-char same-docket prefix heuristic for cross-identifier matching.
- **Appeal harvest** — extract the publication page's appeal references into the
  existing `appealed_court_raw / appealed_date / appealed_docket` and a *forward*
  "appealed-to" link feeding the treatment/citation graph.

## 4. Linkage rules (conservative, evidence-based, 1:1)

- **GE:** primary evidence = shared `source_url` (same portal document); backup =
  decision-number declared in the publication page's front matter (allowlisted
  prefixes) + equal judgment date. Require exactly one target; ambiguous/missing
  → retain unlinked. Canonical = the judgment-text row; merge the publication
  page's metadata + appeal chain.
- **VD:** **procedure-number cross-reference**, NOT body-hash. Verified
  2026-07-15: a normalized full-text hash finds **0** overlap between
  `vd_findinfo` and `vd_gerichte` (the two representations carry different text,
  like GE, and are scraped from different portals — `findinfo-tc.vd.ch` vs
  `prestations.vd.ch`, so `source_url` doesn't link them either, and
  `docket_number_2` is unpopulated). The reliable signal: **74%+ of vd_gerichte
  procedure numbers appear in a `vd_findinfo` publication page's **rubrum**
  (restrict the match to the first ~700 chars — the rubrum — to exclude a
  ~0.15% deep-body citation tail). **Require date-match** (or reconcile to the
  judgment-header date): a single VD procedure number can carry MULTIPLE
  decisions (a 2021 interim + a 2024 final, both citing the same PE-number), so a
  diff-date link may be a *distinct ruling of the same case*, NOT a twin — the
  ~15% date-disagreeing links must be split "date-semantics (merge)" vs
  "different ruling (keep separate)" at adjudication.
  **COMPLEMENTARY metadata — merge, do not pick-and-drop** (verified 2026-07-15):
  `vd_findinfo` carries the **regeste (99%)** — the official headnote —
  which `vd_gerichte` lacks (0%), while `vd_gerichte` carries **legal_area (70%)
  + chamber (100%)** which `vd_findinfo` mostly lacks. The merged canonical must
  harvest the regeste from `vd_findinfo` and the classification from
  `vd_gerichte`; dropping either side loses legally significant data (the regeste
  is the most-cited part). Keep both rows as representations. Fix the scraper to
  record both `affaire.numero` and `decisionHit.numero` going forward. Exclude
  `vd_omni` (own source, 0 overlap confirmed).
- **SH:** fix the date bug first (take the judgment date from the document
  header), then link `sh_gerichte ↔ sh_obergericht` on normalized docket + very
  high full-document similarity; keep the official copy canonical, retain the
  archive id as an alias.

## 5. Serving & reporting semantics

- `get_decision` / `cite` / resolvers resolve **any** identifier (procedure,
  decision, alias) to the canonical decision.
- Search **collapses** representations of one canonical decision to a single
  result; a `representations` field exposes the members + their source URLs.
- Statistics, dashboard, paper and dataset card report **two** figures with the
  methodology stated: `unique_decisions` (~915k) and `source_representations`
  (~1.05M). A transition dashboard shows both, never a silent relabel.

## 6. Guardrails (do NOT)

- Do not delete any GE `A/`/`P/`, `vd_findinfo`, or `vd_omni` rows.
- Do not dedup globally on `(court, date, full_text_hash)` — it catches none of
  these (copies are not byte-identical) and risks merging distinct rulings.
- Do not use `content_hash` as a body-only fingerprint; do not pick the longest
  row and discard the other's metadata; do not fuzzy-cluster corpus-wide.
- Do not bypass the swap gates with `OCL_SKIP_SWAP_GATE=1`. The 95% row-count
  floor and the per-court gate would (correctly) block a ~130k change; instead
  drive it from a **reviewed linkage manifest with an exact expected delta**, and
  update the 950k QC/`make verify` floors to test *canonical-retention*
  invariants (representations present, every id resolves) rather than raw rows.

## 7. Phased plan (invariant-#5-gated)

1. **Verify + gold set** (read-only): audit ≥200 GE candidates stratified by
   year/prefix, VD 2a/2b, all SH; record accept/reject/ambiguous.
2. **Linkage manifest only**: build the representation links + aliases; no
   deletions; measure projected collapse by court/year/language.
3. **Shadow build** (on a copy): materialize canonical + representations; assert
   every member has one canonical and every old id resolves.
4. **Link-only production canary**: expose aliases + `representations` + dual
   counts while retaining all rows; verify lookups, search-collapsing, graph
   redirects, dashboard dual counts. **Reversible.**
5. **Search/count canonicalization**: search collapses + stats switch to
   `unique_decisions`; rows still retained. Requires explicit approval
   (invariant #5).
6. **Downstream regen**: reference graph + Parquet rebuilt over canonical
   decisions with representation links; refresh stats/paper/benchmarks; withdraw
   or qualify the paper's "three VD sources are distinct snapshots" claim.

## 8. Future — divergence versioning

To fully realize the "one variant changes, the other doesn't" value (appeals, TF
outcomes, corrections, re-anonymization), re-scrape publication pages
periodically and **version** the representation, capturing the appeal outcome
when it lands. This turns the publication pages into a live treatment feed — a
Tier-1 goal — rather than a static snapshot. Prerequisite: confirm whether the
scrapers currently update or freeze existing rows on re-scrape.
