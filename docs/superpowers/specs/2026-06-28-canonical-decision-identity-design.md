# Canonical Decision Identity — derive-from-text enrichment + ECLI

**Date:** 2026-06-28
**Status:** Draft for review
**Scope:** Pipeline (build) + serving. Pipeline-critical (invariant #5) — gated, shadow-validated before cutover.

---

## 1. Problem & evidence

A 2026-06-28 corpus audit (~10 probes) surfaced a cluster of accuracy/completeness
defects. Each was re-verified against the live corpus (994,759 decisions):

| Finding | Verified | Severity |
|---|---|---|
| BGE dates an unflagged mix of real/synthetic | **63.6% of BGE (22,507/35,395) dated `YYYY-01-01`**; BGE 152 II 1 = stored 2026-01-01, real 2025-09-27 | Critical |
| Every published BGE exists as two unlinked records (excerpt + docket) | confirmed; `bge` excerpt has no Dispositiv, synthetic date; `bger` docket has real date + outcome | Critical |
| `find_leading_cases` ranks by global citation count | confirmed in code (statute path gates candidates but ranks by global in-degree) | Critical |
| NULL-dated records invisible to date filters | **exactly 1,301** (mkg 542, ti_gerichte 263, hudoc_ch 247 — not be_zivilstraf) | High |
| Structured outcome/reasoning federal-only; BGE has no Dispositiv | confirmed (excerpt omits it by design — it is on the docket) | High |
| Coverage theater | **12 "courts" with <15 decisions** (zh_mietgericht 1, bs_gerichte 2…) | High |
| Impossible future dates | **9 fr_gerichte rows**, max 2026-08-17; no date sanity gate | Moderate |
| Citation arrays unreliable (self-loops, empties) | credible | Moderate |
| `by_year`/`by_language` unusable | confirmed — polluted by synthetic dates; **only de/fr/it**, no English (ECHR) or Romansh | Moderate |
| Non-adjudicative junk ingested | credible (bazg customs brochure as "decision") | Moderate |
| Docket-format inconsistency | confirmed (`2C 838/2018` / `2C_838/2018` / `2P.139/2004`) | Moderate |

### Root cause

Two structural shortcuts, not eleven independent bugs:

1. **Parsers assert scraped metadata as fact** even when the authoritative answer
   is in the decision's own text. The BGE date falls back to the *volume year*
   (`152` → 2026) although the Urteilskopf states it plainly:
   `… 9C_113/2025 vom 27. September 2025 Regeste …`.
2. **No logical-decision layer.** A single ruling has up to two *source documents*
   — the BGE excerpt (clir: regeste + canonical numbering, anonymised, no
   Dispositiv) and the docket (aza: full text, real date, Dispositiv, outcome) —
   stored as two rows with no link. Hence overcounting, conflicting dates, missing
   outcome.

Everything Critical/High follows from these two.

---

## 2. Principle

> **The decision's own text is the source of truth. Derive every asserted field
> from it, mark provenance, and reference each ruling by a single canonical
> identifier (ECLI) computed from the verified fields.**

This turns the corpus from "asserts what the scraper captured" into "asserts what
the document says, and admits when it doesn't know."

---

## 3. Architecture — one enrichment pass feeding one identifier

```
                 ┌─────────────────────── derive_from_text (build-time) ──────────────────────┐
 decision text ──▶ extract: real date (docket-validated) · normalized docket · structure flag │
                 │            │                                                                 │
                 │            ▼                                                                 │
                 │   verified fields ──▶ build ECLI:CH:<court>:<year>:<ordinal> (canonical key) │
                 │            ▲                    │                                             │
                 │   docket-year VALIDATES ────────┘ (ruling can't predate its docket)          │
                 └────────────┼──────────────────────────────────────────────────────────────┘
                              ▼
        canonical_key (ECLI) ──▶ dedup BGE↔docket · cross-fill date/Dispositiv · interop citation
```

The keystone is a single pure module, `derive_from_text.py` (**already built,
11 unit tests green, proven read-only on the corpus**), applied per decision
during the build. It emits: corrected `decision_date` + `date_provenance`,
normalized `docket_number`, `ecli`, `canonical_key`, and the BGE↔docket link.

The coupling with standardized referencing is **bidirectional**:

1. **Extraction is prerequisite to a correct ECLI** — the year must be real; a
   synthetic `2026-01-01` yields a wrong-year, non-resolving ECLI.
2. **The identifier validates the extraction** — the docket embeds the filing
   year (`9C_113/2025`→2025); a ruling cannot predate its docket, so the docket
   year is an independent check on the extracted date. *(Measured: lifts accuracy
   87% → 91%.)*
3. **The ECLI is the dedup/join key** — one identifier per ruling, so the BGE
   excerpt and its docket map to the **same** `ECLI:CH:BGER:2025:9C_113.2025`;
   the identifier dissolves the duplication at the identity level.

This plugs into the already-live layered `cli:ch + ECLI` identifier and the
RFC-6962 Merkle provenance at `/integrity` — making that infrastructure *correct*
rather than built on synthetic dates.

---

## 4. Components

### 4.1 Date extraction (accuracy keystone) — built

`extract_urteilskopf(text)` reads the decision's own date from the header,
anchored to the docket (`<docket> vom|du|del <date>`, DE/FR/IT) and validated by
the docket year. `derive_date(stored, text)` returns `(best_date, provenance)`:

- `source_metadata` — stored date present and not synthetic (trusted as-is).
- `extracted_from_text` — real date recovered from the document.
- `volume_synthetic` — only the `YYYY-01-01` placeholder; kept but flagged.
- `null` — no date anywhere.

**Measured (read-only, live corpus):** synthetic BGE — **76.3% recovered
(17,167)**, 23.7% flagged unverified; **91.2% accurate** vs the independent docket
date after docket-year validation. NULL-dated — **68.5% recovered (891/1,301)**.

**Precision tiering (refinement):** split `extracted_from_text` into
`extracted_verified` (docket-adjacent + year-validated + agrees with the linked
docket where available) vs `extracted_unverified` (bare-date fallback). Anything
safety-critical (limitation-period reasoning) uses only the verified tier.

#### 4.1.1 decision_date vs publication_date are DISTINCT (the both-dates rule)

A ruling has two different dates and both matter: the **decision date**
(Urteilsdatum — governs limitation periods, chronology, "still good law") and the
**publication date** (when it became citable/indexed). 68.3% of the corpus
(679,744 rows) currently lacks a publication date (led by 189k BGer dockets).

Key finding: a BGE's synthetic `YYYY-01-01` is **not a wrong decision date — it is
the Amtliche-Sammlung VOLUME (publication) year mis-filed into `decision_date`**
(verified: `YYYY == 1874 + volume` for 97% of synthetic BGE). So the fix is to
**demux**, not overwrite (`derive_dates()`):

- `decision_date` ← the real Urteilsdatum from the text, else `null` (never leave
  the volume year masquerading as a decision date);
- `publication_date` ← the volume year (year-precision, provenance `volume_year`)
  when no real publication date is stored.

**Measured (read-only):** 19,837 decision dates corrected **and** 25,493
publication dates recovered from the volume year (previously NULL) — the both-dates
requirement advanced in the same pass.

### 4.2 Canonical identifier (ECLI) — built

`build_ecli(court, date, docket)` → `ECLI:CH:<court>:<year>:<ordinal>` from the
verified year + normalized docket. Where the docket is absent (old BGE volumes —
currently ~96% of synthetic BGE expose no federal docket in text), the **BGE
citation forms the ordinal**: `ECLI:CH:BGER:2026:152.II.1` (ECLI permits
court-defined ordinals). So every federal ruling gets an ECLI; the docket form is
preferred, the volume-page form is the fallback.

### 4.3 Logical-decision layer (dedup + cross-fill)

The ECLI is the join key. At the **serving** layer:
- **Search dedup** — when both a BGE excerpt and its docket match, collapse to one
  logical hit carrying both citation forms (fixes overcounting).
- **Cross-fill** — `get_decision` on a BGE shows the linked docket's
  Dispositiv/outcome/real date; the docket shows the BGE's regeste + canonical
  numbering. Where the docket isn't in the corpus, *flag* the pairing, don't hide it.

### 4.4 Provenance & honest aggregates

`date_provenance` column on every decision. `by_year` excludes/flags non-`*_from_text`/`source_metadata`
dates; never present a synthetic date as real in a date filter or histogram.

### 4.5 find_leading_cases — topical ranking (independent)

Route statute queries to the statute path; rank by **intra-topic** citation count
(incoming edges *from other decisions applying the same provision*), not global
in-degree. The graph already has decision→statute edges. Algorithm change, no new
data. Validate on the auditor's Art. 41 OR probe.

### 4.6 Data hygiene (Tier 4 quick wins, independent, parallelizable)

- **Build-time date sanity gate** — reject dates > today (the 9 fr_gerichte rows)
  and < 1875; prevent recurrence.
- **Self-citation-loop filter** in the graph build.
- **Non-adjudicative content filter** — require decision structure (Erwägungen /
  Dispositiv signal) or a source/type allowlist; drops the bazg brochure class.
- **Docket normalization in lookup** — `normalize_docket()` already built; apply
  at `/api/lookup` so `2C 838/2018 ≡ 2C_838/2018 ≡ 2P.139/2004`.
- **Thin-collection surfacing + dead-scraper audit** — flag the 12 <15-decision
  courts; `zh_mietgericht: 1` smells like a broken scraper, not a thin court.
- **ECHR English + Romansh language tagging.**

---

## 5. Schema changes

Add to `decisions` (and the build/export):

| Column | Type | Meaning |
|---|---|---|
| `decision_date` | TEXT | the Urteilsdatum — corrected in place for `extracted_*`; `null` if unrecoverable (never the volume year) |
| `decision_date_provenance` | TEXT | `source_metadata` / `extracted_verified` / `extracted_unverified` / `null` |
| `publication_date` | TEXT | distinct from decision_date; recovered from the volume year where NULL |
| `publication_date_provenance` | TEXT | `source_metadata` / `volume_year` (year-precision) / `null` |
| `ecli` | TEXT | canonical `ECLI:CH:…` (docket ordinal preferred, volume-page fallback) |
| `canonical_key` | TEXT | logical-decision key (= ECLI); shared by an excerpt/docket pair |

`decision_date` and `publication_date` are **never conflated** (§4.1.1).

`canonical_key` already exists as a column; repurpose/populate it as the ECLI.
The BGE↔docket relation is derivable by grouping on `canonical_key` (no separate
table required); add `decision_links(ecli, bge_id, docket_id)` only if a
materialized relation proves faster for serving.

`decision_date` is **corrected in place** for `extracted_*` rows; the original
stays recoverable via `json_data`.

---

## 6. Pipeline integration

A new build step `2x_derive_canonical` (after FTS text is assembled, before the
reference graph so the graph can key on `canonical_key`), or folded into
`build_fts5` row assembly. It calls `derive_from_text` per row. Pure-Python, no
network, ~1M rows; budget against the nightly. **Gated:** test on a copy of
`decisions.db`, never the live volume.

---

## 7. Serving changes (`mcp_server.py`)

- `get_decision` / `get_decision_structure`: merge the linked pair (real date,
  Dispositiv from docket, regeste + numbering from BGE, both citations + ECLI).
- `search_decisions`: dedup by `canonical_key`; one hit per ruling.
- `cite()` / `citation_string_*`: include the ECLI as the stable interop reference
  (respecting R1 — sourced from the field, never constructed ad hoc).
- `find_leading_cases`: intra-topic ranking (4.5).

---

## 8. Verification & shadow validation

1. **Offline:** `derive_from_text` unit tests (built); add corpus-level assertions
   (recovery ≥ target, accuracy ≥ 90% on the docket-cross-check sample).
2. **Build on a copy** of `decisions.db`; diff vs the live corpus: how many dates
   changed, provenance distribution, ECLI coverage, dedup collapse count.
3. **Drift gate:** never lose a date that was real; never overwrite a
   `source_metadata` real date; future-date count → 0.
4. **Shadow N nights** (as with the incremental-builder cutover): build the
   enriched DB alongside, compare, require N green drift nights before serving.
5. `make verify` headline numbers unchanged except the intended date/`by_year`
   corrections.

---

## 9. Risk, gating, sequencing

Pipeline + serving changes → high blast radius (nightly 03:30 UTC). Per invariant
#5: explicit approval per change, test on copies, shadow before cutover.

**Sequence (highest accuracy-per-risk first):**

1. **Tier 0 + Tier 4 quick wins** — `date_provenance` flag + the date-sanity gate +
   docket-normalization lookup + junk filter + thin-collection flags. Cheap, safe,
   immediate honesty/accuracy. (Some are serving-only.)
2. **Tier 1 — date recovery** (the big accuracy win; data pass, gated, shadowed).
3. **Tier 2 — ECLI/canonical_key + dedup/cross-fill** (identity layer).
4. **Tier 3 — find_leading_cases** (independent, anytime).

---

## 10. Deferred / open questions

- **Precision vs recall** of date extraction beyond 91% — is the
  verified/unverified tier sufficient, or do we re-scrape the BGE source for the
  ~24% unrecovered (old volumes)?
- **Old-BGE dockets** not in corpus — recover the originating docket from the BGE
  source (bger.ch) to raise docket-form ECLI coverage above ~4%?
- **cli:ch ordinal** alignment with the existing layered identifier (reuse its
  ordinal scheme, don't fork).
- **Cantonal tier** — extraction patterns are federal-tuned; cantonal date
  recovery (heterogeneous headers) is a later, lower-yield phase.
- **Anonymisation** — cross-filling docket text into a BGE view must preserve the
  BGE's anonymisation (the docket may be less redacted); serve the BGE text, only
  borrow structured fields (date, Dispositiv outcome) from the docket.

---

## Appendix — built artifacts (this session)

- `derive_from_text.py` — pure extraction + validation + ECLI module.
- `tests/test_derive_from_text.py` — 11 tests (real BGE 152 II 1 fixture).
- Read-only corpus measurements (§4.1) establishing recovery/accuracy.
