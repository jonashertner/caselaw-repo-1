# Semantic rescue for `search_laws` — design

- **Date:** 2026-06-30
- **Status:** Design (gated — touches the nightly build + the serving core; requires explicit approval before implementation per CLAUDE.md invariant #5)
- **Origin:** GitHub issue #31 follow-on. The AND-first-fill join fix (commit `14e08a0`) addresses users who typed ≥1 real statutory term; this addresses the deeper failure it cannot reach.

## Problem

`search_laws` is FTS5/BM25 over statute-article text. Two distinct failure modes:

1. **Guessed-some-terms** (semi-experts): one absent term zeroes an AND query. **Fixed** by AND-first-fill (#31).
2. **Pure-colloquial** (laypersons, journalists, students): the words the user typed are *in no statute*, so neither AND nor OR can match. Measured live:
   - `"Gerichtsverfahren pausieren lassen"` → AND 0, OR 10 but all noise; the wanted **Art. 126 ZPO is absent** (statute says "Sistierung").
   - `"Datenschutz Gesundheitsdaten"` → the core **DSG is absent** (statute says "besonders schützenswerte Personendaten").

Failure mode #2 is a **vocabulary-mismatch** problem. The colloquial→statutory expansion dictionary (`_expand_law_query`) helps but is hand-curated and unbounded; it cannot cover the long tail. Embeddings bridge it generically: "Gerichtsverfahren pausieren" lands near "Sistierung des Verfahrens" in vector space.

## Goal / non-goals

**Goal:** when lexical search under-serves a `search_laws` query, fall back to a **semantic** match over statute articles, so colloquial queries reach the on-point article. Cross-lingual as a bonus (the model is multilingual — a DE query can hit FR/IT articles).

**Non-goals:** replacing FTS (lexical stays primary and authoritative for exact terms/abbreviations/article numbers); cantonal laws (those route to LexFind — out of scope); changing the R1–R3 citation contract (semantic only *ranks* articles; the served text is still verbatim from the row).

## Why this is low-risk-to-build: the infra already exists

The repo already runs this exact pattern for the **pinpoint resolver**:
- `search_stack/build_paragraph_embeddings.py` encodes decision paragraphs with `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` (384-dim) → `paragraph_embeddings.db` (raw BLOB rows).
- `mcp_server.py:406–437`: lazy-loaded model, **semantic-rescue** wiring ("only consulted when lexical fails"), env-flag gating (`PINPOINT_SEMANTIC_ENABLED`, default off until the corpus is encoded), calibrated cosine thresholds (0.55 medium / 0.70 high), ~30–50 ms/call.
- `docs/pinpoint_semantic_rollout.md` documents the phased shadow→enable rollout.

This design **mirrors that pattern** for statute articles. Same model, same storage shape, same lazy-load, same env-flag rollout. New work is an embedding build step + a rescue branch in the federal search, not new infrastructure.

## Architecture

### Build (offline, nightly — gated)
- New `search_stack/build_article_embeddings.py` (mirrors `build_paragraph_embeddings.py`): encode each statute article's `heading + text` → `article_embeddings.db` table `article_embeddings(sr_number TEXT, article_num TEXT, lang TEXT, embedding BLOB, model_name TEXT, PRIMARY KEY(sr_number, article_num, lang))`.
- **Incremental**, keyed on an article content hash (re-encode only changed/new articles), like the paragraph builder.
- Wired as a publish step **after** `statutes.db` is built; produces a sibling DB, atomically swapped. Default-off on the serving side until fully encoded.

### Serve (gated serving change)
In `_search_laws_federal`, after AND-first-fill, when the page is **still under-filled** (`len(priority) < limit`) and `ARTICLE_SEMANTIC_ENABLED`:
1. Encode the raw query once (reuse `_SEMANTIC_MODEL`, already lazy-loaded for pinpoint).
2. Cosine over the article matrix; take top-K above a calibrated threshold.
3. Append as `match: "semantic"` results **below** the lexical hits (dedup via the existing `seen_keys`). Lexical/exact always ranks first; semantic is a labeled rescue.
- **Defensive:** absent/stale `article_embeddings.db`, model load failure, or flag off → lexical-only, exactly as today. No new hard dependency on the serving path.

## Key design decisions (recommendation first)

1. **Corpus scope.** Encode all three languages, but **start with the ~1,168 named laws' articles** (the set people actually search; bounded) before the full 400k. *Rec: named-laws first, expand to full DE (133k) once latency/quality validated.*
2. **Vector search method.** 133k × 384-dim ≈ 200 MB held per worker; a brute-force `numpy` matmul cosine is ~tens of ms — same family as the existing pinpoint cosine, just a bigger matrix. *Rec: brute-force first (no new dep), measure p95; add an ANN index (sqlite-vec / hnswlib) only if the latency budget is breached.*
3. **Trigger.** Rescue only when lexical under-fills (`< limit`), not on every query. *Rec: under-fill trigger (cheap, default-safe); a full hybrid+RRF mode behind a second flag later, mirroring `PINPOINT_SEMANTIC_HYBRID`.*
4. **Merge.** Append semantic below lexical (rescue), not RRF-fused. *Rec: append (keeps exact-first contract from #31); revisit RRF if we go hybrid.*
5. **Latency / memory.** Query encode ~10–30 ms + cosine ~tens ms ≈ <100 ms added, only on under-filled queries. Model (~120 MB) + matrix (~200 MB) per worker × 4 ≈ 1.3 GB on a 64 GB host. The model is already loaded for pinpoint, so marginal cost is the matrix. *Acceptable; budget p95 < 150 ms for the rescue branch.*

## Verification

- **Eval set** (colloquial → expected article), offline, in `make test` with a tiny fixture embeddings DB: `"Gerichtsverfahren pausieren"`→Art. 126 ZPO, `"Gesundheitsdaten Schutz"`→DSG, `"Wohnung kündigen"`→Art. 266 ff OR, plus FR/IT cross-lingual probes.
- **No-regression:** every query that lexical already answers must be byte-identical (rescue only fires on under-fill); a defensive test with the embeddings DB absent must return the lexical result unchanged.
- **Latency:** measure the rescue-branch p95 on the live box; gate at < 150 ms.
- **Rollout (phased, mirrors pinpoint):** build & encode → ship serving code default-OFF → shadow-log rescue hits vs lexical → enable for named-laws → measure quality+latency → expand corpus → optional hybrid mode.

## Safety / gating

- Nightly-build change (new step + new DB) **and** serving-core change → **gated** (invariant #5): surface diffs, test on a **copy** of `statutes.db`, never touch the live volume in dev. Default-off env flag (`ARTICLE_SEMANTIC_ENABLED=false`) until encoded and validated.
- Atomic-swap the sibling DB; serving opens it `?mode=ro&immutable=1`; absent → lexical fallback (no dead-end).
- R1–R3 untouched: semantic affects *ranking only*; citations and quoted text still come verbatim from the row / `cite()`.

## Open questions

- Encode `heading + text`, or also the law title / marginal notes (the heading is the concept label — likely the strongest signal; a "heading-boost" lexical tweak is a cheap orthogonal win worth bundling)?
- Brute-force vs ANN at full 400k scale — decide after the named-laws latency measurement.
- Should the rescue also surface on the REST/dashboard path, or MCP-only first?
