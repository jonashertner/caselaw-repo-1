# LLM cost levers — measured verdicts, 2026-08-24

Four parallel read-only analyses against production, then an adversarial
review. Baseline: Haiku ~$26/day (~$780/mo), Sonnet ~$8-11/day.
Every search fires THREE Haiku calls (parse + expansion + rerank).

**Three of the five original proposals were WRONG and are rejected below.**

---

## SHIP — measured, safe, reversible in one line

### 1. `SWISS_CASELAW_LLM_RERANK_TOP_N=8`  (~$4.61/day, ~$138/mo)

Currently 15. Evidence from **7,474 production rerank traces**: the final
#1 result came from retrieval positions 8-14 in **0.25%** of reranks
(19 of 7,474). Cutting to 8 reduces input 31% AND output 47% — output is
**44% of rerank cost** because the model echoes every candidate id back
as JSON, so output scales with TOP_N.

Rerank only ever REORDERS; it never drops results (mcp_server.py:8149-8157
iterates all scored rows; the boost is additive). So the failure mode is a
slightly different ordering, not a missing decision.

Honest caveat: position #1 moves 0.25%, but page composition below #1 moves
more — the model puts an idx>=8 candidate first in 13.4% of calls, and the
traces cannot replay positions 2-10. Run the benchmark the same evening as a
gross-regression guardrail (below); roll back if MRR@10 or nDCG@10 drops >2pt.

```
# /opt/caselaw/repo/.env.mcp
SWISS_CASELAW_LLM_RERANK_TOP_N=8
```
Constants are read at import → rolling restart required.
Rollback: delete the line, rolling restart. No data/schema/pipeline surface.

### 2. ops-dashboard timer 60s -> 300s  (~$0.8/day, ~$24/mo)

`ops_dashboard_snapshot.py` fires a real German query every 60 s = ~1,323
pipeline runs/day, ~192 of which reach Haiku. **We are paying Haiku to
monitor ourselves.** A 5-minute ops dashboard is fine. Zero serving-path
risk; it is our own probe, not the smoke canary.

### 3. search cache `maxsize` 256 -> 1024  (small, but free)

DONE locally in mcp_server.py. Replay of 3 days of captured traffic shows
1024 saturates the hit rate; ~350MB across 8 workers.

**TTL deliberately left at 1h** — see the rejected item below.

---

## VALIDATE FIRST — real money, real quality risk

### 4. Drop `query_expansion` behind a NEW flag  (~$3/day, ~$90-110/mo)

`_expand_query_with_llm` was superseded by `_parse_query_structured`
(which already returns doctrine in DE/FR/IT + synonyms) but never retired.
Measured overlap on 5,365 traces: the expansion's BGE channel is worthless
(its refs reach top-1 in 0.04% of traces vs the parse's 2.5% — 67x worse).

**THE TRAP:** `LLM_EXPANSION_ENABLED` gates **BOTH** Haiku calls
(mcp_server.py:849 and :1702). Setting it false would ALSO kill the
structured parse — losing doctrine_fr/doctrine_it (weight 3.0, the paper's
cross-lingual MRR headline) and leading_bge. **Never use that flag for this.**
It needs a NEW flag guarding only the call site at :7117.

Unmeasured risk: 50.8% of traces get statute refs from the expansion that
the parse did not supply. Value unknown. Per the no-regression rule, A/B at
**production scale over SSH** (local fixtures are 1-3% of prod and can invert
the result), watching concept-match MRR and cross-lingual MRR.

---

## REJECT

### 5. Sonnet 5 migration — NO-GO

- Intro pricing ends **2026-08-31**; from 09-01 Sonnet 5 lists at $3/$15,
  **identical** to Sonnet 4.6. Durable saving is **$0.00 by construction**;
  max upside is <$10 in a <=5-day shippable window.
- Sonnet 5 uses the Opus-4.7-generation tokenizer, ~30% more tokens for the
  same text → expected **+$100/mo permanent increase**.
- It is not a one-word change. On Sonnet 4.6 omitting `thinking` means
  thinking-off; on Sonnet 5 it means **adaptive thinking ON**. All five call
  sites do `content[0]["text"]`, which becomes a `thinking` block →
  `KeyError` (mcp_server.py) or **silent empty string** (stripe_billing.py,
  i.e. paying Pro subscribers fail quietly). `max_tokens` 400/500 would be
  consumed by thinking before the JSON is emitted.
- `reflect` (mcp_server.py:15838) passes `temperature: 1.0`, which Sonnet 5
  rejects with a 400 — and that temperature is half a documented fix for a
  real duplicate-output bug. Unmigratable as written.

### 6. `SWISS_CASELAW_FULL_TEXT_RERANK_CHARS` — NO-OP, do not set

Consumed only by `_build_rerank_document` → `_apply_cross_encoder_boosts`,
the **local CPU cross-encoder**. **Zero Haiku tokens pass through it.** And
`CROSS_ENCODER_MAX_LENGTH=256` truncates long before 1400 chars binds, so
even the CPU saving is nil. (This was in my original proposal. It was wrong.)

### 7. `SWISS_CASELAW_LLM_RERANK_GATE` — leave at 2.0

Measured across 9,504 traces: in the 1.05-1.30 band the skipped cohort
changes #1 about half as often as baseline — weak discrimination. Gate 1.10
saves $3.08/day but changes #1 on 2.3% of reranked searches, vs TOP_N=8 at
$4.61/day for 0.25%. **~9x worse quality per dollar.** If ever forced, 1.15
is the floor.

### 8. Shared cross-worker cache — do not build

The premise is wrong. Genuine third-party MCP agent traffic has a
byte-identical repeat ceiling of **0.9%** (48/5,064) — agents phrase every
query differently. Of the ~353/day extra hits a shared cache would buy,
**~306 are our own probes**. Real third-party saving: ~$0.2-0.5/day.
The shared cache buys $26-43/mo over a simple constants bump, for ~150 lines
of new IPC on the serving hot path.

**Landmine if anyone tries anyway:** `mcp-server@.service` sets
`PrivateTmp=true`, so every worker gets its OWN `/tmp` — SQLite-on-tmpfs at
/tmp would silently create 8 separate caches, the exact bug being fixed.
`/dev/shm` is the only shared writable path. And `db_generation` would have
to move INTO the key, because per-process `_cache_clear()` races across a
shared store.

### 9. Cache TTL 1h -> 6/24h — rejected on invariant grounds

Would help, but the probe that benefits most is `quality/smoke.py`'s canary,
which uses a real German query specifically so it reaches `_rerank_rows` —
"precisely the code that broke" in the 2026-08-22 outage. A longer TTL turns
that canary into a cache hit and pushes rerank-breakage detection from
minutes to hours. Fix `smoke.py` to vary its query first, then revisit.

---

## SAVINGS — do NOT add these up naively

Levers 1, 3 and 4 all reduce the SAME per-search cost, and a cache hit skips
all three Haiku calls, so the overlap is real. Likewise the cache bump and the
ops-timer change are largely the SAME money (both target our own probes).

Realistic combined, after overlap: **~$150-170/mo now** (TOP_N + ops timer +
maxsize), rising to **~$240-270/mo** if the expansion drop survives its
benchmark. Against ~$780/mo Haiku that is a 20-35% cut with no user-facing
capability removed.

---

## BUGS FOUND EN ROUTE (independent of cost)

1. **`stripe_billing.py` never calls `_llm_usage_log`** — the Word add-in's
   Pro Sonnet spend (2 call sites) is INVISIBLE to llm_usage.jsonl, /metrics
   and the per-IP ledger. Real Sonnet spend is higher than measured by an
   unknown amount. *This partly explains the ledger-vs-invoice gap.*
2. **`scripts/llm_usage_report.py:237-238`** hardcodes the report footer as
   "claude-haiku-4-5 $0.80/$4 per M" — the pre-correction prices fixed in
   mcp_server.py on 2026-08-24. The HTML cost report still shows the wrong rate.
3. **`mcp_server.py:3654` is `break`, not `continue`** — once the candidate
   pool is full the strategy loop EXITS, killing everything after it,
   including `doctrine_fr`/`doctrine_it` at weight 3.0, which are appended
   last. A free cross-lingual-MRR win, independent of any cost lever.
4. **`content[0]["text"]` at 5 Anthropic call sites** is latent fragility
   worth fixing on Sonnet 4.6 regardless — replace with a type scan.
5. **`scripts/search_optimizer/optimize.py:155`** pins
   `claude-sonnet-4-20250514`, retired 2026-06-15 — dead if run.
6. Expansion timeouts are unlogged (`_llm_usage_log` fires after
   `raise_for_status()`), so expansion cost figures are a floor.

---

## BENCHMARK (guardrail for lever 1, gate for lever 4)

Must run on the VPS against prod (local DB is a 20MB fixture) and needs
ANTHROPIC_API_KEY — without it `_apply_llm_rerank` returns early and the
run measures nothing while still printing numbers. Outside 03:30-17:00 UTC.

```
cd /opt/caselaw/repo
export ANTHROPIC_API_KEY=... SWISS_CASELAW_DIR=/opt/caselaw/repo/output
python3 -m benchmarks.run_search_benchmark --db /opt/caselaw/repo/output/decisions.db -k 10 \
  --golden benchmarks/search_relevance_golden.json \
  --golden benchmarks/search_relevance_candidates_v3.json \
  --json-output /tmp/bench_topn15.json
SWISS_CASELAW_LLM_RERANK_TOP_N=8 python3 -m benchmarks.run_search_benchmark --db ... \
  --json-output /tmp/bench_topn8.json
```
~$1.50 of Haiku per run. Note it cannot resolve a 0.25% top-1 effect —
it is a cliff-detector, not validation. The trace analysis is the validation.
