# Observability — Health Metrics + Synthetic Alerts

What the system collects, where it comes from, what would fire as an
alert, and how to turn dry-run alerts into real notifications after
Monday's PR 1 gate passes.

## Endpoints

| Path | Type | Purpose |
|---|---|---|
| `/health` | JSON | Liveness probe (always-on, public). Returns `{status, decisions, db_generation}`. |
| `/metrics` | JSON | Existing in-process tool counters (latency, calls, error rates, queries). Unchanged by D2. |
| `/metrics/health` | JSON | New (D2). Structured health snapshot + `alerts_dry_run`. Cheap; safe to poll every 30 s. |
| `/dev/health` | HTML | New (D2). Read-only operator dashboard built on `/metrics/health`. |
| `/metrics/history` | JSON | Existing lifetime counters from `metrics.db`. Unchanged. |

## Metrics in `/metrics/health`

| Key | Source | What it means | Default if missing |
|---|---|---|---|
| `ts` | `health_metrics._now()` | Server's wall clock at collection time | — |
| `db_generation` | `mcp_server.get_db_generation()` | Last `PRAGMA user_version` observed (see `db_contract.md`) | `0` |
| `pipeline_last_success_ts` | `mtime(output/decisions.db)` | Last successful atomic swap | `null` |
| `quick_publish_last_run_ts` | `mtime(logs/bger_poller.log)` | Last `bger_poller` wake | `null` |
| `bger_poller_last_run_ts` | Same as above today | Will diverge after A1 lands `quick_publish_metrics.jsonl` | `null` |
| `freshness_seconds_by_court` | `MAX(scraped_at)` per court in `decisions.db` | Per-court age of newest scraped row | `{}` |
| `daily_cost_usd_24h` | Sum of `cost_usd` in `logs/llm_usage.jsonl` (last 24h) | LLM spend | `0.0` |
| `alerts_dry_run` | `health_alerts.check_all()` | Would-fire alerts; never actually fire today | `[]` |

### Why `scraped_at` for freshness, not `ingest_ts`?

`scraped_at` records when a scraper grabbed the row, not when it
landed in the published DB. For "publication-to-MCP visible" we
want a true `ingest_ts` column — not in the schema yet (planned for
the Saturday A6 deploy). Until then `scraped_at` is the best signal
we have; per the v2 plan's edit, this gap is documented, not
worked around.

## Alert rules (currently dry-run)

| Key | Level | Condition | Threshold |
|---|---|---|---|
| `pipeline_stale` | critical | `pipeline_last_success_ts` older than threshold | 26 h |
| `quick_publish_stale` | warning | On a weekday (UTC), `quick_publish_last_run_ts` older than threshold | 2 h |
| `mcp_error_rate_high` | warning | `sum(tool_errors) / sum(tool_calls)` over the in-process counters | 1% (min 100 samples) |
| `pipeline_unknown` | warning | `pipeline_last_success_ts` is `null` | — |
| `quick_publish_unknown` | warning | On a weekday, `quick_publish_last_run_ts` is `null` | — |

Source: `health_alerts.py`. Each rule is wrapped in a try/except so
a bug in one cannot suppress the others — the failing rule is
itself reported as `<rule>_error`.

### Why these thresholds?

- **26 h pipeline staleness**: catches a missed nightly with one
  margin hour. Currently every weekday runs `quick_publish` (mtime
  bump) and Sunday runs full rebuild — both visible via the same
  signal.
- **2 h quick_publish staleness on weekday**: `bger-poller.timer`
  fires every 15 min Mon-Fri 05:00–16:45 UTC. A gap longer than 2 h
  in that window indicates the poller is wedged.
- **1% MCP error rate**: in normal operation the rate is <0.1%.
  Anything sustained above 1% indicates a real malfunction (DB
  unreachable, downstream API down, schema mismatch).

## How alerts become real

The Monday gate (after PR 1 transition lands) determines whether
to wire alerts to a notifier. Sequence:

1. **Monday morning**: verify the PR 1 contract works in production
   (see `runbooks/db_generation_mismatch.md` and the Monday gate
   checklist below).
2. **If the gate passes**: open a follow-up PR (D2b) that adds a
   notifier — push notification, email, or webhook. Wire `check_all`
   results into it; deduplicate via `(key, level, day)`.
3. **If the gate fails**: pause D2, fix PR 1 first.

### Monday gate checklist

The first `bger_poller` wake (~05:00 UTC Monday) is the first
production exercise of the `db_generation` contract. Verify:

- [ ] `/health` reports `db_generation != 0` on all 4 workers
- [ ] Each worker logs exactly one `db_generation transitioned 0 → <ts>`
- [ ] `_query_cache` clear happens without an error spike
- [ ] `decisions` count is stable or increases only by the row count
      reported by `quick_publish`
- [ ] `/api/billing/reflect` and `/search_decisions` smoke pass

If all five hold: deploy notifier wiring. If any fails: pause and
debug per `runbooks/db_generation_mismatch.md`.

## Operating

- `/metrics/health` is safe to poll at 30 s cadence (the freshness
  query is `MAX(scraped_at)` grouped by court, ~50 ms on the
  current DB).
- `/dev/health` is a static HTML page that polls the JSON every
  30 s — keep it open during deploys.
- For external monitoring: use `/health` for uptime, `/metrics/health`
  for structured drift signals. Both return JSON; both work without
  authentication.

## Adding a new alert rule

1. Write `check_<name>(health, now=None)` in `health_alerts.py`.
   Return `None` for clear or `dict{level, key, message, ...}`.
2. Register it in `check_all()`'s rule tuple.
3. Add a unit test in `tests/test_health_metrics.py`.
4. Document in the threshold table above.

## Non-goals (in this phase)

- No external push, email, or webhook notification. Dry-run only.
- No persistence of alert history. The notifier PR will add that.
- No cross-worker aggregation. Each worker reports its own state.
- No alerting on freshness-per-court yet. Will come once A6 lands
  `ingest_ts` and we have a defensible threshold per court.
