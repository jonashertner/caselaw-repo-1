# Privacy-preserving reach analytics

How we estimate distinct user cohorts per client class without breaking the
"no IPs, no UAs, no queries" privacy contract on tier-2 logs.

## The three additions (2026-05-18)

| Surface | Was | Now |
|---|---|---|
| `daily_reach.n_cohorts_hll_estimate` | Only populated for `word_addin` (Word add-in license cohort) | Populated for **all** client classes, via tier-1 cohort derivation (see below) |
| `daily_*.n_public` | `NULL` when exact count < k=10 | Unchanged (legacy column) |
| `daily_*.n_dp` (NEW) | n/a | Always populated with DP-noised count (ε=1.0), regardless of k-anon |
| `weekly_*` (NEW) | n/a | Per-ISO-week aggregates; k-anon at weekly granularity → many more cells clear the gate |

## The cohort identifier (privacy contract)

The cohort hash is `SHA256(remote_addr || user_agent || YYYY-MM)[:8]` —
a 32-bit truncated, monthly-rotating digest. It **cannot** be correlated
across months (the year-month suffix changes), is **lossy** by truncation,
and is **never stored** in raw form. It is only ever fed into a
HyperLogLog sketch (`p=12`, ~±2% relative error) whose state is also
not persisted between rollups.

Computed in two places, identically:

1. **Word add-in** (client-side, in JS): the add-in computes the hash
   from its license_key, sends it in the `X-Install-Cohort` header.
   nginx writes the truncated value to tier-2.
2. **All other clients** (server-side, at rollup time):
   `scripts/derive_cohorts_from_tier1.py` reads the 72h-retention tier-1
   log (which already carries IP + UA for abuse-response purposes),
   computes the same hash in memory, contributes to the per-client HLL.
   Raw IP and UA never leave the script.

## The rollup pipeline

```text
nginx tier-2 log (no IPs, no UAs, no queries)
        │
        └──▶ rollup_analytics.py  ──▶  analytics.db
                                        ├── daily_tool_calls   (n_exact, n_public, n_dp)
                                        ├── daily_reach        (n_cohorts_*, including n_cohorts_dp)
                                        └── daily_status       (n_exact, n_public, n_dp)

nginx tier-1 log (IP + UA, 72h retention)
        │
        └──▶ derive_cohorts_from_tier1.py  ──▶  analytics.db
                                                 └── daily_reach (HLL upsert for all clients)

analytics.db (daily_*)
        │
        └──▶ weekly_rollup.py  ──▶  analytics.db
                                     ├── weekly_tool_calls   (k-anon at weekly level)
                                     ├── weekly_reach        (max-of-week HLL, k-anon gated)
                                     └── weekly_status
```

## Public surface — what's safe to report

For paper / dashboard / external claims:

- **Weekly** `n_cohorts_public` (k-anon-gated, DP-noised) — the right
  signal for "active cohorts in week W".
- **Weekly** `n_cohorts_dp` (always emitted, DP-noised) — safe to publish
  as a noisy lower bound; meaningful even when k-anon would suppress.
- Per-client breakdowns at weekly granularity now clear the k=10 floor
  for every active client class.

What stays internal:

- `n_exact` on any table.
- HLL register state (discarded after estimate).
- Tier-1 raw log lines (purged after 72h by logrotate).

## Operations

- The default nightly rollup (`opencaselaw-analytics.timer`, 04:30 UTC)
  runs `rollup_analytics.py`. After this PR lands, that schedule should
  *also* trigger `derive_cohorts_from_tier1.py` (last 3 days, matching
  tier-1 retention) and `weekly_rollup.py` (last 12 weeks). Suggested
  systemd-unit ordering:

  ```
  04:30  rollup_analytics.py          (writes daily_* for yesterday)
  04:35  derive_cohorts_from_tier1.py (upserts daily_reach HLL)
  04:40  weekly_rollup.py             (aggregates daily_* → weekly_*)
  ```

  Adding the latter two as `ExecStartPost` in the existing analytics
  unit is the smallest deploy.

- All three scripts are idempotent. Re-running on the same window
  rewrites the same rows.

- DP-noise seed is derived from the day under aggregation, so re-runs
  produce stable output (no random drift between runs of the same date).
