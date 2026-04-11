# ops/ — operational files for the VPS

Deployable configuration and deployment notes for the OpenCaseLaw
production server (`caselaw-mcp`, Hetzner `ccx43`, IP 46.225.212.40).

## Contents

```
ops/
├── README.md            ← this file
├── nginx/
│   └── ocl-logging.conf ← privacy-respecting two-tier log formats
└── logrotate/
    ├── ocl-tier1        ← 72h shred for tier1.log (IPs + UAs)
    └── ocl-tier2        ← 14d retention for tier2.log (class-level)
```

The systemd units these files integrate with live in `../systemd/`.
The analytics rollup that consumes the Tier-2 log lives in
`../scripts/rollup_analytics.py`.

---

## Privacy-respecting analytics — deployment

This document describes how to roll out the three-tier logging
architecture described in `docs/datenschutz/`. All changes are
additive and can be deployed without touching the MCP workers.

### Prerequisites

- Root SSH: `ssh -i ~/.ssh/caselaw root@46.225.212.40`
- nginx already installed and running (it serves mcp.opencaselaw.ch).
- Python 3 available at `/usr/bin/python3` (already used by other services).
- Logs directory writable: `/var/log/nginx`.

### Step 1 — Install the nginx logging config

```bash
# from your local repo
scp -i ~/.ssh/caselaw ops/nginx/ocl-logging.conf \
    root@46.225.212.40:/etc/nginx/conf.d/ocl-logging.conf

# On the VPS:
ssh -i ~/.ssh/caselaw root@46.225.212.40 bash -s <<'SH'
set -euo pipefail
# Sanity-check the config
nginx -t
# Edit the server block(s) for mcp.opencaselaw.ch to add the access_log
# directives. Add INSIDE each server { ... } block:
#
#   access_log /var/log/nginx/tier1.log tier1;
#   access_log /var/log/nginx/tier2.log tier2;
#
# Keep the existing `access_log` line too (or remove it if it's now
# redundant). Then reload:
nginx -t && systemctl reload nginx
# Verify both logs receive data
sleep 5
ls -l /var/log/nginx/tier1.log /var/log/nginx/tier2.log
tail -1 /var/log/nginx/tier2.log
SH
```

Expected `tier2.log` line format:

```
2026-04-11T14:23:01+00:00 cursor rest_search_decisions GET 200 0.142 3821 -
```

— no IP, no UA, no query string, no referer. If you see any of those,
the `log_format tier2` block was not picked up; re-check that
`ops/nginx/ocl-logging.conf` is actually included before the `server { }`
blocks that use it.

### Step 2 — Install the logrotate configs

```bash
scp -i ~/.ssh/caselaw \
    ops/logrotate/ocl-tier1 ops/logrotate/ocl-tier2 \
    root@46.225.212.40:/etc/logrotate.d/

ssh -i ~/.ssh/caselaw root@46.225.212.40 bash -s <<'SH'
set -euo pipefail
chmod 644 /etc/logrotate.d/ocl-tier1 /etc/logrotate.d/ocl-tier2
# Dry run (no changes)
logrotate -d /etc/logrotate.d/ocl-tier1
logrotate -d /etc/logrotate.d/ocl-tier2
# Confirm the shred command exists
which shred
SH
```

The first real rotation happens automatically on the next hourly /
daily tick. You don't need to run logrotate manually.

### Step 3 — Install and enable the analytics rollup timer

```bash
# Push the rollup script and systemd units
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
    'cd /opt/caselaw/repo && git pull --rebase origin main'

ssh -i ~/.ssh/caselaw root@46.225.212.40 bash -s <<'SH'
set -euo pipefail
cp /opt/caselaw/repo/systemd/opencaselaw-analytics.service /etc/systemd/system/
cp /opt/caselaw/repo/systemd/opencaselaw-analytics.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now opencaselaw-analytics.timer
systemctl list-timers opencaselaw-analytics.timer
SH
```

### Step 4 — Verify the first rollup

After a day has passed (or you can force an immediate run against
yesterday's log, once any Tier-2 data exists):

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 bash -s <<'SH'
set -euo pipefail
systemctl start opencaselaw-analytics.service
sleep 5
tail -30 /opt/caselaw/repo/logs/analytics_rollup.log
# Inspect the analytics database
sqlite3 /opt/caselaw/repo/output/analytics.db <<'SQL'
.headers on
.mode column
SELECT day, client_class, endpoint_class, n_exact, n_public, p50_ms, p95_ms
  FROM daily_tool_calls
 ORDER BY n_exact DESC
 LIMIT 10;
SELECT day, client_class, n_cohorts_hll_estimate, n_cohorts_public FROM daily_reach;
SELECT * FROM run_metadata;
SQL
SH
```

You should see aggregates in `daily_tool_calls`, install-cohort
estimates for `word_addin` in `daily_reach`, and a row in
`run_metadata` with `k_anon=10` and `dp_epsilon=1.0`.

### Step 5 — Publish traffic stats on the site (optional)

After the first rollup succeeds, the next `publish.py` run will pick
up the new `traffic` block automatically: `generate_stats.py` reads
`output/analytics.db` via `collect_traffic()` and embeds the
DP-noised, k-anon aggregates into `docs/stats.json`. No additional
steps required.

To force an immediate stats regeneration:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 bash -s <<'SH'
cd /opt/caselaw/repo
/usr/bin/python3 generate_stats.py
grep -c traffic docs/stats.json
SH
```

---

## Privacy contract — what this deployment guarantees

The contract this deployment enforces (documented publicly in
`/datenschutz/`):

1. **Tier 1 logs** contain IPs + UAs, retained for strictly 72 hours,
   then shredded (`shred -u`). Used only for abuse response.
2. **Tier 2 logs** contain only class labels (client class, endpoint
   class, status, response time, size, optional 8-hex install cohort).
   Retained for 14 days, then deleted.
3. **Tier 3** (`analytics.db`) contains daily aggregates. Cells with
   n < 10 are suppressed in the public column (`n_public = NULL`).
   Published counts carry Laplace noise at ε=1.0 for formal (ε, 0)-DP.
4. **No cookies.** No client-side storage on the public site.
5. **Word add-in install cohort** is `SHA-256(install_id + YYYY-MM)[:8]`,
   regenerated every month, cannot be correlated across months.
6. **No query logging.** Search query content is never stored in any tier.

Every item above is enforced either by nginx (tiers 1-2), the rollup
script (tier 3), or by not implementing the capability at all (cookies,
query logging, fingerprinting).

## Rollback

If anything goes wrong, remove the include directive or revert:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 bash -s <<'SH'
# Disable the new logs
systemctl disable --now opencaselaw-analytics.timer
rm -f /etc/systemd/system/opencaselaw-analytics.service \
      /etc/systemd/system/opencaselaw-analytics.timer
rm -f /etc/nginx/conf.d/ocl-logging.conf
rm -f /etc/logrotate.d/ocl-tier1 /etc/logrotate.d/ocl-tier2
# Remove access_log tier1/tier2 lines from the server block, then:
nginx -t && systemctl reload nginx
systemctl daemon-reload
SH
```

The existing access log and MCP workers are untouched by any of these
changes — this is purely additive.
