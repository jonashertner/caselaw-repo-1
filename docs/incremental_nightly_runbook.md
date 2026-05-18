# Incremental nightly runbook (Workstream A v0.1)

## Goal

Replace the 7h+ full-rebuild nightly with a ~10-25 min incremental run
for weekdays, keeping a weekly Sunday full rebuild as safety net.

**Projected impact**: nightly 7h 22min → ~25 min on Mon-Sat (one Sunday
full = 7h). Net: ~51h/week → ~6.5h/week of pipeline time. New decisions
searchable within ~15 min of scrape rather than next morning.

## What this directory adds

| File | Role |
|---|---|
| `scripts/incremental_nightly.py` | Orchestrator. Chains `quick_publish` → `build_reference_graph_incremental` → `extract_decision_structure_incremental` → `generate_stats`. Writes a JSONL summary per run. |
| `systemd/opencaselaw-publish-incremental.service` | systemd unit invoking the orchestrator. **Default mode is `--shadow`** — incremental builders write to sibling .db files and live data is untouched. |
| `systemd/opencaselaw-publish-incremental.timer` | Mon-Sat 03:30 UTC. Sunday remains on legacy `opencaselaw-publish.timer` for the weekly full rebuild. |
| `logs/incremental_nightly.jsonl` | Auto-appended summary of every run (success or failure). Feeds drift validation. |

## Prerequisites that are already met (verified 2026-05-18)

- ✅ D3 cache-invalidation contract (PRAGMA user_version) production-proven
  this morning at 10:52 UTC (Monday gate, commit `1ea5645`).
- ✅ `quick_publish.py` deployed, live, bumps `user_version` on swap.
- ✅ `build_reference_graph_incremental.py` deployed (May 8).
- ✅ `extract_decision_structure_incremental.py` deployed (May 7).

## Shadow → in-place cutover (1-week validation)

### Phase 1 — Enable in shadow mode (low risk)

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40
cd /opt/caselaw/repo && git pull
sudo cp systemd/opencaselaw-publish-incremental.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now opencaselaw-publish-incremental.timer
systemctl list-timers opencaselaw-publish-incremental --no-pager
```

Mon-Sat 03:30 UTC the orchestrator fires. Both pipelines run in
parallel — the legacy `opencaselaw-publish.timer` keeps doing its
full rebuild every night, and the incremental orchestrator does its
shadow run alongside. **Live data is served from the full rebuild's
output as today.**

### Phase 2 — Drift validation (during the shadow week)

After each shadow run, the orchestrator writes a summary to
`logs/incremental_nightly.jsonl`. Compare against expected duration
and exit code. After Sunday's full rebuild, compare the freshly-built
live DBs against the prior Saturday-night incremental sibling DBs:

```bash
# Quick check
tail -7 /opt/caselaw/repo/logs/incremental_nightly.jsonl | jq .

# Row count drift (sibling vs live)
python3 -c "
import sqlite3
live = sqlite3.connect('file:/opt/caselaw/repo/output/reference_graph.db?immutable=1', uri=True)
sib  = sqlite3.connect('file:/opt/caselaw/repo/output/reference_graph_incremental.db?immutable=1', uri=True)
print('live edges:', live.execute('SELECT count(*) FROM citation_targets').fetchone()[0])
print('sib  edges:', sib.execute('SELECT count(*) FROM citation_targets').fetchone()[0])
"
```

Acceptance: row count delta < 0.5 %, top-30 cited decisions identical
in both DBs, no extractor-version mismatch warnings in the logs.

### Phase 3 — Cutover

After 7 consecutive green nights:

```bash
# Edit the service file to add --in-place
sudo systemctl edit --full opencaselaw-publish-incremental.service
# Change ExecStart line to:
#   ExecStart=/usr/bin/python3 /opt/caselaw/repo/scripts/incremental_nightly.py --in-place
sudo systemctl daemon-reload

# Move the legacy publish to Sunday-only
sudo systemctl edit opencaselaw-publish.timer
# Change OnCalendar to: Sun *-*-* 03:30:00 UTC
sudo systemctl daemon-reload
sudo systemctl restart opencaselaw-publish.timer
```

After cutover: Mon-Sat incremental updates live DBs in ~25 min;
Sunday full rebuild remains the safety-net (rebuilds wayback_queue,
FTS5 optimize, full parquet export).

## Rollback

```bash
sudo systemctl disable --now opencaselaw-publish-incremental.timer
sudo systemctl restart opencaselaw-publish.timer
```

The legacy full-rebuild path is untouched throughout Phase 1 and
Phase 2 — rollback is just disabling the new timer. Phase 3 cutover
requires reverting both edits + a successful manual full rebuild.

## Open follow-ups

- `scripts/publish_drift_check.py` — automate the row-count + top-30
  diff so it runs daily and writes pass/fail to alerts.
- Bypass `wayback_queue` provisioning in the Sunday full rebuild
  (1h 21min saved) — needs a separate enqueue path so the archiver
  picks up new rows. Currently the only enqueue path is publish.py.
- Skip FTS5 optimize on incremental days — already implicit since
  incremental never runs build_fts5. Worth confirming the FTS5 index
  doesn't degrade significantly between weekly optimizes.
- Enable `OCL_USE_DAG=1` parallel scheduling for the Sunday full
  rebuild (commits `d779404`+`c6572ee` shipped phase B v0.2/v0.3) —
  could cut 2-3h from the Sunday slot by parallelising Steps 2c+2d+2e.
