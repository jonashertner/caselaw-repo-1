# OpenCaseLaw Agent Loop Log

## 2026-06-07 22:40 UTC

Situation report:
- Bootstrap read: `CLAUDE.md`, memory index plus June handover notes, `TECHNICAL_OVERVIEW.txt` relevant sections, and `docs/maintenance-loop-prompt.md`. `docs/agent-loop/LOG.md` was absent before this entry.
- Serving: `curl -s https://mcp.opencaselaw.ch/health` returned `{"status":"ok","decisions":974834,"db_generation":1780826232}`.
- Public QC: `https://opencaselaw.ch/quality.json` had `run_at=2026-06-07T10:44:06+00:00`, `52/52` checks passed, `critical_failures=0`, `warning_failures=0`, `publish_safe=true`.
- Publish: prod `opencaselaw-publish.service` was inactive/dead with `Result=success`, started `2026-06-07 03:30:25 UTC`, exited `2026-06-07 15:30:52 UTC`, `ExecMainStatus=0`. `state/last_publish_success.json` recorded `2026-06-07T15:30:52.428235+00:00`, total `720.5` minutes, no non-fatal failures. `logs/publish.log` summary showed Step 4 HuggingFace OK, Step 5c Quality-Control Gate OK, Step 6 final Git Push OK, and Step 6b Health Check OK.
- Scrapers: prod `logs/scraper_health.json` had `run_at=2026-06-07T02:35:12.594286+00:00`, `60` scrapers, `bad={}`, `silent={}`.
- Freshness monitor: prod `python3 scripts/check_scraper_freshness.py --self-test` passed with `2217` snapshot rows, but the normal run emitted `33` alerts. Investigation found this was monitor noise: `run_scraper.py` writes `source_snapshots` only for changed years or initial backfill, so old `snapshot_date` is not proof that a successful zero-new scrape did not run.
- EVG recovery: prod `opencaselaw-evg-recovery.service` was still `ActiveState=activating`, started `2026-06-07 22:10:54 UTC`; `output/decisions/bger_evg.jsonl` was not yet present in the probe.
- Offsite backup: local search found no backup/restore scripts or existing proposal. Read-only prod unit/timer search found only Ubuntu's `dpkg-db-backup.timer`, no rclone/restic/borg/B2/storage backup unit for OpenCaseLaw data.

Action:
- Added regression coverage in `tests/test_check_scraper_freshness.py`.
- Patched `scripts/check_scraper_freshness.py` so fresh successful `scraper_health.json` entries count as scrape-attempt evidence, while `source_snapshots` remain a fallback/content-change signal.
- Refined the fast zero-new warning so a positive portal count slightly below the corpus count is treated as caught-up, not as a silent skip. This matched the production `ag_gerichte` case (`portal_count=10539`, `our_count=10541`).
- Wrote backup escalation proposal at `docs/agent-loop/proposals/2026-06-07-offsite-backup-tested-restore.md`.

Evidence:
- Before patch, `pytest tests/test_check_scraper_freshness.py` failed on `STALE test_court` despite a fresh successful zero-new health record.
- After patch, `pytest tests/test_check_scraper_freshness.py` passed: `2 passed in 0.10s`.
- Patched checker run against copied prod inputs with `--no-ntfy`: `All checks passed at 2026-06-07 22:36 UTC`.
- Targeted related tests: `pytest tests/test_check_scraper_freshness.py tests/test_health_metrics.py` passed: `27 passed in 0.14s`.
- `make verify-offline` passed all headline checks against committed snapshots.
- `make test` passed: `617 passed, 19 skipped in 38.40s`.

Outcome:
- Local monitor fix is verified but not committed, pushed, or deployed. Production still has the old checker until reviewed/deployed.
- The repaired semantics should prevent the freshness monitor from paging on ordinary no-new scrape runs while preserving failure, stale-health, missing-health, and suspicious fast zero-new signals.
- Human follow-up remains required for `HF_TOKEN`/`NE_PROXY` rotation and for choosing/provisioning an offsite backup destination.

## 2026-06-07 22:58 UTC

Situation report:
- User approved proceeding with the next safe action: verify the one-time pure-official EVG recovery.

Action:
- Performed read-only prod checks of `opencaselaw-evg-recovery.service`, the final shard path, the isolated recovery directory, and `logs/evg_recovery.log`.

Evidence:
- Host time during probe: `Sun Jun 7 22:57:40 UTC 2026`.
- `opencaselaw-evg-recovery.service` was still `ActiveState=activating`, `SubState=start`, started `2026-06-07 22:10:54 UTC`, with main process `/usr/bin/python3 run_scraper.py bger --since 1998-01-01 --until 2007-12-31 --evg-only --output /opt/caselaw/repo/recovery_evg --state /opt/caselaw/repo/recovery_evg/state --no-coverage-snapshot`.
- Final publish shard `output/decisions/bger_evg.jsonl` was not present yet.
- Isolated partial output `recovery_evg/decisions/bger.jsonl` existed and had `665` rows, size `7,669,067` bytes.
- `logs/evg_recovery.log` showed active progress, including `Progress: 600 decisions, 836/hour`, and current official AZA date-window searches through early April 2001.
- Sample recovered rows had single-letter EVG dockets such as `U_49/1998`, `C_161/1998`, `I_350/1999`, and `source_url` values under `http://relevancy.bger.ch/php/aza/http/index.php?...`.

Outcome:
- EVG recovery is not complete yet, but it is progressing in the isolated recovery directory and using official BGer AZA URLs.
- No corpus writes, service restarts, commits, pushes, or deploys were performed.
- Next check should wait until the service exits, then verify `output/decisions/bger_evg.jsonl` line count against the expected approximately `15,215` recovered decisions before the next nightly publish consumes it.
