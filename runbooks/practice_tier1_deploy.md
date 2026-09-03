# Runbook: deploy the Tier 1 practice sources (BSV, SECO AVIG, BAG, SEM Handbuch, BJ SchKG)

Commit `428eb657` (code) + the follow-up commit (systemd unit/timer, this runbook). Plan and rationale:
`docs/plans/2026-09-02-tier1-practice-ingest.md`. Everything below runs on the VPS as root unless marked LOCAL.

## Windows
- Full build: daily 03:30 → ~17:00 UTC. Incremental: Mon–Sat 20:00 UTC (~20 min). Never start step 3+ inside either.
- Evening 1 (weekday, 17:15–19:45 UTC): steps 1–8 — the four small sources go LIVE tonight; BSV crawls overnight
  (network + JSONL writes only; it never opens decisions.db). The shipped description lists BSV as "ingest in progress".
- Evening 2 (a later evening, same window, after BSV finished): steps 9–11.

## Evening 1 — four small sources live, BSV crawl started
1. LOCAL — push (rebase over the bot's stats/feeds commits first):
   ```bash
   git fetch origin && git rebase origin/main && git push origin main
   ```
2. Merge on the VPS (never scp into the tree):
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && git status --porcelain | head && git fetch origin && git merge --ff-only origin/main && git log --oneline -1'
   ```
   If `git status` shows UU paths or leftover stashes, stop and resolve first (see memory: mcp-deploy-path).
3. Mask the practice timer until BSV is indexed, install the new unit + timer, reload:
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'systemctl mask opencaselaw-practice.timer && cp /opt/caselaw/repo/systemd/opencaselaw-practice.service /opt/caselaw/repo/systemd/opencaselaw-practice.timer /etc/systemd/system/ && systemctl daemon-reload && systemctl cat opencaselaw-practice.service | grep -n TimeoutStartSec'
   ```
4. Preconditions (SQLite ≥ 3.25 for the window functions — 3.45.1 verified 2026-09-03; ~5 GB free on / for the BSV JSONL):
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'python3 -c "import sqlite3,fitz; print(sqlite3.sqlite_version)"; df -h / | tail -1; ls -la /opt/caselaw/repo/output/practice.db'
   ```
5. Ingest the four small sources (≈10 min):
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && PYTHONPATH=. python3 -m scrapers.practice.runner --only seco_alv,bag_kvg,sem_handbuch_asyl,bj_schkg 2>&1 | tail -8'
   ```
   Expected: seco_alv +54, bag_kvg +38, sem_handbuch_asyl +92, bj_schkg +174, 0 failed. Stop here if any source failed.
6. Rebuild practice.db (≈1 min without BSV; expect ~150 MB) and check the sources table:
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && PYTHONPATH=. python3 -m search_stack.build_practice_db --jsonl-dir output/practice --db output/practice.db | tail -30 && ls -la output/practice.db'
   ```
7. Rolling restart (health-gated, one worker at a time):
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'bash /opt/caselaw/repo/scripts/rolling_restart_workers.sh'
   ```
   LOCAL smoke: `make smoke`; then via the MCP client: `Arbeitslosenentschädigung Einstellung` (expect AVIG ALE),
   `Existenzminimum Notbedarf` (cantonal Richtlinien), `Dublin-Verfahren` with issuing_authority=SEM (Handbuch C3),
   and `include_superseded=true` on a FINMA query (editions listed).
8. Start the BSV crawl in the background (hours; append-only, resumable by re-running the same command):
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && PYTHONPATH=. nohup python3 -m scrapers.practice.runner --only bsv_weisungen > logs/practice_bsv_first_run.log 2>&1 & echo started'
   ```
   Progress: `tail -3 logs/practice_bsv_first_run.log`, `wc -l output/practice/bsv_weisungen.jsonl`. It also overwrites
   `logs/practice_health.json` with BSV's summary alone (read by scripts/collect_dev_data.py) — accept one distorted rollup.

## Evening 2 — BSV live
9. Confirm BSV finished (`grep "done:" logs/practice_bsv_first_run.log`; re-run the step 8 command if it died — it resumes).
10. LOCAL: promote BSV — move `bsv_weisungen` from EXPERIMENTAL_SCRAPERS to ENABLED_SCRAPERS in `scrapers/practice/runner.py`,
    restore "Covered: BSV (…)" in the search_practice description and the server instructions (the tests key on the runner
    entry, so they flip automatically), refresh the enum counts from `SELECT source, COUNT(*) FROM practice GROUP BY source`
    (budget 1,024 chars: write a five-digit total as "14k+"); `make test`; commit; steps 1–2 again.
11. Rebuild practice.db (step 6; if > 500 MB move it to `/mnt/HC_Volume_*/output/` and symlink — build() resolves symlinks),
    rolling restart (step 7), then unmask the timer:
    ```bash
    ssh -i ~/.ssh/caselaw root@46.225.212.40 'systemctl unmask opencaselaw-practice.timer && systemctl enable --now opencaselaw-practice.timer && systemctl list-timers opencaselaw-practice.timer'
    ```
    Do NOT use `enable --now` on the service itself.

## Lessons from the 2026-09-03 run (read before Evening 2)
- `systemctl mask` refuses when `/etc/systemd/system/opencaselaw-practice.timer` is a real file (it is): use
  `systemctl disable --now opencaselaw-practice.timer` instead, and `enable --now` to bring it back.
- Installing a timer whose OnCalendar moved, then `daemon-reload`, fired the service immediately (Persistent=true catch-up):
  a full ten-source run, its own practice.db rebuild and a rolling restart at 22:40–22:51 UTC, overlapping the manual ingest
  (duplicate JSONL lines, harmless — the upsert collapses them). The repo timer now carries Persistent=false; on Evening 2 copy
  it again before `enable --now`.
- bag.admin.ch serves its `/dam/…pdf` files with HTTP 502 to the Hetzner IP (HTML pages are fine; same files download from the
  dev Mac). BAG is therefore scraped locally and `output/practice/bag_kvg.jsonl` (38 rows) copied to the VPS practice dir —
  legitimate, `output/` is untracked data, not the git tree. Same class as the NE/JU egress constraint.
- `output/practice/` and `practice.db` now live on the data volume behind symlinks (`/mnt/HC_Volume_104655575/output/`);
  practice.db was 203 MB after the four sources. build() resolves the symlink, the unit's `--db` path can stay.
- Process checks over SSH: `pgrep -f "practice.runner"` matches the ssh shell itself; use `pgrep -x python3 -a | grep practice.runner`.
- The MCP code went live at 21:03 UTC (another session's rolling restart after the merge), before practice.db held the new
  sources — for ~1 h 50 min the description promised SECO-ALV/BAG/SEM-Handbuch/BJ with zero rows. Land code and data in one
  window next time, or restart only after the rebuild.

## Rollback
Workers only pick up code on restart, so before step 8 nothing is user-visible. After step 8: `git revert` the two commits,
merge on the VPS, rebuild practice.db (the new JSONL files are harmless to the old code), rolling restart. The old practice.db
is not kept; the rebuild from JSONL takes ~1 min without BSV.
