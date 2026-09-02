# Runbook: deploy the Tier 1 practice sources (BSV, SECO AVIG, BAG, SEM Handbuch, BJ SchKG)

Commit `428eb657` (code) + the follow-up commit (systemd unit/timer, this runbook). Plan and rationale:
`docs/plans/2026-09-02-tier1-practice-ingest.md`. Everything below runs on the VPS as root unless marked LOCAL.

## Windows
- Full build: daily 03:30 → ~17:00 UTC. Incremental: Mon–Sat 20:00 UTC (~20 min). Never start step 3+ inside either.
- Evening 1 (any weekday, 17:15–19:45 UTC or after 20:30 UTC): steps 1–5. BSV crawl runs overnight (network + JSONL writes only;
  it never opens decisions.db).
- Evening 2 (next day, same window): steps 6–9.

## Evening 1
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
4. Preconditions:
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'python3 -c "import sqlite3,fitz; print(sqlite3.sqlite_version)"; df -h / /mnt/HC_Volume_* | tail -3; ls -la /opt/caselaw/repo/output/practice.db'
   ```
   SQLite must be ≥ 3.25 (window functions). Root disk needs ~5 GB free for the BSV JSONL.
5. Ingest the four small sources (≈10 min), then start BSV in the background:
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && PYTHONPATH=. python3 -m scrapers.practice.runner --only seco_alv,bag_kvg,sem_handbuch_asyl,bj_schkg 2>&1 | tail -8'
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && PYTHONPATH=. nohup python3 -m scrapers.practice.runner --only bsv_weisungen > logs/practice_bsv_first_run.log 2>&1 & echo started'
   ```
   Expected: seco_alv +54, bag_kvg +38, sem_handbuch_asyl +92, bj_schkg +174, 0 failed. BSV: hours; check with
   `tail -3 logs/practice_bsv_first_run.log` and `wc -l output/practice/bsv_weisungen.jsonl`.

## Evening 2
6. Confirm BSV finished (`grep "done:" logs/practice_bsv_first_run.log`). If it died, re-run the same command: the JSONL is
   append-only and deduplicated, it resumes where it stopped.
7. Rebuild practice.db and size it:
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && PYTHONPATH=. python3 -m search_stack.build_practice_db --jsonl-dir output/practice --db output/practice.db | tail -25 && ls -la output/practice.db'
   ```
   If > 500 MB: move it to `/mnt/HC_Volume_*/output/practice.db`, symlink from `output/practice.db`, and point the unit's `--db` at
   the real path (build() resolves symlinks, so either path works afterwards).
8. Rolling restart (health-gated, one worker at a time), then smoke:
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'bash /opt/caselaw/repo/scripts/rolling_restart_workers.sh'
   ```
   LOCAL: `make smoke` and `.venv/bin/python scripts/tool_surface_check.py`; then the four probes via the MCP client
   (`Ergänzungsleistungen Vermögensverzicht`, `Arbeitslosenentschädigung Einstellung`, `Existenzminimum Notbedarf`,
   `Dublin-Verfahren` with issuing_authority=SEM).
9. Promote BSV and unmask the timer (one-line change in `scrapers/practice/runner.py`: move `bsv_weisungen` from
   EXPERIMENTAL_SCRAPERS to ENABLED_SCRAPERS; commit, push, merge as in steps 1–2), then:
   ```bash
   ssh -i ~/.ssh/caselaw root@46.225.212.40 'systemctl unmask opencaselaw-practice.timer && systemctl enable --now opencaselaw-practice.timer && systemctl list-timers opencaselaw-practice.timer'
   ```
   Do NOT use `enable --now` on the service itself. Refresh the counts in the `search_practice` enum descriptions from
   `SELECT source, COUNT(*) FROM practice GROUP BY source` (description budget: 1,023/1,024 chars, write a five-digit total as "14k+").

## Rollback
Workers only pick up code on restart, so before step 8 nothing is user-visible. After step 8: `git revert` the two commits,
merge on the VPS, rebuild practice.db (the new JSONL files are harmless to the old code), rolling restart. The old practice.db
is not kept; the rebuild from JSONL takes ~1 min without BSV.
