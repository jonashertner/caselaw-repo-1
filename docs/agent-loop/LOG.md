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

## 2026-06-07 23:05 UTC

Situation report:
- User approved committing and deploying the `check_scraper_freshness.py` false-positive fix and test.

Action:
- Committed `scripts/check_scraper_freshness.py`, `tests/test_check_scraper_freshness.py`, and the initial agent-loop records as `6d59117 fix: prevent false scraper freshness alerts`.
- Pushed `6d59117` to `origin/main`.
- Fast-forwarded the production checkout at `/opt/caselaw/repo` from `8adeb63` to `6d59117`.
- Did not restart MCP workers; the change affects only the scheduled/read-only freshness checker script.

Evidence:
- Pre-commit gates passed: `make verify-offline` and `make test` (`617 passed, 19 skipped in 37.46s`).
- Prod deploy verification: `git rev-parse --short HEAD` returned `6d59117`.
- Prod checker self-test passed: `2217` snapshot rows in `/opt/caselaw/repo/state/coverage.db`; `get_last_scraped('bs_gerichte')=2026-06-07`.
- Prod no-notify checker run returned `All checks passed at 2026-06-07 23:05 UTC`, confirming the previous 33-alert false-positive set is gone without sending ntfy.
- Public health remained OK: `curl -s https://mcp.opencaselaw.ch/health` returned `{"status":"ok","decisions":974834,"db_generation":1780826232}`.

Outcome:
- Freshness monitor false-positive fix is committed, pushed, and deployed.
- Production alert path is now quieter for zero-new successful scrapes while preserving real failure and stale-health checks.

## 2026-06-08 04:16 UTC

Situation report:
- Implemented the first Codex maintenance-automation layer after reading CLAUDE.md, memory notes, TECHNICAL_OVERVIEW.txt, docs/maintenance-loop-prompt.md, docs/agent-loop/LOG.md, and current Codex manual sections for AGENTS.md, skills, hooks, and codex exec.

Action:
- Added AGENTS.md, a repo-scoped opencaselaw-maintenance skill, autonomy policy, assessment and decision schemas, a conservative Codex hook, agent_assess.py, agent_safe_deploy.py, agent_record.py, codex_maintenance_loop.sh, and offline tests.

Evidence:
- pytest tests/test_agent_automation.py -q: 8 passed. make verify-offline: passed. make test: 625 passed, 19 skipped in 37.42s. git diff --check: clean. Public agent_assess probe: health ok with 974834 decisions; quality 52/52, critical_failures=0, warning_failures=0, publish_safe=true. agent_safe_deploy classifies this control-plane change as proposal-only for future autonomous loops.

Outcome:
- Automation foundation is local and verified. It does not commit, push, deploy, write state/, or alter pipeline-gated files. Next owner decision remains the offsite backup destination and secret handling proposal.

## 2026-06-08 08:37 UTC

Situation report:
- User requested a deep real-life multilingual search probe across German, French, Italian, Romansh, and English on the public REST API.

Action:
- Ran 10 read-only production /api/decisions searches with compact results, then 3 diagnostic anchor searches, retried the timed-out English equality query once, and fetched 4 representative top decisions with full_text=false.

Evidence:
- All 10 initial searches returned HTTP 200 and valid JSON. DE/FR/IT language filters returned 5/5 results in the requested language. Romansh language-filtered searches returned 0 results, consistent with no RM decision corpus. English Art. 221 CPP query returned 109 total results; English tenancy queries returned hits but looked semantically weak. Diagnostic Art. 8 BV equality discrimination timed out twice: 30s then 45s with 0 bytes. Representative detail fetches for bge_BGE_118_II_50, bge_BGE_142_III_336, bger_4A_143_2008, and bge_BGE_145_IV_503 all returned HTTP 200 with citation strings and canonical URLs in 0.12-0.16s.

Outcome:
- Core multilingual decision search is live for DE/FR/IT and search-to-detail works. Gaps surfaced: no Romansh corpus hits; unanchored or partly English lexical search can return weak ECHR/French hits; one English equality/statute query has a reproducible >45s timeout and should be investigated as a search-latency/relevance issue.

## 2026-06-08 10:30 UTC

Situation report:
- User requested more natural-language real-life search probes emulating lay and expert users.

Action:
- Ran 14 bounded read-only production /api/decisions searches with natural lay/expert phrasing across DE, FR, IT, RM, and EN using fields=compact&limit=5.

Evidence:
- All 14 requests returned HTTP 200 and valid JSON. DE/FR/IT/RM language filters respected requested language; RM returned zero hits. Expert anchored queries generally produced plausible top legal hits, e.g. IT Art. 221 CPP returned BGer/BGE detention cases; FR family relocation returned BGer 5A family-law cases. Latency remained high: many compact searches took 10-24s; RM expert zero-hit took 35.9s; EN lay asylum took 36.1s; EN expert collusion took 44.3s. German lay tenancy/family queries returned weak top hits including old ch_vb rows and malformed docket HTML such as <td class="metadataCell">20017243</td>.

Outcome:
- Natural-language search is structurally live but not world-class for lay users yet. Main issues surfaced: high latency on long natural-language queries, weak ranking for unanchored lay German/English phrasing, no RM fallback when language=rm is applied, and ch_vb metadata cleanliness affecting surfaced citations.

## 2026-06-09 08:11 UTC

Situation report:
- User requested an investigation of publish workflow duration.
- Performed read-only prod probes of `opencaselaw-publish.service`, `logs/publish.log`, `state/last_publish_success.json`, public `/health`, and the Wayback timer/service.

Action:
- Parsed `/opt/caselaw/repo/logs/publish.log` locally from a copied read-only `/tmp/opencaselaw-publish.log`.
- Compared recent top-level publish durations, Step 2 FTS5 durations, post-build parallel batch durations, and `build_fts5` subphase timings.

Evidence:
- Latest completed green publishes: 2026-06-07 took `43227.3s` / `720.5 min` and 2026-06-08 took `54941.4s` / `915.7 min`.
- June 1-8 median full-publish wall time was `13.6h`; median Step 2 FTS5 time was `8.3h`; median post-build parallel batch long pole was `2.34h`; median reference graph was `2.32h`; median decision structure was `1.83h`.
- Active 2026-06-09 publish started `03:30:45 UTC`; at `08:11 UTC` it was still in Step 2, specifically silent after `Phase: provision wayback_queue...` / `wayback_queue: full backfill (marker NULL)`, with `build_fts5.py` alive, ~35 GB RSS, and heavy data-volume reads.
- `build_fts5.py:_ensure_wayback_queue()` is optimized for an existing table marker, but a full rebuild creates a fresh temporary DB, so `MAX(queued_at)` is NULL each night and the queue is fully rebuilt. Recent `provision wayback_queue` durations were 63, 64, 83 minutes, with prior peaks up to 2h25m.
- Live immutable DB read showed `wayback_queue` had `1,465,369` rows, `1,448,938` pending, and only `71` archived with status 200. Hourly `opencaselaw-wayback.service` drains only hundreds to ~1000 rows per run and sometimes times out.

Outcome:
- Current publish duration is long but consistent with the recent full-rebuild baseline; no production action was taken.
- Highest-leverage duration issue found: `wayback_queue` is being rebuilt and effectively reset every full publish despite an intended incremental marker. A safe fix would require a pipeline-gated proposal: move/preserve Wayback queue state outside the fresh `decisions.db.tmp` rebuild path, or import prior queue state into the temp DB before `_ensure_wayback_queue()`.

## 2026-06-21 18:28 UTC

Situation report:
- Heartbeat: no blocking risks; public health ok (991,677), QC publish_safe (52/52, 0 critical), last publish 2026-06-21 11:01 UTC. Standing recommendation = the open offsite-backup proposal (never_autonomous).

Action:
- Read-only liveness re-probe of KNOWN_DEAD_SOURCES court be_steuerrekurs (Steuerrekurskommission BE, dead since Feb 2026).

Evidence:
- Portal UP (302 -> /tribunapublikation/); old GWT module tribunapublikation.nocache.js = 404; index now loads tribunavtplus/tribunavtplus.nocache.js. Platform upgraded to TribunaVTPlus -- same as ju_gerichte's .../tribunavtplus/loadTable (base_tribuna). Not dead -- migrated.

Outcome:
- Opened proposal docs/agent-loop/proposals/2026-06-21-be-steuerrekurs-tribunavtplus-migration.md (scrapers/ is proposal_only). No code changed. Recovery path = reuse base_tribuna TribunaVTPlus. Also surfaced: 2026-06-07 offsite-backup proposal still open.

## 2026-06-21 19:17 UTC

Situation report:
- Executed the user-approved recovery of be_steuerrekurs (heartbeat had flagged it as a TribunaVTPlus migration, recoverable).

Action:
- Verified the live portal end-to-end via the real scraper session + search; ran a control probe against the working be_verwaltungsgericht portal with identical code.

Evidence:
- Session/permutation(03791D5E)/config(cred 128)/protocol all OK. Search returns //OK[0] (valid empty PagingResultSet) for EVERY filter (STRK/STR/SK/RK/empty) -- not //EX. Control be_verwaltungsgericht = //OK[11420] with real dockets/dates -> probe method sound. Portal UP but source DB empty. 343 historical be_steuerrekurs (2013-03-09..2025-12-16) already in corpus (frozen es) -- nothing lost.

Outcome:
- NO code change -- not a scraper bug, the source genuinely returns 0. Corrected the heartbeat's misdiagnosis (scraper already targets tribunavtplus via base_tribuna default). Keep in KNOWN_DEAD_SOURCES; the periodic re-probe will catch a future DB reconnection (scraper will then just work). Proposal file updated to RESOLVED.

## 2026-08-13 14:30 UTC

Situation report:
- P2 finalization run (approved plan): regenerate dataset, statistics, full paper rewrite to arXiv-ready.

Action:
- Extended scripts/p2_backscan.py (primary universe = prefixed only; per-decision cluster pairs; series-index dump; deterministic mechanism labels; quote-marker flag; pre-1955 pool with +100 pre-screen; distinct-token registry). One read-only pass on the VPS against the consistent 2026-08-12 build pair.
- New tables/build_tables.py: Wilson + decision-cluster bootstrap (multinomial over 1,463 pair types, 10k draws, seed fixed), three rate views, per-language/mechanism/sensitivity/comparability tables, TikZ decidability figure from the released index, data/MANIFEST.json (SHA-256).
- scripts/p2_probe.py: external re-probe of all 292 distinct tokens against relevancy.bger.ch (semantics verified: start=200, interior=redirect-to-containing, nonexistent=404; coverage floor vol 80) + positive existence check of unique repair candidates. DFR found to be selective -> removed as negative-proof source.
- Harvested the seven court-reply threads (mail refs 1477..1649): SVG BS corrected; APG BS correcting; BVGer ATAFDoc committed; BStGer/OGer/HGer/VGr ZH acknowledged; BGer no reply. Voice single-reader pass over all 25 quote-marked findings: 23 court, 2 party (incl. BEZ.2024.26 where the court itself states the correction 149 III 210 = our deterministic repair).

Evidence:
- 591/899,560 prefixed = 657.0 ppm (Wilson 606-712; cluster 605-710); DE 594/FR 701/IT 660 ppm; 0.68% of reporter-citing decisions; mechanisms: division_subst 272, volume_subst 103, page-digit 107, year-for-page 67, dropped-leading 30, unlabelled 12; pre-1955 pool 1,728 with only 278 +100-plausible / 21 exact starts.

Outcome:
- paper.tex fully rewritten (zero \pending), builds 8 pp clean; probe running to completion; remediation facts and 2 voice labels flagged for author confirmation. Nothing committed, nothing submitted (approval gates standing).

## 2026-08-13 22:30 UTC

Situation report:
- GPT-5.6-Sol xhigh adversarial review of P2 returned 21 findings (8 blocking). Independent verification confirmed the data-layer claims; repair wave executed same-day.

Action:
- Verified: FR/IT context attach searched normalized "BGE" in texts that write ATF/DTF (355/356 FR + 25/25 IT contexts missing -> voice screen covered DE only); LEFT JOIN citation_targets multiplied resolved edges (366,400 pairs carry 2 target rows - the BGE/BGer dual-identity twins); series index not deduped (50,467 -> 35,420 unique starts); Ia/Ib forms outside extraction grammar (sized: 10,723 occ / 1.36% of surface).
- Fixed scan (DISTINCT+EXISTS edge unit, prefix-alternation contexts, deduped index); re-froze against the 2026-08-13 build pair with SHA-256 of both DBs at scan time; re-screened quotes over all languages (25 -> 49; voice read: 46 court / 3 party); rewrote paper (unit relabel, provability tiering via resolver-as-endpoint 542/49, Dahl 9-of-14 correction, fed-cant difference stated p=0.04, side-by-side units instead of ratio, pre-1955 risk pool 278 stated, author-reported remediation); reconcile in docs/paper/p2-citations/REVIEW_GPT56SOL.md (21/21 dispositioned).

Evidence:
- HEADLINE CORRECTED: 591/584,524 distinct edges = 1,011 ppm (was 591/899,560 = 657 ppm; old denominator 35% join-inflated, bias DOWNWARD as the review predicted). Wilson 933-1,096; decision bootstrap 931-1,093. Findings set unchanged (same 292 tokens -> probe remains valid: 254/254 in-coverage 404).

Outcome:
- Paper rebuilt clean, zero pendings; real determinism check (file diff) passed; tests 1666 green. Gates for the user: remediation facts, 3 voice labels, commit+release, arXiv approval, Ia/Ib grammar rebuild (pipeline-gated).
