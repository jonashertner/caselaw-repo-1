# Deep review backlog — 2026-07-01

Source: 6-dimension read-only audit (completeness, accuracy/QC, reliability, serving, security, improvements), run wf_072418a4. Full evidence in the session transcript; this file is the durable ranked tracker. Autonomy tags: loop-safe = can run without approval (read-only / docs); plan-mode = gated (pipeline, systemd, deploys, commits, secrets).

## Status one-liner

Surface green (995,076 decisions, QC 52/52, publish OK, materialien bridge wired). Four structural risks underneath: (1) BGer/BGE/JU discovery single-pathed through the Mac tunnel, all three nightly sweeps failed 2026-07-01, bge has no successful snapshot since 06-25, poller docket 5A_402/2026 stuck 7 polls; (2) monitoring partially dark: health_alerts.py computes alerts nobody receives (since 05-17), QC WARNING tier (7 modules) never executed in production (since 05-02), all paging on one unauthenticated free ntfy topic; (3) ~13.6h nightly rebuild is the daily critical path, cutover 0/27 drift-green, last root cause (ge OCR) landed 07-01, verify 07-02; (4) state drift: tested #31/#32 fixes uncommitted, 10 systemd units VPS-only, reprobe-dead timer committed-not-deployed, HF_TOKEN plaintext in root crontab since before 06-07, ~67GB no offsite backup.

## Ranked backlog

| # | Item | Dim | Autonomy | Effort |
|---|------|-----|----------|--------|
| 1 | Deploy the committed dead-source re-probe timer (f9a0902; systemctl not-found on VPS; OW portal now serves a live Vaadin app) | COMPL | plan-mode | S |
| 2 | BGer/BGE nightly backstop: add bger,bge to the 10:00 late-scrapers retry + TUNNEL_DEPENDENT_SOURCES in check_scraper_freshness; scope residential proxy to kill the Mac SPOF | COMPL | plan-mode | S |
| 3 | Verify 07-02 build absorbed ge OCR (clean full_text, drift LOST shrinks toward 0.05%); record go/no-go in LOG.md | IMPR | loop-safe | S |
| 4 | Wire the dark health_alerts engine to ntfy, after making quick_publish_stale poller-schedule-aware (would false-page every evening as-is) | RELI | plan-mode | S |
| 5 | Commit + deploy tested #31/#32 (search_laws AND-first, get_law lang fallback); reconcile local/origin/VPS three-way drift; group the 45 dirty paths into reviewable commits | SERV | plan-mode | M |
| 6 | Incremental Phase-3 cutover once 7 drift-green nights accumulate post-OCR (nightly 13.6h -> ~25min) | RELI | plan-mode | M |
| 7 | Rotate HF_TOKEN (fine-grained, dataset-scoped) out of root crontab; developer-performed | SEC | plan-mode | M |
| 8 | Keep failed-WARNING results in QC gate runs (quality/runner.py:162 one-liner + test) | QC | plan-mode | S |
| 9 | Revive QC WARNING tier: weekly full-suite timer + report; harden mcp_tools/exports import-failure silent-skip to QUARANTINE | QC | plan-mode | M |
| 10 | Materialien Tier-1.5: dead ORDER-BY in get_article_purpose (relation values never match), quotable:false machine-readable, unify search_botschaft keys, stub/format flags | SERV | plan-mode | S |
| 11 | BL Kantonsgericht outreach follow-up (Berndt, silent since 04-17; nudge was due after ~10 days); e-helvetica fallback if 2 more weeks silent (~4,997) | COMPL | plan-mode | S |
| 12 | Copy the 10 VPS-only systemd units into repo systemd/ (DR reproducibility) | RELI | plan-mode | S |
| 13 | Docs sweep batch: alerting topology, QC scope table (corrects "61 checks"), serving-drift ledger, completeness gap table, ops_schedule refresh | RELI | loop-safe | S |
| 14 | Harden alerting: authenticated/reserved ntfy topics + one independent second channel; centralize 6 send sites into one helper | SEC | plan-mode | M |
| 15 | Offsite backup ~67GB (rclone -> Hetzner Storage Box, weekly + tested restore); blocked on owner destination decision | RELI | plan-mode | M |
| 16 | One-time gitleaks full-history scan on the public repo; record outcome | SEC | loop-safe | S |
| 17 | Secrets inventory + rotation runbook (docs/security/secrets-inventory.md; names/locations only) | SEC | loop-safe | S |
| 18 | Bound _STRUCTURED_PARSE_CACHE (LRU ~2k + 1h TTL) + privacy-safe hit-rate counter | SERV | plan-mode | S |
| 19 | Gate-safe CRITICAL NUL/control-char regression check on recent rows (guards d0072b2) | QC | plan-mode | M |
| 20 | pip-audit (non-blocking) + constraints.txt VPS snapshot in CI; npm audit; Dependabot | SEC | plan-mode | S |
| 21 | Poller stuck-docket escalation: after N failed polls, AZA docket-search -> date -> relevancy fetch chain (5A_402/2026 at 7) | COMPL | plan-mode | S |
| 22 | Cantonal recoverables sweep: be_vg backfill 2,159 (pending since 06-19), bs_gerichte ~351 (fresh Omnis session), ti ~106, vs 393 root-cause, sz/gr/fr tails | COMPL | plan-mode | M |
| 23 | Search bench 100 -> 500-1000 stratified queries; re-baseline MRR (stale since Mar 19). Gates all rerank/cache tuning incl. the always-firing LLM-rerank gate fix | IMPR | plan-mode | M |
| 24 | Per-tool latency + error instrumentation on dispatch, exported at /metrics | SERV | plan-mode | M |
| 25 | Gated citation-graph re-derivation: H-4 twin-edge dedup, H-2 search twins, bake corrected dates (retire _override_citation_dates stopgap) | QC | plan-mode | M |
| 26 | R1/R2 data-integrity QC: real cite() exercised per court class; sampled sidecar-vs-full_text verbatim consistency | QC | plan-mode | M |
| 27 | Least-privilege scraper/pipeline units (non-root user + hardening, pilot on output-freshness) | SEC | plan-mode | M |
| 28 | be_bvd disposition: enumerate DLAConfig filter codes or reclassify dead; land/discard working-tree scraper | COMPL | plan-mode | S |
| 29 | Word add-in viability analysis (71 calls/30d vs Stripe): invest vs sunset | SERV | loop-safe | S |
| 30 | Pre-2007 EVG recovery (~15,215): 100-docket recoverability probe first | COMPL | plan-mode | L |
| 31 | Treatment graph spec + deterministic marker prototype (Tier 1 #2) | IMPR | plan-mode | L |
| 32 | Pre-2000 Botschaften OCR track for OR/ZGB/StGB (materialien Tier 2) | IMPR | plan-mode | L |

## LegalStats wishlist adoption (2026-07-02)

Source: /Users/jonashertner/legalstats/docs/OPENCASELAW-WISHLIST.md (downstream consumer, empirically grounded; claims verified against the served DB 2026-07-02: chamber 37.8%, decision_type 13.6%, BGE linkage 2025=8/183 2026=0/10, GR impossible dates confirmed). Sequenced AFTER the incremental cutover.

| # | Item | Autonomy | Effort |
|---|------|----------|--------|
| L1 | DEFECT: BGE<->BGer docket_number_2 linkage collapsed for 2025+ (8/183, 0/10; ES-retirement casualty: direct BGE scraper lacks the back-link). Degrades find_leading_cases/precedence, not just exports | plan-mode | M |
| L2 | DEFECT: gr_gerichte SR2/SBK impossible dates (2025 dockets dated 2012-2019); add QC QUARANTINE rule date in [docket_year-1, docket_year+3] | plan-mode | S |
| L3 | Quick-wins batch (build-side, no scraper edits): branch (zivil/straf/oeffentlich/sozialversicherung) derived from court+chamber; chamber <- parsed docket code where empty; delta-parquet schema parity + has_full_text; typed/validated decision_date; resolved citation edges into parquet (8.65M exist); coverage(court,year,scraped,portal_total) table export from scraper-health data | plan-mode | S each |
| L4 | proceeding_type + procedural_code via versioned build-side dictionary (data/proceeding_codes.yaml), NOT scraper-side: docket codes are already in every row; BGG prefix map first (covers 191k BGer), then ZH/VD/AG/SO vocabularies | plan-mode | M |
| L5 | appealed_docket/date/court derived at build from BGer rubrum parse ("gegen das Urteil des X vom D") + existing citation graph/find_appeal_chain; note appeal_info is an always-null export artifact today (not even a decisions.db column) | plan-mode | M |
| L6 | Section offsets into parquet FROM THE EXISTING decision_structure sidecar (cantonal sections already served since 04-29; export-only gap) | plan-mode | S |
| L7 | Scraper-side opportunistic capture where portals expose it (legal_area verbatim, filing_date, streitwert): fold into regular per-court maintenance, no campaign | plan-mode | ongoing |
| L8 | Export hygiene: outcome column = fixed enum or drop; optional full_text_clean (dehyphenated) as an ADDITIONAL field (raw stays canonical, R2 verbatim depends on it); GE docket-series/foreign-docket noise cleanup | plan-mode | S-M |

## Session addenda (this session's own investigations, same day)

- Search latency root cause (traces, n=5,724): parse+expansion ~2.1s concurrent; REST ~5s dominated by an always-firing Haiku LLM-rerank (confidence gate at 2x-dominance is a de-facto no-op on RRF scores; fired 2,577x vs 2,239 parses today) + CPU cross-encoder tail (max 293s under build load). Rerank is simultaneously the #1 LLM spend ($30.30/wk of $72.52 total). Fix is MRR-gated (item 23).
- get_law 18.6% 5xx root cause: cantonal path fires up to ~7 sequential LexFind calls; mirror fallback only on first-create-empty, not on later timeouts; under build load 47 each call stalls to its 10s ceiling and the stack blows the 120s dispatch cap. LexFind itself is fast (0.12-0.24s probed at load 9). Same shared root as the search tail: the 14h build. Defense: LexFind per-request time budget failing fast to mirror; root: cutover (item 6); structural: mirror-first + mirror completion.
- Docs/stats aligned 07-01 (7b81baf): README 118 courts, ECtHR 2,840 breakdown; canonical numbers verified consistent across MCP desc, README, dataset card, stats.json.
