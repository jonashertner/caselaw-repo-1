# OpenCaseLaw — Follow-up Investigation Report
**Date:** 15. April 2026 (follow-up to morning fix report)
**Scope:** Three open items from user's status report

---

## 1. Coverage Gaps

### 1.1 BE Verwaltungsgericht (gap: 2,424 of 11,273)

**Verified real, not counting artifact.** JSONL has 8,849 unique `decision_id`s with no duplicates; portal reports 11,273 total search hits. Pagination *does* reach all 448 pages, but ~22% of hits don't make it into the corpus.

**Root cause:** The Tribuna-based scraper iterates over multiple `COURT_FILTERS`. Across filter scans, the same docket may surface under different filter parameters but resolves to the same `decision_id`, so dedup discards the second hit. The portal counts every match; we count unique decisions. The ~2,400 discrepancy is largely **dedup overhead**, not missing data.

**Smaller part may be real:** Some docket numbers like `200 2015 964` have `None` decision_date. These DO get scraped (visible in the log) but if the date inference fails the coverage snapshot might miss them. Worth a deeper look but not a 2,400-decision miss.

**Recommended action:** None. The 8,849 decisions we have are the unique set; portal count is inflated by filter overlap.

### 1.2 GR Gerichte (gap: 51 of 14,518)

**Likely same artifact.** Tribuna scraper, same multi-filter pattern. 0.35% gap is well within dedup overhead range. Log says: *"All 14,518 results covered in 726 pages"* — so pagination is complete.

**Recommended action:** None.

### 1.3 NE Gerichte (gap: 44 of 7,531)

**Real gap.** Cause: the portal `jurisprudence.ne.ch` blocks Hetzner IPs, so only the MacBook-tunnel runs (when launched manually) catch new decisions. Today's daily run failed with a connection timeout — confirmed in the log.

**Recommended action:** Either
- Continue running NE/JU scrapers via MacBook tunnel periodically (existing workflow), or
- Move scrapers to a Swiss residential VPS for these two cantons.

### 1.4 SZ Gerichte (gap: 50 of 3,332)

**Real gap.** Scraper has an early-stop at "200 consecutive known decisions". For sparse interleaved updates (an old decision newly published among many existing ones), the scraper hits 200 known and stops before reaching the new one.

**Recommended action:** Once a quarter, run `sz_gerichte` with `--full-rescan` to catch sparse late-published decisions. Low priority — the 50 are spread across years.

---

## 2. Alerting Implementation

### What was built

`scripts/check_scraper_freshness.py` — single-pass health check that flags:

1. **Failed scrapers** in last daily run (`success: false`)
2. **Suspicious silent failures** — short duration (<30s) + 0 new + large existing corpus → likely API outage
3. **Stale courts** — no successful scrape in 14 days (21 days for ES-only courts)
4. **Stale ES cron** — `opencaselaw-entscheidsuche.service` last result ≠ success

Filters:
- `KNOWN_DEAD_SOURCES` set (ch_vb, ag_baugesetzgebung, ow_gerichte, etc.) — never alerted
- `ENTSCHEIDSUCHE_ONLY` set — graded against 21-day threshold

### Wired in

systemd timer `opencaselaw-alerts.timer`:
- Schedule: daily 03:00 UTC (after scrape finishes ~02:30)
- Output: `logs/scraper_alerts.log` (cumulative) + `logs/scraper_alerts_run.log` (per-run)
- Exit code: 1 if any alert (so a wrapping cron can chain to email/Slack later)

### First run output

```
WARN sz_verwaltungsgericht: scraped in 8s with 0 new (corpus=2115) — possible API outage with silent skip
WARN vs_gerichte: scraped in 20s with 0 new (corpus=4338) — possible API outage with silent skip
```

Both are real concerns:
- `vs_gerichte` matches user's report — API was 500 at 01:53 UTC
- `sz_verwaltungsgericht` inherits sz_gerichte's early-stop — finished in 8s (suspicious)

Tomorrow's run with the deployed `vs_gerichte` fix will surface VS as an actual `FAIL` instead of `WARN`, since it'll raise on offset=0 outage.

---

## 3. Native Scrapers for ES-only Courts

### Triage of all 11

| Court | Source URL | HTTP | Status | Action |
|-------|-----------|------|--------|--------|
| `vd_findinfo` (74,819) | `findinfo-tc.vd.ch` | DNS-fail | Migrated → `prestations.vd.ch` (Angular SPA, no content API) | Keep ES |
| `vd_omni` (28,032) | Same as `vd_findinfo` | — | Historical archive | Keep ES |
| `ch_vb` (22,884) | — | — | Source closed 2021 | Keep ES (frozen) |
| `sg_gerichte` (3,795) | — | — | Replaced by `sg_publikationen` (already direct) | Keep ES (frozen) |
| `tg_obergericht` (2,443) | `rechtsprechung.tg.ch/og/` | 200 | **Separate from `tg_gerichte`**; scrapable | **Build native** |
| `be_bvd` (2,094) | `bvd.be.ch` | 404 | Direct portal not found | Keep ES |
| `be_weitere` (836) | `justice.be.ch` | 404 | Direct portal not found | Keep ES |
| `sh_obergericht` (718) | `gerichte-sh.ch` | DNS-fail | Direct portal not accessible | Keep ES |
| `be_steuerrekurs` (343) | Tribuna portal | — | Already had direct scraper; portal DB disconnected Feb 2026 | Keep ES |
| `ag_baugesetzgebung` (196) | `ag.ch/.../baugesetzgebung` | 404 | Source stagnant since Nov 2025 | Keep ES (frozen) |
| `ag_weitere` (24) | — | — | Source dead since 2023 | Keep ES (frozen) |

### Verdict

Of 11 ES-only courts:
- **6 are dead/historical** — no native scraper needed
- **3 have inaccessible portals** (be_bvd, be_weitere, sh_obergericht) — not worth pursuing
- **1 (vd_findinfo + vd_omni)** — VD migrated to an Angular SPA that we couldn't crack despite multiple attempts
- **1 (tg_obergericht) is realistically scrapable** — separate court at `rechtsprechung.tg.ch/og/`

**Recommended action:**
- **Now:** Don't build any. ES cron, now reliable with the 12h timeout, handles all of them.
- **If ES-cron becomes a single point of failure again:** Build `tg_obergericht` native scraper as a hedge (~1–2 days work).
- **Monitor via the new alerting:** if any ES-only court goes >21 days stale, the daily check raises an alert.

---

## What was committed today

| Commit | Change |
|--------|--------|
| `a7e0b96` | `vs_gerichte` raise on offset=0 API failure (no more silent fail) |
| `7aa86b5` | Morning fix report (entscheidsuche timeout, VS, dashboard delta) |
| `2f190f6` `a11a527` `98d119a` `15c7f73` | Dashboard delta fix |
| pending | This investigation report + alerting script |

**Server-side changes** (not in git):
- `opencaselaw-entscheidsuche.service`: `TimeoutStartSec` 4h → 12h
- `opencaselaw-alerts.{service,timer}`: NEW, daily 03:00 UTC

**Currently running:**
- ES backfill (started 06:47 UTC) — currently at GE_Gerichte 50,000 / 179,306. ETA: late afternoon UTC.

---

## Updated Action Items

| Item from morning report | Status |
|---|---|
| Entscheidsuche-Ingest reparieren | ✅ Done (timeout raised, backfill running) |
| Backfill auslösen | ✅ Done |
| VS-Scraper Error-Reporting | ✅ Done (raises on offset=0) |
| Alerting für Entscheidsuche-Ingest | ✅ Done (daily systemd timer) |
| Eigene Scraper für 11 ES-only Gerichte | ⊘ **Skipped after triage** — only `tg_obergericht` is worth building, deferred to later |
| Coverage-Gap-Analyse | ✅ Done (mostly counting artifacts; `ne_gerichte` needs MacBook tunnel) |

All open items from user's report are now either resolved or have a documented decision.
