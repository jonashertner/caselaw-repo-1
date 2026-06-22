# Deploying the beyond‑entscheidsuche scrapers (2026‑06‑22)

Eight scrapers built this session ingest decision sources that **entscheidsuche does not aggregate**
(verified beyond‑es gaps). They are committed locally (`79e064a` … `6650c64`) but **not yet pushed,
deployed, or full‑scraped** — the corpus does not contain these decisions yet.

## What was built

| Registry key | Court code(s) emitted | Source | ~Volume | REQUEST_DELAY | Special |
|---|---|---|---|---|---|
| `esbk` | esbk | Spielbankenkommission | small | 2s | admin.ch DAM |
| `rab` | rab | Revisionsaufsichtsbehörde | ~100 | 2s | Drupal, paginated |
| `eschk` | eschk | Schiedskommission Urheberrecht | ~hundreds | 2s | per‑year pages |
| `preisueberwacher` | preisueberwacher | Preisüberwacher | dozens | 2s | filters appellate+Bundesrat |
| `ne_jurisprudence_adm` | ne_jurisprudence_adm | NE Conseil d'État + depts | **1,648** | 2s | **needs NE_PROXY** |
| `be_direktionen` | be_direktionen | BE GSI/KAIO/BKD | ~280 | 10s | chamber from docket |
| `bs_rekurskommissionen` | bs_steuerrekurskommission, bs_personalrekurskommission | BS commissions | ~84 | 10s | one file → two courts |

## Wiring (confirmed — no manual list to update)

- **Scrape:** `systemd/opencaselaw-scrape` → `run_all_scrapers.py`, which iterates `run_scraper.SCRAPERS.keys()`.
  All 8 keys are registered, none in `SKIP_BY_DEFAULT` (= `{be_steuerrekurs}`), so they run automatically.
- **Build:** `build_fts5.py` globs `output/decisions/*.jsonl` and indexes by each row's `court` field — so the
  new `*.jsonl` files (and the two courts inside `bs_rekurskommissionen.jsonl`) are picked up automatically.
- `DEFAULT_TIMEOUT = 7200` (2h) per scraper covers the slow first full scrape.

## Deploy steps

1. **Push** the 6 beyond‑es commits (needs explicit approval): `git push` (currently nothing pushed).
2. On the VPS (SSH in — connection details are in the local, untracked CLAUDE.md): `cd <repo> && git pull`.
3. **NE_PROXY** — `ne_jurisprudence_adm` hits ne.ch, which blocks Hetzner IPs at TCP. Confirm `NE_PROXY`
   (or `SCRAPER_PROXY`) is set in the scrape service env / tunnel, the same way `ne_gerichte` already relies on
   it. Without it, NE fails from the VPS (it verified fine from a residential IP).
4. Either let the **nightly scrape** absorb them, or pre‑seed each: `python3 run_scraper.py <key>`
   (writes `output/decisions/<key>.jsonl`). First full scrape is slow — NE ~55 min, BE ~46 min, BS ~14 min
   (within the 2h cap); incremental nightly runs are fast.
5. The nightly **publish** (`build_fts5`) then folds them into `decisions.db` via the atomic swap.

## First‑run watch‑outs

- These keys are **not** in `NONE_RETURN_TOLERANT_SCRAPERS`. `run_all_scrapers` flags FAILED if a scraper
  returns `none_count ≥ 200`. Only `ne_jurisprudence_adm` (1,648) is large enough to risk this if PDF/HTML
  extraction degrades — watch its first run; if many legitimately fail extraction, add it to the tolerant set.
- `be_direktionen` / `bs_rekurskommissionen` use `REQUEST_DELAY=10` (robots Crawl‑delay). Respect it.

## Verify after deploy

- `/health` decisions count rises; `list_courts` shows the new court codes.
- `search_decisions` filtered to each new court returns real decisions with full text.
- `python3 scripts/source_coverage_audit.py` + `docs/sources/beyond_es_sources.json` reflect them ingested.

## Rollback

Additive — new court codes only; the atomic‑swap build means no downtime. To disable a misbehaving scraper,
remove its key from `run_scraper.SCRAPERS` and rebuild; existing rows persist until the next full rebuild.

## Not built (deliberately)

- **SO `rrb.so.ch`** — a government‑business register (48k Sachgeschäfte = grants/motions/consultations), not a
  decisions DB; `Beschwerde` = 676/53,579. Recorded as not‑corpus‑appropriate; ingesting it would dilute the
  corpus. SO admin appeals reach us via `so_gerichte` (Verwaltungsgericht) already.
