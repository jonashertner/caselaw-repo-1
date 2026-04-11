# Swiss Law Coverage — Roadmap

Goal: MCP workers retrieve **every** Swiss law — federal, cantonal, communal.

## Current state (2026-04-11)

| Level | Source | Coverage | Retrieval latency |
|---|---|---|---|
| **Federal** — Bundesgesetze, Verordnungen, Staatsverträge | Fedlex SPARQL + local mirror | **80 / ~8,577** curated (0.9 %) | <1 ms (local FTS5) |
| **Cantonal** — 26 cantons + intercantonal concordats | LexFind JSON API (proxied) | ~33,000 acts — structurally complete | ~200-500 ms (network + cache) |
| **Communal** — municipal by-laws (Reglemente) | none | **0** | — |

## Phase 1 — Federal completion (IN PROGRESS)

**Status: crawl kicked off 2026-04-11 07:47 UTC**

- PID 2136155: `python3 -m scrapers.fedlex --delay 0.4`
- PID 2136156: wait-and-build wrapper — will run `build_statutes_db.py` automatically on crawl exit
- Logs: `/opt/caselaw/repo/logs/fedlex_full_crawl.log` and `fedlex_build_wrapper.log`
- ETA: ~5 hours for XML downloads + ~30 min for FTS5 rebuild ≈ **13:30-14:00 UTC**

**Scraper fixes already committed** (`0490d0e`):
- `sparql_query()` now POSTs instead of GETs (removes URL length ceiling)
- Batch sizes halved (100→40 for XML URL resolution, 80→40 for metadata)
- Resilience: single-batch SPARQL failures no longer crash the whole run

**Expected final state**:
- ~8,500 SR numbers in `statutes.db`
- ~500,000-700,000 articles (assuming ~60-80 avg per law × 3 languages)
- ~3-4 GB DB size (vs current 71 MB)
- Full coverage of all SR classes 0–9

**No MCP worker restart needed**: `_get_statutes_conn()` opens a fresh connection per call, so the atomic statutes.db swap is picked up on the next query.

## Phase 2 — Federal freshness (TO DO)

Fedlex publishes new consolidations ~weekly (Tuesdays, tied to AS publication dates). We need a scheduled re-crawl.

**Proposed systemd units** (to add after Phase 1 lands):

```ini
# /etc/systemd/system/opencaselaw-fedlex.service
[Unit]
Description=OpenCaseLaw weekly Fedlex resync
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/opt/caselaw/repo
ExecStart=/bin/bash -c '\
  /usr/bin/python3 -m scrapers.fedlex --delay 0.4 && \
  /usr/bin/python3 -m search_stack.build_statutes_db'
TimeoutStartSec=28800
StandardOutput=append:/opt/caselaw/repo/logs/fedlex_sync.log
StandardError=append:/opt/caselaw/repo/logs/fedlex_sync.log
```

```ini
# /etc/systemd/system/opencaselaw-fedlex.timer
[Unit]
Description=Weekly Fedlex resync (Wednesday 04:00 UTC)

[Timer]
OnCalendar=Wed *-*-* 04:00:00 UTC
Persistent=true
RandomizedDelaySec=600

[Install]
WantedBy=timers.target
```

Scheduled for Wednesday morning — a day after Fedlex's Tuesday publication, gives them time to settle. 8-hour timeout window is generous.

Follow-up runs will skip already-downloaded XMLs (scraper checks `dest.exists()` before each download), so most weeks the re-crawl should finish in ~10 minutes unless Fedlex rolls out many new consolidations.

## Phase 3 — Cantonal audit (TO DO, minimal work)

Cantonal is structurally complete — the existing `get_legislation()` and `search_legislation()` MCP tools proxy arbitrary canton+SR combinations through LexFind. Need to:

1. Write an audit script that iterates known-good cantonal SR numbers (e.g., cantonal constitutions, cantonal tax laws) for each of the 26 cantons and verifies LexFind returns content.
2. Document any cantons where LexFind coverage is thin (if any).
3. Optionally: extend the persistent LexFind cache to pre-fetch the top 100 most-searched cantonal acts per canton (speeds up common queries).

Not urgent — the existing proxy should work for anything LexFind indexes.

## Phase 4 — Communal law (HARD; multi-session)

Swiss municipalities publish their by-laws (Reglemente, Verordnungen) in fragmented ways:

| Source type | Count | Standardised? | Example |
|---|---|---|---|
| Individual municipality websites | ~2,131 municipalities | **No** — each has its own structure | zuerich.ch/amtsblatt, bern.ch/rechtliches |
| Cantonal aggregators (some cantons) | Partial — varies by canton | Slightly | AG, VD, ZH partially publish communal |
| LexFind | Zero | — | LexFind does not cover communal |
| Commercial (Weblaw, Swisslex) | Partial | Paywalled | Not usable |

**There is no single source** for Swiss communal law. Exhaustive coverage of 2,131 municipalities would require ~2,131 custom scrapers. Not realistic in one session.

### Realistic phased approach

**Phase 4a — Top 10 cities** (high value, bounded scope):

Scrape the ten largest Swiss cities' Rechtssammlungen. Collectively they cover ~40% of the Swiss population and the most-used municipal by-laws (Mietrecht annexes, Gewerbepolizei, Hundekontrolle, Bau- und Planungsrecht, etc.).

| Rank | City | Estimated by-law count | Publication system |
|---|---|---:|---|
| 1 | Zürich | ~400 | zuerich.ch/politik-und-verwaltung/rechtliches |
| 2 | Genève | ~350 | ge.ch/legislation/rsg (cantonal already covers city) |
| 3 | Basel | ~300 | bs.ch/rechtssammlung (cantonal already covers city) |
| 4 | Bern | ~280 | bern.ch/gemeinderecht |
| 5 | Lausanne | ~200 | lausanne.ch/officiel/reglements |
| 6 | Winterthur | ~180 | winterthur.ch/rechtserlasse |
| 7 | Luzern | ~150 | luzern.ch/politik-verwaltung |
| 8 | St. Gallen | ~150 | stadt.sg.ch/rechtserlasse |
| 9 | Lugano | ~120 | lugano.ch/regolamenti |
| 10 | Zug | ~100 | stadtzug.ch/rechtssammlung |

**Note**: GE, BS are city-cantons whose cantonal Rechtssammlung already acts as the city's publication. Once Phase 3 (cantonal audit) is solid, GE and BS municipal law is already accessible via LexFind.

So the list effectively becomes 8 custom scrapers. Each takes 2-4 hours to write + test (unique HTML structures, ~no standard API).

**Tool surface**: add a new `get_communal_law` MCP tool and `search_communal_law` that accept `(canton, municipality, reference)`. Storage: new `communal.db` SQLite, same pattern as statutes.db.

**Phase 4b — Cantonal aggregators** (future):

Some cantons actively publish communal ordinances in their own Rechtssammlung. For these, we can opportunistically scrape without per-municipality effort:

- Aargau: publishes communal Reglemente for certain municipalities in the cantonal SAR
- Zürich: Gemeinderecht-Datenbank (GRD)
- Thurgau: RB covers cantonal + selected communal
- St. Gallen: selectively publishes communal in the cantonal collection

This is a research task — inventory what each canton publishes at the communal level.

**Phase 4c — Long tail** (indefinite):

The remaining ~2,000 small municipalities publish sporadically on their own websites with no standardisation. Realistic options:

1. **Opportunistic**: add scrapers when specific municipalities become relevant (e.g., a big Bauprojekt case needs the local Bauordnung).
2. **Partnership**: work with SGV (Schweizerischer Gemeindeverband) to see if they'd aggregate.
3. **Crowdsource**: document submission workflow for users to submit missing communal texts.
4. **Accept the gap**: transparently document that communal law below the top 10 cities is unavailable.

## Honest summary

The user's instruction — "MCP workers must retrieve every single law in Switzerland, federal, cantonal, communal" — is achievable for **federal** (in progress, today) and **cantonal** (already structurally done via LexFind proxy).

**Communal is not achievable in the short term** for all 2,131 municipalities. A credible path is:

- Top 10 cities (~2,100 by-laws total) — 8 custom scrapers, ~1-2 weeks of work
- Cantonal aggregators — additive, whatever publishes communal centrally
- Long tail — opportunistic, partnership, or transparent gap

Current session focus: land Phase 1 (federal) end-to-end, document Phases 2-4 for follow-up.
