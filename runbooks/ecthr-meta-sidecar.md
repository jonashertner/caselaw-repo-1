# ECtHR metadata sidecar (`ecthr_meta.db`)

Structured Strasbourg metadata keyed by `decision_id`: respondent state,
importance, Convention articles, conclusion, violation/non-violation, and
the full application-number list.

## Why it exists

`scrapers/hudoc.py` selects seventeen fields from HUDOC and then flattens
or discards most of them building a `Decision`:

| HUDOC field | What the corpus keeps |
|---|---|
| `respondent` | collapsed to `canton` `'CH'` or `'CE'` — *which* of 46 states is lost |
| `importance` | collapsed to `marked_for_publication` (`importance == '1'`) |
| `article` | rendered as a keyword tail on the regeste |
| `conclusion` / `violation` / `nonviolation` | concatenated into one regeste string |
| `separateopinion` / `originatingbody` | fetched on the wire, dropped before the stub |
| `appno` | truncated to three (`_MAX_DOCKET_APPNOS`); multi-applicant cases reach 3,795 chars |

None of that is recoverable from `decisions.db`, so the builder goes back
to HUDOC rather than to the corpus.

## Key properties

- **Reads no serving database.** Discovery goes to HUDOC, never to
  `decisions.db`, so the builder is not bound by the nightly build window
  and does not compete for the page cache the QC gate depends on.
- **Not a `publish.py` step, on purpose.** A step there inherits the
  nightly's blast radius — a failure cascade-skips the guarded HuggingFace
  upload and both git pushes (08-18, 08-21). Its own timer keeps failures
  local and the run independently repeatable.
- **Respondent is a table, not a column.** 44 of 8,275 judgments name more
  than one respondent state; a column could only express that as a
  substring match.
- **Idempotent.** `INSERT OR REPLACE` + build-to-`.tmp` + `os.replace()`.
- **Writes nothing to `state/`.** It instantiates `HUDOCFullScraper` for
  its discovery paging and so loads scraper state, but never records a
  decision — verified after four local runs. That matters because on the
  VPS it shares `state/` with the real nightly ECtHR scraper.
- **Guarded swap.** See below — this is the part worth understanding.

## The swap guard

Discovery is 68 independent year shards. One HUDOC outage produces a
*well-formed but wrong* sidecar, and an atomic swap installs that as
cleanly as a correct one. So:

- A build holding **< 90 %** of the live sidecar's judgments is **refused**.
  The live DB is untouched; the short read stays at `ecthr_meta.db.tmp`
  for inspection. Exit **2**.
- If a year shard failed but the result still cleared 90 %, it publishes
  and reports exit **3** (incomplete).
- Growth is always allowed. `--allow-shrink` overrides both, for genuine
  HUDOC withdrawals.

Both exit codes are systemd failures by design: `SuccessExitStatus` would
let a chronically broken sidecar read green forever.

## Operating

```bash
# Full rebuild (1959–present), ~3 min
python3 -m search_stack.build_ecthr_meta --output output/ecthr_meta.db

# A single year, for spot checks
python3 -m search_stack.build_ecthr_meta --from-year 2026 --to-year 2026

# Rebuild offline from captured discovery stubs
python3 -m search_stack.build_ecthr_meta --from-jsonl stubs.jsonl

# Publish anyway after a verified shrink
python3 -m search_stack.build_ecthr_meta --allow-shrink
```

### Installing the timer

Units live in `systemd/` and are **copied** into place on the VPS (not
symlinked — `/etc/systemd/system/opencaselaw-ecthr.service` is a plain
copy that currently matches its repo original). `deploy/` also holds ECtHR
units, but that directory is a disaster-recovery snapshot consumed by
nothing; its copy is stale. Do not install from it.

After the VPS has fast-forwarded the repo:

```bash
cd /opt/caselaw/repo
cp systemd/opencaselaw-ecthr-meta.{service,timer} /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now opencaselaw-ecthr-meta.timer
systemctl list-timers opencaselaw-ecthr-meta.timer   # confirm next run
```

First run can be forced with `systemctl start opencaselaw-ecthr-meta.service`.
Note the swap guard will refuse it if `output/ecthr_meta.db` already holds
substantially more judgments than the run produces.

### Schedule

Timer: `opencaselaw-ecthr-meta.timer`, daily 16:30 UTC — after
`opencaselaw-ecthr.timer` (14:00 UTC, `TimeoutStartSec=5400`, so the
scrape can still be live at 15:40). `ExecStartPre` skips the run if the
scrape is active; both hit the same HUDOC endpoint.

```bash
systemctl status opencaselaw-ecthr-meta.service
journalctl -u opencaselaw-ecthr-meta.service -n 50
tail -50 /opt/caselaw/repo/logs/ecthr_meta.log
```

## Expected shape (2026-08-28 backfill)

| Metric | Value |
|---|---|
| Judgments | 8,275 |
| Respondent links | 8,325 |
| Multi-respondent judgments | 44 |
| Distinct respondent states | 47 |
| Swiss respondent (`CHE`) | 184 |
| Importance 1 / 2 / 3 | 1,101 / 1,088 / 6,086 |
| Size | ~5.3 MB |

47 states = 46 current Council of Europe members **plus Russia**, expelled
in 2022 but holding 792 historical judgments. The corpus starts in 1960,
so historical respondents are expected.

**Cross-check that should keep holding:** Swiss-respondent judgments split
165 `ecthr_chamber` + 19 `ecthr_grand_chamber`, matching the corpus's own
`canton='CH'` counts exactly. HUDOC's `respondent` and our `canton` agree
independently — if they ever diverge, one of the two ingests has drifted.

## Coverage limits

- Covers `ecthr_chamber` and `ecthr_grand_chamber` only. **`hudoc_ch` (853)
  and `bge_egmr` (487) do not join** — separate scrapers, different
  `decision_id`s for what are sometimes the same judgments. Bridging them
  needs the corpus identity mapping, not this sidecar.
- `ecthr_committee` is defined in the scraper's court map but has zero
  rows; it will appear automatically if it ever starts ingesting.
- `separate_opinion` and `originating_body` are always NULL until
  `_group_judgments` in `scrapers/hudoc.py` carries those two keys. Both
  are already fetched from HUDOC and discarded — a two-line change.
- The sidecar can legitimately hold judgments **not yet in
  `decisions.db`**: discovery sees a judgment before its body is fetched.
  Always treat the join as a LEFT JOIN from `decisions`.

## Not yet wired

- No consumer. Nothing reads this DB yet — the intended first one is
  `get_decision` enriching ECtHR rows. Until something consumes it, treat
  it as a build artifact, not a served surface.
- No quality-gate check. Checks in `quality/checks/` are auto-discovered,
  so a check added before the DB exists on the VPS would warn every night
  about a missing file. Wire the timer first, then add the check.
