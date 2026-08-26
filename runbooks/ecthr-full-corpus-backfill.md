# ECtHR full-corpus backfill (importance 1-3)

Ships the whole substantial Strasbourg corpus into serving: **8,275
judgments** from 74 respondent states, replacing the 2,364 rows the
`ecthr` scraper had accumulated under the previous scope.

Prepared 2026-08-26; revised the same day after an adversarial review.
All numbers were measured against the live HUDOC API on that date.

## Why the existing rows must be discarded, not topped up

Two things changed that make the old rows unusable as a base:

1. **Scope.** The old scraper had no importance filter, so it had begun
   ingesting importance-4 (low/repetitive) judgments, and `ecthr_committee`
   holds 338 rows — the entire Committee docket is importance 4. Those are
   out of scope now and must leave serving.
2. **Identity.** Dockets were the bare application number. 158 application
   numbers in scope carry more than one judgment (merits, then just
   satisfaction or revision years later) and the second was silently dropped
   by `is_known`. Dockets are now `<appno>[_<appno>[_<appno>]]_<yyyymmdd>`,
   so every old `decision_id` is one the new scraper will never emit.

The backfill re-fetches everything, so discarding costs nothing but time.

## Expected end state

| | |
|---|---|
| judgments | 8,275 (was 2,364) |
| `ecthr_chamber` | 7,761 |
| `ecthr_grand_chamber` | 514 |
| `ecthr_committee` | 0 — the Committee decides repetitive cases, all importance 4 |
| importance 1 / 2 / 3 | 1,101 / 1,088 / 6,086 |
| language | 4,326 `fr`, 3,949 `en` |
| Swiss respondent | 184 (`hudoc_ch`'s 847 are unaffected) |
| runtime | ~3.5-4 h at `REQUEST_DELAY = 1.5` |

## Preconditions

- **Deploy promptly after merging, and cut over the same evening.** The
  pre-swap per-court gate (`build_fts5._check_swap_per_court_gate`) refuses
  the atomic swap when a court holding **500 or more** live rows drops below
  80% of that count. `ecthr_committee` is at 338 and was growing 15-31/day
  under the old scope, so it crosses 500 in roughly 5-11 days. Deploying the
  scope change stops that growth immediately — but if the cutover slips past
  500, the first full rebuild after the backfill will refuse to swap. Check
  before starting:

  ```bash
  sqlite3 "file:/opt/caselaw/repo/output/decisions.db?mode=ro&immutable=1" "SELECT court, COUNT(*) FROM decisions WHERE court LIKE 'ecthr%' GROUP BY court"
  ```

  If `ecthr_committee` is at 500 or more, the first post-backfill rebuild
  must be run once with `OCL_SKIP_SWAP_GATE=1` in the environment of
  `opencaselaw-publish.service`. That is the gate's documented
  court-retirement escape hatch; unset it again straight afterwards.

- **Confirm the delta and snapshot publishers are off, or that they honour
  the licence carve-out.** `export_parquet.EXCLUDED_COURTS` keeps ECtHR text
  out of the CC0 HuggingFace export, but `search_stack/publish_delta.py`
  does not consult it: `snapshot_all_ids` and `build_sqlite_snapshot` take
  every row. ECtHR full text has already reached the CC0-tagged repo through
  that path. This backfill would multiply that by ~24×.

  ```bash
  systemctl show -p Environment opencaselaw-publish.service | tr ' ' '\n' | grep -i 'OCL_PUBLISH_DELTA\|OCL_PUBLISH_SNAPSHOT'
  ```

  If either is enabled, **fix the carve-out in `publish_delta.py` first** or
  disable those steps for the cutover. This is a licensing precondition, not
  a nice-to-have.

- The scope change is deployed to the VPS. **The backfill runs the VPS's
  checkout**, so it must be deployed first — but stop the ECtHR timer before
  deploying (step 2).

- Start after the nightly build window exits. Gate on the condition, not the
  clock (step 5).

## Steps

**1. Confirm the scope size hasn't drifted.** This actually queries HUDOC;
if the count has moved by more than a few percent, stop and find out why
before discarding anything.

```bash
cd /opt/caselaw/repo && python3 -c "
import sys; sys.path.insert(0,'.')
from scrapers.hudoc import _FULL_QUERY
import requests
s = requests.Session(); s.headers['User-Agent'] = 'Mozilla/5.0'
s.get('https://hudoc.echr.coe.int/eng', timeout=30)
total = 0
for y in range(1959, 2027):
    q = f'{_FULL_QUERY} AND kpdate:[{y}-01-01T00:00:00.0Z TO {y}-12-31T23:59:59.0Z]'
    r = s.get('https://hudoc.echr.coe.int/app/query/results',
              params={'query': q, 'select': 'itemid', 'sort': 'kpdate Ascending',
                      'start': 0, 'length': 1}, timeout=60)
    r.raise_for_status(); total += int(r.json().get('resultcount') or 0)
print('documents in scope:', total, '(expected ~10,585 -> ~8,275 judgments)')
"
```

**2. Hold the ECtHR timer first — before deploying.**

```bash
sudo systemctl stop opencaselaw-ecthr.timer
```

Order matters. `opencaselaw-ecthr.timer` fires at 14:00 UTC; if new code is
deployed while the timer is live, that run starts a four-hour backfill inside
a unit with `TimeoutStartSec=5400`, dies half-done, and then `quick_publish`
inserts the partial new-id rows beside the old ones. The `ConditionPathExists`
and `ExecStartPre` guards on the unit only start helping after step 4, so the
deploy-then-stop window is real exposure.

The 01:00 UTC `opencaselaw-scrape.timer` also used to run `ecthr` — through
`run_all_scrapers.py`, which has no backfill guard and a 7200 s per-scraper
cap. This change adds `ecthr` to `SKIP_BY_DEFAULT`, so once deployed that
run no longer touches it. Do **not** stop that timer: it would skip every
other scraper for the night. If the backfill is still running at 01:00,
confirm from its log that `run_all_scrapers` skipped `ecthr`.

**3. Deploy.** Commit + push locally, then on the VPS:

```bash
cd /opt/caselaw/repo && git fetch origin && git merge --ff-only origin/main
```

**4. Archive the old shard and state.** Nothing is deleted; both files move
aside so a rollback is a `mv` back. `output/decisions/ecthr.jsonl` and
`state/ecthr.jsonl` share a basename, so they need separate subdirectories —
moving both into one folder silently overwrites the first.

```bash
cd /opt/caselaw/repo && STAMP=$(date -u +%Y%m%d) && mkdir -p archive/ecthr-$STAMP/output archive/ecthr-$STAMP/state && mv output/decisions/ecthr.jsonl archive/ecthr-$STAMP/output/ && mv state/ecthr.jsonl archive/ecthr-$STAMP/state/ && mv state/ecthr.gaps.jsonl archive/ecthr-$STAMP/state/ 2>/dev/null; find archive/ecthr-$STAMP -type f -exec ls -la {} +
```

`archive/` must not be inside `output/decisions/` — `publish.py` globs
`output/decisions/*.jsonl` and would rebuild the old rows straight back in.

**5. Run the backfill.** Gate on the publish actually being finished, not on
the clock, then start the transient unit the daily service already knows to
defer to.

```bash
systemctl is-active --quiet opencaselaw-publish.service && echo "publish still running — wait" || echo "clear to start"
```

```bash
sudo systemd-run --collect --unit=opencaselaw-ecthr-backfill --property=TimeoutStartSec=28800 --property=CPUWeight=20 --property=IOWeight=20 --working-directory=/opt/caselaw/repo /usr/bin/python3 run_scraper.py ecthr -v
```

`--collect` matters: without it a unit that exits non-zero stays loaded in
`failed` state and re-issuing the same command is refused. The
`CPUWeight`/`IOWeight` match the publish drop-in so an unavoidable overlap
does not outrank the rebuild.

Watch it:

```bash
journalctl -u opencaselaw-ecthr-backfill -f
```

Discovery logs one line per year shard (`2016: 349 rows → 295 judgments`).
Any line containing `shard search failed` means that year is incomplete —
note it and re-run the scraper afterwards; the run is idempotent.

**6. Verify before publishing. This is a gate, not a formality.**

```bash
cd /opt/caselaw/repo && wc -l state/ecthr.jsonl && python3 -c "
import json, collections
rows=[json.loads(l) for l in open('output/decisions/ecthr.jsonl')]
c=collections.Counter(r['court'] for r in rows)
print('rows', len(rows)); print('courts', dict(c)); print('langs', collections.Counter(r['language'] for r in rows))
ids=[r['decision_id'] for r in rows]
print('distinct ids', len(set(ids)), 'of', len(ids)); print('longest id', max(len(i) for i in ids))
print('OK to publish:', len(rows) > 7800 and c['ecthr_chamber'] >= 1550)
"
```

Expect ~8,275 rows, zero `ecthr_committee`, no duplicate ids, longest id
under 60 characters.

**Do not let 03:30 arrive on a short backfill.** Live `ecthr_chamber` is
1,922 rows, so the per-court swap gate's floor is 1,538. A truncated shard
below that makes `build_fts5` refuse the swap for the **whole corpus** — all
118 courts stay frozen on the old inode, and `publish.py` step 2 is critical,
so the HuggingFace upload and both git pushes are skipped too. If the
backfill is short and 03:30 is close, either let it finish, or roll back
(below) and retry another evening.

**7. Let the nightly full rebuild publish it.** `opencaselaw-publish.timer`
runs a full rebuild at 03:30 UTC daily, which reconstructs `decisions.db`
from the shards — that is what makes the discarded rows actually leave
serving.

Do **not** run `scripts/quick_publish.py --courts ecthr` instead: it inserts,
it does not delete, so the old ids would coexist with the new ones until the
next full rebuild.

**8. Restart the ECtHR timer — after the rebuild, not before.**

`opencaselaw-ecthr.timer` has `Persistent=true`, so starting it the same
evening fires the missed 14:00 run immediately. That run ends in
`quick_publish.py --courts ecthr,hudoc_ch`, which inserts without deleting —
the new rows would land beside the archived ids and reintroduce exactly the
coexistence step 7 avoids. Start it once the 03:30 rebuild has finished:

```bash
sudo systemctl start opencaselaw-ecthr.timer
```

**9. After the rebuild, confirm serving.**

```bash
make smoke
```

Then refresh the counts quoted in `mcp_server.py`'s ECHR coverage block,
`README.md` (the overview bullet and the three `ecthr_*` table rows) and
`dataset_card.md` (the ECHR bullet and the CC0 carve-out note) if they have
drifted from what the rebuild actually produced.

## Rollback

Before the 03:30 rebuild, serving is untouched.

```bash
sudo systemctl stop opencaselaw-ecthr-backfill.service opencaselaw-ecthr.timer
```

Stop the unit **first**. `run_scraper.py` re-opens the shard by path in
append mode for every decision, so a still-running backfill would start
appending new-format rows onto the restored archive. Then move the partial
shard aside rather than letting the restore overwrite it, and restore:

```bash
cd /opt/caselaw/repo && STAMP=<the stamp from step 4> && mv output/decisions/ecthr.jsonl archive/ecthr-$STAMP/output/ecthr.partial.jsonl 2>/dev/null; mv state/ecthr.jsonl archive/ecthr-$STAMP/state/ecthr.partial.jsonl 2>/dev/null; mv archive/ecthr-$STAMP/output/ecthr.jsonl output/decisions/ && mv archive/ecthr-$STAMP/state/ecthr.jsonl state/ && mv archive/ecthr-$STAMP/state/ecthr.gaps.jsonl state/ 2>/dev/null
```

To back the code out, revert **on the dev machine**, push, and fast-forward
the VPS. Never commit in `/opt/caselaw/repo`: `publish.py` ends with a
`git pull --rebase origin main` and a bare `git push`, so a commit made on
the production box gets pushed to the shared remote and the dev machine's
next push is rejected as non-fast-forward. If the VPS must be moved back
immediately, do it without creating a commit:

```bash
git -C /opt/caselaw/repo checkout <previous-sha> -- scrapers/hudoc.py models.py quality/checks/languages.py run_all_scrapers.py mcp_server.py seo_pages.py generate_feeds.py ecthr_docket.py
```

After the rebuild, restoring the archived shard and running one more full
rebuild puts the old corpus back.

## Expect a delta alert, and don't treat it as a failure

The rebuild after the backfill posts roughly **+5,800 for `ecthr_chamber`,
+380 for `ecthr_grand_chamber` and -338 for `ecthr_committee`** to the daily
delta/anomaly reporting. A swing that size is the signature the anomaly
tooling exists to flag (cf. the 2026-05-11 aggregation incident). It is
expected here.

## Known consequences, accepted

- **~2,364 published `/entscheid/ecthr_*` URLs retire.** Every ECtHR
  `decision_id` changes shape, so the old URLs 404 and drop out of the
  sitemap on the next `seo_pages` run. No redirects are emitted. Lookups by
  bare application number still work — `mcp_server._lookup_ecthr_appno`
  resolves those against the new dockets — but the old id strings do not.
- **Semantic rescue is language-partitioned** (`vec_decisions.language`), so
  the 3,949 English judgments are unreachable by DE/FR/IT semantic queries.
  FTS5 keyword search reaches them, and the regeste now carries the German
  and Italian Convention abbreviations (`Art. 8 EMRK / CEDH / CEDU`) as a
  lexical bridge, but a full German headnote synthesis is still open.

## Follow-ups this backfill does not do

- **`search_stack/publish_delta.py` does not honour `EXCLUDED_COURTS`** — see
  Preconditions. This is a live licensing defect independent of this change.
- **`scl` and `extractedappno`** in the HUDOC listing give ECtHR-to-ECtHR
  citation edges as structured data (present on 99% of rows). The citation
  graph currently gets nothing from these judgments, since
  `extract_citations` looks for Swiss citation formats.
- **`decision_type`** stores HUDOC's raw numeric `typedescription`
  (15 = judgment, 14, 12 …) rather than a label.
- **`hudoc_ch` accepts GER/ITA language versions**, which HUDOC lists as
  translations contributed by ministries and NGOs — copyright sits with the
  translator, not the Court. The full-corpus scraper now filters those by
  docname; `hudoc_ch` does not. Worth an audit separately from this change.
