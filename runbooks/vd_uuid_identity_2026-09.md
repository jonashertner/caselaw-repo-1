# Runbook: Vaud duplicate ids (prestations.vd.ch dropped affaire numbers)

**What happened (2026-09-04).** prestations.vd.ch now returns `affaireHit.numero = null`
for every hit. `scrapers/cantonal/vd_gerichte.py` minted ids from that number, so the
fallback (`decisionHit.numero`) produced new ids for decisions already served under their
ZD number: 8,133 duplicate records entered `output/decisions/vd_gerichte.jsonl` during the
01:00 UTC scrape (6,685 CASSO docket-dash ids, 1,077 bare sequence numbers, 373 other) and
went live with the 09-04 build. The scraper also ran to its 4 h cap, which delayed the
full build to 05:48 UTC. ~7,600 more CASSO rows (2007 to 2017-08) remain on the portal.

**The fix (this commit).** Identity is keyed on the portal's decision uuid, which is the
pdf_url suffix of every held record. The scraper keeps `state/vd_gerichte.uuids.txt`
("<uuid>\t<decision_id>"), seeds it once from the corpus shard when it is missing (at
startup, automatically), appends after every durable write, and skips any listing whose
uuid is held. Ids: affaire number when present (old scheme), a real docket when the
portal gives one, otherwise the uuid (bare sequence numbers collided across years).

## Deploy (code lands with the next pipeline pull or an ff-merge on the VPS)

Nothing to configure: on the first run after the merge the scraper seeds the sidecar
from the shard (~2 GB streamed once, ~1 min) and logs
`uuid sidecar seeded from ...: N uuids`. Verify after the Saturday 01:00 UTC scrape:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && wc -l state/vd_gerichte.uuids.txt && grep -E "uuid sidecar|Done\." logs/vd_gerichte.log | tail -3'
```

Night 1 (2026-09-05) did this, see below. The seed is a one-time event: since 0cf9b1f4
the scraper appends to the sidecar after every persisted decision (run_scraper's
`on_decision_persisted` hook), so `--seed` is no longer required to catch the sidecar up
after a run. Until the 2007-2016 walk finishes each night still runs to the 4 h cap;
after that expect a VD run of minutes with `+0 new` (or a handful of real ones).

## Belt and braces for Saturday if the merge cannot happen before Sat 01:00 UTC

Exclude VD from the scrape for one night (drop-in, root on the VPS):

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'mkdir -p /etc/systemd/system/opencaselaw-scrape.service.d && printf "[Service]\nExecStart=\nExecStart=/usr/bin/python3 run_all_scrapers.py --exclude vd_gerichte\n" > /etc/systemd/system/opencaselaw-scrape.service.d/exclude-vd.conf && systemctl daemon-reload && systemctl cat opencaselaw-scrape.service | grep -n exclude'
```

Remove the drop-in (`rm` + `daemon-reload`) once the fix is merged.

## Clean the shard (12,233 uuid-duplicate lines)

The shard holds 12,233 lines whose uuid an earlier line already carries: 7,804
different-id duplicates from the 2026-09-04 night, plus 4,429 same-id re-appends from
February that build_fts5 already ignores (same decision_id, so they never reached
`decisions.db`; only the 7,804 sit there under a second id).

Outside the build window only: both `opencaselaw-publish.service` and
`opencaselaw-publish-incremental.service` inactive, and no scrape appending to the shard.
While the Vaud backfill runs the 01:00 UTC scrape to its 4 h cap that window does not
exist on weeknights: the full build starts ~05:40 UTC and ends ~22:30-23:00, the queued
20:00 incremental then runs to ~01:15-01:35 UTC (on 2026-09-05 it exited at 01:34, with
the 01:00 scrape already running). Use a Sunday night instead: there is no incremental
on Sundays, so run after the Sunday full build exits and before Monday 01:00 UTC. The
other clean slot is the first night after the backfill has finished and the VD run is
back to minutes. Dry-run first, then apply. Keeps the FIRST record per uuid (the id the
corpus has served longest), hard-links a backup, writes the dropped ids next to it, and
(re)writes the sidecar from the cleaned shard:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && systemctl is-active opencaselaw-publish.service opencaselaw-publish-incremental.service; python3 scripts/vd_uuid_sidecar.py --dedupe'
```

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 scripts/vd_uuid_sidecar.py --dedupe --apply --seed && wc -l output/decisions/vd_gerichte.jsonl state/vd_gerichte.uuids.txt && ls -la output/decisions/vd_gerichte.jsonl.bak-* output/decisions/vd_gerichte.jsonl.dropped-*'
```

Expect the dry run to print `keep 60296, drop 12233`, where keep grows by whatever later
nights added (60,296 = the 54,707 uuids of the first seed + 5,589 from Night 1); apply
prints the same two numbers. The next full build drops the 7,804 second-id rows from `decisions.db`
(~11 % of one court, 7,804 of ~68,100; the per-court swap gate only requires ≥ 500 rows).
`--seed` stays in the apply command because it rewrites the sidecar from the cleaned
shard and picks up the Night 1 ids (see below); it is not otherwise needed anymore. The
dropped ids stay in `state/vd_gerichte.jsonl` on purpose.
Rollback: `mv output/decisions/vd_gerichte.jsonl.bak-<date> output/decisions/vd_gerichte.jsonl`.

## Night 1 result (2026-09-05)

- The sidecar self-seeded at 01:37 UTC: 54,707 uuids from the shard. No held decision was
  re-fetched.
- 5,589 genuinely new decisions (2017-2026) recovered. These are rulings whose bare
  sequence number had collided with an earlier numeric id, so the old scheme had treated
  them as already held. They carry uuid ids and went live with the 09-05 build.
- The walk reached 2017-01 when the 4 h cap hit. 2007-2016 remain: one or two more capped
  nights, each starting the full build at ~05:40 UTC (and ending it ~22:30-23:00, see the
  window note above).
- The append side of the sidecar did not run: run_scraper marks state per decision and
  never calls `mark_run_complete`, where the append lived, so the sidecar stayed at the
  seed for the whole run and the 5,589 are held by id only until the next `--seed` (the
  dedupe command above includes one). Fixed in 0cf9b1f4: run_scraper calls an
  `on_decision_persisted` hook right after marking state (errors logged, never fatal) and
  the Vaud scraper appends there; two tests pin both ends. Lands on the VPS with the next
  ff-merge or pipeline pull.
- Follow-up: the real case number sits on the PDF's first line ("894 PE18.013205-JON …"),
  not in the listing. It should feed `docket_number`, at persist time for new decisions
  and as a backfill over the held PDFs.

## Still open

- The 2007-2016 CASSO decisions the portal lists under docket-dash numbers: held ones are
  skipped by uuid, the rest are genuinely new and the walk fetches them over one or two
  more capped nights (see Night 1).
- The shard dedupe (Sunday night, above) and the `docket_number` follow-up.
- Whether the null `affaireHit.numero` is permanent. If the portal restores it, ids for
  new decisions revert to the ZD scheme by themselves; the uuid sidecar keeps working.
