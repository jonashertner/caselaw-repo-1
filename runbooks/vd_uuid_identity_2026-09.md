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

Expect ~54,000 uuids and a VD run of minutes with `+0 new` (or a handful of real ones).

## Belt and braces for Saturday if the merge cannot happen before Sat 01:00 UTC

Exclude VD from the scrape for one night (drop-in, root on the VPS):

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'mkdir -p /etc/systemd/system/opencaselaw-scrape.service.d && printf "[Service]\nExecStart=\nExecStart=/usr/bin/python3 run_all_scrapers.py --exclude vd_gerichte\n" > /etc/systemd/system/opencaselaw-scrape.service.d/exclude-vd.conf && systemctl daemon-reload && systemctl cat opencaselaw-scrape.service | grep -n exclude'
```

Remove the drop-in (`rm` + `daemon-reload`) once the fix is merged.

## Clean the shard (the 8,133 duplicates)

Outside the build window only: both `opencaselaw-publish.service` and
`opencaselaw-publish-incremental.service` inactive, and before the next 01:00 UTC scrape
appends to the shard. Dry-run first, then apply. Keeps the FIRST record per uuid (the id
the corpus has served longest), hard-links a backup, writes the dropped ids next to it,
and (re)writes the sidecar from the cleaned shard:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && systemctl is-active opencaselaw-publish.service opencaselaw-publish-incremental.service; python3 scripts/vd_uuid_sidecar.py --dedupe'
```

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 scripts/vd_uuid_sidecar.py --dedupe --apply --seed && wc -l output/decisions/vd_gerichte.jsonl state/vd_gerichte.uuids.txt && ls -la output/decisions/vd_gerichte.jsonl.bak-* output/decisions/vd_gerichte.jsonl.dropped-*'
```

Expect: kept ~54,400, dropped ~8,100. The next full build drops the duplicates from
`decisions.db` (a ~13 % shrink of one court passes the per-court swap gate, which only
requires ≥ 500 rows). The dropped ids stay in `state/vd_gerichte.jsonl` on purpose.
Rollback: `mv output/decisions/vd_gerichte.jsonl.bak-<date> output/decisions/vd_gerichte.jsonl`.

## Still open

- The ~7,600 CASSO decisions dated 2007 to 2017-08 that the portal lists under
  docket-dash numbers: with the sidecar seeded they are held by uuid and never re-fetched.
- Whether the null `affaireHit.numero` is permanent. If the portal restores it, ids for
  new decisions revert to the ZD scheme by themselves; the uuid sidecar keeps working.
