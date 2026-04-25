---
to: Adrian König (adriankoenig.ch)
subject: OpenCaseLaw — federal-data delta gap closed; recommended catch-up path
date: 2026-04-25
status: draft (not yet sent)
---

Hi Adrian,

Following up on your 2026-04-04 report on the federal-data gap in the
daily HF delta feed.

**What happened**: the old delta-publish pipeline lived in a private
GitHub repo running on GitHub-hosted runners that were silently being
blocked by Incapsula at bger.ch — so federal scrapers exited cleanly
with empty results, which produced delta files with zero federal rows
for ~30 days (2026-03-25 → 2026-04-23) without alerting anyone.

**What's fixed (live now)**:

- The delta publish pipeline has been rebuilt inside the canonical
  `caselaw-repo-1` repo on our VPS (where federal scrapers actually
  succeed). It runs as Step 7 of `publish.py` daily at ~10:00 UTC.
- 2026-04-24 was the first daily delta after the rebuild. 2026-04-25
  was the first integration test of the full pipeline including Step 7;
  both succeeded — see `voilaj/swiss-caselaw` commit history.
- Federal rows (`court=bger`, `bvger`, `bge`, `bstger`, `bpatger`,
  plus regulators) now appear in the daily deltas as designed.

**Recommended catch-up path for your `apply_deltas.py` consumer**:

The 30-day backlog is **already in the main parquet** at
`voilaj/swiss-caselaw/data/*.parquet` (Step 4 has been uploading the
full corpus daily throughout the gap — only the *delta files* were
empty). For your consumer to pick up the missing federal decisions,
the cleanest path is a one-shot **full re-sync** rather than a synthetic
catch-up delta:

```python
from huggingface_hub import snapshot_download
local_path = snapshot_download(
    repo_id="voilaj/swiss-caselaw",
    repo_type="dataset",
    allow_patterns=["data/*.parquet"],
)
# Then ingest data/*.parquet, ignoring decision_ids you already have.
```

Reasons we recommend full re-sync over a synthetic catchup delta:

1. The main parquet is the single source of truth and is current.
2. We don't have a snapshot dated 2026-03-25 to define "missing IDs"
   precisely (the snapshot mechanism was bootstrapped 2026-04-23).
3. After the one-time sync, your existing daily-delta path resumes
   normally — no schema change, no consumer code change, no per-row
   migration logic.

If you'd prefer a one-shot synthetic delta artifact instead — we can
generate one from the FTS5 DB for federal `decision_date` between
2026-03-25 and 2026-04-23, ship it as
`artifacts/sqlite/deltas/2026-03-25_2026-04-23-catchup.sqlite.zst` —
say the word and we'll have it published in a few hours.

**Going forward**:

We've added a Step 7 monitoring assertion in our internal queue
("fail Step 7 if a daily delta has zero federal rows") so a repeat
of this scenario triggers a push notification rather than going
silently through. Not yet implemented but on the queue.

Many thanks again for the detailed report — it surfaced both an
operational gap and a class of monitoring we hadn't built. Credit
already added to `CONTRIBUTORS.md` and to the website
(<https://opencaselaw.ch> → Mitwirkende).

If anything else in the dataset behaves unexpectedly, please send.

Best,
Jonas
