# Structure sidecar from the served text (step 2g), 2026-09-06

## Why

`get_erwaegung` answers "no structured Erwägungen" for 34% of BGE and 27.5%
of all decisions. Measured 2026-09-06 (read-only id-joins over the covering
indexes, 38 s):

| | decisions | indexed by the served sidecar | indexed by the shadow sidecar |
|---|---|---|---|
| BGE | 50,980 | 33,676 (66%) | 45,344 (89%) |
| whole corpus | 1,071,112 | 776,346 (72.5%) | 801,585 (74.8%); union 811,517 (75.8%) |

The served sidecar (`decision_structure.db`) is rebuilt nightly by
`extract_decision_structure.py --build` from the raw JSONL shards. The shadow
(`decision_structure_incremental.db`, `scripts/incremental_nightly.py` step 3)
is extracted by `extract_decision_structure_incremental.py` from
`decisions.db` full_text, the text `get_decision` serves. On the historical BGE
volumes (1875–1949: 100% missing in the served index) the raw shard text is
OCR with page headers and running numbers; the served text segments fine (the
repo's own extractor finds numbered considerations for `bge_15_I_14`,
`bge_7_I_335`, `bge_78_II_405` ...). The server opens only the served sidecar.

The shadow also loses 9,932 current decisions the shard build indexes
(ti_gerichte 5,722, ne_gerichte 2,601, bvger 1,244, so_gerichte 318). Checked
on samples: those are decisions whose served text is a 500–2,000 character
cover page (the direct scrape) while a fuller mirror shard exists, or where
the shard build indexed one spurious marker ("E. 10", "E. 24"). A pinpoint
index for text the reader cannot see is not coverage; the loss is honest.

## Change

`publish.py` step 2g now runs the incremental extractor over `decisions.db`
into `decision_structure.db.tmp`, measures coverage (current decisions with
≥1 indexed Erwägung, covering indexes only, ~35 s each side), swaps only if
the new count is ≥ 98% of the old, and otherwise keeps the old sidecar. If
the extractor fails, the shard-based build runs as before
(`_step_2g_from_shards`). `--full-rebuild` maps to `--force-full`.

First night after deploy: the incremental extractor finds no state in the
served sidecar (and the extractor track bumps EXTRACTOR_VERSION), so it does a
full extraction (~85 min on the data volume; the step's timeout is 14,400 s,
stall 9,000 s). Later nights re-extract only new or changed decisions.

## Verification

- Offline: `tests/test_publish_structure_gate.py` (swap, gate, fallback,
  force-full); `tests/test_publish_dag.py`, `tests/test_incremental_per_pair_cutover.py`
  unchanged and green.
- Production evidence for the extractor itself: the shadow has been built
  nightly since 2026-05 (drift check `logs/publish_drift_check.jsonl`).
- After the first night: `_structure_coverage` numbers in `logs/publish.log`;
  `ocl decisions passage bge_15_I_14 1` and `bge_BGE_82_III_33 1` answer;
  the field-test benchmark (`make bench-citations`) shows BGE inline
  pinpoints moving from `pinpoint_unavailable` to `resolved`.

## Rollback

`git revert` of the publish.py commit; the next night rebuilds from shards.
The old sidecar is never deleted before the gate passes.
