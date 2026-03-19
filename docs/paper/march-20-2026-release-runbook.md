# March 20, 2026 Paper Release Runbook

This is the canonical cut plan for the next arXiv-facing paper release.

## Goal

Produce one internally coherent release bundle in which:

- `stats_snapshot.json` is frozen from the same release cut
- the benchmark DB matches that frozen corpus snapshot
- the paper is rewritten to cite that bundle directly

## Required artifacts

- release `decisions.db`
- release `reference_graph.db` if Table 2 should be refreshed
- release `docs/stats.json`

## 1. Freeze the paper bundle

```bash
python3 scripts/build_paper_release_bundle.py \
  --release-id opencaselaw-paper-2026-03-20 \
  --stats-file docs/stats.json \
  --output-dir artifacts/paper_release_2026-03-20
```

This creates:

- `artifacts/paper_release_2026-03-20/manifest.json`
- `artifacts/paper_release_2026-03-20/stats_snapshot.json`
- `artifacts/paper_release_2026-03-20/benchmark_golden.json`
- `artifacts/paper_release_2026-03-20/benchmark_report.json`

## 2. Verify the release DB matches the frozen snapshot

```bash
python3 scripts/run_release_matched_benchmark.py \
  --manifest artifacts/paper_release_2026-03-20/manifest.json \
  --db /path/to/release/decisions.db \
  --report-json artifacts/paper_release_2026-03-20/release_match_check.json \
  --benchmark-json artifacts/paper_release_2026-03-20/benchmark_report_release_matched.json
```

This must exit cleanly. If it fails, do not update the paper yet.

## 3. Rewrite the paper from the frozen release

Without graph-count refresh:

```bash
python3 scripts/update_paper_from_release.py \
  --manifest artifacts/paper_release_2026-03-20/manifest.json
```

With graph-count refresh:

```bash
python3 scripts/update_paper_from_release.py \
  --manifest artifacts/paper_release_2026-03-20/manifest.json \
  --graph-db /path/to/release/reference_graph.db
```

This updates both:

- `docs/paper/opencaselaw-arxiv-final.md`
- `docs/paper/opencaselaw-arxiv-draft.md`

## 4. Sanity checks

```bash
diff -u docs/paper/opencaselaw-arxiv-draft.md docs/paper/opencaselaw-arxiv-final.md
python3 -m py_compile scripts/build_paper_release_bundle.py scripts/run_release_matched_benchmark.py scripts/update_paper_from_release.py
pytest tests/test_release_matched_benchmark.py test_search_benchmark_resolution.py tests/test_bge_egmr.py test_publish_order.py
```

## 5. Submission standard

Do not submit unless all of the following are true:

- the paper points to `artifacts/paper_release_2026-03-20/*`
- the benchmark report used in the paper is `benchmark_report_release_matched.json`
- `release_match_check.json` says `"matched": true`
- the paper and bundle counts agree exactly
