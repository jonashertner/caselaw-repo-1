# opencaselaw-paper-2026-03-20

This directory freezes the paper-facing artifacts used by the arXiv draft.

## Included files

- `manifest.json`: machine-readable release manifest
- `checksums.sha256`: SHA-256 checksums for bundled files
- `paper.md`: bundled copy of the current paper text
- `stats_snapshot.json`: frozen corpus stats snapshot
- `benchmark_golden.json`: frozen benchmark judgments bundled with this release
- `benchmark_report.json`: archived offline benchmark report copied into the bundle
- `benchmark_report_release_matched.json`: canonical release-matched benchmark report for this bundle
- `release_match_check.json`: verification report confirming the benchmark DB matches the frozen snapshot

## Corpus snapshot

- Source snapshot reference: `stats_snapshot.json`
- Snapshot generated at: `2026-03-20T08:54:27.170794+00:00`
- Decisions: `962724`
- Courts/public bodies: `102`
- Date range: `1875-01-01` to `2026-03-19`

## Retrieval benchmark

- Benchmark report: `benchmark_report_release_matched.json`
- Queries evaluated: `100` / `100`
- Benchmark DB rows: `962724`
- MRR@10: `0.6042`
- Recall@10: `0.5835`
- nDCG@10: `0.6062`
- Hit@1: `0.52`

## Release Status

The canonical benchmark report in `benchmark_report_release_matched.json` is release-matched to the corpus snapshot in `stats_snapshot.json`.

## Benchmark Environment

- Reference graph DB available: `True`
- Vector DB available: `False`
- Statutes DB available: `False`
- Commentary DB available: `False`
- Anthropic API configured: `False`

## To Reproduce the Canonical Benchmark

```bash
python3 scripts/run_release_matched_benchmark.py --manifest artifacts/paper_release_2026-03-20/manifest.json --db /path/to/decisions.db --report-json artifacts/paper_release_2026-03-20/release_match_check.json --benchmark-json artifacts/paper_release_2026-03-20/benchmark_report_release_matched.json
```
