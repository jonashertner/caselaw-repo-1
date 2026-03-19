#!/usr/bin/env python3
"""
Orchestrate a full paper release cut:

1. Freeze the release bundle from a stats snapshot
2. Verify that the candidate DB matches the frozen snapshot and run the benchmark
3. Rewrite the paper to cite the new bundle
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cut a full paper release bundle")
    parser.add_argument("--release-id", required=True, help="Release identifier, e.g. opencaselaw-paper-2026-03-20")
    parser.add_argument("--stats-file", type=Path, required=True, help="Frozen stats file for this release")
    parser.add_argument("--db", type=Path, required=True, help="Release-matched decisions.db")
    parser.add_argument("--output-dir", type=Path, required=True, help="Release bundle directory")
    parser.add_argument("--graph-db", type=Path, help="Optional release-matched reference_graph.db")
    return parser.parse_args()


def _run(*args: str) -> None:
    subprocess.run([sys.executable, *args], cwd=REPO_ROOT, check=True)


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    manifest = output_dir / "manifest.json"
    report_json = output_dir / "release_match_check.json"
    benchmark_json = output_dir / "benchmark_report_release_matched.json"

    _run(
        "scripts/build_paper_release_bundle.py",
        "--release-id", args.release_id,
        "--stats-file", str(args.stats_file),
        "--output-dir", str(output_dir),
    )
    _run(
        "scripts/run_release_matched_benchmark.py",
        "--manifest", str(manifest),
        "--db", str(args.db),
        "--report-json", str(report_json),
        "--benchmark-json", str(benchmark_json),
    )

    update_args = [
        "scripts/update_paper_from_release.py",
        "--manifest", str(manifest),
    ]
    if args.graph_db:
        update_args.extend(["--graph-db", str(args.graph_db)])
    _run(*update_args)

    print(f"Paper release cut complete: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
