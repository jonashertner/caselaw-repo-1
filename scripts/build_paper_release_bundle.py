#!/usr/bin/env python3
"""
Build a self-contained paper release bundle with immutable stats, benchmark,
paper text, and checksum metadata.

Example:
    python3 scripts/build_paper_release_bundle.py \
        --release-id opencaselaw-paper-2026-03-18 \
        --stats-rev a566e995123a232c4227ef61c323282d7c4f41de \
        --output-dir artifacts/paper_release_2026-03-18

    python3 scripts/build_paper_release_bundle.py \
        --release-id opencaselaw-paper-2026-03-20 \
        --stats-file docs/stats.json \
        --output-dir artifacts/paper_release_2026-03-20
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "artifacts" / "paper_release_2026-03-18"
DEFAULT_BENCHMARK_JSON = REPO_ROOT / "benchmarks" / "search_benchmark_2026-03-19_offline_full.json"
DEFAULT_GOLDEN_JSON = REPO_ROOT / "benchmarks" / "search_relevance_golden.json"
DEFAULT_PAPER = REPO_ROOT / "docs" / "paper" / "opencaselaw-arxiv-final.md"
DEFAULT_STATS_PATH = "docs/stats.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build immutable paper release bundle")
    parser.add_argument(
        "--release-id",
        default="opencaselaw-paper-2026-03-18",
        help="Human-readable release identifier",
    )
    stats_group = parser.add_mutually_exclusive_group(required=True)
    stats_group.add_argument(
        "--stats-rev",
        help="Git revision containing the frozen stats snapshot",
    )
    stats_group.add_argument(
        "--stats-file",
        type=Path,
        help="Local stats snapshot JSON to bundle directly",
    )
    parser.add_argument(
        "--stats-path",
        default=DEFAULT_STATS_PATH,
        help="Path to stats file within the specified git revision",
    )
    parser.add_argument(
        "--benchmark-json",
        type=Path,
        default=DEFAULT_BENCHMARK_JSON,
        help="Frozen benchmark report JSON to bundle",
    )
    parser.add_argument(
        "--golden-json",
        type=Path,
        default=DEFAULT_GOLDEN_JSON,
        help="Golden relevance JSON to bundle",
    )
    parser.add_argument(
        "--paper",
        type=Path,
        default=DEFAULT_PAPER,
        help="Paper markdown file to bundle",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to write the bundle into",
    )
    return parser.parse_args()


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=REPO_ROOT, text=True).strip()


def _git_file(rev: str, path: str) -> bytes:
    return subprocess.check_output(["git", "show", f"{rev}:{path}"], cwd=REPO_ROOT)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_path(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path.resolve())


def _bundle_readme(
    release_id: str,
    stats: dict,
    benchmark: dict,
    stats_source_ref: str,
    output_dir_ref: str,
) -> str:
    summary = benchmark["summary"]
    return (
        f"# {release_id}\n\n"
        "This directory freezes the paper-facing artifacts used by the arXiv draft.\n\n"
        "## Included files\n\n"
        "- `manifest.json`: machine-readable release manifest\n"
        "- `checksums.sha256`: SHA-256 checksums for bundled files\n"
        "- `paper.md`: bundled copy of the current paper text\n"
        "- `stats_snapshot.json`: frozen corpus stats snapshot\n"
        "- `benchmark_golden.json`: frozen benchmark judgments bundled with this release\n"
        "- `benchmark_report.json`: frozen offline benchmark report bundled with this release\n\n"
        "## Corpus snapshot\n\n"
        f"- Source snapshot reference: `{stats_source_ref}`\n"
        f"- Snapshot generated at: `{stats.get('generated_at')}`\n"
        f"- Decisions: `{stats.get('total')}`\n"
        f"- Courts/public bodies: `{stats.get('court_count')}`\n"
        f"- Date range: `{stats.get('date_range', {}).get('earliest')}` to `{stats.get('date_range', {}).get('latest')}`\n\n"
        "## Retrieval benchmark\n\n"
        f"- Queries evaluated: `{summary['queries_evaluated']}` / `{summary['queries_total']}`\n"
        f"- Benchmark DB rows: `{summary['db_rows']}`\n"
        f"- MRR@{summary['k']}: `{summary['mrr_at_k']:.4f}`\n"
        f"- Recall@{summary['k']}: `{summary['recall_at_k']:.4f}`\n"
        f"- nDCG@{summary['k']}: `{summary['ndcg_at_k']:.4f}`\n"
        f"- Hit@1: `{summary['hit_at_1']:.2f}`\n\n"
        "## Important caveat\n\n"
        "The bundled benchmark report is reproducible and inspectable, but it is not "
        "release-matched to the corpus snapshot in `stats_snapshot.json`: it was run "
        "against a larger local `decisions.db`. The manifest records that mismatch "
        "explicitly so the paper can distinguish corpus-release counts from offline "
        "retrieval-baseline counts.\n\n"
        "## To produce a true release-matched benchmark\n\n"
        "Once the exact release-matched `decisions.db` is available, run:\n\n"
        "```bash\n"
        "python3 scripts/run_release_matched_benchmark.py "
        f"--manifest {output_dir_ref}/manifest.json "
        "--db /path/to/decisions.db "
        f"--report-json {output_dir_ref}/release_match_check.json "
        f"--benchmark-json {output_dir_ref}/benchmark_report_release_matched.json\n"
        "```\n"
    )


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir_ref = _display_path(output_dir)

    benchmark_path = args.benchmark_json.resolve()
    golden_path = args.golden_json.resolve()
    paper_path = args.paper.resolve()

    if not benchmark_path.exists():
        raise FileNotFoundError(f"Benchmark report not found: {benchmark_path}")
    if not golden_path.exists():
        raise FileNotFoundError(f"Golden benchmark file not found: {golden_path}")
    if not paper_path.exists():
        raise FileNotFoundError(f"Paper file not found: {paper_path}")

    repo_head = _git("rev-parse", "HEAD")

    if args.stats_file:
        stats_file_path = args.stats_file.resolve()
        if not stats_file_path.exists():
            raise FileNotFoundError(f"Stats file not found: {stats_file_path}")
        stats_source_rev = None
        stats_source_label = str(stats_file_path.relative_to(REPO_ROOT))
        stats_bytes = stats_file_path.read_bytes()
    else:
        stats_source_rev = _git("rev-parse", args.stats_rev)
        stats_source_label = f"{stats_source_rev}:{args.stats_path}"
        stats_bytes = _git_file(stats_source_rev, args.stats_path)

    stats = json.loads(stats_bytes.decode("utf-8"))
    benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
    golden = json.loads(golden_path.read_text(encoding="utf-8"))

    stats_snapshot_path = output_dir / "stats_snapshot.json"
    benchmark_report_path = output_dir / "benchmark_report.json"
    benchmark_golden_path = output_dir / "benchmark_golden.json"
    paper_bundle_path = output_dir / "paper.md"
    manifest_path = output_dir / "manifest.json"
    checksums_path = output_dir / "checksums.sha256"
    readme_path = output_dir / "README.md"

    stats_snapshot_path.write_bytes(stats_bytes)
    shutil.copyfile(benchmark_path, benchmark_report_path)
    shutil.copyfile(golden_path, benchmark_golden_path)
    shutil.copyfile(paper_path, paper_bundle_path)

    files = {
        "paper.md": paper_bundle_path,
        "stats_snapshot.json": stats_snapshot_path,
        "benchmark_golden.json": benchmark_golden_path,
        "benchmark_report.json": benchmark_report_path,
    }

    file_manifest = {}
    for name, path in files.items():
        file_manifest[name] = {
            "path": name,
            "bytes": path.stat().st_size,
            "sha256": _sha256_path(path),
        }

    benchmark_summary = benchmark["summary"]
    manifest = {
        "release_id": args.release_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repo_head_revision": repo_head,
        "stats_source": {
            "type": "git" if stats_source_rev else "file",
            "label": stats_source_label,
            "git_revision": stats_source_rev,
            "git_path": args.stats_path if stats_source_rev else None,
            "file_path": str(stats_file_path) if args.stats_file else None,
            "generated_at": stats.get("generated_at"),
            "sha256": file_manifest["stats_snapshot.json"]["sha256"],
        },
        "paper": {
            "source_path": str(paper_path.relative_to(REPO_ROOT)),
            "sha256": file_manifest["paper.md"]["sha256"],
        },
        "corpus_snapshot": {
            "decisions": stats.get("total"),
            "court_count": stats.get("court_count"),
            "date_range": stats.get("date_range"),
            "by_language": stats.get("by_language"),
        },
        "benchmark": {
            "source_path": str(benchmark_path.relative_to(REPO_ROOT)),
            "golden_source_path": str(golden_path.relative_to(REPO_ROOT)),
            "db_path": benchmark_summary["db_path"],
            "db_rows": benchmark_summary["db_rows"],
            "queries_total": benchmark_summary["queries_total"],
            "queries_evaluated": benchmark_summary["queries_evaluated"],
            "k": benchmark_summary["k"],
            "metrics": {
                "mrr_at_k": benchmark_summary["mrr_at_k"],
                "recall_at_k": benchmark_summary["recall_at_k"],
                "ndcg_at_k": benchmark_summary["ndcg_at_k"],
                "hit_at_1": benchmark_summary["hit_at_1"],
                "latency_ms_avg": benchmark_summary["latency_ms_avg"],
                "latency_ms_p95": benchmark_summary["latency_ms_p95"],
            },
            "release_matched": benchmark_summary["db_rows"] == stats.get("total"),
            "release_match_note": (
                "Benchmark DB row count matches corpus snapshot."
                if benchmark_summary["db_rows"] == stats.get("total")
                else (
                    "Benchmark DB row count does not match corpus snapshot. "
                    "Treat this as a release-adjacent offline baseline, not as "
                    "the canonical benchmark on the bundled corpus snapshot."
                )
            ),
            "reproduction_command": (
                "python3 benchmarks/run_search_benchmark.py "
                f"--db {benchmark_summary['db_path']} "
                f"-k {benchmark_summary['k']} "
                f"--json-output {str(benchmark_path.relative_to(REPO_ROOT))}"
            ),
            "release_matched_command_template": (
                "python3 scripts/run_release_matched_benchmark.py "
                f"--manifest {output_dir_ref}/manifest.json "
                "--db /path/to/decisions.db "
                f"--report-json {output_dir_ref}/release_match_check.json "
                f"--benchmark-json {output_dir_ref}/benchmark_report_release_matched.json"
            ),
        },
        "golden_summary": {
            "queries": len(golden.get("queries", [])),
            "sha256": file_manifest["benchmark_golden.json"]["sha256"],
        },
        "files": file_manifest,
    }

    _write_json(manifest_path, manifest)
    readme_path.write_text(
        _bundle_readme(
            args.release_id,
            stats,
            benchmark,
            stats_source_rev or stats_source_label,
            output_dir_ref,
        ),
        encoding="utf-8",
    )

    checksum_entries = []
    for name in ["README.md", "manifest.json", "paper.md", "stats_snapshot.json", "benchmark_golden.json", "benchmark_report.json"]:
        path = output_dir / name
        checksum_entries.append(f"{_sha256_path(path)}  {name}")
    checksums_path.write_text("\n".join(checksum_entries) + "\n", encoding="utf-8")

    print(f"Wrote paper release bundle to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
