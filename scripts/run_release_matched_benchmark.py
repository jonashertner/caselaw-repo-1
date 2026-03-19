#!/usr/bin/env python3
"""
Run the search benchmark only if a candidate decisions.db matches a frozen
paper release manifest.

Example:
    python3 scripts/run_release_matched_benchmark.py \
        --manifest artifacts/paper_release_2026-03-18/manifest.json \
        --db /path/to/decisions.db \
        --report-json artifacts/paper_release_2026-03-18/release_match_check.json \
        --benchmark-json artifacts/paper_release_2026-03-18/benchmark_report_release_matched.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run benchmark only for release-matched DBs")
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Path to paper release manifest.json",
    )
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Path to candidate decisions.db",
    )
    parser.add_argument(
        "-k",
        type=int,
        default=10,
        help="Top-k cutoff for the benchmark",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        help="Optional path to write release-match verification report",
    )
    parser.add_argument(
        "--benchmark-json",
        type=Path,
        help="Optional path to write release-matched benchmark JSON",
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _profile_db(db_path: Path) -> dict:
    conn = sqlite3.connect(str(db_path))
    try:
        total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        court_count = conn.execute(
            "SELECT COUNT(DISTINCT court) FROM decisions WHERE court IS NOT NULL AND TRIM(court) != ''"
        ).fetchone()[0]
        earliest, latest = conn.execute(
            """
            SELECT
                MIN(CASE WHEN decision_date IS NOT NULL AND decision_date != '' AND decision_date NOT LIKE '0000-%'
                    THEN decision_date END),
                MAX(CASE WHEN decision_date IS NOT NULL AND decision_date != '' AND decision_date NOT LIKE '0000-%'
                    THEN decision_date END)
            FROM decisions
            """
        ).fetchone()
        langs = {
            row[0]: row[1]
            for row in conn.execute(
                """
                SELECT language, COUNT(*)
                FROM decisions
                WHERE language IS NOT NULL AND TRIM(language) != ''
                GROUP BY language
                """
            ).fetchall()
        }
        return {
            "decisions": total,
            "court_count": court_count,
            "date_range": {
                "earliest": earliest,
                "latest": latest,
            },
            "by_language": langs,
        }
    finally:
        conn.close()


def _compare_profile(expected: dict, actual: dict) -> list[dict]:
    mismatches: list[dict] = []

    def add(field: str, expected_value, actual_value) -> None:
        mismatches.append(
            {
                "field": field,
                "expected": expected_value,
                "actual": actual_value,
            }
        )

    if expected.get("decisions") != actual.get("decisions"):
        add("decisions", expected.get("decisions"), actual.get("decisions"))
    if expected.get("court_count") != actual.get("court_count"):
        add("court_count", expected.get("court_count"), actual.get("court_count"))

    exp_dates = expected.get("date_range", {})
    act_dates = actual.get("date_range", {})
    for key in ("earliest", "latest"):
        if exp_dates.get(key) != act_dates.get(key):
            add(f"date_range.{key}", exp_dates.get(key), act_dates.get(key))

    exp_langs = expected.get("by_language", {})
    act_langs = actual.get("by_language", {})
    for lang in sorted(set(exp_langs) | set(act_langs)):
        if exp_langs.get(lang) != act_langs.get(lang):
            add(f"by_language.{lang}", exp_langs.get(lang), act_langs.get(lang))

    return mismatches


def _resolve_bundle_path(manifest_path: Path, relative_name: str) -> Path:
    return manifest_path.resolve().parent / relative_name


def _run_benchmark(db_path: Path, golden_path: Path, k: int, output_path: Path) -> None:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "benchmarks" / "run_search_benchmark.py"),
        "--db",
        str(db_path),
        "--golden",
        str(golden_path),
        "-k",
        str(k),
        "--json-output",
        str(output_path),
    ]
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def main() -> int:
    args = parse_args()
    manifest_path = args.manifest.resolve()
    db_path = args.db.expanduser().resolve()

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    manifest = _load_json(manifest_path)
    expected = manifest["corpus_snapshot"]
    actual = _profile_db(db_path)
    mismatches = _compare_profile(expected, actual)

    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "manifest_path": str(manifest_path),
        "release_id": manifest.get("release_id"),
        "db_path": str(db_path),
        "expected": expected,
        "actual": actual,
        "matched": not mismatches,
        "mismatches": mismatches,
    }

    if args.report_json:
        _write_json(args.report_json.resolve(), report)

    if mismatches:
        print("Release match check failed:", file=sys.stderr)
        for item in mismatches:
            print(
                f"- {item['field']}: expected {item['expected']!r}, got {item['actual']!r}",
                file=sys.stderr,
            )
        return 2

    golden_path = _resolve_bundle_path(manifest_path, "benchmark_golden.json")
    if not golden_path.exists():
        raise FileNotFoundError(
            f"Bundled golden benchmark file not found next to manifest: {golden_path}"
        )

    if args.benchmark_json:
        benchmark_json_path = args.benchmark_json.resolve()
    else:
        benchmark_json_path = _resolve_bundle_path(manifest_path, "benchmark_report_release_matched.json")

    with tempfile.TemporaryDirectory(prefix="release-benchmark-") as tmpdir:
        tmp_output = Path(tmpdir) / "benchmark.json"
        _run_benchmark(db_path=db_path, golden_path=golden_path, k=args.k, output_path=tmp_output)
        payload = _load_json(tmp_output)

    payload["release_verification"] = {
        "release_id": manifest.get("release_id"),
        "manifest_path": str(manifest_path),
        "db_path": str(db_path),
        "matched": True,
        "expected_snapshot": expected,
    }
    _write_json(benchmark_json_path, payload)
    print(f"Wrote release-matched benchmark: {benchmark_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
