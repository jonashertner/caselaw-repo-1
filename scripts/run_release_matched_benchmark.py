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
import os
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_paper_release_bundle import _bundle_readme, _display_path, _sha256_path, _write_checksums

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


def _sanitize_external_path(path: Path) -> str:
    try:
        return _display_path(path)
    except Exception:
        return f"external:{path.name}"


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes"}


def _environment_profile(db_path: Path) -> dict:
    base = db_path.parent
    return {
        "graph_db_available": (base / "reference_graph.db").exists(),
        "vector_db_available": (base / "vectors.db").exists(),
        "statutes_db_available": (base / "statutes.db").exists(),
        "commentary_db_available": (base / "ok_commentaries.db").exists(),
        "anthropic_api_configured": bool(os.environ.get("ANTHROPIC_API_KEY")),
        "llm_expansion_enabled": _bool_env("LLM_EXPANSION_ENABLED", default=True),
        "llm_rerank_enabled": _bool_env("LLM_RERANK_ENABLED", default=True),
    }


def _profile_db(db_path: Path) -> dict:
    conn = sqlite3.connect(str(db_path))
    try:
        today_iso = datetime.now(timezone.utc).date().isoformat()
        total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        court_count = conn.execute(
            "SELECT COUNT(DISTINCT court) FROM decisions WHERE court IS NOT NULL AND TRIM(court) != ''"
        ).fetchone()[0]
        earliest, latest = conn.execute(
            """
            SELECT
                MIN(CASE WHEN decision_date IS NOT NULL AND decision_date != '' AND decision_date NOT LIKE '0000-%'
                    AND decision_date > '1800-01-01' AND decision_date <= ?
                    THEN decision_date END),
                MAX(CASE WHEN decision_date IS NOT NULL AND decision_date != '' AND decision_date NOT LIKE '0000-%'
                    AND decision_date > '1800-01-01' AND decision_date <= ?
                    THEN decision_date END)
            FROM decisions
            """
            ,
            (today_iso, today_iso),
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


def _refresh_manifest_and_bundle_metadata(
    manifest_path: Path,
    benchmark_json_path: Path,
    report_json_path: Path | None,
    db_path: Path,
    golden_path: Path,
    payload: dict,
) -> None:
    bundle_dir = manifest_path.parent
    manifest = _load_json(manifest_path)
    stats = _load_json(bundle_dir / "stats_snapshot.json")
    summary = payload["summary"]

    benchmark_rel = _display_path(benchmark_json_path)
    report_rel = _display_path(report_json_path) if report_json_path else None
    manifest_rel = _display_path(manifest_path)
    golden_rel = _display_path(golden_path)
    db_label = _sanitize_external_path(db_path)

    manifest["benchmark"] = {
        "source_path": benchmark_rel,
        "golden_source_path": golden_rel,
        "db_path": db_label,
        "db_rows": summary["db_rows"],
        "queries_total": summary["queries_total"],
        "queries_evaluated": summary["queries_evaluated"],
        "k": summary["k"],
        "metrics": {
            "mrr_at_k": summary["mrr_at_k"],
            "recall_at_k": summary["recall_at_k"],
            "ndcg_at_k": summary["ndcg_at_k"],
            "hit_at_1": summary["hit_at_1"],
            "latency_ms_avg": summary["latency_ms_avg"],
            "latency_ms_p95": summary["latency_ms_p95"],
        },
        "release_matched": True,
        "release_match_note": "Benchmark DB row count matches corpus snapshot.",
        "reproduction_command": (
            "python3 scripts/run_release_matched_benchmark.py "
            f"--manifest {manifest_rel} "
            "--db /path/to/decisions.db "
            f"--report-json {report_rel or _display_path(bundle_dir / 'release_match_check.json')} "
            f"--benchmark-json {benchmark_rel}"
        ),
        "release_matched_command_template": (
            "python3 scripts/run_release_matched_benchmark.py "
            f"--manifest {manifest_rel} "
            "--db /path/to/decisions.db "
            f"--report-json {report_rel or _display_path(bundle_dir / 'release_match_check.json')} "
            f"--benchmark-json {benchmark_rel}"
        ),
        "environment": payload.get("environment"),
    }

    if report_json_path and report_json_path.exists():
        manifest["release_match_check"] = {
            "path": report_rel,
            "matched": True,
            "sha256": _sha256_path(report_json_path),
        }
        manifest.setdefault("files", {})["release_match_check.json"] = {
            "path": report_json_path.name,
            "bytes": report_json_path.stat().st_size,
            "sha256": _sha256_path(report_json_path),
        }

    manifest.setdefault("files", {})[benchmark_json_path.name] = {
        "path": benchmark_json_path.name,
        "bytes": benchmark_json_path.stat().st_size,
        "sha256": _sha256_path(benchmark_json_path),
    }
    _write_json(manifest_path, manifest)

    stats_source_ref = manifest["stats_source"].get("label") or manifest["stats_source"].get("git_revision") or "stats_snapshot.json"
    readme = _bundle_readme(
        manifest["release_id"],
        stats,
        summary,
        stats_source_ref,
        _display_path(bundle_dir),
        release_matched=True,
        benchmark_report_name=benchmark_json_path.name,
        benchmark_environment=payload.get("environment"),
    )
    (bundle_dir / "README.md").write_text(readme, encoding="utf-8")
    _write_checksums(bundle_dir)


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
        "manifest_path": _display_path(manifest_path),
        "release_id": manifest.get("release_id"),
        "db_path": _sanitize_external_path(db_path),
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

    payload["summary"]["db_path"] = _sanitize_external_path(db_path)
    payload["summary"]["golden_path"] = _display_path(golden_path)
    payload["summary"]["golden_paths"] = [_display_path(golden_path)]
    payload["environment"] = _environment_profile(db_path)
    payload["release_verification"] = {
        "release_id": manifest.get("release_id"),
        "manifest_path": _display_path(manifest_path),
        "db_path": _sanitize_external_path(db_path),
        "matched": True,
        "expected_snapshot": expected,
    }
    _write_json(benchmark_json_path, payload)
    _refresh_manifest_and_bundle_metadata(
        manifest_path=manifest_path,
        benchmark_json_path=benchmark_json_path,
        report_json_path=args.report_json.resolve() if args.report_json else None,
        db_path=db_path,
        golden_path=golden_path,
        payload=payload,
    )
    print(f"Wrote release-matched benchmark: {benchmark_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
