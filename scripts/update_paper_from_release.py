#!/usr/bin/env python3
"""
Update the arXiv paper files from a frozen paper release bundle.

This rewrites the snapshot/date/path fields from the release manifest and
stats snapshot. If a graph DB is provided, it also refreshes the reference
database counts. If a release-matched benchmark report exists, it updates the
evaluation section to describe that report as the canonical benchmark.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PAPERS = [
    REPO_ROOT / "docs" / "paper" / "opencaselaw-arxiv-final.md",
    REPO_ROOT / "docs" / "paper" / "opencaselaw-arxiv-draft.md",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update paper markdown from release bundle")
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Path to paper release manifest.json",
    )
    parser.add_argument(
        "--graph-db",
        type=Path,
        help="Optional reference_graph.db path for refreshing Table 2 counts",
    )
    parser.add_argument(
        "--paper",
        type=Path,
        action="append",
        help="Paper file(s) to update. Defaults to draft + final.",
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path.resolve())


def _format_int(n: int) -> str:
    return f"{n:,}"


def _format_million(n: int) -> str:
    return f"{n / 1_000_000:.2f} million"


def _human_date(date_str: str) -> str:
    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return dt.strftime("%B %-d, %Y")


def _timestamp_z(date_str: str) -> str:
    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _top_sources_sentence(stats: dict) -> str:
    top = stats.get("top_courts", [])[:4]
    parts = [
        f"`{row['court']}` ({_format_int(row['count'])} decisions)"
        for row in top
    ]
    if len(parts) < 4:
        return ""
    return (
        "The largest single sources in the current snapshot are "
        + ", ".join(parts[:-1])
        + f", and {parts[-1]}."
    )


def _graph_counts(graph_db: Path) -> dict:
    conn = sqlite3.connect(str(graph_db))
    try:
        extracted = conn.execute("SELECT COUNT(*) FROM decision_citations").fetchone()[0]
        resolved = conn.execute(
            """
            SELECT COUNT(*) FROM decision_citations
            WHERE target_decision_id IS NOT NULL AND TRIM(target_decision_id) != ''
            """
        ).fetchone()[0]
        statutes = conn.execute("SELECT COUNT(*) FROM decision_statutes").fetchone()[0]
    finally:
        conn.close()
    resolution_rate = (resolved / extracted * 100.0) if extracted else 0.0
    return {
        "extracted": extracted,
        "resolved": resolved,
        "statutes": statutes,
        "resolution_rate": resolution_rate,
    }


def _benchmark_context(bundle_dir: Path) -> dict:
    matched = bundle_dir / "benchmark_report_release_matched.json"
    default = bundle_dir / "benchmark_report.json"
    report_path = matched if matched.exists() else default
    report = _load_json(report_path)
    summary = report["summary"]
    verification = report.get("release_verification")
    is_release_matched = bool(verification and verification.get("matched"))
    return {
        "path": _display_path(report_path),
        "summary": summary,
        "release_matched": is_release_matched,
    }


def _replace(pattern: str, repl: str, text: str, *, count: int = 0) -> str:
    new_text, n = re.subn(pattern, repl, text, count=count, flags=re.MULTILINE)
    if n == 0:
        raise ValueError(f"Pattern not found: {pattern}")
    return new_text


def update_paper(path: Path, manifest: dict, stats: dict, benchmark: dict, graph: dict | None) -> None:
    text = path.read_text(encoding="utf-8")
    bundle_dir_ref = _display_path(Path(args.manifest).resolve().parent)

    generated_at = stats["generated_at"]
    snapshot_human = _human_date(generated_at)
    snapshot_timestamp = _timestamp_z(generated_at)
    total = stats["total"]
    court_count = stats["court_count"]
    by_language = stats["by_language"]
    federal_decisions = stats["federal_vs_cantonal"]["federal"]
    cantonal_decisions = stats["federal_vs_cantonal"]["cantonal"]
    federal_sources = sum(1 for row in stats["by_court"] if row.get("canton") == "CH")
    cantonal_sources = sum(1 for row in stats["by_court"] if row.get("canton") and row.get("canton") != "CH")
    earliest = stats["date_range"]["earliest"]
    latest = stats["date_range"]["latest"]

    de = by_language["de"]
    fr = by_language["fr"]
    it = by_language["it"]
    de_pct1 = f"{de / total * 100:.1f}"
    fr_pct1 = f"{fr / total * 100:.1f}"
    it_pct1 = f"{it / total * 100:.1f}"
    de_pct2 = f"{de / total * 100:.2f}"
    fr_pct2 = f"{fr / total * 100:.2f}"
    it_pct2 = f"{it / total * 100:.2f}"

    text = _replace(
        r"In the repository snapshot generated on [A-Z][a-z]+ \d{1,2}, \d{4}, the dataset contains [\d,]+ decisions from \d+ federal, cantonal, and regulatory courts or public bodies",
        f"In the repository snapshot generated on {snapshot_human}, the dataset contains {_format_int(total)} decisions from {court_count} federal, cantonal, and regulatory courts or public bodies",
        text,
        count=1,
    )
    text = _replace(
        r"The current snapshot contains [\d,]+ German decisions \([\d.]+%\), [\d,]+ French decisions \([\d.]+%\), and [\d,]+ Italian decisions \([\d.]+%\);",
        f"The current snapshot contains {_format_int(de)} German decisions ({de_pct1}%), {_format_int(fr)} French decisions ({fr_pct1}%), and {_format_int(it)} Italian decisions ({it_pct1}%);",
        text,
        count=1,
    )
    text = _replace(
        r"The [A-Z][a-z]+ \d{1,2}, \d{4} snapshot contains [\d,]+ decisions from \d+ courts or public bodies",
        f"The {snapshot_human} snapshot contains {_format_int(total)} decisions from {court_count} courts or public bodies",
        text,
        count=1,
    )
    text = _replace(
        r"it spans \d+ sources across all cantons and multiple federal and regulatory bodies",
        f"it spans {court_count} sources across all cantons and multiple federal and regulatory bodies",
        text,
        count=1,
    )
    text = _replace(
        r"Table 1 reports the frozen paper-release snapshot in `artifacts/[^`]+/stats_snapshot\.json`, generated on [A-Z][a-z]+ \d{1,2}, \d{4} and indexed in `artifacts/[^`]+/manifest\.json`\.",
        f"Table 1 reports the frozen paper-release snapshot in `{bundle_dir_ref}/stats_snapshot.json`, generated on {snapshot_human} and indexed in `{bundle_dir_ref}/manifest.json`.",
        text,
        count=1,
    )

    row_replacements = {
        r"(\| Snapshot timestamp \| ).+(\|)": rf"\g<1>{snapshot_timestamp} \2",
        r"(\| Decisions \| ).+(\|)": rf"\g<1>{_format_int(total)} \2",
        r"(\| Courts / public bodies \| ).+(\|)": rf"\g<1>{court_count} \2",
        r"(\| Federal sources \| ).+(\|)": rf"\g<1>{federal_sources} \2",
        r"(\| Cantonal sources \| ).+(\|)": rf"\g<1>{cantonal_sources} \2",
        r"(\| Federal decisions \| ).+(\|)": rf"\g<1>{_format_int(federal_decisions)} \2",
        r"(\| Cantonal decisions \| ).+(\|)": rf"\g<1>{_format_int(cantonal_decisions)} \2",
        r"(\| Earliest decision date \| ).+(\|)": rf"\g<1>{earliest} \2",
        r"(\| Latest decision date \| ).+(\|)": rf"\g<1>{latest} \2",
        r"(\| German \| ).+(\|)": rf"\g<1>{_format_int(de)} ({de_pct2}%) \2",
        r"(\| French \| ).+(\|)": rf"\g<1>{_format_int(fr)} ({fr_pct2}%) \2",
        r"(\| Italian \| ).+(\|)": rf"\g<1>{_format_int(it)} ({it_pct2}%) \2",
    }
    for pattern, repl in row_replacements.items():
        text = _replace(pattern, repl, text, count=1)

    top_sentence = _top_sources_sentence(stats)
    if top_sentence:
        text = _replace(
            r"The largest single sources in the current snapshot are .+?\.",
            top_sentence,
            text,
            count=1,
        )

    if graph is not None:
        extracted_m = _format_million(graph["extracted"])
        resolved_m = _format_million(graph["resolved"])
        statutes_m = _format_million(graph["statutes"])
        rate = f"{graph['resolution_rate']:.1f}%"
        text = _replace(
            r"a reference database with [\d.]+ million extracted case-citation references, [\d.]+ million resolved in-corpus decision links, and [\d.]+ million decision-statute links",
            f"a reference database with {extracted_m} extracted case-citation references, {resolved_m} resolved in-corpus decision links, and {statutes_m} decision-statute links",
            text,
            count=1,
        )
        text = _replace(r"(\| Extracted case-citation references \| ).+(\|)", rf"\g<1>{extracted_m} \2", text, count=1)
        text = _replace(r"(\| Resolved source-reference pairs \| ).+(\|)", rf"\g<1>{resolved_m} \2", text, count=1)
        text = _replace(r"(\| Resolution rate \| ).+(\|)", rf"\g<1>{rate} \2", text, count=1)
        text = _replace(r"(\| Decision-statute links \| ).+(\|)", rf"\g<1>{statutes_m} \2", text, count=1)
        text = _replace(
            r"The important distinction is that `[\d.]+ million` refers to extracted case-citation references, whereas `[\d.]+ million` refers to decision-statute mention links\.",
            f"The important distinction is that `{extracted_m}` refers to extracted case-citation references, whereas `{statutes_m}` refers to decision-statute mention links.",
            text,
            count=1,
        )

    # Evaluation assets and benchmark wording
    text = _replace(
        r"- `artifacts/[^`]+/benchmark_golden\.json`",
        f"- `{bundle_dir_ref}/benchmark_golden.json`",
        text,
        count=1,
    )
    text = _replace(
        r"- `artifacts/[^`]+/manifest\.json`",
        f"- `{bundle_dir_ref}/manifest.json`",
        text,
        count=1,
    )

    summary = benchmark["summary"]
    if benchmark["release_matched"]:
        bench_para = (
            f"To anchor the current paper to a versioned result, the repository now includes `{benchmark['path']}`, "
            f"a release-matched run on the 100-query set against the same {_format_int(total)}-decision corpus snapshot summarized in Table 1. "
            f"On that artifact, the benchmark achieved MRR@{summary['k']} = {summary['mrr_at_k']:.4f}, "
            f"Recall@{summary['k']} = {summary['recall_at_k']:.4f}, nDCG@{summary['k']} = {summary['ndcg_at_k']:.4f}, "
            f"and Hit@1 = {summary['hit_at_1']:.2f}."
        )
        limitation = (
            "The benchmark is release-matched, but still not a shared-task evaluation set. "
            "The current report is reproducible and aligned to the frozen corpus snapshot, "
            "but the repository still does not provide multi-annotator labeling, agreement estimates, or a held-out test split."
        )
    else:
        bench_para = (
            f"To anchor the current paper to a versioned result, the repository now includes `{benchmark['path']}`, "
            f"a bundled copy of the frozen offline run on the 100-query set against a {_format_int(summary['db_rows'])}-row local `decisions.db`. "
            f"The corresponding `{bundle_dir_ref}/manifest.json` records explicitly that this operational search DB is a distinct artifact from the "
            f"{_format_int(total)}-decision corpus snapshot summarized in Table 1, so we report it as a release-adjacent offline baseline rather than as the canonical benchmark on the published corpus snapshot. "
            f"On that artifact, the offline baseline achieved MRR@{summary['k']} = {summary['mrr_at_k']:.4f}, "
            f"Recall@{summary['k']} = {summary['recall_at_k']:.4f}, nDCG@{summary['k']} = {summary['ndcg_at_k']:.4f}, "
            f"and Hit@1 = {summary['hit_at_1']:.2f}."
        )
        limitation = (
            f"The archived benchmark is operational, not release-matched. The bundled report is reproducible and useful, "
            f"but it runs on a larger local search DB than the {snapshot_human} corpus snapshot and reflects the offline local configuration available in that environment rather than a fully provisioned hosted deployment."
        )

    text = _replace(
        r"To anchor the current paper to a versioned result, the repository now includes `artifacts/[^`]+/benchmark_report\.json`,.+?Hit@1 = [\d.]+\.",
        bench_para,
        text,
        count=1,
    )
    text = _replace(
        r"- \*\*The archived benchmark is operational, not release-matched\.\*\* .+",
        f"- **{limitation.split('.')[0]}.** {'.'.join(limitation.split('.')[1:]).strip()}",
        text,
        count=1,
    )

    availability_replacements = {
        r"(\| Paper release manifest \| ).+(\|)": rf"\g<1>`{bundle_dir_ref}/manifest.json` \2",
        r"(\| Paper release stats snapshot \| ).+(\|)": rf"\g<1>`{bundle_dir_ref}/stats_snapshot.json` \2",
        r"(\| Paper release benchmark gold set \| ).+(\|)": rf"\g<1>`{bundle_dir_ref}/benchmark_golden.json` \2",
        r"(\| Paper release benchmark report \| ).+(\|)": rf"\g<1>`{benchmark['path']}` \2",
    }
    for pattern, repl in availability_replacements.items():
        text = _replace(pattern, repl, text, count=1)

    path.write_text(text, encoding="utf-8")


def main() -> int:
    parsed = parse_args()
    global args
    args = parsed
    manifest_path = parsed.manifest.resolve()
    manifest = _load_json(manifest_path)
    bundle_dir = manifest_path.parent
    stats = _load_json(bundle_dir / "stats_snapshot.json")
    benchmark = _benchmark_context(bundle_dir)
    graph = _graph_counts(parsed.graph_db.resolve()) if parsed.graph_db else None
    papers = [p.resolve() for p in (parsed.paper or DEFAULT_PAPERS)]
    for paper in papers:
        update_paper(paper, manifest, stats, benchmark, graph)
        print(f"Updated {paper}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
