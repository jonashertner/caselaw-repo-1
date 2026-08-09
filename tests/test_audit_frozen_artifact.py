"""The frozen 400-row audit sample is a published artifact and must never
be mutated by a default invocation (external review, 2026-08-07: a
benchmark script that silently rewrites its input destroys the
evidentiary status of the frozen sample — and the v2 rename caught the
default doing exactly that).
"""
from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
FROZEN = REPO / "benchmarks" / "citation_precision_sample_400.jsonl"


def _sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_default_run_leaves_frozen_artifact_byte_identical(tmp_path):
    before = _sha256(FROZEN)
    r = subprocess.run(
        [sys.executable, "benchmarks/citation_precision_audit_rules.py"],
        cwd=REPO, capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stderr[-800:]
    assert _sha256(FROZEN) == before, (
        "default invocation mutated the frozen v1 artifact")
    assert "400" in r.stdout  # it still adjudicated, it just didn't write


def _dirty_files() -> set[str]:
    out = subprocess.run(["git", "diff", "--name-only", "--", "benchmarks/"],
                         cwd=REPO, capture_output=True, text=True, timeout=60)
    return set(out.stdout.split())


def test_default_run_dirties_no_additional_tracked_file():
    """Compares before/after rather than demanding a clean tree: a developer
    may legitimately have uncommitted edits in benchmarks/, and the property
    under test is that the SCRIPT adds nothing to that set."""
    before = _dirty_files()
    subprocess.run(
        [sys.executable, "benchmarks/citation_precision_audit_rules.py"],
        cwd=REPO, capture_output=True, text=True, timeout=300)
    assert _dirty_files() - before == set(), (
        "default invocation dirtied tracked files")


def test_working_copies_still_get_write_back(tmp_path):
    """The gate protects the frozen file by NAME; working files keep the
    write-back workflow the TUI depends on."""
    work = tmp_path / "sample_working.jsonl"
    shutil.copy(FROZEN, work)
    before = _sha256(work)
    r = subprocess.run(
        [sys.executable, "benchmarks/citation_precision_audit_rules.py",
         "--sample", str(work)],
        cwd=REPO, capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stderr[-800:]
    assert _sha256(work) != before, "working copy should receive verdicts"
    assert '"rule_consistent"' in work.read_text().splitlines()[1]


def test_explicit_write_back_on_frozen_is_still_possible_but_loud(tmp_path):
    """--write-back on a copy named like the frozen file works when asked
    explicitly — the default is what must be safe."""
    copy = tmp_path / "citation_precision_sample_400.jsonl"
    shutil.copy(FROZEN, copy)
    before = _sha256(copy)
    r = subprocess.run(
        [sys.executable, "benchmarks/citation_precision_audit_rules.py",
         "--sample", str(copy), "--write-back"],
        cwd=REPO, capture_output=True, text=True, timeout=300)
    assert r.returncode == 0
    assert _sha256(copy) != before
