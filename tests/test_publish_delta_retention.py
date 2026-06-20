"""Bounded retention for the delta/snapshot build scratch (publish_delta).

The scratch tree under build_dir/{snapshot,delta}/<YYYY-MM-DD> previously only
removed the CURRENT date's dir, so dated dirs accumulated one per nightly run —
~17 GB on the root disk, the suspected cause of the 2026-06-15 root-fill
publish failure. _prune_old_build_dirs keeps only the N newest (incl. the keep
date). (The scratch is also relocated off root /tmp to the data volume.)
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from search_stack import publish_delta  # noqa: E402


def _mk(parent: Path, *dates):
    parent.mkdir(parents=True, exist_ok=True)
    for d in dates:
        (parent / d).mkdir()


def test_keeps_only_retain_newest(tmp_path):
    parent = tmp_path / "delta"
    _mk(parent, "2026-04-27", "2026-06-10", "2026-06-14", "2026-06-15")
    # about to build 2026-06-16; keep last 3 incl. today (today not yet created)
    publish_delta._prune_old_build_dirs(parent, keep="2026-06-16", retain=3)
    assert sorted(p.name for p in parent.iterdir()) == ["2026-06-14", "2026-06-15"]


def test_retains_keep_even_if_not_newest(tmp_path):
    parent = tmp_path / "snapshot"
    _mk(parent, "2026-06-10", "2026-06-14", "2026-06-15")
    publish_delta._prune_old_build_dirs(parent, keep="2026-06-10", retain=2)
    remaining = sorted(p.name for p in parent.iterdir())
    assert "2026-06-10" in remaining          # keep always survives
    assert remaining == ["2026-06-10", "2026-06-15"]


def test_noop_on_missing_parent(tmp_path):
    publish_delta._prune_old_build_dirs(tmp_path / "nope", keep="2026-06-16")  # no raise


def test_under_retain_keeps_all(tmp_path):
    parent = tmp_path / "delta"
    _mk(parent, "2026-06-14", "2026-06-15")
    publish_delta._prune_old_build_dirs(parent, keep="2026-06-16", retain=3)
    assert sorted(p.name for p in parent.iterdir()) == ["2026-06-14", "2026-06-15"]


def test_ignores_non_dir_entries(tmp_path):
    parent = tmp_path / "delta"
    parent.mkdir()
    (parent / "2026-06-10").mkdir()
    (parent / "stray.txt").write_text("x")
    publish_delta._prune_old_build_dirs(parent, keep="2026-06-16", retain=1)
    assert (parent / "stray.txt").exists()          # file untouched
    assert not (parent / "2026-06-10").exists()     # stale dated dir pruned


def test_publish_sqlite_snapshot_retains_n_previous(monkeypatch):
    """Issue #19: keep the N newest HF snapshots; delete only older ones
    (+ their .sha256), giving clients a grace window after a manifest swap."""
    snaps = [
        f"artifacts/sqlite/snapshots/2026-05-2{d}.decisions.sqlite.zst"
        for d in (1, 2, 3, 4, 5)
    ]
    monkeypatch.setattr(publish_delta, "_list_snapshot_paths", lambda repo, token: snaps)
    deletes: list[str] = []
    monkeypatch.setattr(
        publish_delta, "_delete_file",
        lambda repo, path, token, commit_message: deletes.append(path),
    )

    pruned = publish_delta._prune_old_snapshots("example/repo", "token", retain=3)

    # newest 3 (23/24/25) kept; oldest 2 (21/22) pruned with their checksums
    assert pruned == snaps[:2]
    assert deletes == [
        "artifacts/sqlite/snapshots/2026-05-21.decisions.sqlite.zst",
        "artifacts/sqlite/snapshots/2026-05-21.decisions.sqlite.zst.sha256",
        "artifacts/sqlite/snapshots/2026-05-22.decisions.sqlite.zst",
        "artifacts/sqlite/snapshots/2026-05-22.decisions.sqlite.zst.sha256",
    ]


def test_prune_old_snapshots_noop_within_retain(monkeypatch):
    """Fewer snapshots than `retain` → nothing deleted (the grace window)."""
    snaps = ["artifacts/sqlite/snapshots/2026-05-24.decisions.sqlite.zst",
             "artifacts/sqlite/snapshots/2026-05-25.decisions.sqlite.zst"]
    monkeypatch.setattr(publish_delta, "_list_snapshot_paths", lambda repo, token: snaps)
    deletes: list[str] = []
    monkeypatch.setattr(
        publish_delta, "_delete_file",
        lambda repo, path, token, commit_message: deletes.append(path),
    )
    assert publish_delta._prune_old_snapshots("example/repo", "token", retain=3) == []
    assert deletes == []
