from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from search_stack import publish_delta as pd


def _make_db(path: Path, *, rows: int = 2, user_version: int = 123) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY)")
        # Stand-in for the production FTS5 virtual table; the
        # _validate_snapshot_source check verifies its presence by name
        # via sqlite_master.
        conn.execute("CREATE TABLE decisions_fts (rowid INTEGER PRIMARY KEY)")
        for i in range(rows):
            conn.execute("INSERT INTO decisions (decision_id) VALUES (?)", (f"did-{i}",))
        conn.execute(f"PRAGMA user_version = {user_version}")
        conn.commit()
    finally:
        conn.close()


def test_set_snapshot_in_manifest_preserves_deltas() -> None:
    manifest = {
        "schema": "old",
        "generated_at": "old",
        "snapshot": None,
        "deltas": [{"date": "2026-05-24"}],
    }
    snapshot = {
        "date": "2026-05-25",
        "sqlite_zst": {"path": "artifacts/sqlite/snapshots/2026-05-25.decisions.sqlite.zst"},
        "rows": 2,
        "schema_version": 1,
    }

    updated = pd._set_snapshot_in_manifest(manifest, snapshot)

    assert updated["schema"] == pd.MANIFEST_SCHEMA
    assert updated["snapshot"] == snapshot
    assert updated["deltas"] == [{"date": "2026-05-24"}]


def test_build_sqlite_snapshot_compresses_and_records_metadata(tmp_path, monkeypatch) -> None:
    db = tmp_path / "decisions.db"
    _make_db(db, rows=3, user_version=456)

    def fake_compress(src: Path, dst: Path, level: int = 10) -> None:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)

    monkeypatch.setattr(pd, "_compress_zst", fake_compress)
    monkeypatch.setattr(pd, "_producer_commit", lambda: "abc123")

    info = pd.build_sqlite_snapshot(
        db_path=db,
        date="2026-05-25",
        build_dir=tmp_path / "build",
    )

    assert info["rows"] == 3
    assert info["db_generation"] == 456
    assert info["schema_version"] == pd.SQLITE_SNAPSHOT_SCHEMA_VERSION
    assert info["producer_commit"] == "abc123"
    assert info["sqlite_zst"].name == "2026-05-25.decisions.sqlite.zst"
    assert info["sqlite_zst_sha256"] == pd._sha256_file(info["sqlite_zst"])
    assert info["checksum"].read_text(encoding="utf-8").startswith(info["sqlite_zst_sha256"])


def test_validate_snapshot_source_rejects_missing_fts_table(tmp_path) -> None:
    """A DB without the FTS5 virtual table must be rejected before we
    compress + upload ~15-20 GB of useless artifact."""
    db = tmp_path / "decisions.db"
    conn = sqlite3.connect(db)
    try:
        conn.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO decisions (decision_id) VALUES ('x')")
        # NB: no decisions_fts — should trip the schema check
        conn.execute("PRAGMA user_version = 7")
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="missing expected table"):
        pd._validate_snapshot_source(db)


def test_build_sqlite_snapshot_rejects_source_change_during_compress(tmp_path, monkeypatch) -> None:
    db = tmp_path / "decisions.db"
    _make_db(db, rows=3, user_version=456)

    def fake_compress(src: Path, dst: Path, level: int = 10) -> None:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        conn = sqlite3.connect(src)
        try:
            conn.execute("INSERT INTO decisions (decision_id) VALUES ('late')")
            conn.commit()
        finally:
            conn.close()

    monkeypatch.setattr(pd, "_compress_zst", fake_compress)

    with pytest.raises(RuntimeError, match="snapshot source changed"):
        pd.build_sqlite_snapshot(
            db_path=db,
            date="2026-05-25",
            build_dir=tmp_path / "build",
        )


def test_publish_sqlite_snapshot_uploads_manifest_last_and_prunes_previous(tmp_path, monkeypatch) -> None:
    snapshot_file = tmp_path / "snapshot.zst"
    snapshot_file.write_bytes(b"sqlite")
    checksum_file = tmp_path / "snapshot.zst.sha256"
    checksum_file.write_text("sha  snapshot.zst\n", encoding="utf-8")

    build_info = {
        "date": "2026-05-25",
        "sqlite_zst": snapshot_file,
        "sqlite_zst_bytes": snapshot_file.stat().st_size,
        "sqlite_zst_sha256": "sha",
        "checksum": checksum_file,
        "checksum_bytes": checksum_file.stat().st_size,
        "rows": 3,
        "schema_version": 1,
        "db_generation": 456,
        "producer_commit": "abc123",
        "work_dir": tmp_path,
    }
    old_manifest = {
        "schema": pd.MANIFEST_SCHEMA,
        "generated_at": "old",
        "snapshot": {
            "date": "2026-05-24",
            "sqlite_zst": {
                "path": "artifacts/sqlite/snapshots/2026-05-24.decisions.sqlite.zst",
                "sha256": "old",
                "bytes": 1,
            },
            "rows": 2,
            "schema_version": 1,
        },
        "deltas": [{"date": "2026-05-24"}],
    }
    uploads: list[tuple[Path, str, str]] = []
    deletes: list[str] = []

    monkeypatch.setattr(pd, "_download_manifest", lambda hf_repo: old_manifest)

    def fake_upload(local: Path, repo: str, path_in_repo: str, token: str, commit_message: str) -> None:
        uploads.append((Path(local), path_in_repo, commit_message))

    def fake_delete(repo: str, path_in_repo: str, token: str, commit_message: str) -> None:
        deletes.append(path_in_repo)

    monkeypatch.setattr(pd, "_upload_file", fake_upload)
    monkeypatch.setattr(pd, "_delete_file", fake_delete)

    pd.publish_sqlite_snapshot(
        build_info=build_info,
        hf_repo="example/repo",
        hf_token="token",
    )

    assert [u[1] for u in uploads] == [
        "artifacts/sqlite/snapshots/2026-05-25.decisions.sqlite.zst",
        "artifacts/sqlite/snapshots/2026-05-25.decisions.sqlite.zst.sha256",
        "artifacts/manifest.json",
    ]

    manifest_upload = uploads[-1][0]
    manifest = json.loads(manifest_upload.read_text(encoding="utf-8"))
    assert manifest["snapshot"] == {
        "date": "2026-05-25",
        "sqlite_zst": {
            "path": "artifacts/sqlite/snapshots/2026-05-25.decisions.sqlite.zst",
            "sha256": "sha",
            "bytes": 6,
        },
        "rows": 3,
        "schema_version": 1,
        "db_generation": 456,
        "producer_commit": "abc123",
    }
    assert manifest["deltas"] == [{"date": "2026-05-24"}]
    assert deletes == [
        "artifacts/sqlite/snapshots/2026-05-24.decisions.sqlite.zst",
        "artifacts/sqlite/snapshots/2026-05-24.decisions.sqlite.zst.sha256",
    ]
