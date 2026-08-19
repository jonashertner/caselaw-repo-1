from __future__ import annotations

import sys

import publish


def test_publish_manual_weekly_step_forces_execution(monkeypatch):
    called: dict[str, object] = {}

    def _fake_weekly_step(*, dry_run: bool = False, full_rebuild: bool = False) -> bool:
        called["dry_run"] = dry_run
        called["full_rebuild"] = full_rebuild
        return True

    monkeypatch.setattr(publish, "STEPS", [("2d", "Quality Enrichment", _fake_weekly_step)])
    monkeypatch.setattr(sys, "argv", ["publish.py", "--step", "2d", "--dry-run"])

    publish.main()

    assert called["dry_run"] is True
    # Manual step mode should force weekly-gated step execution.
    assert called["full_rebuild"] is True


def test_publish_full_rebuild_flag_reaches_weekly_steps(monkeypatch):
    called: dict[str, object] = {}

    def _fake_weekly_step(*, dry_run: bool = False, full_rebuild: bool = False) -> bool:
        called["dry_run"] = dry_run
        called["full_rebuild"] = full_rebuild
        return True

    monkeypatch.setattr(publish, "STEPS", [("2c", "Reference Graph", _fake_weekly_step)])
    monkeypatch.setattr(sys, "argv", ["publish.py", "--dry-run", "--full-rebuild"])

    publish.main()

    assert called["dry_run"] is True
    assert called["full_rebuild"] is True


def test_publish_skips_ingest_by_default(monkeypatch):
    """Step 1 (ingest) should be skipped unless --ingest is passed."""
    called = {"ingest": False}

    def _fake_ingest(dry_run: bool = False) -> bool:
        called["ingest"] = True
        return True

    monkeypatch.setattr(publish, "STEPS", [(1, "Ingest", _fake_ingest)])
    monkeypatch.setattr(sys, "argv", ["publish.py", "--dry-run"])

    publish.main()

    assert called["ingest"] is False, "Ingest should not run without --ingest flag"


def test_publish_runs_ingest_with_flag(monkeypatch):
    """Step 1 runs when --ingest is passed."""
    called = {"ingest": False}

    def _fake_ingest(dry_run: bool = False) -> bool:
        called["ingest"] = True
        return True

    monkeypatch.setattr(publish, "STEPS", [(1, "Ingest", _fake_ingest)])
    monkeypatch.setattr(sys, "argv", ["publish.py", "--dry-run", "--ingest"])

    publish.main()

    assert called["ingest"] is True


def test_publish_sqlite_snapshot_can_run_without_delta_state(monkeypatch):
    """A one-off SQLite snapshot should not require hf_delta_snapshot.json."""
    called: dict[str, object] = {}

    def _fake_run_cmd(cmd, desc, dry_run=False, **kwargs):
        called["cmd"] = cmd
        called["desc"] = desc
        called["dry_run"] = dry_run
        return True

    monkeypatch.delenv("OCL_PUBLISH_DELTA", raising=False)
    monkeypatch.setenv("OCL_PUBLISH_SQLITE_SNAPSHOT", "1")
    # Force the snapshot path regardless of weekday — this test exercises
    # dispatch, not the Sunday cadence gate. Without this it silently passes
    # only on Sundays (and never ran in CI while the file sat at repo root).
    monkeypatch.setenv("OCL_PUBLISH_SQLITE_SNAPSHOT_WEEKDAY", "-1")
    monkeypatch.setattr(publish, "run_cmd", _fake_run_cmd)

    assert publish.step_7_publish_delta(dry_run=True) is True

    cmd = called["cmd"]
    assert "--snapshot-only" in cmd
    assert "--publish-snapshot" in cmd
    assert "--dry-run" in cmd
    assert called["desc"] == "Publish artifacts"


def test_append_run_record_is_durable_and_never_raises(tmp_path, monkeypatch):
    """publish_runs.jsonl is the pipeline's only durable structured
    record — one line per step and per run, written on success AND
    failure. Until 2026-08-19 a failed run left nothing behind (the
    failure branch exited before any marker), so build creep and gate
    timeouts were invisible until they hurt. Telemetry must also never
    be able to break the pipeline: a write failure is swallowed."""
    import json as _json
    import publish as p
    monkeypatch.setattr(p, "REPO_DIR", tmp_path)
    p._append_run_record({"type": "run_start", "run_id": "r1"})
    p._append_run_record({"type": "step", "run_id": "r1", "step": "2",
                          "status": "failed", "elapsed_s": 1.5})
    p._append_run_record({"type": "run_summary", "run_id": "r1",
                          "outcome": "failed", "failed_steps": ["2 (Build FTS5)"]})
    lines = [_json.loads(l) for l in
             (tmp_path / "state" / "publish_runs.jsonl").read_text().splitlines()]
    assert [l["type"] for l in lines] == ["run_start", "step", "run_summary"]
    assert lines[2]["outcome"] == "failed", "failed runs must leave a record"

    # A broken filesystem must not become a broken pipeline.
    monkeypatch.setattr(p, "REPO_DIR", tmp_path / "nope" / "\0bad")
    p._append_run_record({"type": "step"})   # must not raise
