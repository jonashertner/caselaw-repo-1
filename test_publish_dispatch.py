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
