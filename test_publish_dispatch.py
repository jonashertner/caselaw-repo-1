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
