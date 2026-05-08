"""Regression for the 2026-05-08 'stuck Neuheiten' bug.

Symptom (live): poller log
   10:00 NEW decisions detected: 27 (...)
   10:00 BGer document service error for {docket} — skipping x14+
   10:05 BGer scraper completed: 0 new decisions
   10:15 No new decisions since last check  ← THE BUG
   10:30 No new decisions since last check  ← still stuck
   10:45 No new decisions since last check  ← still stuck

Cause: ``_save_state`` was called BEFORE the scraper ran, with the
full ``current_dockets`` set — including the 27 the scraper failed
to fetch. Next poll computed ``current_dockets - prev_dockets`` =
empty, so the failed dockets were never retried.

Contract this test enforces: only dockets confirmed present in
JSONL get added to the seen-cache. Failed dockets stay un-seen so
the next poll retries them — bounded naturally by Neuheiten
rotation (~24h).
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def _write_jsonl(p: Path, rows: list[dict]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def test_recently_ingested_dockets_picks_up_fresh_only(tmp_path, monkeypatch):
    """Helper must include lines whose scraped_at is within window;
    exclude older ones."""
    import scripts.bger_poller as bp
    monkeypatch.setattr(bp, "REPO_DIR", tmp_path)
    jsonl = tmp_path / "output" / "decisions" / "bger.jsonl"
    now = datetime.now(timezone.utc)
    fresh_iso = now.isoformat()
    old_iso = "2026-04-01T08:00:00+00:00"
    _write_jsonl(jsonl, [
        {"docket_number": "OLD_1/2024", "scraped_at": old_iso},
        {"docket_number": "FRESH_1/2026", "scraped_at": fresh_iso},
        {"docket_number": "FRESH_2/2026", "scraped_at": fresh_iso},
    ])
    out = bp._recently_ingested_dockets(window_seconds=3600)
    assert out == {"FRESH_1/2026", "FRESH_2/2026"}, (
        f"recent-window should exclude OLD_1; got {out}"
    )


def test_failed_dockets_stay_unseen_for_next_poll(tmp_path, monkeypatch):
    """End-to-end: run the poller's inner gate logic. Some dockets
    succeed, some fail — only the successful ones land in state."""
    import scripts.bger_poller as bp
    monkeypatch.setattr(bp, "REPO_DIR", tmp_path)
    monkeypatch.setattr(bp, "STATE_FILE", tmp_path / "state.json")

    # Fake JSONL: only 2 of the 4 'new' dockets actually got saved
    # (the scraper failed to fetch the other 2 due to error pages).
    now_iso = datetime.now(timezone.utc).isoformat()
    jsonl = tmp_path / "output" / "decisions" / "bger.jsonl"
    _write_jsonl(jsonl, [
        {"docket_number": "OK_1/2026", "scraped_at": now_iso},
        {"docket_number": "OK_2/2026", "scraped_at": now_iso},
    ])

    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prev_dockets: set[str] = set()
    current_dockets = {"OK_1/2026", "OK_2/2026", "FAIL_1/2026", "FAIL_2/2026"}
    new_dockets = current_dockets - prev_dockets

    # Mirror the production gate (post-fix) logic
    ingested_recent = bp._recently_ingested_dockets(window_seconds=3600)
    seen_state = prev_dockets | (current_dockets & ingested_recent)
    bp._save_state(today_iso, seen_state)

    saved = json.loads((tmp_path / "state.json").read_text())
    assert set(saved["dockets"]) == {"OK_1/2026", "OK_2/2026"}, (
        f"only ingested dockets should be in state; got {saved['dockets']}"
    )
    # And the failed dockets are still 'new' next poll
    next_prev = set(saved["dockets"])
    next_new = current_dockets - next_prev
    assert next_new == {"FAIL_1/2026", "FAIL_2/2026"}, (
        f"failed dockets should still be retry-eligible; got {next_new}"
    )


def test_all_failed_state_unchanged(tmp_path, monkeypatch):
    """If the scraper failed on EVERY docket — the smoking-gun
    scenario from 2026-05-08 10:00 — state must remain empty so all
    27 are retried."""
    import scripts.bger_poller as bp
    monkeypatch.setattr(bp, "REPO_DIR", tmp_path)
    monkeypatch.setattr(bp, "STATE_FILE", tmp_path / "state.json")

    # JSONL has nothing fresh — scraper failed on all 27
    jsonl = tmp_path / "output" / "decisions" / "bger.jsonl"
    _write_jsonl(jsonl, [
        {"docket_number": "OLD_1/2024", "scraped_at": "2026-04-01T08:00:00+00:00"},
    ])

    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prev_dockets: set[str] = set()
    current_dockets = {f"D_{i}/2026" for i in range(27)}

    ingested_recent = bp._recently_ingested_dockets(window_seconds=3600)
    seen_state = prev_dockets | (current_dockets & ingested_recent)
    bp._save_state(today_iso, seen_state)

    saved = json.loads((tmp_path / "state.json").read_text())
    assert set(saved["dockets"]) == set(), (
        f"all-fail run should keep state empty; got {saved['dockets']}"
    )
    # Next poll: all 27 are 'new' again → retry
    next_new = current_dockets - set(saved["dockets"])
    assert next_new == current_dockets
