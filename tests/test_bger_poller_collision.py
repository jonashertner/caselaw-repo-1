"""Poller classification of 'listed but not ingested' dockets (2026-09-03):
a docket already held under its docket-keyed id is a same-docket second
ruling, not a doc-service error page. Offline."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "bger_poller_collision", REPO / "scripts" / "bger_poller.py")
poller = importlib.util.module_from_spec(spec)
sys.modules["bger_poller_collision"] = poller
spec.loader.exec_module(poller)


def _state_dir(tmp_path, ids):
    (tmp_path / "state").mkdir(exist_ok=True)
    (tmp_path / "state" / "bger.jsonl").write_text("\n".join(ids) + "\n")


def test_held_under_docket_id_reads_scraper_state(tmp_path, monkeypatch):
    monkeypatch.setattr(poller, "REPO_DIR", tmp_path)
    _state_dir(tmp_path, ["bger_2C_532_2025", "bger_1C_1_2026"])
    held = poller._held_under_docket_id({"2C_532/2025", "9X_9/2026"})
    assert held == {"2C_532/2025"}


def test_held_under_docket_id_tolerates_missing_state(tmp_path, monkeypatch):
    monkeypatch.setattr(poller, "REPO_DIR", tmp_path)
    assert poller._held_under_docket_id({"2C_532/2025"}) == set()
    assert poller._held_under_docket_id(set()) == set()


def _run_main(monkeypatch, tmp_path, *, feed, state=None):
    monkeypatch.setattr(poller, "STATE_FILE", tmp_path / "state.json")
    if state is not None:
        (tmp_path / "state.json").write_text(json.dumps(state))
    monkeypatch.setattr(poller, "_fetch_neuheiten", lambda d: set(feed))
    monkeypatch.setattr(poller, "_empty_feed_is_anomalous",
                        lambda now=None: False)
    sent = []
    monkeypatch.setattr(poller, "_alert_ntfy",
                        lambda t, m, tags="warning": sent.append((t, m)))
    monkeypatch.setattr(poller, "_maybe_update_stats", lambda n: None)
    monkeypatch.setattr(poller, "_late_scrapers_running", lambda: False)
    monkeypatch.setattr(poller.time, "sleep", lambda s_: None)
    monkeypatch.setattr(poller, "_trigger_scraper",
                        lambda force_qp=True: (False, 0, False, 0))
    monkeypatch.setattr(poller, "_recently_ingested_dockets",
                        lambda window_seconds=3600: set())
    monkeypatch.setattr(poller.sys, "argv", ["bger_poller.py"])
    poller.main()
    return sent


def test_streak_alert_names_held_docket(tmp_path, monkeypatch):
    monkeypatch.setattr(poller, "REPO_DIR", tmp_path)
    _state_dir(tmp_path, ["bger_2C_532_2025"])
    feed = {"2C_532/2025", "9X_9/2026"}
    sent1 = _run_main(monkeypatch, tmp_path, feed=feed)
    assert sent1 == []
    sent2 = _run_main(monkeypatch, tmp_path, feed=feed,
                      state=poller._load_state())
    assert len(sent2) == 1
    title, msg = sent2[0]
    assert "2C_532/2025 (x2, held under docket id)" in msg
    assert "9X_9/2026 (x2)" in msg
    assert "held under docket id)" not in msg.split("9X_9/2026")[1]
