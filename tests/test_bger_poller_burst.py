"""Offline logic tests for the bger_poller burst/pending design (2026-08-31).

No network, no subprocesses: exercises the pure decision helpers and the
state round-trip that the in-run burst retry relies on.
"""
import importlib.util
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "bger_poller", REPO / "scripts" / "bger_poller.py")
poller = importlib.util.module_from_spec(spec)
sys.modules["bger_poller"] = poller
spec.loader.exec_module(poller)


def test_qp_needed_skips_only_clean_empty_unforced():
    # the ONLY skippable case: clean scrape, nothing new, nothing pending
    assert poller._qp_needed(True, 0, False) is False
    # anything else publishes
    assert poller._qp_needed(True, 3, False) is True    # rows landed
    assert poller._qp_needed(True, 0, True) is True     # pending sweep
    assert poller._qp_needed(False, 0, False) is True   # failed scrape: JSONL may hold rows
    assert poller._qp_needed(False, 0, True) is True


def test_next_pending_transitions():
    # completed qp clears, failed qp sets — regardless of prior state
    assert poller._next_pending(True, True, True) is False
    assert poller._next_pending(False, True, False) is True
    # qp skipped: flag carries forward
    assert poller._next_pending(True, False, False) is True
    assert poller._next_pending(False, False, False) is False


def test_failing_streaks_count_runs_and_recover():
    f1 = poller._update_failing_streaks({}, {"1A_1/2026", "2B_2/2026"})
    assert f1 == {"1A_1/2026": 1, "2B_2/2026": 1}
    f2 = poller._update_failing_streaks(f1, {"1A_1/2026"})
    assert f2 == {"1A_1/2026": 2}          # recovered docket dropped
    assert f2["1A_1/2026"] >= poller.FAILING_STREAK_ALERT  # 2 runs => alert


def test_state_roundtrip_pending_and_alert_dedup(tmp_path, monkeypatch):
    monkeypatch.setattr(poller, "STATE_FILE", tmp_path / "state.json")
    poller._save_state("2026-08-31", {"4F_5/2026"},
                       failing={"4F_5/2026": 1}, pending=True,
                       alerts={"streak": "2026-08-31"})
    st = poller._load_state()
    assert st["pending_publish"] is True
    assert st["failing"] == {"4F_5/2026": 1}
    assert st["alerts"]["streak"] == "2026-08-31"


def test_missing_state_defaults_pending_false(tmp_path, monkeypatch):
    monkeypatch.setattr(poller, "STATE_FILE", tmp_path / "nope.json")
    st = poller._load_state()
    assert bool(st.get("pending_publish", False)) is False


# ── main()-level paths (review 2026-08-31: both shipped bugs lived here) ──

def _run_main(monkeypatch, tmp_path, *, feed, anomalous,
              state=None, argv=("bger_poller.py",),
              trigger=None, ingested=None):
    monkeypatch.setattr(poller, "STATE_FILE", tmp_path / "state.json")
    if state is not None:
        import json as _json
        (tmp_path / "state.json").write_text(_json.dumps(state))
    monkeypatch.setattr(poller, "_fetch_neuheiten", lambda d: set(feed))
    monkeypatch.setattr(poller, "_empty_feed_is_anomalous",
                        lambda now=None: anomalous)
    alerts_sent = []
    monkeypatch.setattr(poller, "_alert_ntfy",
                        lambda t, m, tags="warning": alerts_sent.append(t))
    stats_calls = []
    monkeypatch.setattr(poller, "_maybe_update_stats",
                        lambda n: stats_calls.append(n))
    _run_main.stats_calls = stats_calls
    monkeypatch.setattr(poller, "_late_scrapers_running", lambda: False)
    monkeypatch.setattr(poller.time, "sleep", lambda s_: None)
    if trigger is not None:
        calls = iter(trigger)
        monkeypatch.setattr(
            poller, "_trigger_scraper",
            lambda force_qp=True: next(calls))
    if ingested is not None:
        seq = iter(ingested)
        last = {"v": set()}
        def _ing(window_seconds=3600):
            try:
                last["v"] = set(next(seq))
            except StopIteration:
                pass
            return set(last["v"])
        monkeypatch.setattr(poller, "_recently_ingested_dockets", _ing)
    monkeypatch.setattr(poller.sys, "argv", list(argv))
    poller.main()
    return alerts_sent


def test_empty_feed_anomalous_path_no_crash_and_deduped(tmp_path, monkeypatch):
    # First run: alert fires once and the dedup marker persists.
    sent = _run_main(monkeypatch, tmp_path, feed=set(), anomalous=True)
    assert len(sent) == 1
    st = poller._load_state()
    from datetime import date
    assert st["alerts"]["empty"] == date.today().isoformat()
    # Second run same day: deduped, no second ntfy.
    sent2 = _run_main(monkeypatch, tmp_path, feed=set(), anomalous=True,
                      state=st)
    assert sent2 == []


def test_qp_skip_is_not_failure(tmp_path, monkeypatch):
    """Published subset stays seen and streaks advance across skip slots."""
    from datetime import date
    today = date.today().isoformat()
    # attempt 1: publishes OK; bursts: clean +0 scrapes, qp skipped
    trigger = [(True, 3, True, 3), (False, 0, False, 0),
               (False, 0, False, 0), (False, 0, False, 0)]
    ingested = [{"1A_1/2026", "2B_2/2026", "3C_3/2026"}]  # sticky thereafter
    _run_main(monkeypatch, tmp_path,
              feed={"1A_1/2026", "2B_2/2026", "3C_3/2026",
                    "4D_4/2026", "5E_5/2026"},
              anomalous=False, trigger=trigger, ingested=ingested)
    st = poller._load_state()
    assert set(st["dockets"]) >= {"1A_1/2026", "2B_2/2026", "3C_3/2026"}
    # the two unfetched dockets carry a failing streak despite skipped qp
    assert st["failing"] == {"4D_4/2026": 1, "5E_5/2026": 1}
    assert st["pending_publish"] is False
    assert st["date"] == today


def test_streak_alert_fires_on_pure_lag_runs(tmp_path, monkeypatch):
    """Two consecutive all-skip runs must reach the ntfy threshold."""
    feed = {"9X_9/2026"}
    trigger = [(False, 0, False, 0)] * 4
    sent1 = _run_main(monkeypatch, tmp_path, feed=feed, anomalous=False,
                      trigger=list(trigger), ingested=[set()])
    st1 = poller._load_state()
    assert st1["failing"] == {"9X_9/2026": 1}
    assert sent1 == []          # streak 1 < threshold
    sent2 = _run_main(monkeypatch, tmp_path, feed=feed, anomalous=False,
                      state=st1, trigger=list(trigger), ingested=[set()])
    st2 = poller._load_state()
    assert st2["failing"] == {"9X_9/2026": 2}
    assert len(sent2) == 1      # threshold 2 -> ntfy, deduped for the day


def test_stats_called_once_with_accumulated_total(tmp_path, monkeypatch):
    trigger = [(True, 2, True, 2), (True, 1, True, 1),
               (False, 0, False, 0), (False, 0, False, 0)]
    ingested = [{"1A_1/2026", "2B_2/2026"},
                {"1A_1/2026", "2B_2/2026", "3C_3/2026"}]
    _run_main(monkeypatch, tmp_path,
              feed={"1A_1/2026", "2B_2/2026", "3C_3/2026", "4D_4/2026"},
              anomalous=False, trigger=trigger, ingested=ingested)
    assert _run_main.stats_calls == [3]   # once, accumulated across attempts


def test_deadline_bail_preserves_published(tmp_path, monkeypatch):
    # exhaust the budget after the first attempt: published must survive
    ticks = iter([0.0, 999999.0, 999999.0, 999999.0, 999999.0, 999999.0,
                  999999.0, 999999.0])
    monkeypatch.setattr(poller.time, "monotonic",
                        lambda: next(ticks, 999999.0))
    trigger = [(True, 1, True, 1)]
    _run_main(monkeypatch, tmp_path,
              feed={"1A_1/2026", "9Z_9/2026"}, anomalous=False,
              trigger=trigger, ingested=[{"1A_1/2026"}])
    st = poller._load_state()
    assert "1A_1/2026" in st["dockets"]       # published survived the bail
    assert st["failing"] == {"9Z_9/2026": 1}


def test_uncovered_rows_set_pending(tmp_path, monkeypatch):
    """Rows another writer put in JSONL (late-scrapers) force a sweep."""
    trigger = [(False, 0, False, 0)] * 4      # every qp skipped
    _run_main(monkeypatch, tmp_path, feed={"7Q_7/2026"}, anomalous=False,
              trigger=trigger, ingested=[{"7Q_7/2026"}])
    st = poller._load_state()
    assert st["pending_publish"] is True      # publish debt recorded
    assert st["dockets"] == []                # but NOT marked seen
