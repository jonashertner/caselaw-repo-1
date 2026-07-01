#!/usr/bin/env python3
"""Dispatch health alerts — poll /metrics/health and forward alerts to ntfy.

``health_alerts.py`` computes would-fire alerts on every /metrics/health
request, but nothing ever delivered them (dark 2026-05-17 to 2026-07-01;
the follow-up notifier PR promised in docs/observability.md D2b). This
script is that notifier: a 15-minute systemd timer polls a local MCP
worker, dedups against ``logs/health_alerts_state.json``, and posts to
ntfy with the same dedup / re-nag / all-clear pattern as
``check_output_freshness.py``.

Behavior per alert key:
- newly firing                 -> send immediately
- still firing                 -> re-nag once every RENAG_HOURS
- no longer firing             -> send one all-clear, forget the key
- endpoint unreachable 3x in a row -> that is itself an alert (workers down)

Never raises out of main(); always exits 0 so the timer unit can't land
in a failed state and stop firing.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
STATE_PATH = Path(os.environ.get(
    "HEALTH_ALERTS_STATE", str(REPO / "logs" / "health_alerts_state.json")))
NTFY_URL = os.environ.get("HEALTH_ALERTS_NTFY", "https://ntfy.sh/opencaselaw-publish")
WORKER_PORTS = range(8770, 8778)
RENAG_HOURS = 24.0
UNREACHABLE_LIMIT = 3  # consecutive misses before "workers down" fires


def fetch_health(timeout: float = 6.0) -> dict | None:
    """First worker that answers wins. None if all 8 are unreachable."""
    for port in WORKER_PORTS:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/metrics/health", timeout=timeout,
            ) as resp:
                return json.load(resp)
        except Exception:
            continue
    return None


def decide(alerts: list[dict], state: dict, now: float):
    """Pure dedup logic (unit-tested; no I/O).

    state: {key: {"last_sent": ts, "level": str}} for alerts already sent.
    Returns (to_send, all_clear_keys, new_state).
    """
    current = {a["key"]: a for a in alerts if a.get("key")}
    to_send: list[dict] = []
    new_state: dict = {}
    for key, alert in current.items():
        prev = state.get(key)
        if prev is None or now - prev.get("last_sent", 0) >= RENAG_HOURS * 3600:
            to_send.append(alert)
            new_state[key] = {"last_sent": now, "level": alert.get("level", "warning")}
        else:
            new_state[key] = prev
    all_clear_keys = [k for k in state if k not in current]
    return to_send, all_clear_keys, new_state


def send(title: str, message: str, priority: str = "default", tags: str = "warning") -> bool:
    req = urllib.request.Request(
        NTFY_URL, data=message.encode(),
        headers={"Title": title, "Priority": priority, "Tags": tags},
    )
    try:
        urllib.request.urlopen(req, timeout=10).read()
        return True
    except Exception as e:  # noqa: BLE001
        print(f"ntfy send failed: {e}", file=sys.stderr)
        return False


def _write_state(payload: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=1))
    os.replace(tmp, STATE_PATH)


def main() -> int:
    now = time.time()
    raw: dict = {}
    if STATE_PATH.exists():
        try:
            raw = json.loads(STATE_PATH.read_text())
        except Exception:  # noqa: BLE001
            raw = {}
    state = {k: v for k, v in raw.items() if not k.startswith("_")}
    unreachable = raw.get("_unreachable", {"count": 0, "last_sent": 0})

    payload = fetch_health()
    if payload is None:
        unreachable["count"] = int(unreachable.get("count", 0)) + 1
        if (unreachable["count"] >= UNREACHABLE_LIMIT
                and now - unreachable.get("last_sent", 0) >= RENAG_HOURS * 3600):
            if send("OpenCaseLaw CRITICAL: health endpoint unreachable",
                    f"/metrics/health unreachable on 127.0.0.1:8770-8777 "
                    f"({unreachable['count']} consecutive polls) — MCP workers may be down",
                    priority="high", tags="rotating_light"):
                unreachable["last_sent"] = now
        _write_state({**state, "_unreachable": unreachable})
        print(f"health endpoint unreachable ({unreachable['count']} consecutive)")
        return 0

    alerts = payload.get("alerts_dry_run") or []
    to_send, all_clear_keys, new_state = decide(alerts, state, now)
    for alert in to_send:
        level = alert.get("level", "warning")
        send(f"OpenCaseLaw {level}: {alert['key']}",
             alert.get("message", alert["key"]),
             priority="high" if level == "critical" else "default",
             tags="rotating_light" if level == "critical" else "warning")
    for key in all_clear_keys:
        send(f"OpenCaseLaw all-clear: {key}", f"{key} is no longer firing",
             tags="white_check_mark")
    _write_state(new_state)  # reachable again -> unreachable counter resets
    print(f"{len(alerts)} active, sent {len(to_send)}, cleared {len(all_clear_keys)}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa: BLE001 - the timer must never go failed
        print(f"dispatch_health_alerts crashed: {e}", file=sys.stderr)
        sys.exit(0)
