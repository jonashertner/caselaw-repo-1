"""BGer poller tunnel-routing + workday empty-feed alarm (2026-06-29 Incapsula
hard-block on the Hetzner IP). The poller must prefer the residential proxy
(skipping the Incapsula/PoW dance), and alarm on an empty feed only on workdays
(BGer doesn't publish on weekends, so weekend zeros are normal).
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import importlib.util
spec = importlib.util.spec_from_file_location("bger_poller", REPO / "scripts" / "bger_poller.py")
bp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bp)


def test_proxy_selection(monkeypatch):
    monkeypatch.delenv("BGER_PROXY", raising=False)
    monkeypatch.delenv("SCRAPER_PROXY", raising=False)
    assert bp._proxy() is None
    monkeypatch.setenv("SCRAPER_PROXY", "socks5h://127.0.0.1:1080")
    assert bp._proxy() == "socks5h://127.0.0.1:1080"
    monkeypatch.setenv("BGER_PROXY", "socks5h://127.0.0.1:9999")  # BGER_PROXY wins
    assert bp._proxy() == "socks5h://127.0.0.1:9999"


def test_fetch_prefers_proxy_and_skips_incapsula(monkeypatch):
    monkeypatch.setenv("BGER_PROXY", "socks5h://127.0.0.1:1080")
    calls = {}
    def fake_via_proxy(url, proxy):
        calls["proxy"] = proxy
        return {"5A_1/2026", "2C_2/2026"}
    def fake_direct(url):
        calls["direct"] = True
        return set()
    monkeypatch.setattr(bp, "_fetch_neuheiten_via_proxy", fake_via_proxy)
    monkeypatch.setattr(bp, "_fetch_neuheiten_direct", fake_direct)
    out = bp._fetch_neuheiten("20260629")
    assert out == {"5A_1/2026", "2C_2/2026"}
    assert calls.get("proxy") == "socks5h://127.0.0.1:1080"
    assert "direct" not in calls            # direct path NOT used when proxy works


def test_fetch_falls_back_to_direct_on_proxy_error(monkeypatch):
    monkeypatch.setenv("BGER_PROXY", "socks5h://127.0.0.1:1080")
    def boom(url, proxy):
        raise OSError("tunnel down")
    monkeypatch.setattr(bp, "_fetch_neuheiten_via_proxy", boom)
    monkeypatch.setattr(bp, "_fetch_neuheiten_direct", lambda url: {"9C_9/2026"})
    assert bp._fetch_neuheiten("20260629") == {"9C_9/2026"}


def test_workday_vs_weekend():
    assert bp._is_workday(date(2026, 6, 29)) is True    # Monday
    assert bp._is_workday(date(2026, 6, 26)) is True    # Friday
    assert bp._is_workday(date(2026, 6, 27)) is False   # Saturday
    assert bp._is_workday(date(2026, 6, 28)) is False   # Sunday


def test_alarm_posts_to_ntfy(monkeypatch):
    posted = {}
    class FakeResp:  # noqa
        pass
    def fake_post(url, data=None, headers=None, timeout=None):
        posted["url"] = url; posted["data"] = data
        return FakeResp()
    import requests
    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setenv("NTFY_TOPIC", "test-topic")
    bp._alert_empty_neuheiten("2026-06-29")
    assert posted["url"].endswith("/test-topic")
    assert b"0 dockets on a workday" in posted["data"]
