"""Quota breaches must reach the operator, exactly once, without the IP.

Found 2026-08-23: quota_alerts was a write-only audit trail — a client
burned through the verify_claim quota at 09:43 UTC and the first human
notice was an evening ledger review. The ntfy push is the delivery
channel. Pinned here:

- the push fires on the FIRST over-limit call of a (day, endpoint, ip);
- repeat over-limit calls that day do NOT push again (INSERT OR IGNORE
  rowcount gating);
- the push payload never contains the offending IP — ntfy topics are
  public-by-name, so an IP there would be a personal-data leak;
- a push failure never breaks the throttle response.
"""
import importlib
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


@pytest.fixture()
def quota(tmp_path, monkeypatch):
    monkeypatch.setenv("OCL_QUOTA_DB", str(tmp_path / "quota.db"))
    # Fresh module so QUOTA_DB_PATH and schema bind to the tmp DB.
    import web_api.ocl_quota as q
    q = importlib.reload(q)
    monkeypatch.setattr(q, "ALLOWLIST", set())
    return q


def _burn(q, ip, endpoint, n):
    res = None
    for _ in range(n):
        res = q.check_and_increment(ip=ip, endpoint=endpoint, api_key=None)
    return res


def test_push_fires_once_on_first_breach(quota, monkeypatch):
    pushes = []
    monkeypatch.setattr(quota, "_notify_quota_alert",
                        lambda *a: pushes.append(a))
    limit = quota.DEFAULT_QUOTAS["attest"]
    res = _burn(quota, "203.0.113.9", "attest", limit + 3)
    assert res.allowed is False
    assert len(pushes) == 1, "exactly one push per (day, endpoint, ip)"
    assert pushes[0][0] == "attest"


def test_push_payload_never_contains_ip(quota, monkeypatch):
    """Build the real request object (network stubbed) and inspect the body."""
    captured = {}

    class _Resp:
        def close(self):
            pass

    def _fake_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        captured["body"] = req.data.decode("utf-8")
        return _Resp()

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)
    monkeypatch.setattr(quota, "QUOTA_NTFY_ENABLED", True)
    quota._notify_quota_alert("verify_claim", 201, 200)
    assert "203.0.113" not in captured["body"]
    assert "verify_claim" in captured["body"]
    assert captured["url"].endswith("/opencaselaw-prod")


def test_push_failure_does_not_break_throttle(quota, monkeypatch):
    def _boom(*a):
        raise RuntimeError("ntfy down")
    # _notify_quota_alert itself must swallow; also verify the call site
    # tolerates an unexpected raise from the notifier.
    monkeypatch.setattr(quota, "_notify_quota_alert", _boom)
    limit = quota.DEFAULT_QUOTAS["attest"]
    res = _burn(quota, "203.0.113.10", "attest", limit + 1)
    assert res is not None and res.allowed is False


def test_disabled_flag_suppresses_network(quota, monkeypatch):
    import urllib.request

    def _fail(*a, **k):
        raise AssertionError("network must not be touched when disabled")

    monkeypatch.setattr(urllib.request, "urlopen", _fail)
    monkeypatch.setattr(quota, "QUOTA_NTFY_ENABLED", False)
    quota._notify_quota_alert("attest", 20, 10)  # must be a silent no-op
