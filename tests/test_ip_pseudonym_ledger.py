"""The per-IP LLM cost ledger must not persist raw addresses.

Measured on production 2026-08-26: logs/llm_cost_by_ip.jsonl held 10,413
records for that day, every one a raw dotted-quad IPv4, none hashed. Search
traces hold query text (200 chars) with no address. Neither file identifies
anyone alone — but they share microsecond timestamps, and joining them within
±0.5 s matched 4,079 of 4,710 rerank rows one-to-one. That reconstructs
address + query text for the large majority of searches, including MCP
traffic, and 91% of ledger rows come from ordinary search rather than the
LLM-heavy tools the privacy notice names.

Daily limits are enforced from web_api/ocl_quota.py's own SQLite store, not
from this file, so pseudonymising here costs no enforcement capability.
"""
from __future__ import annotations

import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def _load():
    """Exec just the helper — importing mcp_server.py is expensive."""
    import os
    import secrets
    src = (REPO / "mcp_server.py").read_text(encoding="utf-8")
    start = src.index("_IP_PSEUDONYM_FALLBACK_SALT = ")
    end = src.index("\n# Anthropic public pricing", start)
    ns = {"os": os, "secrets": secrets, "hashlib": hashlib,
          "datetime": datetime, "timezone": timezone}
    exec(src[start:end], ns)
    return ns["_ip_pseudonym"]


def test_ledger_write_site_does_not_emit_a_raw_ip():
    """Pin the actual write, not just the helper."""
    src = (REPO / "mcp_server.py").read_text(encoding="utf-8")
    i = src.index("LLM_LEDGER_LOG_PATH, \"a\"")
    window = src[i:i + 400]
    assert "ip_pseudonym" in window, "ledger no longer pseudonymises the address"
    assert '"ip": _ip' not in window, "ledger writes the raw address again"


def test_pseudonym_is_stable_within_a_day(monkeypatch):
    """Same address, same day -> same pseudonym, so abuse grouping works."""
    monkeypatch.setenv("OCL_IP_PSEUDONYM_SECRET", "test-secret")
    f = _load()
    assert f("203.0.113.7") == f("203.0.113.7")
    assert f("203.0.113.7") != f("203.0.113.8")


def test_pseudonym_is_not_reversible_by_brute_force(monkeypatch):
    """IPv4 is 32 bits — an unsalted digest is searchable in seconds.

    With the secret unknown, sweeping the address space must not reproduce it.
    """
    monkeypatch.setenv("OCL_IP_PSEUDONYM_SECRET", "the-real-secret")
    f = _load()
    target = f("198.51.100.23")
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # An attacker who knows the algorithm and the date but not the secret.
    for guess_secret in ("", "opencaselaw", "salt", day):
        for octet in range(256):
            ip = f"198.51.100.{octet}"
            h = hashlib.sha256(
                f"{guess_secret}|{day}|{ip}".encode("utf-8")
            ).hexdigest()[:16]
            assert h != target, f"recovered with secret={guess_secret!r}"


def test_pseudonym_rotates_across_days(monkeypatch):
    """Yesterday's pseudonym must not equal today's, or linkage survives."""
    monkeypatch.setenv("OCL_IP_PSEUDONYM_SECRET", "test-secret")
    ip = "192.0.2.55"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    mk = lambda day: hashlib.sha256(
        f"test-secret|{day}|{ip}".encode("utf-8")).hexdigest()[:16]
    assert mk(today) != mk("2026-01-01")
    assert _load()(ip) == mk(today)


def test_missing_secret_still_never_yields_the_address(monkeypatch):
    """Absent shared secret degrades grouping, never privacy."""
    monkeypatch.delenv("OCL_IP_PSEUDONYM_SECRET", raising=False)
    f = _load()
    out = f("203.0.113.7")
    assert "203.0.113.7" not in out
    assert re.fullmatch(r"[0-9a-f]{16}", out)


def test_output_shape_is_opaque(monkeypatch):
    monkeypatch.setenv("OCL_IP_PSEUDONYM_SECRET", "s")
    out = _load()("203.0.113.7")
    assert re.fullmatch(r"[0-9a-f]{16}", out)
    assert not re.search(r"\d+\.\d+\.\d+\.\d+", out)
