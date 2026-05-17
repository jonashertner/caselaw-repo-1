#!/usr/bin/env python3
"""
derive_cohorts_from_tier1.py — populate daily_reach HLL for ALL clients
from the 72h tier-1 log retention.

The default daily rollup only sees the install-cohort hash that the
Word add-in sends in the ``X-Install-Cohort`` header. For all other
clients (browsers, MCP front-ends, scripts), the cohort field in
tier-2 is ``-`` and ``daily_reach`` ends up empty for them.

This script derives a privacy-preserving cohort identifier in-memory
from tier-1 lines (which already exist with IP+UA, 72h retention,
abuse-response basis) and updates ``daily_reach`` with per-(day,
client) HLL estimates for ALL classes.

**Privacy properties** (preserved end-to-end):

* The cohort hash is ``SHA256(remote_addr + user_agent + YYYY-MM)[:8]``
  — the same 32-bit truncated monthly-rotating hash the Word add-in
  uses. Cannot be correlated across months.
* Raw IPs and UAs are read from tier-1 ONLY in-memory; never
  persisted, never logged, never written to analytics.db.
* HLL is a lossy distinct-count estimator with ~2% relative error;
  it doesn't preserve individual identifiers.
* K-anonymity and DP-noise gates from rollup_analytics.py still
  apply to the public columns.

The script duplicates the client-class regex matching from the nginx
config (``ops/nginx/ocl-logging.conf``) so it can classify a tier-1
line the same way nginx would have. Keep the patterns synchronized
with the nginx config when adding new client classes.

Usage::

    python3 scripts/derive_cohorts_from_tier1.py            # last 3 days
    python3 scripts/derive_cohorts_from_tier1.py --days 1
    python3 scripts/derive_cohorts_from_tier1.py --log /var/log/nginx/tier1.log
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import math
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

# Tier-1 format (from ops/nginx/ocl-logging.conf):
#   $remote_addr $time_iso8601 "$request_method $uri" $status $request_time "$http_user_agent"
TIER1_RE = re.compile(
    r'^(?P<ip>\S+)\s+'
    r'(?P<ts>\S+)\s+'
    r'"(?P<method>\S+)\s+(?P<uri>[^"]+)"\s+'
    r'(?P<status>\S+)\s+'
    r'(?P<rtime>\S+)\s+'
    r'"(?P<ua>.*)"\s*$'
)


# Mirror of nginx UA-classification logic. Order matters — bots BEFORE
# browsers because crawlers spoof browser UAs.
CLIENT_CLASS_PATTERNS = [
    # bots first
    (re.compile(r"googlebot", re.I),                 "bot_google"),
    (re.compile(r"bingbot", re.I),                   "bot_bing"),
    (re.compile(r"duckduckbot", re.I),               "bot_duckduck"),
    (re.compile(r"yandexbot", re.I),                 "bot_yandex"),
    (re.compile(r"applebot", re.I),                  "bot_apple"),
    (re.compile(r"petalbot", re.I),                  "bot_petal"),
    (re.compile(r"yellowmcp|umai", re.I),            "bot_scanner"),
    (re.compile(r"seranking|ahrefs|semrush|mj12bot", re.I), "bot_seo"),
    (re.compile(r"bot/|bot\.|crawl|spider", re.I),   "bot_other"),

    # hosted LLM bridges
    (re.compile(r"claude-user|claude-web|anthropic", re.I), "claude_hosted"),
    (re.compile(r"chatgpt|openai|gpt-[45]", re.I),   "chatgpt"),

    # local LLM clients
    (re.compile(r"claude.*desktop", re.I),           "claude_desktop"),
    (re.compile(r"claude.*ios", re.I),               "claude_mobile"),
    (re.compile(r"claude", re.I),                    "claude_other"),
    (re.compile(r"cursor", re.I),                    "cursor"),
    (re.compile(r"windsurf", re.I),                  "windsurf"),
    (re.compile(r"copilotstudio", re.I),             "copilot_studio"),
    (re.compile(r"gemini", re.I),                    "gemini_cli"),
    (re.compile(r"grok|xai", re.I),                  "grok"),

    # scripts
    (re.compile(r"mcp.*python", re.I),               "mcp_script"),
    (re.compile(r"python-httpx|python-requests|python-urllib|^python", re.I),
                                                      "python_script"),
    (re.compile(r"node-fetch|undici", re.I),         "node_script"),
    (re.compile(r"go-http-client", re.I),            "go_script"),
    (re.compile(r"curl/", re.I),                     "curl"),
    (re.compile(r"wget/", re.I),                     "wget"),

    # word add-in
    (re.compile(r"word-addin|officeaddin", re.I),    "word_addin"),

    # browsers last
    (re.compile(r"trident", re.I),                   "browser_ie"),
    (re.compile(r"edge/", re.I),                     "browser_edge"),
    (re.compile(r"firefox/", re.I),                  "browser_firefox"),
    (re.compile(r"chrome/", re.I),                   "browser_chrome"),
    (re.compile(r"safari/", re.I),                   "browser_safari"),
    (re.compile(r"mozilla", re.I),                   "browser_other"),
]


def classify_ua(ua: str) -> str:
    if not ua or ua == "-":
        return "empty"
    for pat, label in CLIENT_CLASS_PATTERNS:
        if pat.search(ua):
            return label
    return "other"


def cohort_hash(ip: str, ua: str, yyyymm: str) -> str:
    """Match the nginx X-Install-Cohort shape: SHA256(ip+ua+YYYY-MM)[:8]."""
    h = hashlib.sha256(f"{ip}|{ua}|{yyyymm}".encode("utf-8")).hexdigest()
    return h[:8]


# ── HyperLogLog (same as rollup_analytics.py) ──────────────────────────
class HLL:
    def __init__(self, p: int = 12):
        self.p = p
        self.m = 1 << p
        self.registers = bytearray(self.m)

    def add(self, value: str) -> None:
        h = int.from_bytes(
            hashlib.sha256(value.encode("utf-8")).digest()[:8], "big"
        )
        idx = h & (self.m - 1)
        w = h >> self.p
        if w == 0:
            rank = 64 - self.p + 1
        else:
            rank = 1
            while (w & 1) == 0 and rank < (64 - self.p):
                rank += 1
                w >>= 1
        if rank > self.registers[idx]:
            self.registers[idx] = rank

    def estimate(self) -> int:
        m = self.m
        alpha = 0.7213 / (1 + 1.079 / m)
        s = 0.0
        zeros = 0
        for r in self.registers:
            s += 2.0 ** (-r)
            if r == 0:
                zeros += 1
        est = alpha * m * m / s
        if est <= 2.5 * m and zeros > 0:
            est = m * math.log(m / zeros)
        return int(round(est))


def stream_tier1(log_path: Path, days: list[date]) -> Iterable[tuple[str, str, str]]:
    """Yield (day_iso, client_class, cohort_hash) per tier-1 line in scope.

    Reads tier-1 plus any rotated .gz siblings. Filters by day prefix
    in the timestamp.
    """
    day_prefixes = {d.isoformat() for d in days}
    candidates = [
        p for p in sorted(log_path.parent.glob(log_path.name + "*"))
        if p.is_file()
    ]
    if not candidates:
        return

    for p in candidates:
        opener = gzip.open if p.suffix == ".gz" else open
        try:
            with opener(p, "rt", errors="replace") as f:
                for line in f:
                    if not any(dp in line[:60] for dp in day_prefixes):
                        continue
                    m = TIER1_RE.match(line)
                    if not m:
                        continue
                    d = m.groupdict()
                    ts = d["ts"]
                    day_iso = ts[:10]
                    if day_iso not in day_prefixes:
                        continue
                    ua = d["ua"] or "-"
                    ip = d["ip"] or "-"
                    yyyymm = ts[:7]
                    yield day_iso, classify_ua(ua), cohort_hash(ip, ua, yyyymm)
        except OSError:
            continue


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--log", type=Path,
        default=Path("/var/log/nginx/tier1.log"),
    )
    p.add_argument(
        "--db", type=Path,
        default=Path(os.environ.get("SWISS_CASELAW_DIR", "output"))
        / "analytics.db",
    )
    p.add_argument(
        "--days", type=int, default=3,
        help="How many trailing days to backfill (default: 3, matching "
             "tier-1 72h retention).",
    )
    args = p.parse_args()

    if not args.db.exists():
        print(f"ERROR: {args.db} does not exist", file=sys.stderr)
        return 2

    today = datetime.now(timezone.utc).date()
    days = [today - timedelta(days=i + 1) for i in range(args.days)]

    # (day_iso, client_class) → HLL
    hlls: dict[tuple[str, str], HLL] = defaultdict(lambda: HLL(p=12))
    exact_seen: dict[tuple[str, str], set] = defaultdict(set)

    count = 0
    for day_iso, client, cohort in stream_tier1(args.log, days):
        key = (day_iso, client)
        hlls[key].add(cohort)
        if len(exact_seen[key]) < 50_000:
            exact_seen[key].add(cohort)
        count += 1

    print(f"  parsed {count:,} tier-1 lines across {len(hlls)} (day,client) cells",
          file=sys.stderr)

    conn = sqlite3.connect(str(args.db))
    try:
        # Schema already exists. Upsert into daily_reach.
        for (day_iso, client), hll in hlls.items():
            est = hll.estimate()
            exact_n = len(exact_seen[(day_iso, client)])
            existing = conn.execute(
                "SELECT n_cohorts_exact, n_cohorts_hll_estimate "
                "FROM daily_reach WHERE day = ? AND client_class = ?",
                (day_iso, client),
            ).fetchone()
            if existing:
                old_exact, old_hll = existing
                # Only upgrade if our derived signal is larger (we're
                # adding a previously-unknown cohort source).
                new_exact = max(exact_n, old_exact or 0)
                new_hll = max(est, old_hll or 0)
                conn.execute(
                    "UPDATE daily_reach SET n_cohorts_exact = ?, "
                    "n_cohorts_hll_estimate = ? "
                    "WHERE day = ? AND client_class = ?",
                    (new_exact, new_hll, day_iso, client),
                )
            else:
                conn.execute(
                    "INSERT INTO daily_reach "
                    "(day, client_class, n_cohorts_exact, n_cohorts_hll_estimate) "
                    "VALUES (?,?,?,?)",
                    (day_iso, client, exact_n, est),
                )
        conn.commit()
    finally:
        conn.close()

    print(f"  upserted {len(hlls)} cells into daily_reach", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
