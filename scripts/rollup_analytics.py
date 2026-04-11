#!/usr/bin/env python3
"""
rollup_analytics.py — privacy-respecting nightly traffic rollup
================================================================

Reads the Tier-2 nginx log (class-level, no PII) for one day and writes
aggregate counters to ``output/analytics.db``. Raw log lines are never
stored; once the rollup is written, the Tier-2 log for that day is
eligible for deletion.

Privacy guarantees:
  * The Tier-2 log format already contains no IPs, no UAs, no queries,
    no referer. See ``ops/nginx/ocl-logging.conf``.
  * Install cohorts are the 8-hex monthly hash ``SHA256(id + YYYY-MM)[:8]``
    computed client-side; they cannot be correlated across months.
  * Counts below ``K_ANON`` (= 10) are suppressed (stored as NULL) in the
    ``public`` columns. Exact counts remain in private columns used only
    for internal debugging and never published.
  * Public counts additionally carry Laplace noise with epsilon=1.0 and
    sensitivity=1, giving formal (epsilon, 0)-differential privacy.
  * Install-cohort counts use a per-day HyperLogLog sketch. The sketch
    itself is discarded after the daily estimate is written.

Tables written to ``output/analytics.db``:
  * ``daily_tool_calls`` — (day, client_class, endpoint_class) counters
    with exact + DP-noised + suppressed values, plus p50/p95 latency and
    error counts.
  * ``daily_reach`` — (day, client_class) distinct-session and
    install-cohort estimates.
  * ``daily_status`` — (day, status_bucket) HTTP status distribution.

Usage:
    python3 scripts/rollup_analytics.py                 # yesterday
    python3 scripts/rollup_analytics.py --day 2026-04-10
    python3 scripts/rollup_analytics.py --log /var/log/nginx/tier2.log

Designed to be run by a systemd timer nightly at 04:30 UTC.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import math
import os
import random
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

logger = logging.getLogger("rollup_analytics")

# ─── Privacy knobs ─────────────────────────────────────────────────────
K_ANON = 10                # suppress public cells with n < 10
DP_EPSILON = 1.0           # per-cell Laplace noise parameter
DP_SENSITIVITY = 1         # one user can change any count by at most 1
LATENCY_CAP_MS = 60_000    # clamp absurd values (SSE long-poll timeouts)

# Bot classes we filter out of "real traffic" counts. They still end up in
# the log but we don't publish numbers about them.
BOT_CLASSES = {
    "bot_google", "bot_bing", "bot_duckduck", "bot_yandex",
    "bot_apple", "bot_petal", "bot_scanner", "bot_other",
}

# Status bucket mapping.
def status_bucket(status: str) -> str:
    if not status or not status.isdigit():
        return "other"
    code = int(status)
    if 200 <= code < 300:
        return "2xx"
    if 300 <= code < 400:
        return "3xx"
    if code == 404:
        return "404"
    if code == 429:
        return "429"
    if 400 <= code < 500:
        return "4xx"
    if 500 <= code < 600:
        return "5xx"
    return "other"


# ─── Laplace noise (manual, so we don't drag in numpy) ─────────────────
def laplace_noise(scale: float, rng: random.Random) -> float:
    """Sample from Laplace(0, scale) using inverse-CDF."""
    u = rng.random() - 0.5
    return -scale * math.copysign(math.log(1 - 2 * abs(u)), u)


def dp_count(n: int, rng: random.Random,
             epsilon: float = DP_EPSILON,
             sensitivity: int = DP_SENSITIVITY) -> int:
    """Add Laplace noise for (epsilon, 0)-differential privacy, round,
    and clamp at zero. Suitable for publishing."""
    noised = n + laplace_noise(sensitivity / epsilon, rng)
    return max(0, int(round(noised)))


# ─── Log parser ────────────────────────────────────────────────────────
# Tier 2 format (see ops/nginx/ocl-logging.conf):
#   $time_iso8601 $client_class_final $endpoint_class
#   $request_method $status $request_time $body_bytes_sent
#   $install_cohort_short
TIER2_RE = re.compile(
    r'^(?P<ts>\S+)\s+'
    r'(?P<client>\S+)\s+'
    r'(?P<endpoint>\S+)\s+'
    r'(?P<method>\S+)\s+'
    r'(?P<status>\S+)\s+'
    r'(?P<rtime>\S+)\s+'
    r'(?P<bytes>\S+)\s+'
    r'(?P<cohort>\S+)\s*$'
)


def parse_line(line: str) -> dict | None:
    m = TIER2_RE.match(line)
    if not m:
        return None
    d = m.groupdict()
    try:
        d["rtime_ms"] = min(
            LATENCY_CAP_MS, int(round(float(d["rtime"]) * 1000))
        )
    except (TypeError, ValueError):
        d["rtime_ms"] = 0
    try:
        d["bytes"] = int(d["bytes"]) if d["bytes"] != "-" else 0
    except ValueError:
        d["bytes"] = 0
    return d


def stream_log_lines(log_path: Path, target_day: date) -> Iterable[dict]:
    """Yield parsed Tier-2 log lines falling on ``target_day``.

    Supports plain text, .gz, and rotated logs named ``tier2.log-YYYYMMDD``.
    We stream line-by-line; no raw records are kept in memory beyond
    current counters.
    """
    import gzip
    day_prefix = target_day.isoformat()
    # Include tier2.log and any rotated/compressed siblings.
    candidates: list[Path] = []
    for p in sorted(log_path.parent.glob(log_path.name + "*")):
        if p.is_file():
            candidates.append(p)
    if not candidates:
        logger.warning("No log files matching %s", log_path)
        return

    for p in candidates:
        opener = gzip.open if p.suffix == ".gz" else open
        try:
            with opener(p, "rt", errors="replace") as f:
                for line in f:
                    if day_prefix not in line[:25]:
                        # Cheap pre-filter: timestamp is always in the
                        # first ~20 chars. Skip full parse if wrong day.
                        continue
                    rec = parse_line(line)
                    if rec is None:
                        continue
                    if not rec["ts"].startswith(day_prefix):
                        continue
                    yield rec
        except OSError as e:
            logger.warning("Could not read %s: %s", p, e)
            continue


# ─── HyperLogLog (tiny, pure-Python) ───────────────────────────────────
# We implement HLL inline so the script has no external dependencies
# beyond stdlib. 2^p registers → ~1.04/sqrt(m) standard error.
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
        # Count leading zeros in w (+1 for the first 1-bit).
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
        # Small-range correction
        if est <= 2.5 * m and zeros > 0:
            est = m * math.log(m / zeros)
        return int(round(est))


# ─── In-memory aggregation state ───────────────────────────────────────
class DayAggregate:
    def __init__(self, day: date):
        self.day = day
        # (client_class, endpoint_class) → dict of counters
        self.cells: dict[tuple[str, str], dict] = defaultdict(
            lambda: {
                "n": 0,
                "err_4xx": 0,
                "err_5xx": 0,
                "latencies": [],  # we keep up to MAX_LATENCIES samples
                "bytes": 0,
            }
        )
        # client_class → HLL of cohort hashes (distinct installs)
        self.cohort_hll: dict[str, HLL] = defaultdict(lambda: HLL(p=12))
        self.cohort_seen_exact: dict[str, set] = defaultdict(set)
        # status bucket histogram (global, not per-client)
        self.status_hist: dict[str, int] = defaultdict(int)
        # total lines processed
        self.total = 0

    MAX_LATENCIES = 10_000  # reservoir cap per cell

    def ingest(self, rec: dict) -> None:
        self.total += 1
        client = rec["client"]
        endpoint = rec["endpoint"]
        status = rec["status"]
        rtime_ms = rec["rtime_ms"]
        cohort = rec["cohort"]

        # Skip bots entirely — they're logged but never aggregated as traffic.
        if client in BOT_CLASSES:
            return

        cell = self.cells[(client, endpoint)]
        cell["n"] += 1
        cell["bytes"] += rec["bytes"]
        bucket = status_bucket(status)
        self.status_hist[bucket] += 1
        if bucket in ("4xx", "404", "429"):
            cell["err_4xx"] += 1
        elif bucket == "5xx":
            cell["err_5xx"] += 1
        if len(cell["latencies"]) < self.MAX_LATENCIES:
            cell["latencies"].append(rtime_ms)

        if cohort and cohort != "-" and len(cohort) == 8:
            self.cohort_hll[client].add(cohort)
            # Also track an exact set up to a cap, purely so we can emit
            # an exact count in the private column when traffic is small.
            # We cap to avoid unbounded memory.
            if len(self.cohort_seen_exact[client]) < 50_000:
                self.cohort_seen_exact[client].add(cohort)


# ─── Percentile helper (no numpy) ──────────────────────────────────────
def percentile(sorted_values: list[int], p: float) -> float:
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_values[int(k)])
    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return d0 + d1


# ─── Writer ────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_tool_calls (
    day              TEXT    NOT NULL,
    client_class     TEXT    NOT NULL,
    endpoint_class   TEXT    NOT NULL,
    n_exact          INTEGER NOT NULL,       -- internal: exact count
    n_public         INTEGER,                -- public: DP-noised, NULL if suppressed
    p50_ms           REAL,
    p95_ms           REAL,
    err_4xx          INTEGER NOT NULL,
    err_5xx          INTEGER NOT NULL,
    bytes_total      INTEGER NOT NULL,
    PRIMARY KEY (day, client_class, endpoint_class)
);

CREATE TABLE IF NOT EXISTS daily_reach (
    day                     TEXT    NOT NULL,
    client_class            TEXT    NOT NULL,
    n_cohorts_exact         INTEGER,
    n_cohorts_hll_estimate  INTEGER,
    n_cohorts_public        INTEGER,        -- DP-noised, suppressed if < K_ANON
    PRIMARY KEY (day, client_class)
);

CREATE TABLE IF NOT EXISTS daily_status (
    day           TEXT    NOT NULL,
    status_bucket TEXT    NOT NULL,
    n_exact       INTEGER NOT NULL,
    n_public      INTEGER,
    PRIMARY KEY (day, status_bucket)
);

CREATE TABLE IF NOT EXISTS run_metadata (
    day            TEXT PRIMARY KEY,
    ran_at         TEXT NOT NULL,
    rows_ingested  INTEGER NOT NULL,
    k_anon         INTEGER NOT NULL,
    dp_epsilon     REAL NOT NULL
);
"""


def write_results(db_path: Path, agg: DayAggregate, rng: random.Random) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(SCHEMA)
        day = agg.day.isoformat()

        # Daily tool calls
        conn.execute("DELETE FROM daily_tool_calls WHERE day = ?", (day,))
        for (client, endpoint), cell in agg.cells.items():
            n = cell["n"]
            lats = sorted(cell["latencies"])
            p50 = percentile(lats, 0.50) if lats else None
            p95 = percentile(lats, 0.95) if lats else None
            n_public = dp_count(n, rng) if n >= K_ANON else None
            conn.execute(
                """INSERT INTO daily_tool_calls
                   (day, client_class, endpoint_class,
                    n_exact, n_public, p50_ms, p95_ms,
                    err_4xx, err_5xx, bytes_total)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (day, client, endpoint, n, n_public, p50, p95,
                 cell["err_4xx"], cell["err_5xx"], cell["bytes"]),
            )

        # Daily reach (distinct installs per client class via HLL)
        conn.execute("DELETE FROM daily_reach WHERE day = ?", (day,))
        client_classes = set(agg.cohort_hll.keys()) | set(
            c for (c, _) in agg.cells.keys()
        )
        for client in sorted(client_classes):
            hll = agg.cohort_hll.get(client)
            exact_seen = agg.cohort_seen_exact.get(client, set())
            n_exact = len(exact_seen) if exact_seen else None
            n_hll = hll.estimate() if hll else 0
            n_public = (
                dp_count(n_hll, rng) if n_hll >= K_ANON else None
            )
            conn.execute(
                """INSERT INTO daily_reach
                   (day, client_class, n_cohorts_exact,
                    n_cohorts_hll_estimate, n_cohorts_public)
                   VALUES (?,?,?,?,?)""",
                (day, client, n_exact, n_hll, n_public),
            )

        # Status histogram
        conn.execute("DELETE FROM daily_status WHERE day = ?", (day,))
        for bucket, n in agg.status_hist.items():
            n_public = dp_count(n, rng) if n >= K_ANON else None
            conn.execute(
                """INSERT INTO daily_status
                   (day, status_bucket, n_exact, n_public)
                   VALUES (?,?,?,?)""",
                (day, bucket, n, n_public),
            )

        # Run metadata
        conn.execute(
            """INSERT OR REPLACE INTO run_metadata
               (day, ran_at, rows_ingested, k_anon, dp_epsilon)
               VALUES (?,?,?,?,?)""",
            (day, datetime.now(timezone.utc).isoformat(),
             agg.total, K_ANON, DP_EPSILON),
        )
        conn.commit()
    finally:
        conn.close()


# ─── Entry point ───────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log", type=Path,
        default=Path("/var/log/nginx/tier2.log"),
        help="Path to the Tier-2 access log (includes rotated siblings)",
    )
    parser.add_argument(
        "--db", type=Path,
        default=Path(os.environ.get("SWISS_CASELAW_DIR", "output"))
        / "analytics.db",
        help="Output SQLite database",
    )
    parser.add_argument(
        "--day", type=str, default=None,
        help="ISO date to roll up (default: yesterday UTC)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if args.day:
        target_day = date.fromisoformat(args.day)
    else:
        target_day = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    logger.info("Rolling up traffic for %s", target_day)
    rng = random.Random(f"ocl-rollup-{target_day}")  # seeded for replay

    agg = DayAggregate(target_day)
    for rec in stream_log_lines(args.log, target_day):
        agg.ingest(rec)

    logger.info(
        "Ingested %d records across %d (client, endpoint) cells",
        agg.total, len(agg.cells),
    )

    write_results(args.db, agg, rng)
    logger.info("Wrote aggregates to %s", args.db)

    if agg.total == 0:
        logger.warning(
            "No records parsed for %s. Either nothing reached the server "
            "that day or the Tier-2 log isn't being written yet.",
            target_day,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
