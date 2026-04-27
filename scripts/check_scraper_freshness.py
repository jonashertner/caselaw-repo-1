#!/usr/bin/env python3
"""
check_scraper_freshness.py — Alert on stale or failed scrapers.

Reads scraper_health.json and the per-court last_scraped from the
coverage snapshots, and writes warnings to logs/scraper_alerts.log
when:
  - any scraper failed in today's run
  - any court hasn't had a successful scrape in N days
  - the entscheidsuche cron timer has been failing for >2 weeks

Designed to run after the daily scrape (e.g. 03:00 UTC). Exits 0 on
healthy, 1 if any alert was emitted (so a wrapping cron can chain to
notification channels if desired).

Usage:
    python3 scripts/check_scraper_freshness.py
    python3 scripts/check_scraper_freshness.py --max-stale-days 14
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Courts that are KNOWN-DEAD upstream — don't alert on them
KNOWN_DEAD_SOURCES = {
    "ch_vb",                 # source dead since 2021
    "ag_baugesetzgebung",    # source stagnant since Nov 2025
    "ag_weitere",            # source dead since 2023
    "sg_publikationen",      # entscheidsuche.ch path stale (we have direct scraper anyway)
    "be_steuerrekurs",       # portal DB disconnected (Feb 2026)
    "ow_gerichte",           # portal offline since Dec 2022
    "ta_sst",                # rare publication
    "ch_bundesrat",          # rare publication
    "comcom",                # rare publication
}

# Courts that ONLY come from entscheidsuche.ch — use ES last-modified to grade them
ENTSCHEIDSUCHE_ONLY = {
    "vd_findinfo", "vd_omni", "ch_vb", "sg_gerichte", "tg_obergericht",
    "be_bvd", "be_weitere", "sh_obergericht", "be_steuerrekurs",
    "ag_baugesetzgebung", "ag_weitere",
}

# Courts where partial-fetch failures are expected (upstream limitation,
# not a scraper bug) — downgrade FAIL → WARN. ECtHR's HUDOC HTML
# converter returns empty bodies for many pre-2018 judgments
# (PDF-only); CACHE_NONE_AS_GAP=True caches the gaps but the cumulative
# count keeps creeping up. Treat as informational, not an outage.
TOLERATED_PARTIAL_SOURCES = {
    "ecthr",
}
# JU/NE depend on the MacBook reverse-SOCKS tunnel which sleeps when the
# laptop sleeps. Failures during the 01:00 UTC window are normal; the
# late-scrapers timer at 10:00 UTC retries once the tunnel is back.
TUNNEL_DEPENDENT_SOURCES = {
    "ju_gerichte",
    "ne_gerichte",
}


def get_last_scraped(court: str) -> str | None:
    """Return ISO date of last successful scrape from coverage_report DB."""
    db_path = REPO / "output" / "decisions.db"
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{db_path}?immutable=1", uri=True, timeout=2)
        row = conn.execute(
            "SELECT MAX(snapshot_date) FROM source_snapshots WHERE source_key = ?",
            (court,),
        ).fetchone()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def check_es_cron_health() -> tuple[bool, str]:
    """Check if entscheidsuche timer has been failing.

    Returns (ok, message).
    """
    try:
        result = subprocess.run(
            ["systemctl", "show", "opencaselaw-entscheidsuche.service",
             "-p", "Result", "-p", "ActiveExitTimestamp"],
            capture_output=True, text=True, timeout=10,
        )
        props = dict(line.split("=", 1) for line in result.stdout.strip().split("\n") if "=" in line)
        last_result = props.get("Result", "unknown")
        if last_result not in ("success", "unknown", ""):
            return False, f"ES service last result: {last_result}"
        return True, f"ES service ok (last={last_result})"
    except Exception as e:
        return True, f"ES check skipped: {e}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-stale-days", type=int, default=14,
                        help="Alert if a court hasn't been scraped in this many days")
    parser.add_argument("--max-es-stale-days", type=int, default=21,
                        help="Alert if an ES-only court is older than this")
    parser.add_argument("--health-file", type=str,
                        default=str(REPO / "logs" / "scraper_health.json"))
    parser.add_argument("--alert-log", type=str,
                        default=str(REPO / "logs" / "scraper_alerts.log"))
    parser.add_argument("--quiet", action="store_true",
                        help="Don't print to stdout, only write log")
    args = parser.parse_args()

    alerts: list[str] = []
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d %H:%M UTC")

    # ── 1. Read scraper_health.json ──
    health_path = Path(args.health_file)
    if not health_path.exists():
        alerts.append(f"CRITICAL: scraper_health.json missing at {health_path}")
    else:
        try:
            health = json.loads(health_path.read_text())
            scrapers = health.get("scrapers", {})

            # Failed scrapers
            failed = [(k, v) for k, v in scrapers.items() if not v.get("success")]
            for k, v in failed:
                if k in KNOWN_DEAD_SOURCES:
                    continue
                err = v.get("error") or "unknown"
                if k in TOLERATED_PARTIAL_SOURCES:
                    alerts.append(f"WARN {k}: {err} (tolerated upstream limitation)")
                elif k in TUNNEL_DEPENDENT_SOURCES:
                    alerts.append(f"WARN {k}: {err} (MacBook SOCKS tunnel — late-scrapers retry at 10:00 UTC)")
                else:
                    alerts.append(f"FAIL {k}: {err}")

            # Real silent-failure signal: success=true AND finished suspiciously
            # fast for an active court (under 30s) AND new=0. Most legitimate
            # zero-new scrapes take >60s because the portal must be traversed.
            for k, v in scrapers.items():
                if k in KNOWN_DEAD_SOURCES:
                    continue
                if (v.get("success")
                        and v.get("new_count", 0) == 0
                        and v.get("our_count", 0) > 1000  # large active corpus
                        and v.get("duration_s", 0) < 30):
                    alerts.append(
                        f"WARN {k}: scraped in {v.get('duration_s'):.0f}s with 0 new "
                        f"(corpus={v.get('our_count')}) — possible API outage with silent skip"
                    )

            # Run age
            run_at = health.get("run_at")
            if run_at:
                try:
                    run_dt = datetime.fromisoformat(run_at)
                    age_h = (now - run_dt).total_seconds() / 3600
                    if age_h > 36:
                        alerts.append(f"WARN scrape_health.json is {age_h:.0f}h old (cron may be down)")
                except Exception:
                    pass
        except Exception as e:
            alerts.append(f"CRITICAL: cannot parse scraper_health.json: {e}")

    # ── 2. Per-court freshness via coverage snapshots ──
    cutoff_normal = now - timedelta(days=args.max_stale_days)
    cutoff_es = now - timedelta(days=args.max_es_stale_days)

    if health_path.exists():
        scrapers = json.loads(health_path.read_text()).get("scrapers", {})
        for court in scrapers:
            if court in KNOWN_DEAD_SOURCES:
                continue
            last = get_last_scraped(court)
            if not last:
                continue
            try:
                last_dt = datetime.fromisoformat(last).replace(tzinfo=timezone.utc)
            except Exception:
                continue
            cutoff = cutoff_es if court in ENTSCHEIDSUCHE_ONLY else cutoff_normal
            if last_dt < cutoff:
                age_d = (now - last_dt).days
                tag = "ES-only" if court in ENTSCHEIDSUCHE_ONLY else "direct"
                alerts.append(f"STALE {court} ({tag}): last_scraped {last} ({age_d}d ago)")

    # ── 3. ES cron health ──
    es_ok, es_msg = check_es_cron_health()
    if not es_ok:
        alerts.append(f"FAIL entscheidsuche cron: {es_msg}")

    # ── Output ──
    log_path = Path(args.alert_log)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if alerts:
        line_set = [f"{today}  {alert}" for alert in alerts]
        with log_path.open("a") as f:
            for line in line_set:
                f.write(line + "\n")
        if not args.quiet:
            print(f"=== {len(alerts)} alert(s) at {today} ===")
            for line in line_set:
                print(line)
        sys.exit(1)
    else:
        if not args.quiet:
            print(f"All checks passed at {today}")
        sys.exit(0)


if __name__ == "__main__":
    main()
