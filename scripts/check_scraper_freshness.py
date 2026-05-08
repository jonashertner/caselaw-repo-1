#!/usr/bin/env python3
"""
check_scraper_freshness.py — Alert on stale or failed scrapers.

Reads scraper_health.json and the per-court last_scraped from the
coverage snapshots, and writes warnings to logs/scraper_alerts.log
when:
  - any scraper failed in today's run
  - any court hasn't had a successful scrape in N days
  - the entscheidsuche cron timer has been failing for >2 weeks

Designed to run after the daily scrape (e.g. 03:00 UTC).

**Notification semantics** (changed 2026-05-08):

  - Always exits 0 on a successful run. Alerts are surfaced via
    (a) the per-day log line at ``logs/scraper_alerts.log`` and
    (b) ntfy.sh push at ``ntfy.sh/opencaselaw-scrapers`` when the
    alert SET changes vs. the previous run.
  - The previous behaviour (exit 1 on alerts, no notify) caused this
    service to be journalled as ``failed`` daily for weeks while the
    actual alerts rotted in a logfile nobody read. The dispatch is
    now active.

Usage:
    python3 scripts/check_scraper_freshness.py
    python3 scripts/check_scraper_freshness.py --max-stale-days 14
    python3 scripts/check_scraper_freshness.py --no-ntfy   # local debug
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── ntfy push channel ─────────────────────────────────────────────
# Topic is unique per-deployment; subscribe in the ntfy app to get
# push alerts. Mirrors scripts/publish_failure_alert.py pattern.
NTFY_URL = "https://ntfy.sh/opencaselaw-scrapers"

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

# Courts that legitimately publish rarely (small chamber, archival
# series, historical-only). The "<30s + 0 new" heuristic is a
# false-positive here — a clean caught-up exit looks identical to a
# silent failure. Verified manually 2026-05-08: each is reachable and
# the historical row count is current.
SILENT_SKIP_EXEMPT_SOURCES = {
    # ~1,244-row archival corpus (1915–2025); MKG publishes Bd. 1–16
    # via alexandria.ch, no new volumes expected.
    "mkg",
    # Small civil/criminal-chamber series; portal occasionally returns
    # 0 results when no new publications are pending (legitimate empty).
    "be_zivilstraf",
    # BL portal pre-poll bails out fast when we already have the last
    # batch; large catch-up only in long-poll runs.
    "bl_gerichte",
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


def _alert_set_digest(alerts: list[str]) -> str:
    """Stable hash of the alert SET (order-independent). Used to skip
    ntfy when today's alerts are byte-for-byte the same as yesterday's
    — recurring portal warnings would otherwise spam the topic daily.
    """
    blob = "\n".join(sorted(alerts))
    return hashlib.sha256(blob.encode("utf-8", errors="replace")).hexdigest()


def _ascii_safe(s: str) -> str:
    """ntfy.sh forwards Title/Tags as raw HTTP headers, which urllib
    encodes as latin-1 by default. Strip non-latin-1 chars so an em-dash
    in a translated court name doesn't raise UnicodeEncodeError."""
    return s.encode("ascii", errors="replace").decode("ascii")


def post_ntfy(alerts: list[str], today: str, *, priority: str, title: str) -> bool:
    """Best-effort ntfy push. Never raises (mirrors
    scripts/publish_failure_alert.py — alert path must not generate
    further failure noise). Returns True on HTTP 2xx, False on any
    failure; the caller uses this to decide whether to persist the
    dedupe state file (a failed post should not look 'already
    delivered' on the next run)."""
    body = f"{today}  {len(alerts)} alert(s)\n\n" + "\n".join(alerts[:30])
    if len(alerts) > 30:
        body += f"\n\n...({len(alerts) - 30} more in scraper_alerts.log)"
    try:
        req = urllib.request.Request(
            NTFY_URL,
            data=body.encode("utf-8", errors="replace"),
            headers={
                "Title": _ascii_safe(title),
                "Priority": priority,
                "Tags": "warning",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception as e:  # noqa: BLE001 — best-effort
        print(f"ntfy post failed: {e}", file=sys.stderr)
        return False


def maybe_dispatch_ntfy(
    alerts: list[str],
    today: str,
    state_dir: Path,
) -> str:
    """Dispatch via ntfy if today's alert set differs from the last
    dispatched set.

    Returns one of: ``posted``, ``unchanged``, ``skipped`` (no alerts).
    """
    state_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_dir / "scraper_alerts_last_dispatched.json"

    if not alerts:
        # On clear-up, post one all-clear if we previously dispatched.
        # Drop the state file so the next regression re-notifies.
        if state_file.exists():
            try:
                last = json.loads(state_file.read_text())
                if last.get("digest"):
                    post_ntfy(
                        ["All previously-flagged scraper alerts have cleared."],
                        today,
                        priority="default",
                        title="opencaselaw scrapers all clear",
                    )
            except Exception:
                pass
            try:
                state_file.unlink()
            except OSError:
                pass
        return "skipped"

    digest = _alert_set_digest(alerts)
    last_digest = None
    if state_file.exists():
        try:
            last_digest = json.loads(state_file.read_text()).get("digest")
        except Exception:
            last_digest = None
    if digest == last_digest:
        return "unchanged"

    has_fail_or_critical = any(
        a.startswith(("FAIL ", "CRITICAL", "STALE "))
        for a in alerts
    )
    priority = "high" if has_fail_or_critical else "default"
    title = (
        f"opencaselaw scrapers - {len(alerts)} alert(s)"
        if not has_fail_or_critical
        else f"opencaselaw scrapers - {len(alerts)} alert(s), action needed"
    )
    posted = post_ntfy(alerts, today, priority=priority, title=title)
    if not posted:
        # Don't persist dedupe state on a failed post — the next run
        # should retry instead of declaring 'already delivered'.
        return "post_failed"

    try:
        state_file.write_text(json.dumps({
            "digest": digest,
            "dispatched_at": datetime.now(timezone.utc).isoformat(),
            "alert_count": len(alerts),
        }, indent=2))
    except OSError as e:
        print(f"state write failed: {e}", file=sys.stderr)
    return "posted"


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
    parser.add_argument("--state-dir", type=str,
                        default=str(REPO / "state"),
                        help="Where to remember the last-dispatched alert set "
                             "(used to dedupe recurring ntfy posts).")
    parser.add_argument("--no-ntfy", action="store_true",
                        help="Skip ntfy dispatch (local debug).")
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

            # Real silent-failure signal: success=true AND finished
            # suspiciously fast for an active court (under 30s) AND new=0.
            # Most legitimate zero-new scrapes take >60s because the portal
            # must be traversed.
            #
            # Exemptions:
            #   - portal_count == our_count: scraper actively confirmed
            #     we are caught up (genuine empty, not a silent skip).
            #   - SILENT_SKIP_EXEMPT_SOURCES: archival/small-chamber feeds
            #     where a fast no-new exit is the normal weekday outcome.
            for k, v in scrapers.items():
                if k in KNOWN_DEAD_SOURCES or k in SILENT_SKIP_EXEMPT_SOURCES:
                    continue
                if (v.get("success")
                        and v.get("new_count", 0) == 0
                        and v.get("our_count", 0) > 1000  # large active corpus
                        and v.get("duration_s", 0) < 30):
                    portal_n = v.get("portal_count")
                    our_n = v.get("our_count", 0)
                    if portal_n is not None and portal_n == our_n:
                        # Caught-up confirmation; not a silent skip.
                        continue
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

    if not args.no_ntfy:
        verdict = maybe_dispatch_ntfy(alerts, today, Path(args.state_dir))
        if not args.quiet:
            print(f"ntfy: {verdict}")
    elif not alerts and not args.quiet:
        print(f"All checks passed at {today}")

    # Always exit 0 on a successful run. The exit-1-on-alert semantics
    # were never wired to a notification path; the dispatch is now
    # active via ntfy + state file. systemd no longer reports this
    # service as failed daily.
    sys.exit(0)


if __name__ == "__main__":
    main()
