#!/usr/bin/env python3
"""
BGer Neuheiten Poller
======================
Lightweight poller that checks the BGer Eurospider "Neuheiten" page
for new decisions. If new decisions are detected, triggers the BGer scraper.

Designed to run every 15 min via systemd timer during business hours.
Single HTTP request per run (~1s), minimal load on BGer servers.

Architecture:
  1. Fetch index_aza.php?date=YYYYMMDD&mode=news (today's date)
  2. Extract docket numbers from page
  3. Compare against last known count (stored in state file)
  4. If new decisions found → run bger scraper
  5. Update state file

State: /tmp/bger_poller_state.json
  {"date": "2026-04-07", "count": 22, "dockets": ["5A_24/2024", ...]}

Usage:
  python3 scripts/bger_poller.py          # check and trigger if needed
  python3 scripts/bger_poller.py --dry-run # check only, don't trigger
  python3 scripts/bger_poller.py --force   # trigger scraper regardless
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[1]
STATE_FILE = Path("/tmp/bger_poller_state.json")
LOG_FILE = REPO_DIR / "logs" / "bger_poller.log"

NEUHEITEN_URL = (
    # search.bger.ch — confirmed working 2026-05-06.
    # www.bger.ch silently returned an empty page on this date which made the
    # poller log "0 decisions" all morning while 29 fresh dockets were live
    # at search.bger.ch (the host the rest of the scraper already uses).
    "https://search.bger.ch/ext/eurospider/live/de/php/aza/http/index_aza.php"
    "?date={date}&lang=de&mode=news"
)

DOCKET_RE = re.compile(r"\b\d[A-Z]_\d+/\d{4}\b")

# The aza:// document ids on the Neuheiten page are the authoritative list of
# decisions actually PUBLISHED that day (one id per linked document):
# aza://DD-MM-YYYY-<docket-stem>-<year>, e.g. aza://02-07-2026-1C_146-2025.
AZA_ID_RE = re.compile(r"aza://\d{2}-\d{2}-\d{4}-([0-9A-Za-z_]+)-(\d{4})")


def _extract_feed_dockets(text: str) -> set[str]:
    """Dockets of the decisions actually LINKED on the Neuheiten page.

    Extracts from the aza:// document ids, NOT from a docket regex over the
    whole page: Revisions-/Erläuterungsgesuch entries carry the ATTACKED
    judgment's docket in their title ("Revisionsgesuch gegen das Urteil ...
    5A_402/2026 vom 15. Mai 2026"), and the broad regex swept those up as
    phantom "new decisions" that can never be fetched (2026-07-01: x7
    doc-service-failure alarms for 5A_402/2026; 2026-07-02: 3 more phantoms
    from one Erläuterungsgesuch title — feed had 42 real aza ids but 45
    regex-extracted dockets). Falls back to the broad regex only if the page
    has dockets but no aza ids at all (markup-change safety net: over-
    extraction beats silent blindness)."""
    dockets = {f"{stem}/{year}" for stem, year in AZA_ID_RE.findall(text)}
    if not dockets and DOCKET_RE.search(text):
        logger.warning(
            "Neuheiten page has docket strings but no aza:// ids — markup "
            "change? Falling back to broad docket extraction")
        return set(DOCKET_RE.findall(text))
    return dockets

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _proxy() -> str | None:
    """Residential reverse-SOCKS tunnel for BGer egress, if configured.

    search.bger.ch (Incapsula) hard-blocks the Hetzner datacenter IP, so the
    poller egresses via the Mac's residential tunnel (same as NE/JU). A residential
    IP is NOT challenged, so the proxy path skips the cookie/PoW dance entirely.
    """
    return os.environ.get("BGER_PROXY") or os.environ.get("SCRAPER_PROXY") or None

# quick_publish copies the full decisions.db before inserting (64 GB on
# 2026-06-11, growing daily); that copy+insert took 10m19s under
# publish-tail I/O contention. The old 900 s subprocess.run timeout left
# no headroom — and on expiry subprocess.run SIGKILLs the child,
# bypassing quick_publish's SIGTERM cleanup handler and orphaning the
# multi-GB .quick copy (the 2026-05-04 ENOSPC pattern).
QUICK_PUBLISH_TIMEOUT_S = 2700
QUICK_PUBLISH_GRACE_S = 60

# Escalate to ERROR when the same docket fails this many consecutive
# polls — BGer's document service intermittently serves error pages for
# freshly-listed dockets; one or two polls of lag is normal, three is a
# persistent outage worth surfacing (2026-06-10: two dockets failed
# 14:16/15:15/16:14 with only WARNING-level visibility).
FAILING_STREAK_ALERT = 3

logger = logging.getLogger("bger_poller")


def _get_pow_cookies() -> dict:
    """Generate PoW cookies for BGer Eurospider."""
    sys.path.insert(0, str(REPO_DIR))
    from base_scraper import make_pow_cookies
    return make_pow_cookies(16)


def _fetch_neuheiten(date_str: str) -> set[str]:
    """Fetch today's Neuheiten page and extract docket numbers.

    Prefers the residential tunnel (BGER_PROXY/SCRAPER_PROXY): Incapsula does NOT
    challenge a residential IP, so we skip the cookie/PoW dance entirely. Falls
    back to the direct Incapsula-bypass path only when no proxy is set, or the
    proxied fetch errors (the Hetzner IP is hard-blocked as of 2026-06-29, so the
    direct path currently fails — the fallback is there for if/when it recovers).
    """
    url = NEUHEITEN_URL.format(date=date_str)
    proxy = _proxy()
    if proxy:
        try:
            return _fetch_neuheiten_via_proxy(url, proxy)
        except Exception as e:  # connection/tunnel error — try direct as a backstop
            logger.warning("Neuheiten proxy fetch failed (%s) — falling back to direct", e)
    return _fetch_neuheiten_direct(url)


def _fetch_neuheiten_via_proxy(url: str, proxy: str) -> set[str]:
    """Residential egress (reverse-SOCKS tunnel). A residential IP is not
    challenged, so a plain GET returns the real page — no Incapsula cookies, no
    PoW. An empty result is genuine (e.g. weekend with no publications)."""
    import requests
    session = requests.Session()
    session.headers["User-Agent"] = _UA
    session.proxies = {"http": proxy, "https": proxy}
    r = session.get(url, timeout=45)
    r.raise_for_status()
    return _extract_feed_dockets(r.text)


def _fetch_neuheiten_direct(url: str) -> set[str]:
    """Direct (datacenter IP): harvest Incapsula cookies + PoW. Works only while
    the IP is not hard-blocked; raises loudly on a persistent block."""
    import requests
    sys.path.insert(0, str(REPO_DIR))
    from incapsula_bypass import IncapsulaCookieManager

    session = requests.Session()
    session.headers["User-Agent"] = _UA
    incap_mgr = IncapsulaCookieManager(cache_dir=REPO_DIR / "state")
    session.cookies.update(incap_mgr.get_cookies("search.bger.ch"))
    session.cookies.update(_get_pow_cookies())

    r = session.get(url, timeout=30)
    r.raise_for_status()
    if IncapsulaCookieManager.is_incapsula_blocked_response(r):
        logger.warning("Incapsula block detected — refreshing cookies and retrying")
        session.cookies.clear()
        session.cookies.update(incap_mgr.refresh_cookies("search.bger.ch"))
        session.cookies.update(_get_pow_cookies())
        r = session.get(url, timeout=30)
        r.raise_for_status()
        if IncapsulaCookieManager.is_incapsula_blocked_response(r):
            raise RuntimeError(
                "Incapsula still blocking after cookie refresh — manual "
                "intervention needed (browser automation not bypassing)"
            )
    return _extract_feed_dockets(r.text)


def _is_workday(d=None) -> bool:
    """Mon-Fri. BGer does not publish on weekends, so an empty Neuheiten feed is
    only suspicious on a workday."""
    from datetime import date as _date
    return (d or _date.today()).weekday() < 5


def _alert_empty_neuheiten(today_iso: str) -> None:
    """Workday + zero dockets ⇒ the fetch is almost certainly blocked (the feed is
    never empty on a business day). Surface it loudly — the silent count:0 path is
    exactly what hid a multi-week BGer gap in 2026-06."""
    msg = (f"BGer Neuheiten returned 0 dockets on a workday ({today_iso}) — fetch "
           f"likely Incapsula-blocked. Check the reverse-SOCKS tunnel / BGER_PROXY.")
    logger.error(msg)
    try:
        import requests
        topic = os.environ.get("NTFY_TOPIC", "opencaselaw-prod")
        base = os.environ.get("NTFY_URL", "https://ntfy.sh")
        requests.post(f"{base}/{topic}", data=msg.encode("utf-8"),
                      headers={"Title": "BGer poller: empty Neuheiten on a workday",
                               "Priority": "high", "Tags": "warning,rotating_light"},
                      timeout=10)
    except Exception as e:
        logger.warning("ntfy alert failed: %s", e)


def _load_state() -> dict:
    """Load last known state."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"date": "", "count": 0, "dockets": []}


def _save_state(today: str, dockets: set[str],
                failing: dict[str, int] | None = None):
    """Save current state.

    ``failing`` maps docket → consecutive polls it has failed ingestion
    (doc-service error pages). Dockets recover by dropping out of the dict.
    """
    STATE_FILE.write_text(json.dumps({
        "date": today,
        "count": len(dockets),
        "dockets": sorted(dockets),
        "failing": failing or {},
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }))


def _recently_ingested_dockets(window_seconds: int = 3600) -> set[str]:
    """Return docket_numbers in bger.jsonl whose ``scraped_at`` is within
    the last ``window_seconds`` seconds.

    The poller uses this to confirm that a triggered scraper actually
    saved its targets to JSONL. Dockets that BGer's document-service
    returns as error pages (the chronic failure mode 2026-05-08 onward)
    never make it into JSONL — those should NOT be marked "seen" so
    the next poll retries them. Naturally bounded: BGer rotates older
    decisions off the Neuheiten feed, so a permanent doc-service
    failure stops being retried within ~24h.

    Reads the last ``tail_bytes`` of the file (default 8 MB). A typical
    BGer decision JSONL row is 30–80 KB (full_text + metadata), so 8 MB
    holds ~100–200 rows — comfortable for any plausible poller batch
    (Neuheiten exposes ~30 dockets / day at peak). Earlier 256 KB cap
    silently truncated batches with substantial text bodies and made
    the success/fail accounting wrong (2026-05-08: 14 of 27 ingested
    rows were misreported as 'not in JSONL').
    """
    p = REPO_DIR / "output" / "decisions" / "bger.jsonl"
    if not p.exists():
        return set()
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    tail_bytes = 8 * 1024 * 1024  # 8 MB
    out: set[str] = set()
    try:
        with open(p, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            seek_back = min(tail_bytes, size)
            f.seek(size - seek_back, 0)
            chunk = f.read().decode("utf-8", errors="replace")
        # Drop first partial line if we seeked into the middle of one.
        if seek_back == tail_bytes and "\n" in chunk:
            chunk = chunk.split("\n", 1)[1]
        for line in chunk.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            sa = d.get("scraped_at")
            dn = d.get("docket_number")
            if not sa or not dn:
                continue
            try:
                sa_dt = datetime.fromisoformat(sa.replace("Z", "+00:00"))
            except Exception:
                continue
            if sa_dt >= cutoff:
                out.add(dn)
    except Exception as e:
        logger.warning("could not scan bger.jsonl tail: %s", e)
    return out


def _parse_scraper_new_count(output: str) -> int:
    """Extract N from run_scraper's ``[bger] Done. +N new, ...`` summary.

    run_scraper logs via a bare logging.StreamHandler, which writes to
    STDERR — callers must pass stdout and stderr combined. Scanning
    stdout alone made the poller log "0 new decisions" on every run
    while the scraper was genuinely ingesting (second regression of
    this kind: the 2026-05-07 refactor broke the regex; the fix
    corrected the regex but kept reading the wrong stream).
    """
    m = re.search(r"Done\.\s*\+(\d+)\s+new\b", output)
    return int(m.group(1)) if m else 0


def _parse_quick_publish_output(output: str) -> tuple[int, bool]:
    """Return (inserted_count, skipped_due_to_lock) from quick_publish
    output (stdout + stderr combined — quick_publish also logs via
    StreamHandler → stderr)."""
    inserted = 0
    m = re.search(r"Inserted (\d+)/", output)
    if m:
        inserted = int(m.group(1))
    # quick_publish exits rc=0 with this exact phrase when publish.py
    # holds the exclusive lock.
    skipped = ("skipping quick_publish" in output
               or "Full publish.py is running" in output)
    return inserted, skipped


def _update_failing_streaks(prev_failing: dict[str, int],
                            unsaved: set[str]) -> dict[str, int]:
    """Consecutive-poll ingestion-failure count per docket.

    Dockets still failing increment; dockets that recovered (or rotated
    off the Neuheiten page) drop out.
    """
    return {d: int(prev_failing.get(d, 0)) + 1 for d in sorted(unsaved)}


def _communicate_graceful(proc: subprocess.Popen, timeout_s: float,
                          grace_s: float = 60.0):
    """communicate() with SIGTERM-then-SIGKILL on timeout.

    Returns (out, err, returncode, timed_out). SIGTERM first so the
    child's cleanup handlers run — quick_publish removes its 60+ GB
    .quick copy on SIGTERM; a bare SIGKILL (what subprocess.run's
    timeout does) orphans it on disk.
    """
    try:
        out, err = proc.communicate(timeout=timeout_s)
        return out, err, proc.returncode, False
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            out, err = proc.communicate(timeout=grace_s)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
        return out, err, proc.returncode, True


def _trigger_scraper():
    """Run the BGer scraper, then quick-publish new decisions into FTS5.

    The scraper is allowed up to 10 minutes to run. If it stalls past
    that (the typical failure mode is camoufox returning partial
    Incapsula cookies under heavy challenge state, which then causes
    bger.py's retry loop to grind without progress), we kill it and
    still run quick_publish on whatever decisions it managed to write
    to JSONL before stalling. This is essential because today's stall
    showed the previous code path was: timeout → exception → poller
    crashes → quick_publish never fires → decisions sit unpublished.
    """
    # Step 1: Scrape — capped at 10 min via Popen + process-group kill
    # (subprocess.run with timeout doesn't reliably kill grandchildren
    # like camoufox's headless browser).
    import os
    import signal as _sig
    # --neuheiten-only: skip the AZA search backfill step, which was
    # the stall site under heavy Imperva challenge today (3 sockets in
    # CLOSE_WAIT, 16 min stuck). Neuheiten alone covers the daily delta
    # the poller is responsible for; manual `run_scraper.py bger` (no
    # flag) remains the path for backfill + nightly publish.
    cmd = [
        sys.executable, str(REPO_DIR / "run_scraper.py"),
        "bger", "--neuheiten-only",
    ]
    logger.info("Triggering BGer scraper: %s", " ".join(cmd))
    SCRAPER_TIMEOUT_S = 600
    # Route the scraper's egress through the same residential tunnel (the direct
    # datacenter IP is Incapsula-blocked). base_scraper reads SCRAPER_PROXY.
    scraper_env = dict(os.environ)
    _p = _proxy()
    if _p:
        scraper_env["SCRAPER_PROXY"] = _p
    proc = subprocess.Popen(
        cmd,
        cwd=str(REPO_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        env=scraper_env,
    )
    try:
        out, err = proc.communicate(timeout=SCRAPER_TIMEOUT_S)
        rc = proc.returncode
        timed_out = False
    except subprocess.TimeoutExpired:
        logger.warning(
            "BGer scraper exceeded %ds — process-group SIGKILL",
            SCRAPER_TIMEOUT_S,
        )
        try:
            os.killpg(os.getpgid(proc.pid), _sig.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            out, err = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            out, err = "", ""
        rc = -1
        timed_out = True

    if rc == 0:
        # Combined streams: run_scraper's "Done. +N new" line goes to
        # stderr (bare StreamHandler), not stdout — see
        # _parse_scraper_new_count.
        new_count = _parse_scraper_new_count(
            (out or "") + "\n" + (err or ""))
        logger.info("BGer scraper completed: %d new decisions", new_count)
    elif timed_out:
        logger.warning(
            "BGer scraper killed after timeout — proceeding to quick-publish "
            "on whatever data is in JSONL"
        )
    else:
        logger.error(
            "BGer scraper failed (exit %d): %s", rc, (err or "")[-500:],
        )
        # No `return` here — even on non-timeout failures, JSONL may
        # contain decisions worth publishing.

    # Step 2: Quick-publish into FTS5 DB (so decisions are searchable
    # immediately). Returns whether quick_publish COMPLETED — the caller
    # must not mark dockets "seen" otherwise, or scraped-but-unpublished
    # rows sit in JSONL until the nightly rebuild with nothing retrying
    # them.
    quick_pub = REPO_DIR / "scripts" / "quick_publish.py"
    inserted_count = 0
    qp_ok = False
    if quick_pub.exists():
        cmd = [sys.executable, str(quick_pub), "--courts", "bger"]
        logger.info("Quick-publishing: %s", " ".join(cmd))
        qp_proc = subprocess.Popen(
            cmd,
            cwd=str(REPO_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        qp_out, qp_err, qp_rc, qp_timed_out = _communicate_graceful(
            qp_proc, QUICK_PUBLISH_TIMEOUT_S, QUICK_PUBLISH_GRACE_S)
        combined = (qp_out or "") + "\n" + (qp_err or "")
        if qp_timed_out:
            logger.error(
                "Quick-publish exceeded %ds — sent SIGTERM (its cleanup "
                "handler removes the .quick copy); dockets stay unmarked "
                "and the next poll retries. Last output: %s",
                QUICK_PUBLISH_TIMEOUT_S, combined[-300:].strip(),
            )
        elif qp_rc == 0:
            inserted_count, skipped_due_to_lock = (
                _parse_quick_publish_output(combined))
            qp_ok = not skipped_due_to_lock
            if skipped_due_to_lock:
                # Dockets stay unmarked (qp_ok=False), so the next poll
                # re-detects them, re-runs the scraper (idempotent
                # append: +0 new) and retries quick_publish.
                logger.warning(
                    "Quick-publish SKIPPED: full publish.py or another "
                    "quick_publish holds a lock — freshly-scraped BGer dockets "
                    "remain in bger.jsonl; the next poll retries once the lock "
                    "releases"
                )
            else:
                logger.info("Quick-publish: inserted %d new decisions",
                            inserted_count)
        else:
            logger.error("Quick-publish failed (exit %d): %s",
                         qp_rc, (qp_err or "")[-300:])

    # Step 3: refresh dashboard stats.json (only if we actually inserted
    # decisions, since regen takes ~22 min). Without this, the
    # opencaselaw.ch dashboard's "Neueste Einträge" section stays stale
    # until the next 03:00 UTC nightly publish — even though the
    # decisions are already searchable via /api and MCP.
    _maybe_update_stats(inserted_count)
    return qp_ok


PUBLISH_UNIT = "opencaselaw-publish.service"


def _full_publish_running() -> bool:
    """True while the nightly full publish is active OR in its post-swap
    I/O-heavy tail (parquet / HuggingFace upload / reference-graph / stats).

    Why ActiveState and NOT ``systemctl is-active --quiet``: opencaselaw-publish
    is a *oneshot* unit, so for the entire ~6-15 h run its ActiveState is
    "activating", which ``is-active --quiet`` reports as NOT active (exit 3) —
    a false negative that would silently defeat this guard. Why not the publish
    *lock*: publish.py releases it at OCL_SWAP_DONE, before the I/O-heavy tail.
    Reading ActiveState directly covers the whole pipeline incl. the tail.
    Fail-open (return False) on any probe error: never block the dashboard
    refresh on an absent systemd or a probe timeout."""
    try:
        r = subprocess.run(
            ["systemctl", "show", "-p", "ActiveState", "--value", PUBLISH_UNIT],
            capture_output=True, text=True, timeout=10,
        )
    except Exception as e:  # FileNotFoundError (no systemd), timeout, etc.
        logger.warning("could not probe %s state: %s", PUBLISH_UNIT, e)
        return False
    return r.stdout.strip() in {"active", "activating", "reloading", "deactivating"}


def _maybe_update_stats(new_decisions: int) -> None:
    """Regenerate stats.json + git push when quick_publish inserted rows."""
    if new_decisions <= 0:
        logger.info("No new decisions inserted — skipping stats.json refresh")
        return

    # Don't fire the ~22-min generate_stats while the nightly full publish is
    # running: its parquet/HF/graph/stats tail saturates the data volume and
    # our scan over the ~69 GB decisions.db goes D-state (the build/serve
    # I/O-coupling stall the post-mortems warn about). The publish regenerates
    # stats.json itself (Step 5), and the +N rows are already searchable via
    # quick_publish, so skipping here loses only a little dashboard freshness
    # on the rare publish-overlap polls.
    if _full_publish_running():
        logger.info(
            "Full publish (%s) is running — skipping stats.json refresh "
            "(+%d new rows already searchable via quick_publish; the nightly "
            "Step 5 will regenerate stats.json)",
            PUBLISH_UNIT, new_decisions,
        )
        return

    # Single-flight lock: a 22-min generate_stats can outlive the 15-min
    # poll interval. The next poll's stats step would race with this
    # one; flock makes it skip cleanly.
    import fcntl
    lock_path = Path("/tmp/opencaselaw-stats.lock")
    lock_fd = open(lock_path, "w")
    try:
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logger.warning(
                "Another stats.json refresh is in progress (lock held) — "
                "skipping; the running instance will catch up our %d new rows",
                new_decisions,
            )
            return

        # Step 3a: regenerate stats.json. --no-interesting-stats keeps
        # the heavy weekly block from the on-disk file (the weekly timer
        # owns it) and re-runs only the daily/recent blocks.
        gs = REPO_DIR / "generate_stats.py"
        if not gs.exists():
            logger.warning("generate_stats.py not found — cannot refresh stats.json")
            return
        gs_cmd = [sys.executable, str(gs), "--no-interesting-stats"]
        logger.info("Regenerating stats.json (~22 min): %s", " ".join(gs_cmd))
        gs_t0 = time.time()
        gs_res = subprocess.run(
            gs_cmd,
            cwd=str(REPO_DIR),
            capture_output=True,
            text=True,
            timeout=2700,  # 45 min cap (steady-state ~22 min)
        )
        if gs_res.returncode != 0:
            logger.error(
                "generate_stats failed (exit %d): %s",
                gs_res.returncode, (gs_res.stderr or "")[-500:],
            )
            return
        logger.info(
            "stats.json regenerated in %.0fs",
            time.time() - gs_t0,
        )

        # Step 3b: only push if there's actually a diff (the generator
        # rewrites generated_at every run, so this is essentially always
        # true after a content change — cheap to verify).
        diff_check = subprocess.run(
            ["git", "diff", "--quiet", "docs/stats.json"],
            cwd=str(REPO_DIR), capture_output=True, text=True, timeout=30,
        )
        if diff_check.returncode == 0:
            logger.info("No diff in docs/stats.json — nothing to push")
            return

        # Step 3c: commit + push to GitHub Pages.
        for cmd in (
            ["git", "add", "docs/stats.json"],
            [
                "git",
                "-c", "user.name=opencaselaw-bot",
                "-c", "user.email=bot@opencaselaw.ch",
                "commit", "-m",
                f"Update stats.json — BGer poller +{new_decisions} new decisions",
            ],
            ["git", "push", "origin", "main"],
        ):
            r = subprocess.run(
                cmd,
                cwd=str(REPO_DIR),
                capture_output=True,
                text=True,
                timeout=300,
            )
            if r.returncode != 0:
                logger.error(
                    "git step failed (%s): %s",
                    " ".join(cmd[:3]), (r.stderr or "")[-300:],
                )
                return
        logger.info(
            "stats.json pushed (+%d decisions) — opencaselaw.ch refreshes in 1-2 min",
            new_decisions,
        )
    finally:
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        lock_fd.close()


def main():
    parser = argparse.ArgumentParser(description="BGer Neuheiten poller")
    parser.add_argument("--dry-run", action="store_true", help="Check only, don't trigger scraper")
    parser.add_argument("--force", action="store_true", help="Trigger scraper regardless")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        # Only StreamHandler — systemd captures stdout to the log file.
        # Adding FileHandler would cause duplicate lines.
    )

    today = date.today().strftime("%Y%m%d")
    today_iso = date.today().isoformat()

    if args.force:
        logger.info("Force mode — triggering scraper")
        _trigger_scraper()
        return

    # Fetch current Neuheiten
    try:
        current_dockets = _fetch_neuheiten(today)
    except Exception as e:
        logger.error("Failed to fetch Neuheiten: %s", e)
        return

    logger.info("Neuheiten %s: %d decisions", today_iso, len(current_dockets))

    # Silent-failure guard: the feed is never empty on a business day, so 0 dockets
    # on a workday means the fetch is blocked (not "no new decisions"). Alert
    # instead of quietly recording count:0 — that path hid a multi-week gap.
    if not current_dockets and _is_workday():
        _alert_empty_neuheiten(today_iso)

    # Compare against last state
    state = _load_state()
    prev_dockets = set(state.get("dockets", []))
    prev_failing: dict[str, int] = dict(state.get("failing", {}))

    if state.get("date") != today_iso:
        # New day — start fresh (don't carry over yesterday's dockets;
        # failing dockets rotate off the polled Neuheiten page with the
        # date, so their streaks are stale too — the nightly full scrape
        # is the backstop that still retries them via older date pages)
        prev_dockets = set()
        prev_failing = {}

    new_dockets = current_dockets - prev_dockets

    if new_dockets:
        logger.info(
            "NEW decisions detected: %d (%s)",
            len(new_dockets), ", ".join(sorted(new_dockets)[:5]),
        )
        if args.dry_run:
            logger.info("[dry-run] Would trigger BGer scraper")
            _save_state(today_iso, current_dockets, failing=prev_failing)
        else:
            qp_ok = _trigger_scraper()
            # Confirm which dockets ACTUALLY landed in JSONL (BGer's
            # document-service intermittently returns error pages for
            # freshly-Neuheiten'd dockets — those return None from the
            # scraper and never reach JSONL). Mark only the ingested
            # subset as "seen"; the rest stay un-seen so the next poll
            # retries them. Naturally bounded by Neuheiten rotation.
            #
            # 3 h window (not 1 h): after a quick_publish failure the
            # next poll re-confirms rows scraped by the PREVIOUS poll
            # (>1 h old); presence in JSONL is what the seen-cache
            # contract requires, so a wider window is strictly safer.
            #
            # If quick_publish did not complete, mark NOTHING — rows may
            # be in JSONL but not in the live DB; the next poll re-runs
            # the scraper (idempotent, +0 new) and retries quick_publish,
            # which sweeps all pending JSONL-vs-DB rows.
            ingested_recent = (
                _recently_ingested_dockets(window_seconds=10800)
                if qp_ok else set()
            )
            seen_state = prev_dockets | (current_dockets & ingested_recent)
            unsaved = new_dockets - ingested_recent
            failing = (_update_failing_streaks(prev_failing, unsaved)
                       if qp_ok else dict(prev_failing))
            _save_state(today_iso, seen_state, failing=failing)
            if unsaved and qp_ok:
                logger.warning(
                    "%d/%d new dockets NOT in JSONL (likely BGer "
                    "doc-service error pages); will retry next poll. "
                    "First few: %s",
                    len(unsaved), len(new_dockets),
                    ", ".join(sorted(unsaved)[:5]),
                )
                persistent = {d: n for d, n in failing.items()
                              if n >= FAILING_STREAK_ALERT}
                if persistent:
                    logger.error(
                        "BGer doc-service failure persisting >=%d "
                        "consecutive polls: %s — dockets are listed on "
                        "Neuheiten but their documents keep returning "
                        "error pages",
                        FAILING_STREAK_ALERT,
                        ", ".join(f"{d} (x{n})"
                                  for d, n in sorted(persistent.items())),
                    )
            elif unsaved:
                logger.warning(
                    "quick_publish did not complete — %d scraped dockets "
                    "left unmarked; next poll re-runs the scraper "
                    "(idempotent) and retries quick_publish. First few: %s",
                    len(unsaved), ", ".join(sorted(unsaved)[:5]),
                )
    else:
        logger.info("No new decisions since last check")
        _save_state(today_iso, current_dockets)


if __name__ == "__main__":
    main()
