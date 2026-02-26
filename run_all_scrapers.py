#!/usr/bin/env python3
"""
run_all_scrapers.py — Daily scraper orchestration
===================================================

Runs all registered scrapers with controlled parallelism, timeouts, and
health checks. Designed to be called by cron before publish.py.

Architecture:
- Scrapers are grouped into batches that run concurrently (max_parallel)
- Each scraper gets a per-scraper timeout (default 2h, configurable)
- Scrapers that complete quickly (few new decisions) free up slots
- All output is logged per-scraper to logs/{court}.log
- A summary is written to logs/daily_scrape.log

Special scrapers:
- ow_gerichte: Needs Playwright (chromium), ~6s/decision, ~3.7h full run
- ju_gerichte: Needs SOCKS proxy on localhost:1080 (SSH tunnel)
- bger, bge: Need Incapsula bypass via Playwright

Cron (run before publish.py):
    0 1 * * * cd /opt/caselaw/repo && python3 run_all_scrapers.py >> logs/daily_scrape.log 2>&1
    15 3 * * * cd /opt/caselaw/repo && python3 publish.py >> logs/publish.log 2>&1

Usage:
    python3 run_all_scrapers.py                    # run all scrapers
    python3 run_all_scrapers.py --courts bger,bge  # run specific scrapers
    python3 run_all_scrapers.py --exclude ow_gerichte  # skip specific scrapers
    python3 run_all_scrapers.py --parallel 4       # max 4 concurrent
    python3 run_all_scrapers.py --timeout 3600     # 1h timeout per scraper
    python3 run_all_scrapers.py --dry-run          # show what would run
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("daily_scrape")

REPO_DIR = Path(__file__).parent.resolve()

# Default timeout per scraper (seconds)
DEFAULT_TIMEOUT = 7200  # 2 hours

# Maximum concurrent scrapers
DEFAULT_PARALLEL = 6

# Scrapers that need extra time (Playwright-based, large volume)
SLOW_SCRAPERS = {
    "ow_gerichte": 14400,   # 4h — Playwright, ~6s/decision
    "vd_gerichte": 14400,   # 4h — 40k+ decisions, PDF heavy
    "bger": 10800,          # 3h — 90k decisions, Incapsula
    "bge": 10800,           # 3h — Incapsula
    "bvger": 10800,         # 3h — 90k decisions
    "zh_gerichte": 10800,   # 3h — 34k decisions
    "zh_sozialversicherungsgericht": 10800,  # 3h — 34k decisions
}

# Scrapers to skip by default (broken, redundant, or handled separately)
SKIP_BY_DEFAULT: set[str] = {
    "be_steuerrekurs",  # Portal DB disconnected (Feb 2026), returns 0 results
"ne_gerichte",      # Portal blocks Hetzner IPs; needs NE_PROXY env var (SOCKS5 tunnel)
}

# Disk usage thresholds (percent)
DISK_WARN_PERCENT = 85
DISK_CRITICAL_PERCENT = 95


def check_disk_usage() -> dict:
    """Check disk usage for the data volume and repo directory.

    Returns dict with total_gb, used_gb, free_gb, used_percent for each path.
    """
    paths_to_check = {
        "data_volume": Path("/mnt/HC_Volume_104655575"),
        "repo": REPO_DIR,
    }
    result = {}
    for label, path in paths_to_check.items():
        if not path.exists():
            continue
        try:
            usage = shutil.disk_usage(path)
            info = {
                "path": str(path),
                "total_gb": round(usage.total / (1024**3), 1),
                "used_gb": round(usage.used / (1024**3), 1),
                "free_gb": round(usage.free / (1024**3), 1),
                "used_percent": round(usage.used / usage.total * 100, 1),
            }
            result[label] = info

            if info["used_percent"] >= DISK_CRITICAL_PERCENT:
                logger.error(
                    f"CRITICAL: {label} disk at {info['used_percent']}% "
                    f"({info['free_gb']} GB free) — scraping may fail!"
                )
            elif info["used_percent"] >= DISK_WARN_PERCENT:
                logger.warning(
                    f"WARNING: {label} disk at {info['used_percent']}% "
                    f"({info['free_gb']} GB free) — consider cleanup"
                )
            else:
                logger.info(
                    f"Disk {label}: {info['used_percent']}% used "
                    f"({info['free_gb']} GB free)"
                )
        except Exception as e:
            logger.warning(f"Could not check disk for {label}: {e}")
    return result


def get_all_courts() -> list[str]:
    """Get all registered court codes from run_scraper.py."""
    # Import here to avoid circular imports
    sys.path.insert(0, str(REPO_DIR))
    from run_scraper import SCRAPERS
    return sorted(SCRAPERS.keys())


def run_single_scraper(court: str, timeout: int) -> dict:
    """
    Run a single scraper as a subprocess.

    Returns dict with: court, success, new_count, duration, error
    """
    start = time.time()
    log_path = REPO_DIR / "logs" / f"{court}.log"
    log_path.parent.mkdir(exist_ok=True)
    log_start = log_path.stat().st_size if log_path.exists() else 0

    cmd = [sys.executable, str(REPO_DIR / "run_scraper.py"), court]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
            cwd=str(REPO_DIR),
        )
        duration = time.time() - start

        # Parse only this run's appended log region:
        # [court] Done. New: 42, Skips: 5, Errors: 3, ...
        new_count = 0
        skip_count = 0
        error_count = 0
        none_count = 0
        error_tail: deque[str] = deque(maxlen=6)
        if log_path.exists():
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                if log_start > 0:
                    f.seek(log_start)
                for line in f:
                    if "Done. New:" in line:
                        try:
                            new_count = int(line.split("New:")[1].split(",")[0].strip())
                        except (ValueError, IndexError):
                            pass
                        if "Skips:" in line:
                            try:
                                skip_count = int(line.split("Skips:")[1].split(",")[0].strip())
                            except (ValueError, IndexError):
                                pass
                        if "NoneReturns:" in line:
                            try:
                                none_count = int(line.split("NoneReturns:")[1].split(",")[0].strip())
                            except (ValueError, IndexError):
                                pass
                        try:
                            error_count = int(line.split("Errors:")[1].split(",")[0].strip())
                        except (ValueError, IndexError):
                            pass
                    if " ERROR " in line or "Traceback" in line:
                        error_tail.append(line.strip())

        error = None
        note = None
        failed = result.returncode != 0
        if result.returncode != 0:
            error = " | ".join(error_tail) if error_tail else f"Exit code {result.returncode}"
        elif error_count > 0 and error_count > none_count:
            # Real exceptions (not just NoneReturns)
            real_errors = error_count - none_count
            error = f"{real_errors} scraping errors"
            if real_errors > 20 and real_errors > new_count:
                failed = True

        # NoneReturns are expected for portals with a few broken entries.
        # Only flag as a note, not an error, unless excessive.
        if none_count > 0:
            if none_count >= 200:
                error = f"{none_count} unavailable decisions (possible portal issue)"
                failed = True
            else:
                note = f"{none_count} listed on portal but content not downloadable (empty page or missing PDF)"

        return {
            "court": court,
            "success": not failed,
            "new_count": new_count,
            "skip_count": skip_count,
            "error_count": max(0, error_count - none_count),  # real errors only
            "none_count": none_count,
            "duration": duration,
            "error": error,
            "note": note,
        }

    except subprocess.TimeoutExpired:
        duration = time.time() - start
        # Parse any progress from the log before timeout
        new_count = 0
        if log_path.exists():
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                if log_start > 0:
                    f.seek(log_start)
                for line in f:
                    if "Scraped:" in line:
                        new_count += 1
        return {
            "court": court,
            "success": False,
            "timed_out": True,
            "new_count": new_count,
            "skip_count": 0,
            "error_count": 0,
            "none_count": 0,
            "duration": duration,
            "error": None,
            "note": None,
        }
    except Exception as e:
        duration = time.time() - start
        return {
            "court": court,
            "success": False,
            "new_count": 0,
            "skip_count": 0,
            "error_count": 0,
            "none_count": 0,
            "duration": duration,
            "error": str(e)[:200],
            "note": None,
        }


def main():
    parser = argparse.ArgumentParser(description="Run all scrapers daily")
    parser.add_argument(
        "--courts", type=str, default="",
        help="Comma-separated court codes to run (default: all)",
    )
    parser.add_argument(
        "--exclude", type=str, default="",
        help="Comma-separated court codes to skip",
    )
    parser.add_argument(
        "--parallel", type=int, default=DEFAULT_PARALLEL,
        help=f"Max concurrent scrapers (default: {DEFAULT_PARALLEL})",
    )
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT,
        help=f"Default timeout per scraper in seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would run")
    parser.add_argument(
        "--source", type=str, default="cron", choices=["cron", "manual"],
        help="Run source: 'cron' writes to scraper_health.json, "
             "'manual' writes to scraper_health_manual.json",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.parallel < 1:
        parser.error("--parallel must be at least 1")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Determine which courts to run
    all_courts = get_all_courts()

    if args.courts:
        courts = [c.strip() for c in args.courts.split(",") if c.strip()]
        unknown = set(courts) - set(all_courts)
        if unknown:
            logger.error(f"Unknown courts: {unknown}. Available: {all_courts}")
            sys.exit(1)
    else:
        courts = [c for c in all_courts if c not in SKIP_BY_DEFAULT]

    if args.exclude:
        exclude = {c.strip() for c in args.exclude.split(",")}
        courts = [c for c in courts if c not in exclude]

    # Ensure logs directory exists
    (REPO_DIR / "logs").mkdir(exist_ok=True)

    now = datetime.now(timezone.utc)
    logger.info(f"=== Daily scrape — {now.isoformat()} ===")
    logger.info(f"Courts: {len(courts)}, Parallel: {args.parallel}, Timeout: {args.timeout}s")

    # Pre-flight disk check
    disk_before = check_disk_usage()
    for label, info in disk_before.items():
        if info["used_percent"] >= DISK_CRITICAL_PERCENT:
            logger.error(f"Aborting: {label} disk at {info['used_percent']}% — not enough space to scrape safely")
            sys.exit(2)

    if args.dry_run:
        for court in courts:
            t = SLOW_SCRAPERS.get(court, args.timeout)
            logger.info(f"  [dry-run] Would run: {court} (timeout: {t}s)")
        return

    # Run scrapers with controlled parallelism
    results = []
    total_start = time.time()

    with ProcessPoolExecutor(max_workers=args.parallel) as executor:
        futures = {}
        for court in courts:
            # SLOW_SCRAPERS sets the default for known-slow scrapers,
            # but --timeout always acts as an upper bound (for health checks)
            default = SLOW_SCRAPERS.get(court, DEFAULT_TIMEOUT)
            timeout = min(default, args.timeout) if args.timeout != DEFAULT_TIMEOUT else default
            future = executor.submit(run_single_scraper, court, timeout)
            futures[future] = court

        for future in as_completed(futures):
            court = futures[future]
            try:
                result = future.result()
                results.append(result)
                if result.get("timed_out"):
                    status = "RUNNING"
                elif result["success"]:
                    status = "OK"
                else:
                    status = "FAILED"
                logger.info(
                    f"  [{status}] {result['court']}: "
                    f"+{result['new_count']} new, "
                    f"{result['duration']:.0f}s"
                    f"{' — ' + result['error'] if result['error'] else ''}"
                )
            except Exception as e:
                logger.error(f"  [ERROR] {court}: {e}")
                results.append({
                    "court": court,
                    "success": False,
                    "new_count": 0,
                    "skip_count": 0,
                    "error_count": 0,
                    "duration": 0,
                    "error": str(e)[:200],
                })

    # Summary
    total_elapsed = time.time() - total_start
    succeeded = sum(1 for r in results if r["success"] and not r.get("timed_out"))
    timed_out = sum(1 for r in results if r.get("timed_out"))
    failed = sum(1 for r in results if not r["success"] and not r.get("timed_out"))
    total_new = sum(r["new_count"] for r in results)

    logger.info("\n=== Summary ===")
    logger.info(f"  Succeeded: {succeeded}/{len(results)}")
    if timed_out:
        logger.info(f"  Still running: {timed_out}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Total new decisions: {total_new}")
    logger.info(f"  Total time: {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)")

    if timed_out:
        logger.info("\n  Still running (hit time cap):")
        for r in results:
            if r.get("timed_out"):
                logger.info(f"    - {r['court']} (+{r['new_count']} new in >{r['duration']:.0f}s)")

    if failed:
        logger.info("\n  Failed scrapers:")
        for r in results:
            if not r["success"]:
                logger.info(f"    - {r['court']}: {r['error']}")

    # Persist health data for dashboard
    health = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "run_duration_s": round(total_elapsed, 1),
        "scrapers": {
            r["court"]: {
                "success": r["success"],
                "timed_out": r.get("timed_out", False),
                "new_count": r["new_count"],
                "skip_count": r["skip_count"],
                "error_count": r["error_count"],
                "none_count": r.get("none_count", 0),
                "duration_s": round(r["duration"], 1),
                "error": r["error"],
                "note": r.get("note"),
            }
            for r in results
        },
    }

    # Post-run disk check
    disk_after = check_disk_usage()
    health["disk"] = disk_after
    logger.info("\n  Disk usage after run:")
    for label, info in disk_after.items():
        logger.info(f"    {label}: {info['used_percent']}% ({info['free_gb']} GB free)")

    if args.source == "manual":
        health_filename = "scraper_health_manual.json"
    else:
        health_filename = "scraper_health.json"
    health_path = REPO_DIR / "logs" / health_filename
    health_path.write_text(json.dumps(health, indent=2))
    logger.info(f"Health data written to {health_path}")

    # Only fail on real errors, not timeouts
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
