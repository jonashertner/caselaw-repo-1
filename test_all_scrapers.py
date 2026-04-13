#!/usr/bin/env python3
"""
test_all_scrapers.py — Smoke-test every registered scraper.

Runs each scraper with --max 1 and a 90s timeout to verify it can:
1. Import and instantiate
2. Connect to its source portal
3. Discover and fetch at least one decision

Usage:
    python3 test_all_scrapers.py              # test all scrapers
    python3 test_all_scrapers.py --parallel 8 # max 8 concurrent
    python3 test_all_scrapers.py --courts bger,bge  # test specific ones
    python3 test_all_scrapers.py --timeout 120      # per-scraper timeout
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

REPO_DIR = Path(__file__).parent.resolve()

# Scrapers known to be broken/offline — document why
KNOWN_BROKEN = {
    "be_steuerrekurs": "Portal DB disconnected (Feb 2026)",
    "ow_gerichte": "Portal offline (503) since late 2022",
}


def get_all_courts() -> list[str]:
    sys.path.insert(0, str(REPO_DIR))
    from run_scraper import SCRAPERS
    return sorted(SCRAPERS.keys())


def test_one(court: str, timeout: int, max_decisions: int) -> dict:
    """Run a single scraper with --max N and return result."""
    start = time.time()
    cmd = [
        sys.executable,
        str(REPO_DIR / "run_scraper.py"),
        court,
        "--max", str(max_decisions),
    ]

    log_path = REPO_DIR / "logs" / f"{court}.log"
    log_start = log_path.stat().st_size if log_path.exists() else 0

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(REPO_DIR),
        )
        duration = time.time() - start

        # Parse log for the Done line
        new_count = 0
        our_count = None
        portal_count = None
        error_msg = None
        if log_path.exists():
            import re
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                if log_start > 0:
                    f.seek(log_start)
                lines = f.readlines()
                for line in lines:
                    if "Done. +" in line or "Done. New:" in line:
                        m = re.search(r'\+(\d+) new', line)
                        if m:
                            new_count = int(m.group(1))
                        elif "New:" in line:
                            try:
                                new_count = int(line.split("New:")[1].split(",")[0].strip())
                            except (ValueError, IndexError):
                                pass
                        m = re.search(r'new, (\d+)/(\d+)', line)
                        if m:
                            our_count = int(m.group(1))
                            portal_count = int(m.group(2))
                        else:
                            m = re.search(r'new, (\d+),', line)
                            if m:
                                our_count = int(m.group(1))
                # Grab last ERROR lines if failed
                if result.returncode != 0:
                    error_lines = [l.strip() for l in lines if " ERROR " in l or "Traceback" in l]
                    error_msg = " | ".join(error_lines[-3:]) if error_lines else None

        if result.returncode != 0 and not error_msg:
            # Check stderr
            stderr_tail = result.stderr.strip().split("\n")[-3:] if result.stderr else []
            error_msg = " | ".join(stderr_tail) if stderr_tail else f"exit code {result.returncode}"

        return {
            "court": court,
            "ok": result.returncode == 0,
            "new": new_count,
            "our_count": our_count,
            "portal_count": portal_count,
            "duration": round(duration, 1),
            "error": error_msg,
        }

    except subprocess.TimeoutExpired:
        return {
            "court": court,
            "ok": False,
            "new": 0,
            "duration": round(time.time() - start, 1),
            "error": f"TIMEOUT after {timeout}s",
        }
    except Exception as e:
        return {
            "court": court,
            "ok": False,
            "new": 0,
            "duration": round(time.time() - start, 1),
            "error": str(e)[:200],
        }


def main():
    parser = argparse.ArgumentParser(description="Smoke-test all scrapers")
    parser.add_argument("--courts", type=str, default="")
    parser.add_argument("--exclude", type=str, default="")
    parser.add_argument("--parallel", type=int, default=6)
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--max", type=int, default=1, help="Max decisions per scraper")
    parser.add_argument("--include-broken", action="store_true",
                        help="Include known-broken scrapers")
    args = parser.parse_args()

    all_courts = get_all_courts()

    if args.courts:
        courts = [c.strip() for c in args.courts.split(",") if c.strip()]
    else:
        courts = all_courts

    if args.exclude:
        exclude = {c.strip() for c in args.exclude.split(",")}
        courts = [c for c in courts if c not in exclude]

    if not args.include_broken:
        skipped = {c for c in courts if c in KNOWN_BROKEN}
        courts = [c for c in courts if c not in KNOWN_BROKEN]
    else:
        skipped = set()

    print(f"Testing {len(courts)} scrapers (parallel={args.parallel}, "
          f"timeout={args.timeout}s, max={args.max})")
    if skipped:
        print(f"Skipping known-broken: {', '.join(sorted(skipped))}")
    print()

    results = []
    with ProcessPoolExecutor(max_workers=args.parallel) as executor:
        futures = {
            executor.submit(test_one, court, args.timeout, args.max): court
            for court in courts
        }
        for future in as_completed(futures):
            r = future.result()
            results.append(r)
            status = "OK" if r["ok"] else "FAIL"
            icon = "\u2705" if r["ok"] else "\u274c"
            coverage = ""
            if r.get("portal_count") is not None and r.get("our_count") is not None:
                gap = r["portal_count"] - r["our_count"]
                coverage = f" {r['our_count']}/{r['portal_count']}"
                if gap > 0:
                    coverage += f" (gap {gap})"
            elif r.get("our_count") is not None:
                coverage = f" {r['our_count']}"
            line = f"  {icon} {status:4s} {r['court']:35s} +{r['new']:3d} new{coverage:>20s}  {r['duration']:5.1f}s"
            if r["error"]:
                line += f"  -- {r['error'][:80]}"
            print(line)

    # Sort results for summary
    results.sort(key=lambda r: (r["ok"], r["court"]))

    passed = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]

    print(f"\n{'='*60}")
    print(f"PASSED: {len(passed)}/{len(results)}")
    if failed:
        print(f"FAILED: {len(failed)}/{len(results)}")
        for r in failed:
            print(f"  - {r['court']}: {r['error']}")
    if skipped:
        print(f"SKIPPED (known-broken): {len(skipped)}")
        for c in sorted(skipped):
            print(f"  - {c}: {KNOWN_BROKEN[c]}")

    # Write results to JSON
    out_path = REPO_DIR / "logs" / "scraper_test_results.json"
    out_path.parent.mkdir(exist_ok=True)
    with open(out_path, "w") as f:
        json.dump({
            "tested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total": len(results),
            "passed": len(passed),
            "failed": len(failed),
            "skipped_known_broken": sorted(skipped),
            "results": sorted(results, key=lambda r: r["court"]),
        }, f, indent=2)
    print(f"\nResults written to {out_path}")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
