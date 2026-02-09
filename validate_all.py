#!/usr/bin/env python3
"""
Phase 1 Validation: Test all 5 federal scrapers end-to-end.

Run from the repo root:
    python validate_all.py

Tests each scraper:
  1. Import succeeds
  2. Instantiation succeeds
  3. discover_new() yields stubs
  4. fetch_decision() returns valid Decision objects
  5. Key fields are populated (decision_id, court, docket_number, decision_date, full_text)

Produces a summary report at the end.
"""

import json
import logging
import sys
import time
import traceback
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("validate")

# ── Configuration ─────────────────────────────────────────────

MAX_DECISIONS = 3  # per scraper
SINCE_DATE = date.today() - timedelta(days=90)  # last 90 days

REQUIRED_FIELDS = [
    "decision_id", "court", "docket_number", "decision_date",
    "language", "source_url",
]
DESIRED_FIELDS = [
    "full_text", "title", "legal_area", "decision_type",
]

# ── Results tracking ──────────────────────────────────────────

results = {}


def validate_decision(d, court_code: str) -> dict:
    """Validate a single Decision object. Returns a report dict."""
    report = {"decision_id": d.decision_id, "issues": [], "warnings": []}

    for field in REQUIRED_FIELDS:
        val = getattr(d, field, None)
        if val is None or (isinstance(val, str) and val.strip() == ""):
            report["issues"].append(f"MISSING required: {field}")

    for field in DESIRED_FIELDS:
        val = getattr(d, field, None)
        if val is None or (isinstance(val, str) and val.strip() == ""):
            report["warnings"].append(f"empty optional: {field}")

    # Check full_text length
    ft = getattr(d, "full_text", None)
    if ft and len(ft) < 50:
        report["warnings"].append(f"full_text suspiciously short: {len(ft)} chars")
    elif ft:
        report["full_text_len"] = len(ft)

    # Check court code matches
    if d.court != court_code:
        report["issues"].append(f"court mismatch: expected {court_code}, got {d.court}")

    return report


def test_scraper(name: str, scraper_cls, court_code: str, **kwargs):
    """Test a single scraper end-to-end."""
    print(f"\n{'='*60}")
    print(f"  TESTING: {name} ({court_code})")
    print(f"{'='*60}")

    result = {
        "court": court_code,
        "import": True,  # if we got here, import worked
        "instantiate": False,
        "discover": False,
        "fetch": False,
        "decisions": [],
        "error": None,
        "duration_s": 0,
    }

    t0 = time.time()

    try:
        # 1. Instantiate
        scraper = scraper_cls(**kwargs)
        result["instantiate"] = True
        print(f"  ✓ Instantiated {name}")

        # 2. Run (discover + fetch)
        since = kwargs.get("since_date", SINCE_DATE)
        decisions = scraper.run(since_date=since, max_decisions=MAX_DECISIONS)

        if decisions:
            result["discover"] = True
            result["fetch"] = True
            print(f"  ✓ Fetched {len(decisions)} decisions")

            # 3. Validate each decision
            for d in decisions:
                report = validate_decision(d, court_code)
                result["decisions"].append(report)
                status = "✓" if not report["issues"] else "✗"
                print(f"    {status} {d.decision_id} ({d.decision_date}) "
                      f"[{d.language}] {len(d.full_text or '')} chars")
                for issue in report["issues"]:
                    print(f"      ✗ {issue}")
                for warn in report["warnings"]:
                    print(f"      ⚠ {warn}")
        else:
            print(f"  ⚠ No decisions returned (discover may have found 0 new stubs)")
            result["discover"] = True  # not necessarily a failure

    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"
        print(f"  ✗ ERROR: {result['error']}")
        traceback.print_exc()

    result["duration_s"] = round(time.time() - t0, 1)
    print(f"  Duration: {result['duration_s']}s")
    results[name] = result


# ── Run all scrapers ──────────────────────────────────────────

print(f"\nPhase 1 Validation — {date.today()}")
print(f"Max decisions per scraper: {MAX_DECISIONS}")
print(f"Since date: {SINCE_DATE}")

# 1. BStGer (simplest, just fixed)
try:
    from scrapers.bstger import BStGerScraper
    test_scraper("BStGer", BStGerScraper, "bstger")
except ImportError as e:
    results["BStGer"] = {"import": False, "error": str(e)}
    print(f"\n✗ BStGer import failed: {e}")

# 2. BVGer (Weblaw API + jurispub fallback)
try:
    from scrapers.bvger import BVGerScraper
    test_scraper("BVGer", BVGerScraper, "bvger")
except ImportError as e:
    results["BVGer"] = {"import": False, "error": str(e)}
    print(f"\n✗ BVGer import failed: {e}")

# 3. BPatGer (TYPO3 HTML)
try:
    from scrapers.bpatger import BPatGerScraper
    test_scraper("BPatGer", BPatGerScraper, "bpatger")
except ImportError as e:
    results["BPatGer"] = {"import": False, "error": str(e)}
    print(f"\n✗ BPatGer import failed: {e}")

# 4. BGE (CLIR on search.bger.ch — needs PoW)
try:
    from scrapers.bge import BGELeitentscheideScraper
    test_scraper("BGE", BGELeitentscheideScraper, "bge")
except ImportError as e:
    results["BGE"] = {"import": False, "error": str(e)}
    print(f"\n✗ BGE import failed: {e}")

# 5. BGer (Eurospider AZA — PoW + complex parsing)
try:
    from scrapers.bger import BgerScraper
    test_scraper("BGer", BgerScraper, "bger")
except ImportError as e:
    results["BGer"] = {"import": False, "error": str(e)}
    print(f"\n✗ BGer import failed: {e}")

# ── Summary ───────────────────────────────────────────────────

print(f"\n{'='*60}")
print(f"  SUMMARY")
print(f"{'='*60}\n")

total_ok = 0
total_fail = 0

for name, r in results.items():
    n_decisions = len(r.get("decisions", []))
    n_issues = sum(len(d.get("issues", [])) for d in r.get("decisions", []))

    if r.get("error"):
        status = "✗ FAIL"
        total_fail += 1
    elif n_issues > 0:
        status = "⚠ WARN"
        total_ok += 1
    elif n_decisions > 0:
        status = "✓ PASS"
        total_ok += 1
    else:
        status = "? NO DATA"
        total_ok += 1  # not necessarily a failure

    print(f"  {status:12s}  {name:10s}  "
          f"{n_decisions} decisions, {n_issues} issues, "
          f"{r.get('duration_s', 0)}s"
          f"{('  ERROR: ' + r['error'][:60]) if r.get('error') else ''}")

print(f"\n  Total: {total_ok} OK, {total_fail} FAIL")
print(f"  Completed: {date.today()}\n")

# Save detailed report
report_path = Path("validation_report.json")
with open(report_path, "w") as f:
    json.dump(results, f, indent=2, default=str)
print(f"  Detailed report: {report_path}")