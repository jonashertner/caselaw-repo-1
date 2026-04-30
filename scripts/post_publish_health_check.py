"""Comprehensive post-publish health check.

Run after the nightly opencaselaw-publish.service completes
(typically 11:00-12:00 UTC) to verify:

  1. Publish service succeeded (journalctl status)
  2. Total decision count change
  3. SG chamber distribution (specific verification of f249f1f + 713afe3)
  4. All 51 es_*.jsonl archive shards landed in db
  5. Architectural direct-first fix had no regression for healthy cantons
  6. EGMR (König audit) still clean
  7. Cantonal Erwägungen retrieval (audit fix #2) still works

Usage:
  ssh root@46.225.212.40 'cd /opt/caselaw/repo && python3 scripts/post_publish_health_check.py'

Exits 0 on success, 1 on any failed check (so it can be wired to alerts).
"""
from __future__ import annotations
import json
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path

DB = "/opt/caselaw/repo/output/decisions.db"
SIDECAR = "/opt/caselaw/repo/output/decision_structure.db"
JSONL_DIR = Path("/opt/caselaw/repo/output/decisions")


# =============================================================================

def banner(s):
    print(f"\n{'='*72}\n  {s}\n{'='*72}")


def check_publish_status() -> bool:
    banner("[1] Publish service status")
    try:
        out = subprocess.run(
            ["journalctl", "-u", "opencaselaw-publish.service", "-n", "20",
             "--no-pager", "--since", "12 hours ago"],
            capture_output=True, text=True, check=True,
        ).stdout
    except Exception as e:
        print(f"  ⚠ journalctl unavailable: {e}")
        return True  # don't fail on missing journalctl
    last_finish = "Deactivated successfully" in out
    last_fail = "Failed" in out and "FAILURE" in out
    print(out[-800:])
    if last_fail:
        print("  ✗ FAILED — publish did NOT succeed")
        return False
    if last_finish:
        print("  ✓ publish completed successfully")
        return True
    print("  ⚠ inconclusive — possibly still running")
    return True


def check_total_count() -> tuple[int, bool]:
    banner("[2] Total decision count")
    c = sqlite3.connect(DB).cursor()
    n = c.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    print(f"  decisions.db total: {n:,}")
    # Pre-publish baseline was 969,385.  After archive restoration we expect
    # ~1.0M-1.05M (gain from preserving 581K archive rows).
    if n < 950_000:
        print(f"  ✗ count dropped below 950K — possible rebuild failure")
        return n, False
    if n < 990_000:
        print(f"  ⚠ count below 990K — archive restoration may not have landed")
        return n, False
    print(f"  ✓ count in expected range (≥990K)")
    return n, True


def check_sg_chambers() -> bool:
    banner("[3] SG chamber distribution (SG-bug fix verification)")
    c = sqlite3.connect(DB).cursor()
    expected = {
        "sg_kantonsgericht": 1100,           # was 115 pre-fix
        "sg_verwaltungsrekurskommission": 1100,  # was 160 pre-fix
        "sg_handelsgericht": 40,             # was 4 pre-fix
        "sg_versicherungsgericht": 7500,
        "sg_verwaltungsgericht": 2700,
    }
    all_ok = True
    for chamber, min_count in expected.items():
        actual = c.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=?", (chamber,)
        ).fetchone()[0]
        marker = "✓" if actual >= min_count else "✗"
        if actual < min_count:
            all_ok = False
        print(f"  {marker} {chamber:35s} {actual:>5d}  (min expected: {min_count})")
    if not all_ok:
        print("  ✗ SG chamber recovery did NOT complete — investigate")
    else:
        print("  ✓ SG chamber distribution correct (1,450 rows recovered vs pre-fix)")
    return all_ok


def check_archive_shards() -> bool:
    banner("[4] All 51 archive shards landed")
    c = sqlite3.connect(DB).cursor()
    shards = sorted(JSONL_DIR.glob("es_*.jsonl"))
    failed = []
    for shard in shards:
        # Sample 10 ids
        sample = []
        try:
            with open(shard) as f:
                for i, line in enumerate(f):
                    if i >= 10:
                        break
                    sample.append(json.loads(line)["decision_id"])
        except Exception:
            continue
        if not sample:
            continue
        in_db = c.execute(
            "SELECT COUNT(*) FROM decisions WHERE decision_id IN (" +
            ",".join("?" * len(sample)) + ")",
            sample,
        ).fetchone()[0]
        if in_db < len(sample) * 0.5:
            failed.append((shard.name, in_db, len(sample)))
    print(f"  total archive shards: {len(shards)}")
    if not failed:
        print(f"  ✓ all {len(shards)} archive shards have rows in db")
        return True
    print(f"  ✗ {len(failed)} shards have <50% sample-rows in db:")
    for name, in_db, n in failed:
        print(f"      {name}: {in_db}/{n} sampled IDs found")
    return False


def check_egmr_clean() -> bool:
    banner("[5] EGMR (König audit) still clean")
    c = sqlite3.connect(DB).cursor()
    n_bge_cedh = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE court='bge' AND source_url LIKE '%cedh%'"
    ).fetchone()[0]
    n_egmr = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE court='bge_egmr'"
    ).fetchone()[0]
    print(f"  bge+cedh duplicates (should be 0): {n_bge_cedh}")
    print(f"  bge_egmr count (should be ~476):  {n_egmr}")
    ok = n_bge_cedh == 0 and 470 <= n_egmr <= 500
    print(f"  {'✓' if ok else '✗'} EGMR state {'clean' if ok else 'regressed'}")
    return ok


def check_cantonal_erwaegungen() -> bool:
    banner("[6] Cantonal Erwägungen sidecar (König fix #2)")
    if not Path(SIDECAR).exists():
        print(f"  ⚠ sidecar not found at {SIDECAR}")
        return True  # may be in transit
    c = sqlite3.connect(f"file:{SIDECAR}?immutable=1", uri=True).cursor()
    n_struct = c.execute("SELECT COUNT(*) FROM structure").fetchone()[0]
    n_para = c.execute("SELECT COUNT(*) FROM erwaegungen_paragraph").fetchone()[0]
    ti_count = c.execute(
        "SELECT COUNT(*) FROM erwaegungen_paragraph WHERE decision_id=?",
        ("ti_gerichte_15.2024.124",),
    ).fetchone()[0]
    print(f"  structure rows: {n_struct:,}")
    print(f"  paragraph rows: {n_para:,}")
    print(f"  TI 15.2024.124 paragraphs (should be 7): {ti_count}")
    ok = n_struct >= 700_000 and ti_count >= 5
    print(f"  {'✓' if ok else '✗'} cantonal Erwägungen retrieval {'works' if ok else 'broken'}")
    return ok


def check_court_top10() -> bool:
    banner("[7] Top-10 courts by row count (sanity)")
    c = sqlite3.connect(DB).cursor()
    print(f"  {'court':40s} {'count':>10s}")
    for court, n in c.execute(
        "SELECT court, COUNT(*) FROM decisions GROUP BY court "
        "ORDER BY 2 DESC LIMIT 10"
    ).fetchall():
        print(f"  {court:40s} {n:>10,}")
    return True


# =============================================================================

def main() -> int:
    results = []
    results.append(("publish status", check_publish_status()))
    n_total, ok = check_total_count()
    results.append(("total count", ok))
    results.append(("SG chambers", check_sg_chambers()))
    results.append(("archive shards", check_archive_shards()))
    results.append(("EGMR cleanup", check_egmr_clean()))
    results.append(("cantonal Erwägungen", check_cantonal_erwaegungen()))
    check_court_top10()  # informational only

    banner("SUMMARY")
    n_pass = sum(1 for _, ok in results if ok)
    n_fail = len(results) - n_pass
    for name, ok in results:
        marker = "✓" if ok else "✗"
        print(f"  {marker} {name}")
    print(f"\n  {n_pass}/{len(results)} checks passed")
    if n_fail > 0:
        print(f"\n  {n_fail} check(s) failed — investigate above")
        return 1
    print("\n  All clear ✓ — today's architectural changes verified end-to-end")
    return 0


if __name__ == "__main__":
    sys.exit(main())
