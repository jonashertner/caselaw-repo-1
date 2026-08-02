"""Pure logic of the ops-dashboard snapshot generator (v2)."""
from __future__ import annotations

import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts import ops_dashboard_snapshot as ods  # noqa: E402

MEMINFO = """MemTotal:       64228700 kB
MemFree:         1122304 kB
MemAvailable:   55677500 kB
Buffers:          511220 kB
Cached:         52428800 kB
"""

TIMERS = """NEXT LEFT LAST PASSED UNIT ACTIVATES
Sun 2026-08-02 21:30:00 UTC 4min - - opencaselaw-anomaly-audit.timer opencaselaw-anomaly-audit.service
Sun 2026-08-02 21:35:12 UTC 9min - - ocl-ops-dashboard.timer ocl-ops-dashboard.service
Mon 2026-08-03 00:00:00 UTC 2h - - unrelated.timer unrelated.service
"""


def test_parse_meminfo_gib_rounding():
    m = ods.parse_meminfo(MEMINFO)
    assert m["mem_avail_g"] == 53.1
    assert m["mem_cache_g"] == 50.5


def test_parse_timers_filters_and_shortens():
    t = ods.parse_timers(TIMERS)
    units = [x["unit"] for x in t]
    assert "anomaly-audit" in units and "ocl-ops-dashboard" in units
    assert all("unrelated" not in u for u in units)
    assert t[0]["next"].startswith("Sun 2026-08-02 21:30:00")


def test_friendly_build_state_vocabulary():
    assert ods.friendly_build_state("activating", None) == ("running", None)
    assert ods.friendly_build_state("inactive", 0) == ("idle", True)
    assert ods.friendly_build_state("inactive", 1) == ("idle", False)
    assert ods.friendly_build_state("failed", 1) == ("failed", False)


def _tier2_lines(now: float) -> str:
    def line(age_s: int, actor: str, status: str, rt: float) -> str:
        ts = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.localtime(now - age_s))
        return f"{ts} {actor} rest_search GET {status} {rt} 1234 -"
    rows = [line(10, "claude_hosted", "200", 0.2),
            line(20, "claude_hosted", "200", 1.0),
            line(30, "bot_google", "200", 0.01),
            line(40, "word_addin", "500", 4.0),
            line(5000, "claude_hosted", "200", 9.9)]        # outside window
    return "\n".join(rows)


def test_parse_tier2_window_splits_humans_and_errors():
    now = time.time()
    t = ods.parse_tier2_window(_tier2_lines(now), now)
    assert t["requests"] == 4                # 5000s-old line excluded
    assert t["r5xx"] == 1 and t["err_rate_pct"] == 25.0
    assert t["human_requests"] == 3          # bot_google excluded
    assert t["human_p50_ms"] in (200, 1000)  # median of 0.2/1.0/4.0 -> 1000
    assert t["human_p95_ms"] == 4000
    assert t["top_actors"][0][0] == "claude_hosted"


def test_shape_intake_movers_and_failures():
    d = {"run_at": "2026-08-03T03:00:00Z", "run_duration_s": 900,
         "scrapers": {
             "a": {"success": True, "new_count": 12, "error_count": 0},
             "b": {"success": False, "new_count": 0, "error_count": 3},
             "c": {"success": True, "new_count": 5, "error_count": 1}}}
    i = ods.shape_intake(d)
    assert i["scrapers"] == 3 and i["failed"] == ["b"]
    assert i["top_new"][0] == ("a", 12)
    assert ("b", 3) in i["errors"] and ("c", 1) in i["errors"]
    assert i["new_last_round"] == 17


def test_update_history_ring_and_min_gap(tmp_path):
    p = tmp_path / "history.jsonl"
    h1 = ods.update_history({"ts": 1000, "disk_free_g": 40.0}, path=p)
    h2 = ods.update_history({"ts": 1100, "disk_free_g": 39.9}, path=p)   # <600s: dropped
    h3 = ods.update_history({"ts": 1700, "disk_free_g": 39.8}, path=p)
    assert len(h1) == 1 and len(h2) == 1 and len(h3) == 2
    assert h3[-1]["disk_free_g"] == 39.8


def test_verdict_worst_of_and_details():
    snap = {"serving": {"workers_up": 8, "workers_total": 8,
                        "probe": {"ok": True, "ms": 2100}},
            "traffic_1h": {"requests": 5000, "err_rate_pct": 0.1},
            "build": {"state": "idle", "result_ok": True},
            "intake": {"scrapers": 67, "failed": []},
            "integrity": {"audit_fresh": True, "toolcheck_pass": True},
            "uniques": {"state_age_s": 120},
            "host": {"disk_used_pct": 73}}
    v = ods.verdict(snap)
    assert v["level"] == "ok"
    snap["host"]["disk_used_pct"] = 91
    snap["serving"]["probe"] = {"ok": False, "error": "timeout"}
    v = ods.verdict(snap)
    assert v["level"] == "fail"
    names = {c["name"]: c["level"] for c in v["checks"]}
    assert names["disk"] == "fail" and names["search probe"] == "fail"
