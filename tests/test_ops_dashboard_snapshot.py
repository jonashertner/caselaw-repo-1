"""Pure parsers of the ops-dashboard snapshot generator."""
from __future__ import annotations

import sys
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
SwapTotal:             0 kB
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
    assert "anomaly-audit" in units
    assert "ocl-ops-dashboard" in units
    assert all("unrelated" not in u for u in units)
    assert t[0]["next"].startswith("Sun 2026-08-02 21:30:00")
