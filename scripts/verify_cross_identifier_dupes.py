#!/usr/bin/env python3
"""Verify (READ-ONLY) the new cross-identifier candidates from the detection scan.

Uses idx_decisions_court so each court reads only its own rows (fast). For each
watchlist court, groups rows by source_url, and for groups of EXACTLY 2 reports:
docket pair, dates, whether the two bodies are byte-identical (content_hash), and
which side carries a regeste / how long each text is. This distinguishes:
  * genuine twins (same decision, 2 dockets, same URL) -> duplication
  * complementary-metadata twins (GE/VD pattern: regeste on one side only)
  * byte-identical republication (content_hash equal)
vs false alarms (a URL legitimately covering 2 distinct rulings).
"""
from __future__ import annotations
import collections
import sqlite3

import os
DB = "file:%s?mode=ro&immutable=1" % os.environ.get("OCL_DECISIONS_DB", "output/decisions.db")
COURTS = ["ch_vb", "lu_gerichte", "nw_gerichte", "edoeb",
          "be_verwaltungsgericht", "ur_gerichte", "weko"]

c = sqlite3.connect(DB, uri=True)
for court in COURTS:
    by_url = collections.defaultdict(list)
    for r in c.execute(
        "SELECT source_url, decision_id, docket_number, decision_date, "
        "length(full_text), (regeste IS NOT NULL AND regeste!=''), content_hash "
        "FROM decisions WHERE court=? AND source_url IS NOT NULL AND source_url!=''",
        (court,),
    ):
        by_url[r[0]].append(r[1:])
    pairs = {u: v for u, v in by_url.items() if len(v) == 2}
    trip = {u: v for u, v in by_url.items() if len(v) >= 3}
    same_hash = sum(1 for v in pairs.values() if v[0][5] == v[1][5])
    diff_docket = sum(1 for v in pairs.values() if v[0][1] != v[1][1])
    same_date = sum(1 for v in pairs.values() if v[0][2] == v[1][2])
    reg_asym = sum(1 for v in pairs.values() if v[0][4] != v[1][4])
    print(f"\n===== {court}: {len(pairs):,} exactly-2 URL groups, "
          f"{len(trip):,} groups of >=3 =====")
    print(f"  of the 2-groups: different docket={diff_docket:,}  same date={same_date:,}  "
          f"byte-identical body={same_hash:,}  regeste-asymmetric={reg_asym:,}")
    shown = 0
    for u, v in pairs.items():
        if v[0][1] == v[1][1]:
            continue  # same docket twice -> not a cross-identifier case
        a, b = v
        print(f"  URL {u[:70]}")
        print(f"     A dk={a[1]!r:22} date={a[2]} len={a[3]:>6} reg={a[4]} hash={a[5][:8]}")
        print(f"     B dk={b[1]!r:22} date={b[2]} len={b[3]:>6} reg={b[4]} hash={b[5][:8]}")
        shown += 1
        if shown >= 4:
            break
