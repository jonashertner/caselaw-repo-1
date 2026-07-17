#!/usr/bin/env python3
"""Corpus-wide cross-identifier double-publication DETECTION (READ-ONLY).

Finds courts (beyond the known GE/VD/SH) where one legal act is stored under two
rows. Four independent signals, computed per court (bounded memory via ORDER BY):

  A. shared source_url   — >=2 rows point to the SAME portal document URL. The GE
     proof signal. Split into "twin-ish" (a URL covering 2-6 rows) vs "generic"
     (a URL covering >20 rows = a listing/index page, NOT duplication).
  B. shared pdf_url      — same idea on the PDF link.
  C. content_hash reused across DIFFERENT dockets — byte-identical body filed under
     two docket numbers (catches byte-identical republication, which A/B miss when
     URLs differ). content_hash = SHA256(regeste||full_text).
  D. docket_number_2 populated — the scraper already captured a second identifier.

NO writes anywhere. Prints a ranked table. Interpretation is left to the caller.
"""
from __future__ import annotations
import collections
import sqlite3
import sys

import os
DB = "file:%s?mode=ro&immutable=1" % os.environ.get("OCL_DECISIONS_DB", "output/decisions.db")
GENERIC = 20          # a URL covering more rows than this is a listing page, not a twin
TWIN_MAX = 6          # a URL covering 2..TWIN_MAX rows is a representation candidate


def flush(court, total, surls, purls, hash_dockets, dk2, out):
    def url_stats(counter):
        twin_rows = twin_urls = generic_rows = 0
        for _, n in counter.items():
            if n < 2:
                continue
            if n <= TWIN_MAX:
                twin_urls += 1
                twin_rows += n
            elif n > GENERIC:
                generic_rows += n
            else:  # 7..20 — moderate, count as twin-ish rows but not urls
                twin_rows += n
        return twin_rows, twin_urls, generic_rows

    s_twin_rows, s_twin_urls, s_gen = url_stats(surls)
    p_twin_rows, p_twin_urls, p_gen = url_stats(purls)
    hash_dupe_rows = sum(len(d) for d in hash_dockets.values() if len(d) >= 2)
    hash_dupe_keys = sum(1 for d in hash_dockets.values() if len(d) >= 2)
    out.append({
        "court": court, "total": total,
        "surl_twin_rows": s_twin_rows, "surl_twin_urls": s_twin_urls, "surl_generic": s_gen,
        "purl_twin_rows": p_twin_rows, "purl_generic": p_gen,
        "xhash_rows": hash_dupe_rows, "xhash_keys": hash_dupe_keys,
        "dk2": dk2,
    })


def main() -> int:
    c = sqlite3.connect(DB, uri=True)
    # No ORDER BY: an external sort of the whole table gets starved under idle IO
    # during the nightly build. Accumulate per court in RAM (server has ~50GB free).
    per = {}  # court -> [total, surls Counter, purls Counter, hash_dockets, dk2]

    def bucket(court):
        b = per.get(court)
        if b is None:
            b = [0, collections.Counter(), collections.Counter(),
                 collections.defaultdict(set), 0]
            per[court] = b
        return b

    cur = c.execute(
        "SELECT court, source_url, pdf_url, docket_number, content_hash, docket_number_2 "
        "FROM decisions"
    )
    n = 0
    for court, surl, purl, dk, chash, dk2v in cur:
        n += 1
        b = bucket(court)
        b[0] += 1
        if surl:
            b[1][surl] += 1
        if purl:
            b[2][purl] += 1
        if chash and dk:
            b[3][chash].add(dk)
        if dk2v:
            b[4] += 1

    out = []
    for court, b in per.items():
        flush(court, b[0], b[1], b[2], b[3], b[4], out)

    def suspect_score(r):
        # rows implicated by the strongest per-doc signals (exclude generic listing URLs)
        return max(r["surl_twin_rows"], r["purl_twin_rows"], r["xhash_rows"], r["dk2"])

    out.sort(key=suspect_score, reverse=True)
    print(f"scanned {n:,} rows across {len(out)} courts\n")
    hdr = (f"{'court':22} {'rows':>8} {'surl_twin':>10} {'surl_urls':>9} "
           f"{'surl_gen':>9} {'pdf_twin':>9} {'xhash_rw':>9} {'xhash_k':>8} {'dk2':>8} {'score%':>7}")
    print(hdr)
    print("-" * len(hdr))
    for r in out:
        sc = suspect_score(r)
        pct = (100.0 * sc / r["total"]) if r["total"] else 0
        if sc == 0 and r["surl_generic"] == 0:
            continue  # nothing to report for this court
        print(f"{r['court'][:22]:22} {r['total']:>8,} {r['surl_twin_rows']:>10,} "
              f"{r['surl_twin_urls']:>9,} {r['surl_generic']:>9,} {r['purl_twin_rows']:>9,} "
              f"{r['xhash_rows']:>9,} {r['xhash_keys']:>8,} {r['dk2']:>8,} {pct:>6.1f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
