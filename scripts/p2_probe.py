"""External re-probe of the P2 findings against the Federal Supreme
Court's own citation resolver (relevancy.bger.ch).

Semantics (verified 2026-08-13):
  - a decision START page returns 200 at its own docid
    (atf://139-II-404 -> 200);
  - an INTERIOR page redirects to the containing decision
    (atf://139-II-405 -> .../139-II-404#page405) -> existence proven;
  - a nonexistent locus returns 404 (atf://139-II-4040 -> 404).
The resolver's coverage floor is volume 80 (1954): REAL older citations
also 404 (verified with BGE 40 II 1, BGE 75 II 57), so tokens in volumes
< 80 are marked out_of_coverage, not confirmed. The DFR mirror indexes
selected leading cases only and can therefore never prove absence; it is
not used for negative checks.

Second pass: for findings with a UNIQUE deterministic repair candidate
(build artifact field candidate_unique), the candidate itself is probed
— a 200/redirect POSITIVELY confirms that the repaired citation exists,
closing the loop on the mechanism labels.

~1 request/second against one endpoint; ≈500 requests total.

  .venv/bin/python3 scripts/p2_probe.py \
      --tokens docs/paper/p2-citations/data/p2_distinct_tokens.json \
      --out docs/paper/p2-citations/data/p2_probe.json
"""
from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
import time
import urllib.error
import urllib.request

BASE = ("http://relevancy.bger.ch/php/clir/http/index.php"
        "?highlight_docid=atf%3A%2F%2F{loc}%3Ade&lang=de&type=show_document")
UA = ("OpenCaseLaw citation-integrity probe "
      "(https://opencaselaw.ch; jonashertner@protonmail.ch)")
FLOOR = 80  # resolver coverage floor: volume 80 = 1954
_TOK = re.compile(r"^BGE (\d{1,3}) ([IVX]+[ab]?) (\d{1,4})$")


def probe(token: str):
    m = _TOK.match(token.strip())
    if not m:
        return {"token": token, "status": "unparsed"}
    vol, div, page = int(m.group(1)), m.group(2), int(m.group(3))
    loc = f"{vol}-{div.upper()}-{page}"
    if vol < FLOOR:
        return {"token": token, "status": "out_of_coverage", "volume": vol}
    req = urllib.request.Request(BASE.format(loc=loc),
                                 headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            final = resp.geturl()
            redirected = f"-{page}" not in final.split("highlight_docid")[-1]
            return {"token": token, "status": "exists",
                    "http": resp.status, "final_url": final,
                    "via_redirect": redirected}
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {"token": token, "status": "confirmed_nonexistent",
                    "http": 404}
        return {"token": token, "status": f"http_{e.code}", "http": e.code}
    except Exception as e:  # noqa: BLE001 — probe must never crash the run
        return {"token": token, "status": "error", "error": str(e)[:200]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tokens", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--sleep", type=float, default=1.1)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    data = json.load(open(a.tokens))
    tokens = data["tokens"]
    if a.limit:
        tokens = tokens[:a.limit]

    results, cand_results = [], []
    for i, t in enumerate(tokens):
        r = probe(t["token"])
        r["occurrences"] = t["occurrences"]
        r["reason"] = t["reason"]
        results.append(r)
        print(f"[{i + 1}/{len(tokens)}] {t['token']} -> {r['status']}",
              file=sys.stderr, flush=True)
        time.sleep(a.sleep)

    # unique repair candidates: positive existence check
    seen = set()
    for t in tokens:
        c = t.get("candidate_unique")
        if not c or c in seen:
            continue
        seen.add(c)
        r = probe(c)
        r["repairs"] = t["token"]
        cand_results.append(r)
        print(f"[cand] {c} -> {r['status']}", file=sys.stderr, flush=True)
        time.sleep(a.sleep)

    n_conf = sum(1 for r in results if r["status"] == "confirmed_nonexistent")
    n_exist = sum(1 for r in results if r["status"] == "exists")
    n_oo = sum(1 for r in results if r["status"] == "out_of_coverage")
    n_other = len(results) - n_conf - n_exist - n_oo
    c_exist = sum(1 for r in cand_results if r["status"] == "exists")
    out = {
        "_method": "relevancy.bger.ch resolver; 200/redirect = exists, "
                   "404 = nonexistent; coverage floor volume "
                   f"{FLOOR} (1954). Second pass probes unique repair "
                   "candidates for positive existence.",
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "n_tokens": len(results),
        "n_confirmed_nonexistent": n_conf,
        "n_exists": n_exist,
        "n_out_of_coverage": n_oo,
        "n_other": n_other,
        "n_candidates_probed": len(cand_results),
        "n_candidates_exist": c_exist,
        "results": results,
        "candidate_results": cand_results,
    }
    with open(a.out, "w") as fh:
        json.dump(out, fh, indent=1, ensure_ascii=False)
    print(f"wrote {a.out}: {n_conf} confirmed nonexistent, {n_exist} exist, "
          f"{n_oo} below coverage floor, {n_other} other; "
          f"candidates {c_exist}/{len(cand_results)} exist", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
