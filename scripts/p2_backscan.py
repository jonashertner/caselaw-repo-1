"""Paper-2 back-scan: provably nonexistent BGE citations since 2024, with
the denominator computed in the same pass.

Runs on the post-ATF/DTF graph (2026-08-07 onwards), where French and
Italian reporter citations are first-class bge-typed tokens. The bare-form
path stays for continuation citations. Every finding carries decision
metadata, form, and a context excerpt from the source text so the
voice-adjudication pass (C2) and the source-fidelity check can work from
this file alone.

Denominator discipline (external review): counted from the SAME iteration
that produces the findings — same token population, same filters — never
from a separate query.

  python3 scripts/p2_backscan.py --since 2024-01-01 \
      --out output/release_meta/p2_backscan.json \
      --findings output/release_meta/p2_findings.jsonl
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sqlite3
import sys
from collections import Counter

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from quality.checks.citation_anomalies import (  # noqa: E402
    _BARE_BGE, _bge_series_index, _classify_bge)

OUT_DIR = os.environ.get("OCL_OUTPUT_DIR", "/mnt/HC_Volume_104655575/output")


def _log(m):
    print(f"[{datetime.datetime.now(datetime.UTC).strftime('%H:%M:%S')}] {m}",
          file=sys.stderr, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2024-01-01")
    ap.add_argument("--out", required=True)
    ap.add_argument("--findings", required=True)
    ap.add_argument("--context-chars", type=int, default=380)
    a = ap.parse_args()

    dec = sqlite3.connect(
        f"file:{os.path.join(OUT_DIR, 'decisions.db')}?mode=ro&immutable=1",
        uri=True)
    rg = sqlite3.connect(
        f"file:{os.path.join(OUT_DIR, 'reference_graph.db')}?mode=ro&immutable=1",
        uri=True)
    rg.row_factory = sqlite3.Row

    _log("decision metadata (since %s)" % a.since)
    meta = {}
    for did, court, dk, date, lang in dec.execute(
            "SELECT decision_id, court, docket_number, decision_date, "
            "language FROM decisions WHERE decision_date >= ?", (a.since,)):
        meta[did] = (court, dk, date, lang)
    _log(f"  {len(meta):,} decisions in window")

    _log("BGE series index")
    idx, max_vol = _bge_series_index(rg)
    _log(f"  {len(idx)} (volume, division) families, max volume {max_vol}")

    # single pass: denominator and findings from the same iteration
    denom = Counter()          # (language, form) -> tokens considered
    findings = []
    upper = (datetime.date.today() + datetime.timedelta(days=366)).isoformat()
    q = """SELECT dc.source_decision_id AS sid, dc.target_ref AS ref,
                  dc.target_type AS tt,
                  (ct.target_ref IS NULL) AS unresolved
           FROM decision_citations dc
           JOIN decisions d ON d.decision_id = dc.source_decision_id
           LEFT JOIN citation_targets ct
             ON ct.source_decision_id = dc.source_decision_id
            AND ct.target_ref = dc.target_ref
           WHERE d.decision_date >= ? AND d.decision_date <= ?"""
    n = 0
    for r in rg.execute(q, (a.since, upper)):
        n += 1
        if n % 2_000_000 == 0:
            _log(f"  {n:,} token rows")
        sid = r["sid"]
        m = meta.get(sid)
        if m is None:
            continue
        court, dk, date, lang = m
        ref = (r["ref"] or "").strip()
        if r["tt"] == "bge":
            bare = False
        elif r["tt"] == "docket" and _BARE_BGE.match(ref):
            bare = True
        else:
            continue
        form = "bare" if bare else "prefixed"
        denom[(lang or "?", form)] += 1
        if not r["unresolved"]:
            continue
        reason = _classify_bge(ref, idx, max_vol, bare=bare)
        if not reason:
            continue
        findings.append({
            "decision_id": sid, "court": court, "docket": dk,
            "decided": date, "language": lang, "token": ref,
            "form": form, "reason": reason,
        })
    _log(f"  {n:,} token rows total; {len(findings)} findings")

    _log("context excerpts from source texts")
    ids = sorted({f["decision_id"] for f in findings})
    texts = {}
    CH = a.context_chars
    for i in range(0, len(ids), 400):
        chunk = ids[i:i + 400]
        ph = ",".join("?" * len(chunk))
        for did, txt in dec.execute(
                f"SELECT decision_id, full_text FROM decisions "
                f"WHERE decision_id IN ({ph})", chunk):
            texts[did] = txt or ""
    for f in findings:
        txt = texts.get(f["decision_id"], "")
        tok = f["token"]
        pat = re.compile(re.escape(tok).replace(r"\ ", r"\s+"))
        m = pat.search(txt)
        if m:
            s, e = max(0, m.start() - CH), min(len(txt), m.end() + CH // 2)
            f["context"] = " ".join(txt[s:e].split())
            pre = txt[max(0, m.start() - 12):m.start()]
            f["prefix_in_text"] = bool(
                re.search(r"(BGE|ATF|DTF)\s*$", pre))
            f["digit_before"] = bool(re.search(r"\d\s*$", pre))
        else:
            f["context"] = None
            f["prefix_in_text"] = None
            f["digit_before"] = None
        # adjudication fields for the C2 human pass — deliberately empty
        f["voice"] = ""            # court | party-quote | unclear
        f["source_verified"] = ""  # yes | no | n/a
        f["intended_target"] = ""  # human-proposed correction, if evident

    # The paper's stated guard, applied IN the artifact: a bare token
    # counts only if the source text shows the prefix immediately before
    # it and the preceding character is not a digit (digit-split OCR).
    def _qualified(f):
        if f["form"] == "prefixed":
            return True
        return (f.get("prefix_in_text") is True
                and f.get("digit_before") is False)

    for f in findings:
        f["qualified"] = _qualified(f)
    qual = [f for f in findings if f["qualified"]]

    with open(a.findings, "w") as fh:
        for f in findings:
            fh.write(json.dumps(f, ensure_ascii=False) + "\n")

    by_lang = Counter(f["language"] for f in qual)
    by_reason = Counter(f["reason"].split()[0] for f in qual)
    by_form = Counter(f["form"] for f in qual)
    by_court = Counter(f["court"] for f in qual)
    tok_counter = Counter(f["token"] for f in qual)
    den_prefixed = {l: c for (l, fm), c in denom.items() if fm == "prefixed"}
    summary = {
        "_status": "machine findings; voice adjudication (C2) pending",
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "since": a.since,
        "graph_mtime": datetime.datetime.fromtimestamp(
            os.path.getmtime(os.path.join(OUT_DIR, "reference_graph.db")),
            datetime.UTC).isoformat(),
        "decisions_in_window": len(meta),
        "denominator_tokens_by_language_and_form": {
            f"{l}|{fm}": c for (l, fm), c in sorted(denom.items())},
        "denominator_tokens_total": sum(denom.values()),
        "findings_raw": len(findings),
        "findings_qualified": len(qual),
        "dropped_by_guard": len(findings) - len(qual),
        "primary_universe": "prefixed reporter citations",
        "denominator_prefixed_total": sum(den_prefixed.values()),
        "primary_rate_ppm": round(
            1e6 * by_form.get("prefixed", 0)
            / max(1, sum(den_prefixed.values())), 1),
        "rate_per_language_ppm": {
            l: round(1e6 * by_lang.get(l, 0)
                     / max(1, den_prefixed.get(l, 0)), 1)
            for l in sorted(den_prefixed)},
        "findings_by_language": dict(by_lang),
        "findings_by_form": dict(by_form),
        "findings_by_reason": dict(by_reason),
        "findings_by_court_top": dict(by_court.most_common(20)),
        "distinct_tokens": len(tok_counter),
        "tokens_repeated": {t: c for t, c in tok_counter.most_common(15)
                            if c > 1},
        
    }
    with open(a.out, "w") as fh:
        json.dump(summary, fh, indent=1, ensure_ascii=False, sort_keys=True)
    _log(f"wrote {a.out} and {a.findings}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
