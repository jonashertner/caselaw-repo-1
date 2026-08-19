"""Paper-2 back-scan: provably nonexistent BGE citations since 2024, with
the denominator computed in the same pass.

Runs on the post-ATF/DTF graph (2026-08-07 onwards), where French and
Italian reporter citations are first-class bge-typed tokens. The bare-form
path stays for continuation citations. Every finding carries decision
metadata, form, a context excerpt, deterministic mechanism labels, and a
quotation-marker flag, so the voice pass and the source-fidelity check can
work from this file alone.

Universe discipline (2026-08-13): the PRIMARY universe is prefixed reporter
citations, defined once. Per-language counts and rates derive from prefixed
findings only; the bare channel is reported separately (it contributed one
qualified finding on 2026-08-11 and made per-language counts sum to 591
against a headline of 590 — the mismatch this rewrite removes).

Denominator discipline (external review): counted from the SAME iteration
that produces the findings — same token population, same filters — never
from a separate query.

  python3 scripts/p2_backscan.py --since 2024-01-01 \
      --out out/p2_backscan.json --findings out/p2_findings.jsonl \
      --series-out out/bge_series_index.json \
      --pool-out out/p2_pre1955_pool.jsonl \
      --tokens-out out/p2_distinct_tokens.json
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
    _BARE_BGE, _BGE_TOKEN, _REAL_DIVISIONS, _bge_series_index, _classify_bge,
    _family_pages, _last_case_window)

OUT_DIR = os.environ.get("OCL_OUTPUT_DIR", "/mnt/HC_Volume_104655575/output")

# Court classes for the cluster structure. Federal is the closed list the
# paper names; cantonal is the two-letter-prefix convention every cantonal
# scraper follows; the remainder (MKG, VPB, regulators) is "other".
_FEDERAL = {"bge", "bger", "bvger", "bstger", "bpatger"}
_CANTONAL = re.compile(r"^[a-z]{2}_")

# Pre-1955 pool: volume 80 (=1954) is also the DFR mirror's coverage floor,
# so the hidden-typo pool and the external-check floor coincide.
_PRE1955_MAX_VOL = 80

_QUOTE_CHARS = "«»„“”‹›\""


def _court_class(court: str) -> str:
    if court in _FEDERAL:
        return "federal"
    if _CANTONAL.match(court or ""):
        return "cantonal"
    return "other"


def _log(m):
    print(f"[{datetime.datetime.now(datetime.UTC).strftime('%H:%M:%S')}] {m}",
          file=sys.stderr, flush=True)


def _parse_bge(ref: str):
    m = _BGE_TOKEN.match(ref) or _BARE_BGE.match(ref)
    if not m:
        return None
    return int(m.group(1)), m.group(2).upper(), int(m.group(3))


def _plausible(idx, vol, div, page) -> bool:
    """Page lies within the (vol, div) series extent — a possible locus,
    including deep pin-cites. Not an existence proof."""
    pages = _family_pages(idx, vol, div)
    return bool(pages) and 1 <= page <= pages[-1] + _last_case_window(pages)


def _is_start(idx, vol, div, page) -> bool:
    return page in _family_pages(idx, vol, div)


def _digit_variants(n: int):
    """Single-deletion and adjacent-transposition variants of n's digits."""
    s = str(n)
    dele, trans = [], []
    for i in range(len(s)):
        if len(s) > 1:
            v = int(s[:i] + s[i + 1:])
            dele.append((v, i > 0 and s[i] == s[i - 1] or
                         i + 1 < len(s) and s[i] == s[i + 1]))
    for i in range(len(s) - 1):
        if s[i] != s[i + 1]:
            t = s[:i] + s[i + 1] + s[i] + s[i + 2:]
            trans.append(int(t))
    return dele, trans


def _mechanisms(idx, max_vol, vol, div, page):
    """Deterministic mechanism labels + candidate corrections, in priority
    order. Every rule is a pure function of (token, series index); the
    labels replace the campaign-anecdotal taxonomy with a full-window one.

    Candidates assert only that the repaired locus EXISTS in the series,
    never that it is the intended one — a digit-edit repair can name a
    19th-century volume for a modern doctrine. Intent stays with the
    human adjudication fields (intended_target).
    """
    labels, cands = [], []

    # 1. year shape: the decision year intruding into the page position
    if 1875 <= page <= 2100 and page > 700:
        labels.append("year_for_page")

    # 2. dropped leading volume digit: BGE 48 IV 137 for 148 IV 137
    if vol <= 99 and vol + 100 <= max_vol and _plausible(idx, vol + 100, div, page):
        labels.append("dropped_leading_volume_digit")
        cands.append((f"BGE {vol + 100} {div} {page}",
                      _is_start(idx, vol + 100, div, page)))

    # 3. division substitution: same volume+page under a sibling division
    fam = {"I", "IA", "IB"}
    own = fam if div in fam else {div}
    for d2 in sorted(_REAL_DIVISIONS - own):
        if _is_start(idx, vol, d2, page):
            labels.append("division_substitution")
            cands.append((f"BGE {vol} {d2} {page}", True))

    # 4. volume substitution: same division+page as an exact start in a
    #    near or digit-edit volume (146 III 666 for 143 III 666)
    seen = set()
    variants = set(range(max(1, vol - 10), min(max_vol, vol + 10) + 1))
    d_del, d_tr = _digit_variants(vol)
    variants |= {v for v, _ in d_del} | set(d_tr)
    for v2 in sorted(variants):
        if v2 == vol or v2 < 1 or v2 > max_vol or v2 in seen:
            continue
        seen.add(v2)
        if _is_start(idx, v2, div, page):
            labels.append("volume_substitution")
            cands.append((f"BGE {v2} {div} {page}", True))

    # 5. page digit damage: deletion (doubled digit or extra digit) or
    #    adjacent transposition yields a plausible locus
    p_del, p_tr = _digit_variants(page)
    for p2, doubled in p_del:
        if p2 >= 1 and _plausible(idx, vol, div, p2):
            labels.append("page_doubled_digit" if doubled else
                          "page_extra_digit")
            cands.append((f"BGE {vol} {div} {p2}", _is_start(idx, vol, div, p2)))
    for p2 in p_tr:
        if p2 >= 1 and _plausible(idx, vol, div, p2):
            labels.append("page_transposition")
            cands.append((f"BGE {vol} {div} {p2}", _is_start(idx, vol, div, p2)))

    # dedupe, keep order
    ulabels = list(dict.fromkeys(labels))
    ucands = list(dict.fromkeys(cands))[:6]
    primary = ulabels[0] if ulabels else "unlabelled"
    exact = [c for c, is_start in ucands if is_start]
    return primary, ulabels, [c for c, _ in ucands], (
        exact[0] if len(exact) == 1 else None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2024-01-01")
    ap.add_argument("--out", required=True)
    ap.add_argument("--findings", required=True)
    ap.add_argument("--series-out", required=True)
    ap.add_argument("--pool-out", required=True)
    ap.add_argument("--tokens-out", required=True)
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

    # single pass: denominator, findings, cluster tallies, pre-1955 pool.
    # UNIT (review 2026-08-13): one row per DISTINCT (decision, locus,
    # form) citation edge. The extractor already deduplicates mentions
    # within a decision; the former LEFT JOIN against citation_targets
    # re-multiplied edges whose (source, ref) carried several target rows
    # (1,728 pool rows vs 1,642 unique pairs), inflating the denominator.
    # DISTINCT + EXISTS makes the count edge-exact.
    denom = Counter()          # (language, form) -> edges considered
    distinct_refs = {}         # lang -> set of prefixed refs
    dec_tok = Counter()        # sid -> prefixed edges considered
    dec_find = Counter()       # sid -> prefixed findings
    findings = []
    pool = []
    upper = (datetime.date.today() + datetime.timedelta(days=366)).isoformat()
    q = """SELECT dc.sid AS sid, dc.ref AS ref, dc.tt AS tt,
                  NOT EXISTS(
                    SELECT 1 FROM citation_targets ct
                     WHERE ct.source_decision_id = dc.sid
                       AND ct.target_ref = dc.ref) AS unresolved
           FROM (SELECT DISTINCT dc0.source_decision_id AS sid,
                        dc0.target_ref AS ref, dc0.target_type AS tt
                   FROM decision_citations dc0
                   JOIN decisions d ON d.decision_id = dc0.source_decision_id
                  WHERE d.decision_date >= ? AND d.decision_date <= ?) dc"""
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
        if not bare:
            dec_tok[sid] += 1
            distinct_refs.setdefault(lang or "?", set()).add(ref)
            if not r["unresolved"]:
                parsed = _parse_bge(ref)
                if parsed and parsed[0] <= _PRE1955_MAX_VOL:
                    v, d, p = parsed
                    pool.append({
                        "decision_id": sid, "court": court, "docket": dk,
                        "decided": date, "language": lang, "token": ref,
                        "vol": v, "div": d, "page": p,
                    })
        if not r["unresolved"]:
            continue
        reason = _classify_bge(ref, idx, max_vol, bare=bare)
        if not reason:
            continue
        if not bare:
            dec_find[sid] += 1
        findings.append({
            "decision_id": sid, "court": court, "docket": dk,
            "decided": date, "language": lang, "token": ref,
            "form": form, "reason": reason,
        })
    _log(f"  {n:,} token rows total; {len(findings)} raw findings; "
         f"pool {len(pool)}")

    # mechanism labels (pure function of token + series index)
    for f in findings:
        parsed = _parse_bge(f["token"])
        if parsed:
            prim, labels, cands, unique = _mechanisms(idx, max_vol, *parsed)
            f["mechanism"] = prim
            f["mechanism_labels"] = labels
            f["candidates"] = cands
            f["candidate_unique"] = unique
        else:
            f["mechanism"] = "unparsed"
            f["mechanism_labels"] = []
            f["candidates"] = []
            f["candidate_unique"] = None
    # pre-screen for the hidden-typo pool: would this resolved old-volume
    # citation also read as a modern one with the leading digit restored?
    for p in pool:
        p["plus100_plausible"] = _plausible(idx, p["vol"] + 100, p["div"],
                                            p["page"])
        p["plus100_start"] = _is_start(idx, p["vol"] + 100, p["div"],
                                       p["page"])

    _log("context excerpts (findings + pool), chunked")
    CH = a.context_chars

    def _attach_contexts(rows):
        ids = sorted({x["decision_id"] for x in rows})
        by_id = {}
        for x in rows:
            by_id.setdefault(x["decision_id"], []).append(x)
        for i in range(0, len(ids), 200):
            chunk = ids[i:i + 200]
            ph = ",".join("?" * len(chunk))
            for did, txt in dec.execute(
                    f"SELECT decision_id, full_text FROM decisions "
                    f"WHERE decision_id IN ({ph})", chunk):
                txt = txt or ""
                for x in by_id.get(did, ()):
                    tok = x["token"]
                    # The graph normalizes ATF/DTF to the BGE prefix, but
                    # FR/IT source texts write ATF/DTF. Searching for the
                    # normalized form found 3/210 DE but 355/356 FR and
                    # 25/25 IT contexts MISSING (review 2026-08-13) — the
                    # voice/quote screen silently covered German only.
                    core = re.escape(tok).replace(r"\ ", r"\s+")
                    if tok.startswith("BGE"):
                        core = core.replace("BGE", r"(?:BGE|ATF|DTF)", 1)
                    pat = re.compile(core)
                    mm = pat.search(txt)
                    if mm:
                        s, e = max(0, mm.start() - CH), min(len(txt),
                                                            mm.end() + CH // 2)
                        x["context"] = " ".join(txt[s:e].split())
                        pre = txt[max(0, mm.start() - 12):mm.start()]
                        x["prefix_in_text"] = bool(
                            re.search(r"(BGE|ATF|DTF)\s*$", pre))
                        x["digit_before"] = bool(re.search(r"\d\s*$", pre))
                    else:
                        x["context"] = None
                        x["prefix_in_text"] = None
                        x["digit_before"] = None
                    x["quote_marker"] = bool(
                        x["context"] and any(c in x["context"]
                                             for c in _QUOTE_CHARS))

    _attach_contexts(findings)
    _attach_contexts(pool)
    for f in findings:
        # adjudication fields for the voice pass — deliberately empty
        f["voice"] = ""            # court | party-quote | unclear
        f["source_verified"] = ""  # yes | no | n/a
        f["intended_target"] = ""  # human-confirmed correction, if evident
    for p in pool:
        p["adjudication"] = ""     # legitimate-historical | landed-typo | unclear

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
    # PRIMARY universe: prefixed only. Bare channel reported separately.
    prim = [f for f in qual if f["form"] == "prefixed"]
    bare_q = [f for f in qual if f["form"] == "bare"]

    with open(a.findings, "w") as fh:
        for f in findings:
            fh.write(json.dumps(f, ensure_ascii=False) + "\n")
    with open(a.pool_out, "w") as fh:
        for p in pool:
            fh.write(json.dumps(p, ensure_ascii=False) + "\n")
    with open(a.series_out, "w") as fh:
        json.dump({
            "max_volume": max_vol,
            "families": {
                f"{v}|{d}": {"starts": pages,
                             "window": _last_case_window(pages)}
                for (v, d), pages in sorted(idx.items())},
        }, fh, ensure_ascii=False)

    # cluster pairs over the PRIMARY universe, for the decision bootstrap
    pairs = Counter()
    dec_with_tok = 0
    dec_with_find = 0
    for sid, t in dec_tok.items():
        court, dk, date, lang = meta[sid]
        fnd = dec_find.get(sid, 0)
        dec_with_tok += 1
        if fnd:
            dec_with_find += 1
        pairs[f"{lang or '?'}|{(date or '?')[:4]}|{_court_class(court)}"
              f"|{t}|{fnd}"] += 1

    by_lang = Counter(f["language"] for f in prim)
    by_reason = Counter(f["reason"].split()[0] for f in prim)
    by_court = Counter(f["court"] for f in prim)
    by_class = Counter(_court_class(f["court"]) for f in prim)
    by_year = Counter((f["decided"] or "?")[:4] for f in prim)
    by_mech = Counter(f["mechanism"] for f in prim)
    tok_counter = Counter(f["token"] for f in prim)
    quote_n = sum(1 for f in prim if f.get("quote_marker"))
    den_prefixed = {l: c for (l, fm), c in denom.items() if fm == "prefixed"}

    with open(a.tokens_out, "w") as fh:
        json.dump({
            "_status": "distinct provably-nonexistent prefixed tokens for "
                       "the external probe (bger.ch 301/404; DFR < vol 80)",
            "tokens": [
                {"token": t, "occurrences": c,
                 "reason": next(f["reason"] for f in prim if f["token"] == t),
                 "mechanism": next(f["mechanism"] for f in prim
                                   if f["token"] == t),
                 "candidate_unique": next(f["candidate_unique"] for f in prim
                                          if f["token"] == t),
                 "sample_decision": next(f["decision_id"] for f in prim
                                         if f["token"] == t)}
                for t, c in tok_counter.most_common()],
        }, fh, ensure_ascii=False, indent=1)

    summary = {
        "_status": "machine findings; voice adjudication pending",
        "_unit": "distinct (decision, locus, form) citation edges; "
                 "mentions are deduplicated by the extractor",
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "since": a.since,
        "graph_mtime": datetime.datetime.fromtimestamp(
            os.path.getmtime(os.path.join(OUT_DIR, "reference_graph.db")),
            datetime.UTC).isoformat(),
        "decisions_in_window": len(meta),
        # ── primary universe: prefixed reporter citations ──
        "primary_universe": "prefixed reporter citations",
        "denominator_prefixed_total": sum(den_prefixed.values()),
        "denominator_prefixed_by_language": dict(sorted(den_prefixed.items())),
        "denominator_distinct_refs_total": len(
            set().union(*distinct_refs.values()) if distinct_refs else set()),
        "denominator_distinct_refs_by_language": {
            l: len(s) for l, s in sorted(distinct_refs.items())},
        "decisions_with_prefixed_token": dec_with_tok,
        "decisions_with_finding": dec_with_find,
        "findings_primary": len(prim),
        "primary_rate_ppm": round(
            1e6 * len(prim) / max(1, sum(den_prefixed.values())), 1),
        "rate_per_language_ppm": {
            l: round(1e6 * by_lang.get(l, 0)
                     / max(1, den_prefixed.get(l, 0)), 1)
            for l in sorted(den_prefixed)},
        "findings_by_language": dict(by_lang),
        "findings_by_reason": dict(by_reason),
        "findings_by_court_top": dict(by_court.most_common(20)),
        "findings_by_court_class": dict(by_class),
        "findings_by_year": dict(sorted(by_year.items())),
        "findings_by_mechanism": dict(by_mech.most_common()),
        "findings_with_quote_marker": quote_n,
        "distinct_tokens": len(tok_counter),
        "tokens_repeated": {t: c for t, c in tok_counter.most_common(15)
                            if c > 1},
        "cluster_pairs": dict(sorted(pairs.items())),
        # ── secondary channels, reported separately ──
        "bare_channel": {
            "denominator_tokens_by_language": {
                l: c for (l, fm), c in sorted(denom.items()) if fm == "bare"},
            "findings_qualified": len(bare_q),
            "findings": [
                {k: f[k] for k in ("decision_id", "court", "decided",
                                   "language", "token", "reason")}
                for f in bare_q],
        },
        "findings_raw": len(findings),
        "dropped_by_guard": len(findings) - len(qual),
        "pre1955_pool": {
            "definition": f"resolved prefixed citations targeting volumes "
                          f"<= {_PRE1955_MAX_VOL} (pre-1955; DFR floor)",
            "n": len(pool),
            "rate_ppm": round(1e6 * len(pool)
                              / max(1, sum(den_prefixed.values())), 1),
            "plus100_plausible": sum(1 for p in pool
                                     if p["plus100_plausible"]),
            "plus100_start": sum(1 for p in pool if p["plus100_start"]),
        },
        "denominator_tokens_total_all_forms": sum(denom.values()),
    }
    with open(a.out, "w") as fh:
        json.dump(summary, fh, indent=1, ensure_ascii=False, sort_keys=True)
    _log(f"wrote {a.out}, {a.findings}, {a.series_out}, {a.pool_out}, "
         f"{a.tokens_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
