"""Machine-readable release comparison for the reference graph.

The evidentiary bridge between wording changes (Wave 0) and new numerical
claims (Wave 1): capture the metrics of the live graph before a build,
capture again after, and emit a field-by-field comparison. Every metric is
computed by the same queries on both sides.

  python3 scripts/release_comparison.py --capture before.json
  python3 scripts/release_comparison.py --against before.json --out cmp.json

Read-only against the serving DBs (mode=ro&immutable=1). Progress lines go
to stderr so a nohup log shows liveness during the long scans.
"""
from __future__ import annotations

import argparse
import datetime
import glob
import hashlib
import json
import os
import re
import subprocess
import sqlite3
import sys

OUT_DIR = os.environ.get("OCL_OUTPUT_DIR", "/mnt/HC_Volume_104655575/output")
REPO = os.environ.get("OCL_REPO", "/opt/caselaw/repo")

_BARE_BGE = re.compile(r"^(\d{1,3})\s+([IVX]{1,4}[ABab]?)\s+(\d{1,4})$")
_BGE_TYPED = re.compile(r"^BGE ")
_FED = re.compile(r"^\d{1,2}[A-Z][ _.\-]?\d{1,4}[/_]\d{4}$")
_BVGER = re.compile(r"^[A-F]-\d{1,6}[/_]\d{4}$")
_BSTGER = re.compile(r"^[A-Z]{2}\.\d{4}\.\d{1,6}$")


def _log(msg: str) -> None:
    print(f"[{datetime.datetime.utcnow().strftime('%H:%M:%S')}] {msg}",
          file=sys.stderr, flush=True)


def _open(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    c.execute("PRAGMA busy_timeout=60000")
    return c


def _family(ref: str) -> str:
    ref = (ref or "").strip()
    if _BGE_TYPED.match(ref):
        return "bge_prefixed"
    if _BARE_BGE.match(ref):
        return "bge_bare_shape"
    if _FED.match(ref):
        return "federal_docket"
    if _BVGER.match(ref):
        return "bvger_docket"
    if _BSTGER.match(ref):
        return "bstger_docket"
    return "other"


def capture() -> dict:
    dec = _open(os.path.join(OUT_DIR, "decisions.db"))
    rg = _open(os.path.join(OUT_DIR, "reference_graph.db"))
    m: dict = {}

    _log("record counts")
    m["decision_record_count"] = dec.execute(
        "SELECT COUNT(*) FROM decisions").fetchone()[0]
    m["canonical_decision_count"] = dec.execute(
        "SELECT COUNT(DISTINCT COALESCE(canonical_key, decision_id)) "
        "FROM decisions").fetchone()[0]
    m["duplicate_group_count"] = dec.execute(
        "SELECT COUNT(*) FROM (SELECT canonical_key FROM decisions "
        "WHERE canonical_key IS NOT NULL GROUP BY canonical_key "
        "HAVING COUNT(*) > 1)").fetchone()[0]

    _log("language map")
    lang = dict(dec.execute("SELECT decision_id, language FROM decisions"))

    _log("token scan (decision_citations)")
    tot = 0
    by_family: dict = {}
    bge_typed_by_lang: dict = {}
    bare_shape_by_lang: dict = {}
    tok_by_src_lang: dict = {}
    for sid, ref, tt in rg.execute(
            "SELECT source_decision_id, target_ref, target_type "
            "FROM decision_citations"):
        tot += 1
        fam = _family(ref)
        by_family[fam] = by_family.get(fam, 0) + 1
        lg = lang.get(sid, "?")
        tok_by_src_lang[lg] = tok_by_src_lang.get(lg, 0) + 1
        if tt == "bge":
            bge_typed_by_lang[lg] = bge_typed_by_lang.get(lg, 0) + 1
        elif tt == "docket" and fam == "bge_bare_shape":
            bare_shape_by_lang[lg] = bare_shape_by_lang.get(lg, 0) + 1
    m["extracted_tokens_total"] = tot
    m["tokens_by_parser_family"] = by_family
    m["tokens_by_source_language"] = tok_by_src_lang
    m["bge_typed_tokens_by_source_language"] = bge_typed_by_lang
    m["bare_bge_shaped_docket_tokens_by_source_language"] = bare_shape_by_lang

    _log("link rows + match types")
    m["expanded_link_rows_total"] = rg.execute(
        "SELECT COUNT(*) FROM citation_targets").fetchone()[0]
    m["counts_by_match_type"] = dict(rg.execute(
        "SELECT match_type, COUNT(*) FROM citation_targets GROUP BY 1"))

    _log("resolved tokens (EXISTS join)")
    m["resolved_tokens_total"] = rg.execute(
        "SELECT COUNT(*) FROM decision_citations dc WHERE EXISTS("
        " SELECT 1 FROM citation_targets ct"
        " WHERE ct.source_decision_id = dc.source_decision_id"
        "   AND ct.target_ref = dc.target_ref)").fetchone()[0]

    _log("unresolved by parser family")
    unres: dict = {}
    for ref, in rg.execute(
            "SELECT dc.target_ref FROM decision_citations dc "
            "WHERE NOT EXISTS(SELECT 1 FROM citation_targets ct "
            " WHERE ct.source_decision_id = dc.source_decision_id "
            "  AND ct.target_ref = dc.target_ref)"):
        fam = _family(ref)
        unres[fam] = unres.get(fam, 0) + 1
    m["unresolved_tokens_by_parser_family"] = unres

    _log("language matrix over link rows")
    tgt_lang: dict = {}
    pair: dict = {}
    chron: dict = {}
    date = dict(dec.execute(
        "SELECT decision_id, decision_date FROM decisions"))
    self_links = 0
    for sid, tid, mt in rg.execute(
            "SELECT source_decision_id, target_decision_id, match_type "
            "FROM citation_targets"):
        sl, tl = lang.get(sid, "?"), lang.get(tid, "?")
        tgt_lang[tl] = tgt_lang.get(tl, 0) + 1
        key = f"{sl}->{tl}"
        pair[key] = pair.get(key, 0) + 1
        if sid == tid:
            self_links += 1
        sd, td = date.get(sid), date.get(tid)
        if sd and td and td > sd:
            chron[mt] = chron.get(mt, 0) + 1
    m["counts_by_target_language"] = tgt_lang
    m["counts_by_source_target_language"] = pair
    m["chronology_violations_by_match_type"] = chron
    m["self_link_rows_present"] = self_links   # excluded by construction: 0

    _log("metadata")
    graph_path = os.path.join(OUT_DIR, "reference_graph.db")
    m["_meta"] = {
        "captured_at": datetime.datetime.utcnow().isoformat() + "Z",
        "graph_mtime": datetime.datetime.utcfromtimestamp(
            os.path.getmtime(graph_path)).isoformat() + "Z",
        "release_id": datetime.datetime.utcfromtimestamp(
            os.path.getmtime(graph_path)).strftime("%Y-%m-%d"),
        "source_git_commit": subprocess.run(
            ["git", "-C", REPO, "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True).stdout.strip(),
    }
    roots = sorted(glob.glob(os.path.join(REPO, "docs/integrity/*.root")))
    if roots:
        m["_meta"]["merkle_root_file"] = os.path.basename(roots[-1])
        m["_meta"]["merkle_root"] = open(roots[-1]).read().strip()
    core = json.dumps({k: v for k, v in m.items() if k != "_meta"},
                      sort_keys=True)
    # A hash over sorted metrics is a METRICS DIGEST, not a release-
    # manifest hash (which must commit to artifact content hashes;
    # see scripts/release_manifest.py, Wave 1).
    m["_meta"]["metrics_digest"] = hashlib.sha256(core.encode()).hexdigest()
    return m


def compare(before: dict, after: dict) -> dict:
    out = {
        "old_release_id": before["_meta"]["release_id"],
        "new_release_id": after["_meta"]["release_id"],
        "old_source_git_commit": before["_meta"]["source_git_commit"],
        "new_source_git_commit": after["_meta"]["source_git_commit"],
        "old_metrics_digest": before["_meta"].get("metrics_digest") or before["_meta"].get("manifest_hash"),
        "new_metrics_digest": after["_meta"]["metrics_digest"],
        "old_merkle_root": before["_meta"].get("merkle_root", "")[:16],
        "new_merkle_root": after["_meta"].get("merkle_root", "")[:16],
        "fields": {},
    }
    for k in sorted(set(before) | set(after)):
        if k.startswith("_"):
            continue
        b, a = before.get(k), after.get(k)
        if isinstance(b, dict) or isinstance(a, dict):
            b, a = b or {}, a or {}
            out["fields"][k] = {
                kk: {"before": b.get(kk, 0), "after": a.get(kk, 0),
                     "delta": (a.get(kk, 0) or 0) - (b.get(kk, 0) or 0)}
                for kk in sorted(set(b) | set(a))}
        else:
            out["fields"][k] = {"before": b, "after": a,
                                "delta": (a or 0) - (b or 0)}
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--capture", metavar="OUT_JSON")
    ap.add_argument("--against", metavar="BEFORE_JSON")
    ap.add_argument("--out", metavar="CMP_JSON")
    args = ap.parse_args()
    if args.capture:
        m = capture()
        with open(args.capture, "w") as f:
            json.dump(m, f, indent=1, sort_keys=True)
        _log(f"wrote {args.capture}")
        return 0
    if args.against:
        before = json.load(open(args.against))
        after = capture()
        cmp_ = compare(before, after)
        out = args.out or "release_comparison.json"
        with open(out, "w") as f:
            json.dump(cmp_, f, indent=1, sort_keys=True)
        _log(f"wrote {out}")
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
