"""Same-input paired extractor comparison + before-state dumps.

Gate-1 causal isolation (review 2026-08-07): run OLD and NEW extractor
commits over the IDENTICAL decision corpus and report token-level
transition classes with traceable examples — extractor effects cannot be
separated from corpus churn in a live before/after alone.

Also dumps the before-state key columns (decisions identity, tokens,
link rows) into before_inventory.sqlite so tomorrow's after-capture can
compute common-record-set deltas, churn, and resolution transitions
exactly rather than from aggregates.

Run niced on the VPS while decisions.db is still the pre-build corpus:
  nice -n 19 ionice -c3 python3 scripts/paired_extraction_diff.py \
      --old /tmp/re_old.py --new /tmp/re_new.py \
      --outdir output/release_meta
"""
from __future__ import annotations

import argparse
import datetime
import importlib.util
import json
import os
import re
import sqlite3
import sys

OUT_DIR = os.environ.get("OCL_OUTPUT_DIR", "/mnt/HC_Volume_104655575/output")
_BARE = re.compile(r"^\d{1,3}\s+[IVX]{1,4}[ABab]?\s+\d{1,4}$")


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _log(m):
    print(f"[{datetime.datetime.now(datetime.UTC).strftime('%H:%M:%S')}] {m}",
          file=sys.stderr, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    os.makedirs(a.outdir, exist_ok=True)

    OLD = _load("re_old", a.old)
    NEW = _load("re_new2", a.new)

    dec = sqlite3.connect(
        f"file:{os.path.join(OUT_DIR, 'decisions.db')}?mode=ro&immutable=1",
        uri=True)
    inv_path = os.path.join(a.outdir, "before_inventory.sqlite")
    inv = sqlite3.connect(inv_path)
    inv.executescript("""
      PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
      DROP TABLE IF EXISTS records;
      CREATE TABLE records(decision_id TEXT PRIMARY KEY,
                           language TEXT, content_hash TEXT);
      DROP TABLE IF EXISTS tokens_before;
      CREATE TABLE tokens_before(source_decision_id TEXT, target_ref TEXT,
                                 target_type TEXT);
      DROP TABLE IF EXISTS links_before;
      CREATE TABLE links_before(source_decision_id TEXT, target_ref TEXT,
                                target_decision_id TEXT, match_type TEXT);
    """)

    _log("dump graph key columns")
    rg = sqlite3.connect(
        f"file:{os.path.join(OUT_DIR, 'reference_graph.db')}?mode=ro&immutable=1",
        uri=True)
    inv.executemany("INSERT INTO tokens_before VALUES (?,?,?)",
                    rg.execute("SELECT source_decision_id, target_ref, "
                               "target_type FROM decision_citations"))
    inv.executemany("INSERT INTO links_before VALUES (?,?,?,?)",
                    rg.execute("SELECT source_decision_id, target_ref, "
                               "target_decision_id, match_type "
                               "FROM citation_targets"))
    inv.commit()
    rg.close()

    _log("paired extraction pass")
    classes = {}          # class -> per-language counts
    examples = {}         # class -> [up to 50 traceable examples]
    n = 0
    q = "SELECT decision_id, language, content_hash, full_text FROM decisions"
    if a.limit:
        q += f" LIMIT {a.limit}"
    ins = []
    for did, lg, ch, txt in dec.execute(q):
        n += 1
        ins.append((did, lg, ch))
        if len(ins) >= 20000:
            inv.executemany("INSERT OR REPLACE INTO records VALUES (?,?,?)", ins)
            inv.commit(); ins = []
            _log(f"  {n:,} records")
        if not txt:
            continue
        o = {(c.citation_type, c.normalized)
             for c in OLD.extract_case_citations(txt)}
        w = {(c.citation_type, c.normalized)
             for c in NEW.extract_case_citations(txt)}
        if o == w:
            continue
        o_norm = {x[1] for x in o}
        w_norm = {x[1] for x in w}
        for tt, nz in w - o:
            if tt == "bge" and nz[4:] in o_norm:
                cls = "retyped_docket_to_bge"
            elif nz in o_norm:
                cls = "retyped_other"
            else:
                cls = f"newly_extracted_{tt}"
            classes.setdefault(cls, {}).setdefault(lg, 0)
            classes[cls][lg] += 1
            if len(examples.setdefault(cls, [])) < 50:
                examples[cls].append({"decision_id": did, "language": lg,
                                      "token": nz})
        for tt, nz in o - w:
            if tt == "docket" and _BARE.match(nz) and ("BGE " + nz) in w_norm:
                cls = "bare_absorbed_into_bge"     # the intended movement
            elif nz in w_norm:
                cls = "retyped_bge_to_docket" if tt == "bge" else "retyped_other_loss"
            else:
                cls = f"no_longer_extracted_{tt}"
            classes.setdefault(cls, {}).setdefault(lg, 0)
            classes[cls][lg] += 1
            if len(examples.setdefault(cls, [])) < 50:
                examples[cls].append({"decision_id": did, "language": lg,
                                      "token": nz})
    if ins:
        inv.executemany("INSERT OR REPLACE INTO records VALUES (?,?,?)", ins)
    inv.commit()
    inv.execute("CREATE INDEX IF NOT EXISTS idx_tok ON tokens_before(source_decision_id)")
    inv.commit(); inv.close()

    out = {
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "records_scanned": n,
        "old_extractor": a.old, "new_extractor": a.new,
        "transition_classes": classes,
    }
    with open(os.path.join(a.outdir, "paired_extraction_summary.json"), "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    with open(os.path.join(a.outdir, "paired_extraction_examples.json"), "w") as f:
        json.dump(examples, f, indent=1, sort_keys=True)
    _log(f"done: {n:,} records")
    return 0


if __name__ == "__main__":
    sys.exit(main())
