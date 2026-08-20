#!/usr/bin/env python3
"""Remove document bodies that reached the capture files by oversight.

`attest_response` names its document `draft_text`, which was missing from
_CAPTURE_BODY_FIELDS, so 166 drafted legal opinions were written to disk
over 2026-08-19/20 before it was caught. Among them a named taxpayer with
her canton and employer, successive revisions of a criminal defence, and
sentencing analysis in a sexual-offence case — data on criminal
proceedings is besonders schützenswert under nFADP Art. 5(c), and Art.
321 StGB protects the subject of a mandate even without a name.

This does to the existing records exactly what the fixed code now does to
new ones: the body is replaced by its length. The record itself is kept —
timestamp, tool, session, client, outcome — because that part is the
telemetry the notice describes and it is worth keeping. Only the content
goes.

Dry run by default. On --apply it writes a temp file, checks the line
count is unchanged, and only then replaces the original. No backup is
written on purpose: a backup of exactly the material being removed would
defeat the point.

    python3 scripts/strip_captured_bodies.py --dir output/research_logs
    python3 scripts/strip_captured_bodies.py --dir output/research_logs --apply
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import stat
import sys
from pathlib import Path

log = logging.getLogger("strip_bodies")

# Everything the fixed _capture_args treats as a body, plus the length
# backstop it applies to any oversized string.
BODY_FIELDS = {"response_text", "document_text", "text", "claim", "facts",
               "content", "body", "draft_text", "redacted_text",
               "statement", "paragraph_text", "selected_text"}
MAX_ARG_CHARS = 600


def strip_record(rec: dict) -> tuple[dict, int]:
    """-> (record, number of bodies removed)"""
    args = rec.get("args")
    if not isinstance(args, dict):
        return rec, 0
    out, n = {}, 0
    for k, v in args.items():
        if isinstance(v, str) and (k.lower() in BODY_FIELDS
                                   or len(v) > MAX_ARG_CHARS):
            out[f"{k}_len"] = len(v)
            n += 1
        else:
            out[k] = v
    if n:
        rec = {**rec, "args": out}
    return rec, n


def process(path: Path, apply: bool) -> tuple[int, int]:
    lines_in = stripped = 0
    tmp = path.with_suffix(path.suffix + ".stripped")
    fh = tmp.open("w", encoding="utf-8") if apply else None
    try:
        with path.open(encoding="utf-8", errors="replace") as src:
            for line in src:
                raw = line.rstrip("\n")
                if not raw.strip():
                    if fh:
                        fh.write(line)
                    continue
                lines_in += 1
                try:
                    rec = json.loads(raw)
                except json.JSONDecodeError:
                    if fh:
                        fh.write(line)      # keep unparseable lines verbatim
                    continue
                rec, n = strip_record(rec)
                stripped += n
                if fh:
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    finally:
        if fh:
            fh.close()

    if apply:
        out_lines = sum(1 for ln in tmp.open(encoding="utf-8") if ln.strip())
        if out_lines != lines_in:
            tmp.unlink(missing_ok=True)
            raise SystemExit(
                f"{path.name}: line count changed {lines_in} -> {out_lines}, "
                f"refusing to replace")
        # Carry over owner and mode BEFORE the swap. The server appends to
        # this file as `mcp`; run as root, replace() would leave it
        # root-owned and every subsequent append would fail with EACCES —
        # which _capture_event swallows, so capture would stop dead and
        # say nothing. That is exactly what happened on the first run:
        # 5 minutes of capture went to /dev/null before the file was
        # noticed not growing.
        st = path.stat()
        os.chown(tmp, st.st_uid, st.st_gid)
        os.chmod(tmp, stat.S_IMODE(st.st_mode))
        tmp.replace(path)
    return lines_in, stripped


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--dir", type=Path, default=Path("output/research_logs"))
    ap.add_argument("--glob", default="capture_*.jsonl")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    total = 0
    for p in sorted(a.dir.glob(a.glob)):
        lines, n = process(p, a.apply)
        total += n
        log.info("%s: %d records, %d bodies %s",
                 p.name, lines, n, "removed" if a.apply else "would be removed")
    log.info("%s %d document bodies", "removed" if a.apply else "would remove",
             total)
    if not a.apply:
        log.info("dry run — nothing written. Re-run with --apply.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
