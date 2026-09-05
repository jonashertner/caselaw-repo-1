#!/usr/bin/env python3
"""Every passage example in the server's tool descriptions must exist in the corpus.

Scans mcp_server.py for "BGE <vol> <div> <page> E. <n>" patterns and checks each
against the live /api/erwaegung route. A description that cites a passage the
structure index lacks teaches every model that talks to us to do the same.
Live check (network); run with `make smoke-cli`. Exit 1 on any missing example.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

PATTERN = re.compile(r"\b(BGE|ATF|DTF) (\d{1,3}) ([IVX]+[ab]?) (\d{1,4}) E\. (\d+(?:\.\d+)*)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", default="https://mcp.opencaselaw.ch")
    ap.add_argument("--source", default=str(Path(__file__).resolve().parent.parent / "mcp_server.py"))
    args = ap.parse_args()
    text = Path(args.source).read_text(encoding="utf-8")
    examples = sorted({(f"bge_BGE_{v}_{d}_{p}", e, f"{c} {v} {d} {p} E. {e}") for c, v, d, p, e in PATTERN.findall(text)})
    missing = 0
    for decision_id, e_number, label in examples:
        url = f"{args.base.rstrip('/')}/api/erwaegung/{urllib.parse.quote(decision_id, safe='')}/{urllib.parse.quote(e_number, safe='')}"
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.load(r)
        except Exception as exc:  # noqa: BLE001
            data = {"error": str(exc)}
        ok = isinstance(data.get("text"), str) and data["text"].strip() != ""
        source = data.get("text_source", "")
        print(f"  {'OK  ' if ok else 'MISS'} {label}" + (f"  ({source})" if source else "") + ("" if ok else f"  {data.get('error', '')[:80]}"))
        missing += 0 if ok else 1
    print(f"{len(examples)} example(s), {missing} missing")
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
