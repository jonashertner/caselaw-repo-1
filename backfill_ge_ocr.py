#!/usr/bin/env python3
"""OCR-backfill for ge_gerichte decisions with CID-encoded PDFs.

Root cause (proven 2026-07-01): ~7,052 ge_gerichte (justice.ge.ch ATAS/etc.)
PDFs embed custom-encoded fonts with NO ToUnicode CMap. The scraper's text
extraction therefore produced binary garbage (>15% C0 control chars); fitz
yields raw glyph bytes and pdfplumber yields "(cid:N)" placeholders. Neither
is recoverable by re-extraction. OCR (tesseract `fra`) renders the glyphs as
images and recovers clean text — verified on ge_gerichte_ATAS_1001_2007.

These decisions have no entscheidsuche fallback, so OCR is the only path. This
script re-fetches each garbled decision's `pdf_url`, renders every page with
fitz, OCRs it, and writes clean `full_text` back.

SAFETY:
  --sample N   process N garbled rows, write a review file to --out, NEVER
               touch the shard. (default mode for validation)
  --full       rewrite garbled rows into <shard>.ocr (a NEW file), with a
               resumable checkpoint. The operator swaps it in after review —
               this script never edits the live shard in place.

Usage:
  python backfill_ge_ocr.py --sample 20
  python backfill_ge_ocr.py --full --workers 12      # gated; writes <shard>.ocr
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request

SHARD = os.environ.get(
    "GE_SHARD", "/opt/caselaw/repo/output/decisions/ge_gerichte.jsonl")
CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
GARBLE_THRESHOLD = 0.15  # >15% control chars ⇒ failed CID-font extraction
UA = "Mozilla/5.0 (OpenCaseLaw OCR backfill; +https://opencaselaw.ch)"


def ctrl_frac(s: str) -> float:
    n = len(s)
    return (len(CTRL_RE.findall(s)) / n) if n else 0.0


def is_garbled(row: dict) -> bool:
    ft = row.get("full_text") or ""
    return bool(ft) and ctrl_frac(ft) > GARBLE_THRESHOLD and bool(row.get("pdf_url"))


def fetch_pdf(url: str, dest: str, retries: int = 2) -> bool:
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            data = urllib.request.urlopen(req, timeout=30).read()
            if data[:4] == b"%PDF":
                with open(dest, "wb") as fh:
                    fh.write(data)
                return True
        except Exception:
            time.sleep(1.5 * (i + 1))
    return False


def ocr_pdf(pdf_path: str, dpi: int = 300, lang: str = "fra",
            max_pages: int = 200) -> str:
    import fitz  # PyMuPDF

    doc = fitz.open(pdf_path)
    parts: list[str] = []
    try:
        for n, page in enumerate(doc):
            if n >= max_pages:
                break
            pix = page.get_pixmap(dpi=dpi)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tf:
                png = tf.name
                pix.save(png)
            try:
                # Pin tesseract to a single OpenMP thread so N parallel workers
                # use N cores, not N*16 — otherwise the box oversubscribes and
                # serving suffers. Belt-and-suspenders vs the launch env.
                out = subprocess.run(
                    ["tesseract", png, "stdout", "-l", lang],
                    capture_output=True, text=True, timeout=180,
                    env={**os.environ, "OMP_THREAD_LIMIT": "1"})
                parts.append(out.stdout)
            finally:
                os.unlink(png)
    finally:
        doc.close()
    return "\n".join(parts)


def normalize(t: str) -> str:
    t = CTRL_RE.sub("", t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def recover(row: dict, dpi: int, lang: str, max_pages: int) -> tuple[str, str]:
    """Return (recovered_text, status). status: 'ok' | 'fetch' | 'ocr' | 'thin'."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
        pdf = tf.name
    try:
        if not fetch_pdf(row["pdf_url"], pdf):
            return "", "fetch"
        try:
            rec = normalize(ocr_pdf(pdf, dpi, lang, max_pages))
        except Exception as e:  # noqa: BLE001
            return "", "ocr:" + str(e)[:60]
    finally:
        if os.path.exists(pdf):
            os.unlink(pdf)
    if len(rec) < 200 or ctrl_frac(rec) > 0.02:
        return rec, "thin"
    return rec, "ok"


def iter_garbled(limit: int | None = None):
    n = 0
    with open(SHARD, errors="ignore") as fh:
        for line in fh:
            if '"full_text"' not in line:
                continue
            try:
                row = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            if is_garbled(row):
                yield row
                n += 1
                if limit and n >= limit:
                    return


def run_sample(args) -> None:
    out = open(args.out, "w")
    n = ok = 0
    for row in iter_garbled(limit=args.sample):
        n += 1
        did = row["decision_id"]
        old = row.get("full_text") or ""
        rec, status = recover(row, args.dpi, args.lang, args.max_pages)
        ok += status == "ok"
        out.write(json.dumps({
            "decision_id": did, "pdf_url": row.get("pdf_url"),
            "old_len": len(old), "old_ctrl_pct": round(100 * ctrl_frac(old), 1),
            "new_len": len(rec), "new_ctrl_pct": round(100 * ctrl_frac(rec), 2),
            "status": status, "sample": rec[:400],
        }, ensure_ascii=False) + "\n")
        out.flush()
        print("  %-30s old=%5d(%2.0f%%) -> ocr=%6d(%.2f%%) [%s]" % (
            did, len(old), 100 * ctrl_frac(old), len(rec),
            100 * ctrl_frac(rec), status))
        sys.stdout.flush()
    out.close()
    print("\nSAMPLE: %d processed, %d clean-recovered (status=ok). Review: %s"
          % (n, ok, args.out))


def _ocr_worker(item):
    """Top-level (picklable) worker: fetch + OCR one decision."""
    did, url, dpi, lang, max_pages = item
    try:
        rec, status = recover({"pdf_url": url, "decision_id": did},
                              dpi, lang, max_pages)
    except Exception as e:  # noqa: BLE001
        rec, status = "", "exc:" + str(e)[:60]
    return {"decision_id": did, "status": status,
            "text": rec if status == "ok" else "", "len": len(rec)}


def run_ocr(args) -> None:
    """OCR phase: parallel-OCR every garbled decision into a resumable
    checkpoint JSONL. NEVER touches the shard."""
    garbled = {}
    for row in iter_garbled():
        garbled.setdefault(row["decision_id"], row["pdf_url"])
    print("garbled unique decisions: %d" % len(garbled), flush=True)
    done = set()
    if os.path.exists(args.checkpoint):
        with open(args.checkpoint, errors="ignore") as fh:
            for ln in fh:
                try:
                    done.add(json.loads(ln)["decision_id"])
                except Exception:  # noqa: BLE001
                    pass
    todo = [(d, u, args.dpi, args.lang, args.max_pages)
            for d, u in garbled.items() if d not in done]
    print("checkpoint done=%d  todo=%d  workers=%d  dpi=%d"
          % (len(done), len(todo), args.workers, args.dpi), flush=True)
    if not todo:
        print("nothing to do.", flush=True)
        return
    import multiprocessing as mp
    n = n_ok = 0
    with open(args.checkpoint, "a") as ck, mp.Pool(args.workers) as pool:
        for res in pool.imap_unordered(_ocr_worker, todo, chunksize=1):
            ck.write(json.dumps(res, ensure_ascii=False) + "\n")
            ck.flush()
            n += 1
            n_ok += res["status"] == "ok"
            if n % 50 == 0:
                print("  %d/%d  ok=%d" % (n, len(todo), n_ok), flush=True)
    print("OCR phase done: %d processed, %d ok. Checkpoint: %s"
          % (n, n_ok, args.checkpoint), flush=True)


def run_apply(args) -> None:
    """Apply phase: stream the live shard into <shard>.ocr, substituting OCR
    text for recovered decisions. Preserves every other row verbatim. Writes a
    NEW file; the operator swaps it in after review."""
    rec = {}
    with open(args.checkpoint, errors="ignore") as fh:
        for ln in fh:
            try:
                r = json.loads(ln)
                if r.get("status") == "ok" and r.get("text"):
                    rec[r["decision_id"]] = r["text"]
            except Exception:  # noqa: BLE001
                pass
    print("recovered texts available: %d" % len(rec), flush=True)
    out_path = SHARD + ".ocr"
    subbed = total = 0
    with open(SHARD, errors="ignore") as src, open(out_path, "w") as dst:
        for line in src:
            total += 1
            try:
                row = json.loads(line)
            except Exception:  # noqa: BLE001
                dst.write(line if line.endswith("\n") else line + "\n")
                continue
            did = row.get("decision_id")
            if did in rec:
                row["full_text"] = rec[did]
                row["text_source"] = "ocr"
                dst.write(json.dumps(row, ensure_ascii=False) + "\n")
                subbed += 1
            else:
                dst.write(line if line.endswith("\n") else line + "\n")
    print("wrote %s: %d rows total, %d OCR-substituted"
          % (out_path, total, subbed), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", type=int, default=0,
                    help="process N garbled rows to --out, never touch the shard")
    ap.add_argument("--ocr", action="store_true",
                    help="OCR phase: parallel-OCR all garbled rows into --checkpoint (no shard write)")
    ap.add_argument("--apply", action="store_true",
                    help="apply phase: write <shard>.ocr from --checkpoint (never edits in place)")
    ap.add_argument("--dpi", type=int, default=250)
    ap.add_argument("--lang", default="fra")
    ap.add_argument("--max-pages", type=int, default=200)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--out", default="/tmp/ocr_ge_sample.jsonl")
    ap.add_argument("--checkpoint",
                    default="/opt/caselaw/repo/output/ocr_ge_results.jsonl")
    args = ap.parse_args()

    if args.sample:
        run_sample(args)
    elif args.ocr:
        run_ocr(args)
    elif args.apply:
        run_apply(args)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
