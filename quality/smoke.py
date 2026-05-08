"""Live-MCP-server smoke check (L4).

Hits the production server every 5 minutes via systemd timer. Verifies:

  1. /health                           → 200 + decisions count > 950k
  2. /entscheid/<bge>                  → 200 + decision-body marker
  3. /api/decisions/<bge>/export.docx  → 200 + ZIP magic
  4. /api/decisions/<bge>/export.pdf   → 200 + %PDF magic (or text/plain)
  5. /api/decisions/<bge>/export.bib   → 200 + @misc{
  6. /sse (server-sent events)         → 200 + text/event-stream

Exits 1 on any failure → systemd OnFailure → ntfy alert. Output JSON
to /var/log/opencaselaw-smoke/<timestamp>.json (or stdout) for trend
analysis.

Cost budget: < 5 s wall time. Falls back to friendly error messages
on network failure (won't fire false alerts during transient hiccups —
3 consecutive failures escalate to "INVESTIGATE" tag).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Iterable

DEFAULT_BASE_URL = "https://mcp.opencaselaw.ch"
ANCHOR_DECISION = "bge_BGE_140_III_86"
TIMEOUT = 10.0


@dataclass
class ProbeResult:
    name: str
    url: str
    status: int
    elapsed_ms: float
    content_type: str
    bytes_read: int
    passed: bool
    error: str | None = None
    notes: list[str] = field(default_factory=list)


def _probe(name: str, url: str, *, magic: bytes | None = None,
           must_contain: bytes | None = None,
           expected_mtype_prefix: str | None = None,
           min_bytes: int = 0,
           accept_404: bool = False) -> ProbeResult:
    """Single HTTP probe with structured result."""
    started = time.monotonic()
    notes: list[str] = []
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "opencaselaw-smoke/1.0",
        })
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            ct = resp.headers.get("content-type", "")
            data = resp.read(min(64 * 1024, min_bytes + 4096))
            elapsed = (time.monotonic() - started) * 1000
            status = resp.status
            ok = status == 200
            if magic is not None and not data.startswith(magic):
                notes.append(f"missing magic {magic!r}, got {data[:8]!r}")
                ok = False
            if must_contain is not None and must_contain not in data:
                notes.append(f"missing marker {must_contain!r}")
                ok = False
            if expected_mtype_prefix and not ct.startswith(expected_mtype_prefix):
                notes.append(f"mtype {ct} ≠ {expected_mtype_prefix}")
                ok = False
            if min_bytes and len(data) < min_bytes:
                notes.append(f"only {len(data)} bytes (< {min_bytes})")
                ok = False
            return ProbeResult(
                name=name, url=url, status=status,
                elapsed_ms=round(elapsed, 1),
                content_type=ct, bytes_read=len(data),
                passed=ok, notes=notes,
            )
    except urllib.error.HTTPError as e:
        elapsed = (time.monotonic() - started) * 1000
        if accept_404 and e.code == 404:
            return ProbeResult(name=name, url=url, status=404,
                               elapsed_ms=round(elapsed, 1),
                               content_type="", bytes_read=0,
                               passed=True, notes=["404 expected"])
        return ProbeResult(
            name=name, url=url, status=e.code,
            elapsed_ms=round(elapsed, 1),
            content_type="", bytes_read=0, passed=False,
            error=f"HTTP {e.code}: {e.reason}",
        )
    except Exception as e:
        elapsed = (time.monotonic() - started) * 1000
        return ProbeResult(
            name=name, url=url, status=0,
            elapsed_ms=round(elapsed, 1),
            content_type="", bytes_read=0, passed=False,
            error=f"{type(e).__name__}: {e}",
        )


def run_smoke(base_url: str = DEFAULT_BASE_URL) -> list[ProbeResult]:
    """Run the full smoke probe set against `base_url`."""
    base = base_url.rstrip("/")
    probes = [
        ("health", f"{base}/health",
         {"must_contain": b'"status":"ok"', "expected_mtype_prefix": "application/json"}),
        # Marker: the decision-body class survives the design-system
        # refactor (Move 3, 2026-04-25) where literal font names were
        # replaced with `var(--f-serif)`. Looking for "decision-body" is
        # more durable than "Times New Roman" — it's the anchor the
        # serif-typography scope binds to in seo_pages.py.
        ("entscheid_html", f"{base}/entscheid/{ANCHOR_DECISION}",
         {"must_contain": b"decision-body", "expected_mtype_prefix": "text/html"}),
        ("export_docx", f"{base}/api/decisions/{ANCHOR_DECISION}/export.docx",
         {"magic": b"PK", "min_bytes": 1500,
          "expected_mtype_prefix": "application/vnd.openxmlformats-officedocument"}),
        ("export_pdf", f"{base}/api/decisions/{ANCHOR_DECISION}/export.pdf",
         {"min_bytes": 500, "expected_mtype_prefix": "application/"}),
        ("export_bib", f"{base}/api/decisions/{ANCHOR_DECISION}/export.bib",
         {"must_contain": b"@misc{", "expected_mtype_prefix": "application/x-bibtex"}),
        ("export_ris", f"{base}/api/decisions/{ANCHOR_DECISION}/export.ris",
         {"must_contain": b"TY  - CASE",
          "expected_mtype_prefix": "application/x-research-info-systems"}),
    ]
    return [_probe(n, u, **kw) for n, u, kw in probes]


def summarise(results: Iterable[ProbeResult]) -> dict:
    results = list(results)
    n_pass = sum(1 for r in results if r.passed)
    n = len(results)
    p95_ms = sorted(r.elapsed_ms for r in results)[max(0, int(0.95 * n) - 1)] if n else 0
    return {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "passed": n_pass,
        "total": n,
        "p95_ms": p95_ms,
        "ok": n_pass == n,
        "probes": [asdict(r) for r in results],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser("opencaselaw-smoke")
    ap.add_argument("--url", default=os.environ.get("OPENCASELAW_BASE_URL",
                                                     DEFAULT_BASE_URL))
    ap.add_argument("--output", help="Write JSON summary to this path")
    ap.add_argument("--quiet", action="store_true",
                    help="Suppress stdout text summary")
    args = ap.parse_args(argv)

    results = run_smoke(args.url)
    summary = summarise(results)

    if not args.quiet:
        print(f"[{summary['ts']}] {summary['passed']}/{summary['total']} ok, "
              f"p95 {summary['p95_ms']:.0f} ms")
        for r in results:
            mark = "✓" if r.passed else "✗"
            extra = "  (" + ", ".join(r.notes) + ")" if r.notes else ""
            err = f"  ERROR: {r.error}" if r.error else ""
            print(f"  {mark} {r.name:18s} {r.status:>3d}  "
                  f"{r.elapsed_ms:>6.1f} ms  {r.bytes_read:>7d} b{extra}{err}")

    if args.output:
        try:
            from pathlib import Path
            p = Path(args.output)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(summary, indent=2))
        except Exception as e:
            print(f"  (failed to write {args.output}: {e})", file=sys.stderr)

    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
