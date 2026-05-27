#!/usr/bin/env python3
"""Shared full-text extractor used by Options B (batch) + C (on-demand MCP).

Given a publication's URL or pdf_url:
  1. License-gate: only proceed for permissively-licensed records
     (CC-BY/SA/NC + non-ND variants, OA-Swiss-federal,
     OA-author-permitted-reuse). Skip CC-BY-ND, rightsstatements-in-
     copyright, raw copyright statements.
  2. Resolve to a PDF URL (per-source heuristics for landing pages).
  3. Download with rate-limit + retries.
  4. Extract text with pymupdf (fitz).
  5. Return text + provenance + bytes-fetched.

The PDF file is optionally cached on disk for re-extraction.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

log = logging.getLogger("fulltext_extractor")

USER_AGENT = (
    "OpenCaseLaw-scholarship/0.1 (mailto:scholarship@opencaselaw.ch; "
    "+https://opencaselaw.ch)"
)

# Permissive licenses we'll fully extract for. CC-BY-ND-* is excluded
# because ND restricts derivative works; storing extracted text could
# be argued as a derivative form (conservative interpretation).
_PERMISSIVE = re.compile(
    r"^(CC-BY(?!.*-ND)[A-Z0-9.\-]*|OA-Swiss-federal|"
    r"OA-author-permitted-reuse)$",
    re.I,
)


def is_permissive_license(license_code: str | None) -> bool:
    if not license_code:
        return False
    return bool(_PERMISSIVE.match(license_code))


def fetch_bytes(url: str, timeout: int = 60, max_bytes: int = 50_000_000) -> bytes | None:
    """Fetch a URL up to max_bytes. Returns None on failure."""
    req = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read(max_bytes)
    except Exception as e:
        log.debug("fetch failed %s: %s", url, e)
        return None


# Per-source PDF-link resolution. Returns the direct PDF URL given
# a publication record (dict), or None.

def _eperiodica_pdf(record: dict) -> str | None:
    """e-periodica: PDF URL is in all_identifiers as cntmng?type=pdf&pid=..."""
    raw = record.get("raw_metadata")
    if raw:
        try:
            d = json.loads(raw) if isinstance(raw, str) else raw
            for ident in d.get("all_identifiers") or []:
                if "cntmng?type=pdf" in ident:
                    return ident
        except Exception:
            pass
    return None


def _anci_pdf(record: dict) -> str | None:
    return record.get("pdf_url")


def _onlinekommentar_pdf(record: dict) -> str | None:
    return record.get("pdf_url")


def _generic_pdf(record: dict) -> str | None:
    """Last-resort: if url itself ends in .pdf use it directly."""
    url = record.get("url") or record.get("pdf_url")
    if url and url.lower().endswith(".pdf"):
        return url
    return None


# Per-source resolvers, by source key (most-specific first)
PDF_RESOLVERS = {
    "e_periodica_law": _eperiodica_pdf,
    "e_periodica_polsci": _eperiodica_pdf,
    "e_periodica_pubadmin": _eperiodica_pdf,
    "anci_ch": _anci_pdf,
    "onlinekommentar": _onlinekommentar_pdf,
}


def resolve_pdf_url(record: dict) -> str | None:
    """Find a directly-downloadable PDF URL for a publication record."""
    src = record.get("source") or ""
    resolver = PDF_RESOLVERS.get(src)
    if resolver:
        pdf = resolver(record)
        if pdf:
            return pdf
    return _generic_pdf(record)


def extract_text_from_pdf(data: bytes) -> str | None:
    """Extract plain text from PDF bytes via pymupdf (fitz)."""
    try:
        import fitz  # type: ignore
    except ImportError:
        log.error("pymupdf (fitz) not installed")
        return None
    try:
        doc = fitz.open(stream=data, filetype="pdf")
        text_parts = []
        for page in doc:
            text_parts.append(page.get_text("text"))
        doc.close()
        text = "\n".join(text_parts)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip() or None
    except Exception as e:
        log.warning("PDF extract failed: %s", e)
        return None


def fetch_and_extract(
    record: dict,
    *,
    rate_limit_secs: float = 1.0,
    require_permissive: bool = True,
    pdf_cache_dir: Path | None = None,
) -> dict:
    """End-to-end: resolve PDF URL → fetch → extract → return dict.

    Returns dict with: ok, reason, text, text_chars, pdf_url, bytes,
    sha256.
    """
    if require_permissive and not is_permissive_license(record.get("license")):
        return {"ok": False, "reason": "non_permissive_license"}

    pdf_url = resolve_pdf_url(record)
    if not pdf_url:
        return {"ok": False, "reason": "no_resolvable_pdf_url"}

    time.sleep(rate_limit_secs)
    data = fetch_bytes(pdf_url)
    if not data:
        return {"ok": False, "reason": "fetch_failed", "pdf_url": pdf_url}

    if not data.startswith(b"%PDF"):
        return {
            "ok": False, "reason": "not_a_pdf",
            "pdf_url": pdf_url, "bytes": len(data),
        }

    text = extract_text_from_pdf(data)
    if not text:
        return {
            "ok": False, "reason": "extract_failed",
            "pdf_url": pdf_url, "bytes": len(data),
        }

    sha256 = hashlib.sha256(data).hexdigest()
    if pdf_cache_dir:
        pdf_cache_dir.mkdir(parents=True, exist_ok=True)
        out = pdf_cache_dir / f"{sha256[:2]}" / f"{sha256}.pdf"
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            out.write_bytes(data)
        except Exception as e:
            log.debug("cache write failed: %s", e)

    return {
        "ok": True,
        "text": text,
        "text_chars": len(text),
        "pdf_url": pdf_url,
        "bytes": len(data),
        "sha256": sha256,
    }
