"""Derive an extractor version from the extraction source code itself.

Both incremental builders gate their diff-base on ``extractor_version``:
a mismatch forces a full bootstrap, which is what propagates extraction
improvements to rows whose *inputs* never changed. Until 2026-08-22 that
version was a hand-bumped integer, and it was forgotten twice:

* graph:     c5edb71c (ATF/DTF citation prefixes) shipped without a bump —
  the shadow extended its 2026-06-11 base for ten weeks and drifted to
  −13.2 % citation_targets (1.52M rows), −1.33 % resolved edges.
* structure: 9cf68db5 (TI 'ritenuto in fatto' fix) shipped without a bump —
  3,995 decisions' paragraph coverage LOST vs the full rebuild. The file's
  own comment block narrates the identical 2026-07-03 incident, and it
  still recurred.

A version derived from the bytes of the extraction modules cannot be
forgotten: change the code and the stored version no longer matches, so the
next incremental run bootstraps itself. The manual integer remains as a
major-version escape hatch (e.g. to force a rebuild when a *data* semantic
changes without any code change).

Format: ``"<manual>+src.<12 hex>"``. The stored value is compared as an
opaque string by both builders, so pre-existing shadows (stored ``"1"`` /
``"2"``) mismatch on first run with this scheme and bootstrap once — which
is exactly the reseed the 12-nights-red drift needs.
"""
from __future__ import annotations

import hashlib
from pathlib import Path


def effective_version(manual: int | str, *source_files: Path | str) -> str:
    """Compose the manual version with a hash of the given source files.

    Files are hashed in sorted-name order so the result is independent of
    argument order. A missing file raises — a silently wrong version is the
    exact failure class this module exists to remove.
    """
    if not source_files:
        raise ValueError("effective_version needs at least one source file")
    h = hashlib.sha256()
    for p in sorted((Path(f) for f in source_files), key=lambda p: p.name):
        h.update(p.name.encode("utf-8"))
        h.update(b"\x00")
        h.update(p.read_bytes())
    return f"{manual}+src.{h.hexdigest()[:12]}"
