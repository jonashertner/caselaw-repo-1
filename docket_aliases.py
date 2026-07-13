"""Joined-docket alias extraction for consolidated federal proceedings (issue #41).

A consolidated Federal Supreme Court decision (DE *vereinigte Verfahren*,
FR *causes jointes*) is stored under ONLY its lead docket. Example: the decision
whose Urteilskopf reads

    1B_242/2022, 1B_243/2022 und 1B_244/2022
    Urteil vom 30. Mai 2022

is stored as ``bger_1B_242_2022`` with ``docket_number = "1B 242/2022"``. The
joined (secondary) dockets 1B_243/2022 and 1B_244/2022 survive only inside the
decision's own ``full_text`` caption, so a lookup by a secondary docket returns
"Decision not found" even though it identifies the very same decision.

This module derives, from the caption, a mapping every-secondary-docket ->
lead-decision. It is pure (no DB, no I/O): ``build_fts5`` uses it to populate the
``decision_docket_aliases`` table, and ``mcp_server`` uses ``normalize_docket_key``
to compute the same lookup key at serve time.

Design constraints (see issue #41, and the review notes):
  * Extraction is deliberately conservative: it keys off the multilingual
    judgment-date line ("Urteil vom" / "Arrêt du" / "Sentenza del") that closes
    the caption, and requires the FIRST docket in the head to be the stored lead
    docket. Body citations appear AFTER that line and are structurally excluded.
  * A row that fails any invariant yields NO aliases (skip, never guess).
"""
from __future__ import annotations

import re

# A Federal Supreme Court docket as printed in a caption: PREFIX (1-2 digits +
# 1-3 letters), a "_"/" "/"." gap, a serial, a slash, and a 4-digit year. The
# year is always slash-preceded in running text (the all-underscore form only
# occurs in synthetic decision_ids, never in the caption). Negative look-arounds
# stop mid-token matches (e.g. inside a longer number).
_DOCKET_TXT = r"\d{1,2}[A-Za-z]{1,3}[._ ]\d{1,6}/\d{4}"
_DOCKET_IN_TEXT_RE = re.compile(
    r"(?<![A-Za-z0-9])(\d{1,2}[A-Za-z]{1,3})[._ ](\d{1,6})/(\d{4})(?!\d)"
)

# A LIST SEPARATOR between joined dockets in a caption: comma, semicolon, or the
# multilingual "and" connector (DE und, FR et, IT e/ed), or an ampersand. Bare
# whitespace is deliberately NOT a separator — the joined dockets in a genuine
# "vereinigte Verfahren" caption are always explicitly punctuated
# ("1B_242/2022, 1B_243/2022 und 1B_244/2022"), whereas a docket that merely sits
# in a nearby prose line (e.g. a revision reference "... (1A.278/2004)") is not.
_SEP = r"(?:\s*,\s*|\s*;\s*|\s+und\s+|\s+et\s+|\s+ed?\s+|\s+and\s+|\s*&\s*)"

# A maximal run of >=2 dockets joined ONLY by list separators. The lead docket of
# a consolidated decision heads this run; body citations to unrelated cases form
# their own runs that never start with the lead.
_DOCKET_RUN_RE = re.compile(
    r"(?<![A-Za-z0-9])(%s(?:%s%s)+)" % (_DOCKET_TXT, _SEP, _DOCKET_TXT)
)

# Strip a court prefix from a canonical decision_id form ("bger_1B_243_2022").
_COURT_PREFIX_RE = re.compile(
    r"^(?:bger|bge|bvger|bstger|bpatger)_", re.IGNORECASE
)

# Match a docket in any form (caption slash-year OR decision_id underscore-year)
# for key normalisation.
_KEY_RE = re.compile(
    r"(\d{1,2}[A-Za-z]{1,3})[._ ](\d{1,6})[/_](\d{4})"
)

# Only the head of the document holds the caption; scanning further would start
# to admit body citations even with the date-line guard on malformed captions.
_HEAD_SCAN_CHARS = 2500

# A caption listing an implausibly long run of dockets is almost certainly a
# parse artefact (e.g. a table of unrelated cases), not a genuine consolidation.
_MAX_JOINED_DOCKETS = 12


def normalize_docket_key(s: str | None) -> str | None:
    """Canonical lookup key for a docket, or ``None`` if not docket-shaped.

    Accepts raw dockets ("1B_243/2022", "1B 243/2022", "1B.243/2022"),
    canonical decision_ids ("bger_1B_243_2022"), and stored ``docket_number``
    values; returns the uniform key "1B_243/2022" (upper-cased prefix,
    underscore prefix-serial gap, slash year). Applying the SAME function to the
    stored alias and to a lookup reference makes the match separator-agnostic.
    """
    if not s:
        return None
    t = _COURT_PREFIX_RE.sub("", str(s).strip())
    m = _KEY_RE.search(t)
    if not m:
        return None
    return f"{m.group(1).upper()}_{m.group(2)}/{m.group(3)}"


def _normalize_run_dockets(run_text: str) -> list[str]:
    """Ordered, de-duplicated normalised keys for every docket in a run."""
    keys: list[str] = []
    seen: set[str] = set()
    for m in _DOCKET_IN_TEXT_RE.finditer(run_text):
        key = f"{m.group(1).upper()}_{m.group(2)}/{m.group(3)}"
        if key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def extract_joined_dockets(full_text: str | None, lead_docket: str | None) -> list[str]:
    """Return the joined (secondary) dockets of a consolidated decision.

    Each returned value is a normalised key (as ``normalize_docket_key``
    produces), excluding the lead. Returns ``[]`` unless the head region holds a
    separator-joined docket RUN that:
      1. contains at least two distinct dockets,
      2. begins with the stored lead docket (so a body-citation run to unrelated
         cases, which never starts with the lead, is excluded), and
      3. is not implausibly long.

    The run structure is what distinguishes a genuine consolidation
    ("1B_100/2011, 1B_99/2011") from a revision/prior-instance reference that
    merely names another docket in nearby prose ("Revision des ... (1A.278/2004)").
    """
    if not full_text or not lead_docket:
        return []
    lead_key = normalize_docket_key(lead_docket)
    if not lead_key:
        return []

    head = full_text[:_HEAD_SCAN_CHARS]
    for run in _DOCKET_RUN_RE.finditer(head):
        keys = _normalize_run_dockets(run.group(1))
        if len(keys) < 2 or len(keys) > _MAX_JOINED_DOCKETS:
            continue
        if keys[0] != lead_key:
            continue
        return keys[1:]
    return []
