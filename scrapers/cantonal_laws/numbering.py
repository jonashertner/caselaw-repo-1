"""Split a cantonal law's systematic number off the front of its title.

Cantonal registers publish a law as one string — "101 Constitution de la
République", "101.000  Costituzione della Repubblica", "A 1 01 Acte
d'union" — and the number in front is the key a practitioner actually
cites and looks up. Getting it out is the difference between
get_law(canton='TI', sr_number='101.000') working and returning nothing.

Two shapes occur, and the order they are tried in matters:

  numeric      101, 101.000, 210.1, 831.2a   — every canton but Geneva
  alphanumeric A 1 01, B 5 15.24, L 2 40.01  — Geneva's RSG only

Numeric is tried first and is unchanged from what `sil.py` always did,
so the cantons that already parse correctly cannot regress. The Geneva
branch only ever sees titles the numeric branch rejected.

Nothing is guessed: a title matching neither shape keeps the caller's
existing value, because a wrong systematic number is worse than a known
placeholder — it silently resolves a lookup to the wrong act.
"""
from __future__ import annotations

import re

# "101", "101.000", "210.1", "831.2a" then the title.
_NUMERIC = re.compile(r"^([\d.]+[a-z]?)\s+(.*)", re.DOTALL)

# Geneva RSG: a letter, then two spaced numeric groups, optionally
# sub-numbered — "A 1 01", "B 5 15.24", "L 2 40.01".
_GENEVA = re.compile(r"^([A-Z]\s+\d+\s+\d+(?:\.\d+)?)\s+(.*)", re.DOTALL)


def split_number_and_title(raw_title: str,
                           fallback: str = "") -> tuple[str, str]:
    """-> (sr_number, title).

    On no match returns (fallback, raw_title) — the caller's status quo.
    """
    raw = (raw_title or "").strip()
    for pattern in (_NUMERIC, _GENEVA):
        m = pattern.match(raw)
        if m and m.group(2).strip():
            return m.group(1).rstrip("."), m.group(2).strip()
    return fallback, raw


def looks_like_geneva_slug(sr_number: str) -> bool:
    """True for the `rsg_a1_01` filenames sil.py falls back to for GE."""
    return bool(re.match(r"^rsg_", (sr_number or "").strip(), re.IGNORECASE))


def slug_matches_number(slug: str, sr_number: str) -> bool:
    """Cross-check a Geneva slug against a number parsed from the title.

    `rsg_a1_01` and "A 1 01" are the same key written two ways, so
    agreement is evidence the parse is right rather than merely
    well-formed. Compares the alphanumerics only, case-folded.

    The slug spells a decimal point `p`, because a dot is awkward in a
    filename: `rsg_a1_11p0` is "A 1 11.0". Two thirds of Geneva's laws
    are sub-numbered that way, so a comparison that misses it reports a
    correct parse as a mismatch.
    """
    def norm(s: str) -> str:
        return re.sub(r"[^0-9a-z]", "", (s or "").casefold())

    s = norm(re.sub(r"(?<=\d)p(?=\d)", ".", slug or ""))
    if s.startswith("rsg"):
        s = s[3:]
    return bool(s) and s == norm(sr_number)
