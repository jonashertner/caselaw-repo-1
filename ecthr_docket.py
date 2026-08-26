"""Display form of an ECtHR docket.

ECtHR ``docket_number`` values carry the judgment date —
``47358/20_20220830`` — because 158 application numbers in the corpus name
more than one judgment (merits, then just satisfaction or revision, years
apart) and the bare number collides. The date belongs in the key, not in
anything a reader sees: a page title, an RSS item, a Schema.org ``name`` or
a citation string must show ``47358/20``.

Kept as its own dependency-free module (like ``docket_aliases``) so the MCP
server, the SEO page generator and the feed generator share one definition
instead of three regexes that can drift apart.
"""
from __future__ import annotations

import re

# Courts whose dockets carry the date suffix. hudoc_ch and bge_egmr use their
# own older formats and are deliberately absent.
ECTHR_DATE_SUFFIX_COURTS = frozenset({
    "ecthr_chamber", "ecthr_committee", "ecthr_grand_chamber",
})

# Trailing "_yyyymmdd". Unambiguous against an application number, which
# always contains a slash.
DATE_SUFFIX_RE = re.compile(r"_\d{8}$")


def strip_date_suffix(docket: str | None) -> str:
    """Drop a trailing ``_yyyymmdd`` from a docket. Idempotent."""
    return DATE_SUFFIX_RE.sub("", docket or "")


def display_docket(court: str | None, docket: str | None) -> str:
    """Reader-facing docket: application numbers only, date suffix removed.

    Courts other than the three ECtHR ones are returned untouched, so this is
    safe to apply unconditionally at any display site.
    """
    d = (docket or "").strip()
    if (court or "") not in ECTHR_DATE_SUFFIX_COURTS:
        return d
    return strip_date_suffix(d)
