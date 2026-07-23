"""Offline tests for the SH and UR decision-date fixes (2026-07-23).

SH: CMS metadata (custom_publication_date_date / publication_date) is a
publication date, never the judgment date; the judgment date is parsed from the
PDF header and never falls back to the publication date.

UR: the portal 1905-01-01 placeholder is rejected; the datum-sort ISO fallback
(previously dead code) works; the header rescues sentinel/missing dates; the
docket year is never fabricated into a full date.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
for p in (str(REPO), str(REPO / "scrapers" / "cantonal")):
    if p not in sys.path:
        sys.path.insert(0, p)

import scrapers.cantonal.sh_gerichte as sh  # noqa: E402
import scrapers.cantonal.ur_gerichte as ur  # noqa: E402


# ----------------------------- SH -----------------------------

def test_sh_head_date_german_month():
    txt = "Obergericht des Kantons Schaffhausen\nOGE 60/2024/13 vom 20. Dezember 2024\n..."
    assert sh._parse_head_date(txt) == date(2024, 12, 20)


def test_sh_head_date_numeric():
    txt = "Urteil vom 05.03.2024 in Sachen ..."
    assert sh._parse_head_date(txt) == date(2024, 3, 5)


def test_sh_head_date_absent_returns_none():
    assert sh._parse_head_date("Kein Datum im Kopf, nur Fliesstext.") is None


def test_sh_parse_item_does_not_put_publication_date_in_decision_date():
    item = {
        "contentid": "42",
        "kachellabel": "Nr. 60/2017/43",
        "articleHeadline": "Titel",
        "custom_publication_date_date": "10.01.2020",  # portal publication date
        "publication_date": "02.02.2021",              # CMS migration timestamp
        "sliderguid": "uuid",
        "permalink": "/x",
    }
    stub = sh.SHGerichteScraper._parse_item(object(), item)
    # decision_date must NOT be set from CMS metadata
    assert stub.get("decision_date") is None
    # publication_date prefers the portal date over the migration timestamp
    assert stub["publication_date"] == date(2020, 1, 10)


# ----------------------------- UR -----------------------------

def test_ur_reject_sentinel():
    assert ur._reject_sentinel(date(1905, 1, 1)) is None
    assert ur._reject_sentinel(date(1949, 12, 31)) is None
    assert ur._reject_sentinel(date(2015, 6, 24)) == date(2015, 6, 24)
    assert ur._reject_sentinel(None) is None


def test_ur_iso_fallback_now_parses():
    # datum empty, datum-sort ISO -> previously dead, now recovered.
    assert ur._parse_iso_date("2012-09-25") == date(2012, 9, 25)
    assert ur._parse_swiss_date("2012-09-25") is None  # confirms the old path missed it


def test_ur_parse_entity_rejects_sentinel_date():
    entity = {
        "name": "2015_OG V 14 24",
        "_downloadBtn": '<a href="/_rte/publikation/40241">PDF</a>',
        "herausgeber": "Obergericht",
        "datum": "01.01.1905",       # sentinel
        "datum-sort": "1905-01-01",  # sentinel
    }
    stub = ur.URGerichteScraper._parse_entity(object(), entity)
    assert stub is not None
    assert stub["decision_date"] is None  # sentinel rejected, not stored as 1905


def test_ur_parse_entity_keeps_real_date():
    entity = {
        "name": "2015_OG V 14 24",
        "_downloadBtn": '<a href="/_rte/publikation/40241">PDF</a>',
        "herausgeber": "Obergericht",
        "datum": "24.06.2015",
        "datum-sort": "2015-06-24",
    }
    stub = ur.URGerichteScraper._parse_entity(object(), entity)
    assert stub["decision_date"] == date(2015, 6, 24)


def test_ur_head_date_rescue():
    txt = "OG Z 24 2\nEntscheid vom 24. Juni 2025\nin Sachen ..."
    assert ur._parse_head_date(txt) == date(2025, 6, 24)


def test_ur_head_date_rejects_sentinel_in_header():
    txt = "Beschluss vom 01.01.1905 (Platzhalter)"
    assert ur._parse_head_date(txt) is None
