"""Tribuna GWT-RPC protocol regression tests (offline).

Covers two coupled fixes (2026-06-19):

1. be.ch upgraded the VTPlus servers for the Zivil-/Strafgerichte and the
   Anwaltsaufsichtsbehörde, removing the old 46-param ``search()`` method. Our
   scrapers still sent ``SEARCH_FIELD_COUNT = 20`` (46 params), so the server
   answered every request with a GWT ``//EX IncompatibleRemoteServiceException``.
   They must send 21 fields (47 params), like be_verwaltungsgericht already does.

2. ``TribunaBaseScraper._parse_search_response`` swallowed any non-``//OK``
   response — including that ``//EX`` — as ``(0, [])``. That masked a hard
   protocol failure as an empty portal for months and defeated the
   scraper-health silent-success detector. It must now raise
   ``TribunaProtocolError`` so the failure is loud.

All assertions are offline (no network): config is class-level and
``_build_search_body`` / ``_parse_search_response`` are pure.
"""
from __future__ import annotations

import sys

sys.path.insert(0, ".")

import pytest


def _zsg():
    from scrapers.cantonal.be_zivilstraf import BEZivilStrafScraper
    return BEZivilStrafScraper()


# --- Fix 1: the BE Tribuna scrapers must use the 47-param (nf=21) search() ---

def test_be_zivilstraf_uses_47_param_search():
    from scrapers.cantonal.be_zivilstraf import BEZivilStrafScraper
    assert BEZivilStrafScraper.SEARCH_FIELD_COUNT == 21


def test_be_anwaltsaufsicht_uses_47_param_search():
    from scrapers.cantonal.be_anwaltsaufsicht import BEAnwaltsaufsichtScraper
    assert BEAnwaltsaufsichtScraper.SEARCH_FIELD_COUNT == 21


def test_search_body_param_count_tracks_field_count():
    """The wire payload's param count must follow SEARCH_FIELD_COUNT (5+nf+21)."""
    s = _zsg()
    nf = s.SEARCH_FIELD_COUNT
    body = s._build_search_body("ab" * 48, page=0, total=None, court_filter="OG")
    # GWT method-invocation marker: refs 1|2|3|4 (base|hash|service|"search")
    # followed by the param count.
    assert f"|1|2|3|4|{nf + 26}|" in body
    # And exactly nf empty search-field refs ("11") are emitted in the values.
    assert ("11|" * nf) in body.replace("||", "|")


# --- Fix 2: //EX must raise, not be swallowed as zero results ---

def test_parse_search_response_raises_on_gwt_exception():
    from scrapers.cantonal.base_tribuna import TribunaProtocolError
    s = _zsg()
    ex = (
        '//EX[2,1,["com.google.gwt.user.client.rpc.IncompatibleRemoteServiceException",'
        '"This application is out of date, please click the refresh button... '
        'Could not locate requested method search(...) "],0,7]'
    )
    with pytest.raises(TribunaProtocolError):
        s._parse_search_response(ex)


def test_parse_search_response_ok_zero_does_not_raise():
    """A genuine empty result (//OK total=0) must still return cleanly."""
    s = _zsg()
    total, decisions = s._parse_search_response('//OK[0,[],0,7]')
    assert total == 0
    assert decisions == []


def test_parse_search_response_other_nonok_returns_empty():
    """Non-//OK, non-//EX junk stays conservative (return empty, no raise)."""
    s = _zsg()
    assert s._parse_search_response("garbage-not-a-gwt-response") == (0, [])
