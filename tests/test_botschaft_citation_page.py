"""Issue #30: search_botschaft's bbl_citation used the Fedlex `fga` ELI segment
(an internal sequence number) instead of the printed Bundesblatt page
(jolux:memorialPage, the page in the print/PDF gazette edition).

Confirmed corpus-wide: of 600 Botschaften that carry a memorialPage in Fedlex,
594 (99%) had ELI-segment != memorialPage. The citation must use memorialPage;
the ELI URI keeps its segment for fetching. Post-2022 Bundesblatt has no
memorialPage (the segment doubles as the doc number) -> fall back to the segment.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.fedlex_materialien import _fga_candidate  # noqa: E402
from search_stack.build_botschaft_corpus import bbl_citation  # noqa: E402

BASE = "https://fedlex.data.admin.ch/eli/fga"


def test_memorialpage_wins_over_eli_segment():
    # VVG: ELI seg 1325, printed page 7705 -> cite 7705, keep ELI for fetching.
    uri = f"{BASE}/2011/1325"
    assert _fga_candidate(uri, "7705", "Botschaft VVG") == (2011, 7705, uri, "Botschaft VVG")


def test_no_memorialpage_falls_back_to_segment():
    # post-2022 Bundesblatt: no memorialPage -> the segment is the doc number.
    uri = f"{BASE}/2022/3190"
    assert _fga_candidate(uri, None, "X") == (2022, 3190, uri, "X")


def test_composite_segment_recovered_via_memorialpage():
    # composite ELI segment int() can't parse — currently dropped; memorialPage recovers it.
    uri = f"{BASE}/1999/1_9194_8542_8123"
    assert _fga_candidate(uri, "9194", "Y") == (1999, 9194, uri, "Y")


def test_composite_segment_without_memorialpage_is_skipped():
    assert _fga_candidate(f"{BASE}/1999/1_9194_8542_8123", None, "Y") is None


def test_citation_uses_printed_page_and_eli_preserved():
    year, page, uri, _ = _fga_candidate(f"{BASE}/2011/1325", "7705", "z")
    assert bbl_citation(year, page) == "BBl 2011 7705"   # the citable PDF-edition page
    assert uri == f"{BASE}/2011/1325"                    # ELI kept intact for fetch


def test_malformed_uri_returns_none():
    assert _fga_candidate("", "7705", "x") is None
    assert _fga_candidate(None, None, "x") is None
    assert _fga_candidate(f"{BASE}/notayear/1325", "7705", "x") is None
