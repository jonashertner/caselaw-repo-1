"""Unit test for the BGer Neuheiten HTML parser.

Background (2026-05-06): the existing scraper was using
``_parse_search_results`` to read the Neuheiten page, but that parser
expects ``<div class="ranklist_content"><ol><li>`` whereas Neuheiten
uses a flat ``<table>`` layout. As a result every Neuheiten check
logged "0 published, 0 new" for months and 18 recently-published
dockets escaped the daily ingest. This test pins the fix so the same
class can't recur silently — if the BGer markup ever changes again
the test fails loudly.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scrapers.bger import BgerScraper  # noqa: E402


# Minimal but representative Neuheiten HTML — captured from
# search.bger.ch on 2026-05-06 (date param 20260505).
NEUHEITEN_HTML_FRAGMENT = """
<html><body>
<table>
  <tr>
    <td>05.05.2026</td>
    <td>
      <a href="/ext/eurospider/live/de/php/aza/http/index.php?highlight_docid=aza://19-03-2026-2C_111-2025&amp;lang=de&amp;zoom=&amp;type=show_document">2C_111/2025</a>
    </td>
    <td><cite>Unterrichtswesen und Berufsausbildung</cite></td>
  </tr>
  <tr>
    <td>05.05.2026</td>
    <td>
      <a href="/ext/eurospider/live/de/php/aza/http/index.php?highlight_docid=aza://22-04-2026-4A_123-2026&amp;lang=de&amp;zoom=&amp;type=show_document">4A_123/2026</a>
    </td>
    <td><cite>Vertragsrecht</cite></td>
  </tr>
  <tr>
    <td>05.05.2026</td>
    <td>
      <a href="/ext/eurospider/live/de/php/aza/http/index.php?highlight_docid=aza://14-04-2026-7B_379-2023&amp;lang=de&amp;zoom=&amp;type=show_document">7B_379/2023</a>
    </td>
    <td><cite>Strafrecht</cite></td>
  </tr>
  <tr>
    <td>—</td>
    <td><a href="/some/other/link">Help</a></td>
  </tr>
</table>
</body></html>
"""


def _make_scraper() -> BgerScraper:
    """Construct the scraper without running any network init."""
    s = BgerScraper.__new__(BgerScraper)
    # The parser only touches helpers that don't need network state.
    return s


def test_neuheiten_parser_extracts_three_decisions() -> None:
    soup = BeautifulSoup(NEUHEITEN_HTML_FRAGMENT, "html.parser")
    scraper = _make_scraper()
    stubs = list(scraper._parse_neuheiten_html(soup, "de"))

    # The "Help" link has no aza:// pattern → must be skipped.
    assert len(stubs) == 3, f"expected 3 stubs, got {len(stubs)}"

    by_docket = {s["docket_number"]: s for s in stubs}
    assert "2C_111/2025" in by_docket
    assert "4A_123/2026" in by_docket
    assert "7B_379/2023" in by_docket

    # Decision date is parsed from the aza:// URI, not from "today".
    assert by_docket["2C_111/2025"]["decision_date"] == date(2026, 3, 19)
    assert by_docket["4A_123/2026"]["decision_date"] == date(2026, 4, 22)
    assert by_docket["7B_379/2023"]["decision_date"] == date(2026, 4, 14)


def test_neuheiten_parser_builds_decision_id() -> None:
    soup = BeautifulSoup(NEUHEITEN_HTML_FRAGMENT, "html.parser")
    scraper = _make_scraper()
    stubs = list(scraper._parse_neuheiten_html(soup, "de"))
    ids = {s["decision_id"] for s in stubs}
    assert "bger_2C_111_2025" in ids
    assert "bger_4A_123_2026" in ids
    assert "bger_7B_379_2023" in ids


def test_neuheiten_parser_carries_url_for_fetch_decision() -> None:
    """The stub's ``url`` field is the Eurospider link the scraper
    falls back to when relevancy.bger.ch's JumpCGI path fails. Without
    it, ``fetch_decision`` cannot recover the body."""
    soup = BeautifulSoup(NEUHEITEN_HTML_FRAGMENT, "html.parser")
    scraper = _make_scraper()
    for stub in scraper._parse_neuheiten_html(soup, "de"):
        # The relative href in the Neuheiten page is rooted on
        # search.bger.ch (where the page is served).
        assert stub["url"].startswith(("https://www.bger.ch/", "https://search.bger.ch/"))
        assert "highlight_docid=aza://" in stub["url"]


def test_neuheiten_parser_skips_malformed_dates() -> None:
    """Defensive: a docket whose URI has an invalid date (e.g. 32-13)
    must be skipped, not crash the discovery loop."""
    bad_html = """
    <html><body><table>
      <tr><td><a href="?highlight_docid=aza://32-13-2026-2C_999-2025">2C_999/2025</a></td></tr>
    </table></body></html>
    """
    soup = BeautifulSoup(bad_html, "html.parser")
    scraper = _make_scraper()
    stubs = list(scraper._parse_neuheiten_html(soup, "de"))
    assert stubs == []


def test_neuheiten_only_flag_skips_aza_search(monkeypatch) -> None:
    """When neuheiten_only=True, discover_new() must not hit AZA search.

    AZA search is the stall site under heavy Imperva challenge — the
    poller sets this flag to bound the blast radius. We assert the
    AZA path isn't even called when the flag is set.
    """
    scraper = _make_scraper()
    # Stub the bits _init_session() reaches for; we don't want network.
    scraper._init_session = lambda: None  # type: ignore[method-assign]
    scraper.neuheiten_only = True

    called: dict[str, int] = {"neuheiten": 0, "search": 0}

    def fake_neuheiten():
        called["neuheiten"] += 1
        return iter([])

    def fake_search(_since):
        called["search"] += 1
        return iter([])

    monkeypatch.setattr(scraper, "_discover_via_neuheiten", fake_neuheiten)
    monkeypatch.setattr(scraper, "_discover_via_search", fake_search)

    list(scraper.discover_new(since_date=None))  # consume generator

    assert called["neuheiten"] == 1, "Neuheiten path must run"
    assert called["search"] == 0, (
        "AZA search must NOT run when neuheiten_only=True"
    )


def test_default_calls_both_neuheiten_and_search(monkeypatch) -> None:
    """Regression check: default behaviour (no flag) must keep walking
    both Neuheiten and AZA search — neuheiten_only is opt-in only."""
    scraper = _make_scraper()
    scraper._init_session = lambda: None  # type: ignore[method-assign]
    scraper.neuheiten_only = False  # explicit default

    called: dict[str, int] = {"neuheiten": 0, "search": 0}

    def fake_neuheiten():
        called["neuheiten"] += 1
        return iter([])

    def fake_search(_since):
        called["search"] += 1
        return iter([])

    monkeypatch.setattr(scraper, "_discover_via_neuheiten", fake_neuheiten)
    monkeypatch.setattr(scraper, "_discover_via_search", fake_search)

    list(scraper.discover_new(since_date=None))

    assert called["neuheiten"] == 1
    assert called["search"] == 1
