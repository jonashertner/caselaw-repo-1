"""ch_bundesrat scraper tests (offline).

The bj.admin.ch CMS migration (2026) retired the legacy
``/bj/de/home/publiservice/publikationen/beschwerdeentscheide.html`` listing
(now a hard 404, which crashed discovery) and replaced the detail URLs with
flat descriptive slugs of the form
``/de/entscheid-des-bundesrates-vom-13-juni-2025-eda``.

These tests pin the new slug parsing + discovery against a synthetic copy of
the new listing markup (no network), including the same-day-multiple case
(three ``13-juni-2025`` entries must yield three distinct dockets) and the
exclusion of non-detail links (entscheidsuche.ch, the listing self-link).
"""
from __future__ import annotations

import sys

sys.path.insert(0, ".")


def test_parse_slug_basic():
    from scrapers.ch_bundesrat import _parse_bundesrat_slug
    assert _parse_bundesrat_slug("entscheid-des-bundesrates-vom-13-juni-2025-eda") == (
        "2025-06-13",
        "eda",
    )


def test_parse_slug_transliterated_month_and_multiword_descriptor():
    from scrapers.ch_bundesrat import _parse_bundesrat_slug
    assert _parse_bundesrat_slug(
        "entscheid-des-bundesrates-vom-13-maerz-2015-kanton-graubuenden"
    ) == ("2015-03-13", "kanton-graubuenden")


def test_parse_slug_no_descriptor():
    from scrapers.ch_bundesrat import _parse_bundesrat_slug
    assert _parse_bundesrat_slug("entscheid-des-bundesrates-vom-8-mai-2020") == (
        "2020-05-08",
        "",
    )


def test_parse_slug_non_matching():
    from scrapers.ch_bundesrat import _parse_bundesrat_slug
    assert _parse_bundesrat_slug("some-other-page") == (None, "")


class _FakeResp:
    def __init__(self, text):
        self.text = text


def _listing_html():
    return """
    <html><body>
      <h2>Beschwerdeentscheide des Bundesrates</h2>
      <ul>
        <li><a href="/de/entscheid-des-bundesrates-vom-13-juni-2025-eda">EDA</a></li>
        <li><a href="/de/entscheid-des-bundesrates-vom-13-juni-2025-kanton-zuerich">Kanton Zürich</a></li>
        <li><a href="/de/entscheid-des-bundesrates-vom-13-juni-2025-bundesgerichtliches-verfahren">BGer Verfahren</a></li>
        <li><a href="/de/entscheid-des-bundesrates-vom-12-februar-2025-eda">EDA Feb</a></li>
        <li><a href="https://entscheidsuche.ch/">entscheidsuche</a></li>
        <li><a href="/de/beschwerdeentscheide-des-bundesrates">Übersicht (self)</a></li>
      </ul>
    </body></html>
    """


def _make_scraper():
    from scrapers.ch_bundesrat import CHBundesratScraper
    s = CHBundesratScraper()
    # Deterministic: nothing already known.
    s.state._seen = set()
    s.state._gaps = {}
    return s


def test_discover_new_yields_distinct_dockets_for_same_day():
    s = _make_scraper()
    s.get = lambda url, *a, **k: _FakeResp(_listing_html())
    stubs = list(s.discover_new())
    dockets = {st["docket_number"] for st in stubs}
    # 4 real detail entries; the 3 same-day ones must not collapse.
    assert dockets == {
        "2025-06-13-eda",
        "2025-06-13-kanton-zuerich",
        "2025-06-13-bundesgerichtliches-verfahren",
        "2025-02-12-eda",
    }


def test_discover_new_excludes_non_detail_links():
    s = _make_scraper()
    s.get = lambda url, *a, **k: _FakeResp(_listing_html())
    urls = [st["detail_url"] for st in s.discover_new()]
    assert not any("entscheidsuche.ch" in u for u in urls)
    assert not any(u.rstrip("/").endswith("beschwerdeentscheide-des-bundesrates") for u in urls)


def test_discover_new_parses_real_decision_date_from_slug():
    s = _make_scraper()
    s.get = lambda url, *a, **k: _FakeResp(_listing_html())
    by_docket = {st["docket_number"]: st for st in s.discover_new()}
    assert by_docket["2025-06-13-eda"]["decision_date"] == "2025-06-13"
    assert by_docket["2025-02-12-eda"]["decision_date"] == "2025-02-12"
