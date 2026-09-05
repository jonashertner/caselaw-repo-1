"""AZA search host fallback (2026-09-05): www.bger.ch stopped answering the
date search through the residential tunnel while search.bger.ch served the
same pages; the nightly run logged 46 "Max retries exceeded" windows and the
scraper was flagged CRITICAL although Neuheiten still worked. Offline."""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scrapers.bger import AZA_SEARCH_PATH, HOST, SEARCH_HOST, BgerScraper  # noqa: E402


class FakeState:
    def is_known(self, decision_id):
        return False


class FakeResp:
    text = "<html><body>Keine Treffer</body></html>"


def scraper_with(monkeypatch, dead_hosts: set[str]):
    sc = BgerScraper.__new__(BgerScraper)
    sc.state = FakeState()
    sc.until_date = date(2026, 9, 5)
    requested: list[str] = []

    def fake_get(url, retry=0):
        requested.append(url)
        if any(url.startswith(h) for h in dead_hosts):
            raise requests.ConnectionError("Max retries exceeded")
        return FakeResp()

    monkeypatch.setattr(sc, "_get_with_pow", fake_get)
    monkeypatch.setattr(sc, "_get_hit_count", lambda soup: 0)
    monkeypatch.setattr(sc, "_is_no_results", lambda soup: True)
    return sc, requested


def test_url_shape_is_unchanged():
    assert BgerScraper._aza_url(HOST, "01.09.2026", "04.09.2026") == \
        HOST + AZA_SEARCH_PATH.format(von="01.09.2026", bis="04.09.2026")


def test_www_first_when_it_works(monkeypatch):
    sc, requested = scraper_with(monkeypatch, dead_hosts=set())
    since = date(2026, 9, 5) - timedelta(days=BgerScraper.WINDOW_DAYS * 2 - 1)
    list(sc._discover_via_search(since))
    assert len(requested) == 2                       # two windows, one request each
    assert all(u.startswith(HOST) for u in requested)
    assert SEARCH_HOST not in "".join(requested)


def test_falls_back_to_search_host_and_sticks(monkeypatch):
    sc, requested = scraper_with(monkeypatch, dead_hosts={HOST})
    since = date(2026, 9, 5) - timedelta(days=BgerScraper.WINDOW_DAYS * 3 - 1)
    list(sc._discover_via_search(since))
    # window 1: www fails, search answers; windows 2 and 3: search directly.
    assert requested[0].startswith(HOST)
    assert requested[1].startswith(SEARCH_HOST)
    assert len(requested) == 4
    assert all(u.startswith(SEARCH_HOST) for u in requested[1:])
    assert sc._aza_host == SEARCH_HOST


def test_both_hosts_dead_skips_the_window_and_moves_on(monkeypatch):
    sc, requested = scraper_with(monkeypatch, dead_hosts={HOST, SEARCH_HOST})
    since = date(2026, 9, 5) - timedelta(days=BgerScraper.WINDOW_DAYS * 2 - 1)
    assert list(sc._discover_via_search(since)) == []
    assert len(requested) == 4                       # 2 windows x 2 hosts, no crash
