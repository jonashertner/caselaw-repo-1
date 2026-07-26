"""bge_historical burns its whole scrape budget when a source host dies.

The DFR index links a subset of decisions as PDF-only on www.fallrecht.ch;
the rest are HTML on servat.unibe.ch. On 2026-07-26 fallrecht.ch stopped
answering on TCP entirely (verified dead from two networks, apex and www,
:80 and :443). With TIMEOUT=60 and urllib3 Retry(total=3, backoff 2/4/8),
each dead stub costs ~4 minutes, so the run hit the 7200s systemd cap after
~28 of 161 stubs and was reported FAILED — every night, having fetched
nothing.

The guard stops probing a host after UNREACHABLE_HOST_STREAK consecutive
connection failures. Deliberately in-memory only: nothing is written to
state/, so a transient outage costs one run, not a cached gap.
"""
from __future__ import annotations

import pytest
import requests

from scrapers.bge_historical import BGEHistoricalScraper


class _Boom(requests.exceptions.ConnectionError):
    """What requests raises once urllib3 has exhausted its retries."""


def _scraper():
    return BGEHistoricalScraper.__new__(BGEHistoricalScraper)


def _stub(page, host="www.fallrecht.ch"):
    return {
        "url": f"https://{host}/c103{page}.pdf",
        "docket_number": f"36_I_{page}",
        "bge_ref": f"BGE 36 I {page}",
        "volume": 36, "section": "I", "page": page, "year": 1910,
        "is_pdf": True,
    }


def test_dead_host_is_abandoned_after_the_streak(monkeypatch):
    s = _scraper()
    calls = []

    def _get(url, **kw):
        calls.append(url)
        raise _Boom("connect timeout")

    monkeypatch.setattr(s, "get", _get, raising=False)

    for page in range(1, 26):
        assert s.fetch_decision(_stub(page)) is None

    assert len(calls) == BGEHistoricalScraper.UNREACHABLE_HOST_STREAK, (
        f"probed the dead host {len(calls)} times; should stop after "
        f"{BGEHistoricalScraper.UNREACHABLE_HOST_STREAK}")


def test_a_second_host_is_unaffected(monkeypatch):
    """servat.unibe.ch stayed up throughout; it must keep being tried."""
    s = _scraper()
    calls = []

    def _get(url, **kw):
        calls.append(url)
        raise _Boom("connect timeout")

    monkeypatch.setattr(s, "get", _get, raising=False)

    for page in range(1, 11):
        s.fetch_decision(_stub(page, host="www.fallrecht.ch"))
    dead_calls = len(calls)
    for page in range(1, 11):
        s.fetch_decision(_stub(page, host="servat.unibe.ch"))

    assert len(calls) - dead_calls == BGEHistoricalScraper.UNREACHABLE_HOST_STREAK


@pytest.mark.parametrize("status", [404, 403, 429, 500, 503])
def test_an_http_response_never_counts_as_unreachable(monkeypatch, status):
    """Any status code means the host answered, so it is reachable — only a
    connection-level failure should trip the guard. The 161 PDF-only stubs
    404'd for months without any of this firing; that must stay true, and a
    run of 500s must not be misreported as an unreachable host either."""
    s = _scraper()
    calls = []

    def _get(url, **kw):
        calls.append(url)
        resp = requests.Response()
        resp.status_code = status
        raise requests.exceptions.HTTPError(response=resp)

    monkeypatch.setattr(s, "get", _get, raising=False)

    for page in range(1, 21):
        assert s.fetch_decision(_stub(page)) is None
    assert len(calls) == 20, f"HTTP {status} must never stop the run"


def test_an_http_response_resets_a_partial_streak(monkeypatch):
    """Two dead sockets then a 503 (host back, erroring) must clear the count,
    so the host is not abandoned on the next single failure."""
    s = _scraper()
    calls = []
    seq = iter([_Boom(), _Boom(), 503, _Boom(), _Boom()])

    def _get(url, **kw):
        calls.append(url)
        item = next(seq, _Boom())
        if isinstance(item, int):
            resp = requests.Response()
            resp.status_code = item
            raise requests.exceptions.HTTPError(response=resp)
        raise item

    monkeypatch.setattr(s, "get", _get, raising=False)
    for page in range(1, 6):
        s.fetch_decision(_stub(page))

    assert len(calls) == 5, "the 503 should have reset the streak"
    assert s._host_failures["www.fallrecht.ch"] == 2


def test_a_success_resets_the_streak(monkeypatch):
    """An intermittent host must not be abandoned."""
    s = _scraper()
    calls = []
    outcomes = iter([_Boom(), _Boom(), None, _Boom(), _Boom(), _Boom(), None])

    def _get(url, **kw):
        calls.append(url)
        exc = next(outcomes, _Boom())
        if exc is not None:
            raise exc
        resp = requests.Response()
        resp.status_code = 200
        resp._content = b"%PDF-1.4 short"
        return resp

    monkeypatch.setattr(s, "get", _get, raising=False)
    monkeypatch.setattr("scrapers.bge_historical._extract_pdf_text",
                        lambda b: "", raising=False)

    for page in range(1, 8):
        s.fetch_decision(_stub(page))

    # Two failures, a success (reset), then three failures trips the guard on
    # the 6th call; the 7th is skipped without a request.
    assert len(calls) == 6, f"expected the guard to trip once, got {len(calls)} calls"


def test_guard_state_is_per_instance_and_in_memory(monkeypatch):
    """The recovery property: the counter lives on the instance only, so a
    fresh run starts clean and re-probes the host."""
    def _get(url, **kw):
        raise _Boom("connect timeout")

    first = _scraper()
    monkeypatch.setattr(first, "get", _get, raising=False)
    for page in range(1, 11):
        first.fetch_decision(_stub(page))
    assert first._host_failures["www.fallrecht.ch"] >= (
        BGEHistoricalScraper.UNREACHABLE_HOST_STREAK)

    # The class default is untouched, so the next run's instance is clean.
    assert BGEHistoricalScraper._host_failures is None
    assert _scraper()._host_failures is None
