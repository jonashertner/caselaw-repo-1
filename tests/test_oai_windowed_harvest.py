"""Windowed OAI harvesting: the silent-truncation countermeasure.

UNIGE's OAI ends long resumption chains with a clean "no more token" ~21k
records into ~124k — indistinguishable from completion, so the weekly
harvest "succeeded" at 1/6th coverage for months and froze the source at
datestamp ≤ 2012 (the French-scholarship gap behind GitHub #89). SONAR
adds a second failure mode: date-only from/until are rejected with HTTP
422 (not even an OAI badArgument), and migration-burst days need sub-day
window bisection, which only seconds-granularity servers can address.

These tests run a fake OAI server through the real harvest() and pin:
truncation → bisect; oversize probe → bisect without consuming; boundary
dedupe; granularity-aware window formatting; retry-through-transient; and
the atomic install contract (an aborted harvest must never clobber a
larger existing file — the old code opened the real file 'w' first).
"""
from __future__ import annotations

import io
import json
import sys
import urllib.parse
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.scholarship import oai_pmh  # noqa: E402


def _record(rid: str, title: str = "t", subject: str = "law") -> str:
    return f"""<record>
      <header><identifier>{rid}</identifier>
      <datestamp>2020-01-01T00:00:00Z</datestamp></header>
      <metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
        xmlns:dc="http://purl.org/dc/elements/1.1/">
        <dc:title>{title}</dc:title><dc:subject>{subject}</dc:subject>
        <dc:identifier>https://x/{rid}</dc:identifier>
      </oai_dc:dc></metadata></record>"""


def _page(records: list[str], token: str | None, size: int | None) -> bytes:
    attrs = f' completeListSize="{size}"' if size is not None else ""
    tok = f'<resumptionToken{attrs}>{token or ""}</resumptionToken>'
    return (f'<?xml version="1.0"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
            f'<ListRecords>{"".join(records)}{tok}</ListRecords></OAI-PMH>').encode()


def _error(code: str) -> bytes:
    return (f'<?xml version="1.0"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
            f'<error code="{code}"/></OAI-PMH>').encode()


IDENTIFY_SECONDS = (b'<?xml version="1.0"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
                    b'<Identify><granularity>YYYY-MM-DDThh:mm:ssZ</granularity></Identify></OAI-PMH>')


class FakeServer:
    """Routes harvest() requests to scripted responses; records every query."""

    def __init__(self, handler):
        self.handler = handler
        self.queries: list[dict] = []

    def __call__(self, req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else req
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        self.queries.append(q)
        body = self.handler(q)
        if isinstance(body, Exception):
            raise body
        return io.BytesIO(body)


@pytest.fixture
def serve(monkeypatch):
    def _install(handler):
        srv = FakeServer(handler)
        monkeypatch.setattr(oai_pmh.urllib.request, "urlopen", srv)
        monkeypatch.setattr(oai_pmh, "_RETRY_BACKOFF_S", (0, 0))
        monkeypatch.setattr(oai_pmh.time, "sleep", lambda s: None)
        return srv
    return _install


def _run(tmp_path, **kw):
    return oai_pmh.harvest("https://fake/oai", "src", output_dir=tmp_path,
                           subject_filter=["law"], **kw)


def test_silent_truncation_is_detected_and_bisected(serve, tmp_path):
    """A chain that ends cleanly short of completeListSize must not be
    believed — that is precisely how UNIGE lost 13 years."""
    def handler(q):
        if q.get("verb") == "Identify":
            return IDENTIFY_SECONDS
        f = q.get("from", "")
        # Full window declares 4 but serves only 2 then ends "cleanly".
        if f.startswith("2020-01-01") and q.get("until", "").startswith("2020-01-04"):
            return _page([_record("a"), _record("b")], None, 4)
        # Sub-windows serve the full truth.
        if f.startswith("2020-01-01"):
            return _page([_record("a"), _record("b")], None, 2)
        return _page([_record("c"), _record("d")], None, 2)

    serve(handler)
    s = _run(tmp_path, windowed=True,
             from_date="2020-01-01", until_date="2020-01-04")
    assert s["aborted"] is False
    assert s["total_records"] == 4                      # a, b, c, d — none lost
    assert s["windows"] >= 3                            # parent + two halves


def test_oversize_window_is_probed_not_walked(serve, tmp_path):
    """completeListSize > max_window_records → one probe request, bisect,
    and the probe page's records still arrive via the sub-windows."""
    def handler(q):
        if q.get("verb") == "Identify":
            return IDENTIFY_SECONDS
        f, u = q.get("from", ""), q.get("until", "")
        if f.startswith("2020-01-01") and u.startswith("2020-01-04"):
            return _page([_record("a")], "tok-should-not-be-followed", 100)
        if f.startswith("2020-01-01"):
            return _page([_record("a")], None, 1)
        return _page([_record("b")], None, 1)

    srv = serve(handler)
    s = _run(tmp_path, windowed=True, max_window_records=10,
             from_date="2020-01-01", until_date="2020-01-04")
    assert s["total_records"] == 2
    rows = [json.loads(l) for l in (tmp_path / "src.jsonl").open()]
    assert {r["source_record_id"] for r in rows} == {"a", "b"}
    # the oversize chain's token must never have been followed
    assert not any(q.get("resumptionToken") == "tok-should-not-be-followed"
                   for q in srv.queries)


def test_boundary_duplicates_are_deduped(serve, tmp_path):
    seen = {"n": 0}
    def handler(q):
        if q.get("verb") == "Identify":
            return IDENTIFY_SECONDS
        f, u = q.get("from", ""), q.get("until", "")
        if f.startswith("2020-01-01") and u.startswith("2020-01-04"):
            return _page([_record("dup")], None, 3)     # short → bisect
        seen["n"] += 1
        return _page([_record("dup"), _record(f"u{seen['n']}")], None, 2)

    serve(handler)
    s = _run(tmp_path, windowed=True,
             from_date="2020-01-01", until_date="2020-01-04")
    rows = [json.loads(l) for l in (tmp_path / "src.jsonl").open()]
    ids = [r["source_record_id"] for r in rows]
    assert ids.count("dup") == 1                        # written once, ever


def test_seconds_granularity_formats_windows_as_datetimes(serve, tmp_path):
    """SONAR 422s bare dates; a seconds-granularity server must receive
    THH:MM:SSZ boundaries."""
    srv_holder = {}
    def handler(q):
        if q.get("verb") == "Identify":
            return IDENTIFY_SECONDS
        return _page([_record("a")], None, 1)
    srv = serve(handler); srv_holder["s"] = srv
    _run(tmp_path, windowed=True,
         from_date="2020-01-01", until_date="2020-01-02")
    listy = [q for q in srv.queries if q.get("verb") == "ListRecords"]
    assert listy and all("T" in q.get("from", "") and q["from"].endswith("Z")
                         for q in listy if "from" in q)


def test_transient_error_is_retried_through(serve, tmp_path):
    calls = {"n": 0}
    def handler(q):
        if q.get("verb") == "Identify":
            return IDENTIFY_SECONDS
        calls["n"] += 1
        if calls["n"] == 1:
            return OSError("flaky")
        return _page([_record("a")], None, 1)
    serve(handler)
    s = _run(tmp_path, windowed=True,
             from_date="2020-01-01", until_date="2020-01-02")
    assert s["aborted"] is False and s["total_records"] == 1


def test_aborted_harvest_never_clobbers_a_larger_existing_file(serve, tmp_path):
    existing = tmp_path / "src.jsonl"
    existing.write_text('{"r":1}\n{"r":2}\n{"r":3}\n')
    def handler(q):
        if q.get("verb") == "Identify":
            return IDENTIFY_SECONDS
        return OSError("server on fire")                # every fetch fails
    serve(handler)
    s = _run(tmp_path, windowed=True,
             from_date="2020-01-01", until_date="2020-01-02")
    assert s["aborted"] is True and s["replaced"] is False
    assert existing.read_text().count("\n") == 3        # untouched
    assert (tmp_path / "src.jsonl.aborted").exists()


def test_clean_harvest_installs_atomically(serve, tmp_path):
    (tmp_path / "src.jsonl").write_text('{"old": true}\n')
    def handler(q):
        if q.get("verb") == "Identify":
            return IDENTIFY_SECONDS
        return _page([_record("new1"), _record("new2")], None, 2)
    serve(handler)
    s = _run(tmp_path, windowed=True,
             from_date="2020-01-01", until_date="2020-01-02")
    assert s["replaced"] is True
    rows = [json.loads(l) for l in (tmp_path / "src.jsonl").open()]
    assert len(rows) == 2 and not (tmp_path / "src.jsonl.tmp").exists()
