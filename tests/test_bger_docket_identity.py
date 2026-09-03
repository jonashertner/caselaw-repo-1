"""Date-aware identity for the BGer scraper (2026-09-03: 2C_532/2025's
final judgment of 2026-07-21 was suppressed because the docket was held
under its 2025-11-18 recusal ruling). Offline. Mirrors tests/test_ne_fiche_identity.py."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from bs4 import BeautifulSoup

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from models import make_decision_id  # noqa: E402
from scrapers.bger import BgerScraper  # noqa: E402


class FakeState:
    def __init__(self, known, state_file):
        self._seen = set(known)
        self.state_file = str(state_file)

    def is_known(self, decision_id):
        return decision_id in self._seen


def scraper_with(tmp_path, known, sidecar_lines):
    sc = BgerScraper.__new__(BgerScraper)   # no network init
    sc.state = FakeState(known, tmp_path / "bger.jsonl")
    sc.evg_only = False          # __init__ knobs the parsers read
    sc.until_date = None
    sc.neuheiten_only = False
    if sidecar_lines is not None:
        (tmp_path / "bger.dates.txt").write_text(
            "\n".join(sidecar_lines) + ("\n" if sidecar_lines else ""))
    sc._load_known_dates()
    return sc


def stub(docket, d):
    return {
        "decision_id": make_decision_id("bger", docket),
        "docket_number": docket,
        "decision_date": d,
        "url": "https://search.bger.ch/x",
        "language": "de",
    }


def pair(docket, d_iso, did=None):
    base = make_decision_id("bger", docket)
    return f"{base}\t{d_iso}\t{did or base}"


HELD = {"bger_2C_532_2025"}
SIDE = [pair("2C_532/2025", "2025-11-18")]


# ── legacy mode ──────────────────────────────────────────────────────────

def test_legacy_without_sidecar_behaves_as_before(tmp_path):
    sc = scraper_with(tmp_path, HELD, None)
    assert sc._known_dates is None
    assert sc._stub_filter(stub("2C_532/2025", date(2026, 7, 21))) is None
    s = sc._stub_filter(stub("2C_1/2026", date(2026, 7, 21)))
    assert s["decision_id"] == "bger_2C_1_2026"


def test_legacy_mark_never_writes_sidecar(tmp_path):
    sc = scraper_with(tmp_path, set(), None)
    sc._mark_date("bger_2C_1_2026", "2026-07-21", "bger_2C_1_2026")
    assert not (tmp_path / "bger.dates.txt").exists()


def test_empty_sidecar_is_legacy(tmp_path):
    sc = scraper_with(tmp_path, HELD, [])
    assert sc._known_dates is None
    assert sc._stub_filter(stub("2C_532/2025", date(2026, 7, 21))) is None


def test_unreadable_sidecar_is_legacy(tmp_path):
    (tmp_path / "bger.dates.txt").mkdir()      # a directory: read_text raises
    sc = BgerScraper.__new__(BgerScraper)
    sc.state = FakeState(HELD, tmp_path / "bger.jsonl")
    sc._load_known_dates()
    assert sc._known_dates is None


def test_half_seeded_sidecar_is_refused(tmp_path):
    known = {f"bger_1C_{i}_2026" for i in range(300)} | HELD
    sc = scraper_with(tmp_path, known, SIDE)
    assert sc._known_dates is None
    assert sc._stub_filter(stub("2C_532/2025", date(2026, 7, 21))) is None


# ── date-aware mode ──────────────────────────────────────────────────────

def test_second_ruling_under_held_docket_gets_dated_id(tmp_path):
    sc = scraper_with(tmp_path, HELD, SIDE)
    s = sc._stub_filter(stub("2C_532/2025", date(2026, 7, 21)))
    assert s is not None
    assert s["decision_id"] == "bger_2C_532_2025-D20260721"
    assert s["docket_number"] == "2C_532/2025"        # citations stay real
    assert s["decision_date"] == date(2026, 7, 21)
    assert s["url"] == "https://search.bger.ch/x"


def test_exact_held_ruling_is_skipped(tmp_path):
    sc = scraper_with(tmp_path, HELD, SIDE)
    assert sc._stub_filter(stub("2C_532/2025", date(2025, 11, 18))) is None


def test_new_docket_keeps_plain_id(tmp_path):
    sc = scraper_with(tmp_path, HELD, SIDE)
    s = sc._stub_filter(stub("4A_1/2026", date(2026, 8, 1)))
    assert s["decision_id"] == "bger_4A_1_2026"


def test_same_run_siblings_route_to_dated_id(tmp_path):
    """Batch callers mark state after the run: a second ruling of a docket
    first seen THIS run must not be swallowed, and a repeat of the same
    (docket, date) must not be fetched twice."""
    sc = scraper_with(tmp_path, HELD, SIDE)
    a = sc._stub_filter(stub("4A_1/2026", date(2026, 3, 1)))
    b = sc._stub_filter(stub("4A_1/2026", date(2026, 8, 1)))
    c = sc._stub_filter(stub("4A_1/2026", date(2026, 8, 1)))
    d = sc._stub_filter(stub("4A_1/2026", date(2026, 3, 1)))
    assert a["decision_id"] == "bger_4A_1_2026"
    assert b["decision_id"] == "bger_4A_1_2026-D20260801"
    assert c is None and d is None


def test_held_docket_without_date_info_is_skipped(tmp_path):
    """A held id the seed could not date (row without decision_date) must
    fall back to the legacy skip, never to a speculative -D refetch."""
    sc = scraper_with(tmp_path, HELD | {"bger_5A_9_2020"}, SIDE)
    assert sc._stub_filter(stub("5A_9/2020", date(2021, 1, 1))) is None


def test_undated_listing_of_held_docket_is_skipped(tmp_path):
    sc = scraper_with(tmp_path, HELD, SIDE)
    assert sc._stub_filter(stub("2C_532/2025", None)) is None


def test_dated_id_already_held_is_skipped(tmp_path):
    known = HELD | {"bger_2C_532_2025-D20260721"}
    side = SIDE + [pair("2C_532/2025", "2026-07-21",
                        "bger_2C_532_2025-D20260721")]
    sc = scraper_with(tmp_path, known, side)
    assert sc._stub_filter(stub("2C_532/2025", date(2026, 7, 21))) is None


def test_pair_not_in_state_is_dropped_and_ruling_re_yields(tmp_path):
    """Crash window: the sidecar was marked at fetch time but the JSONL
    write / state mark never happened → the pair must not count."""
    side = SIDE + [pair("2C_532/2025", "2026-07-21",
                        "bger_2C_532_2025-D20260721")]
    sc = scraper_with(tmp_path, HELD, side)      # dated id NOT in state
    s = sc._stub_filter(stub("2C_532/2025", date(2026, 7, 21)))
    assert s["decision_id"] == "bger_2C_532_2025-D20260721"


def test_malformed_and_id_less_pairs_are_dropped(tmp_path):
    side = SIDE + ["bger_1C_1_2026\t2026-01-01", "garbage",
                   "bger_1C_2_2026\tnot-a-date\tbger_1C_2_2026"]
    sc = scraper_with(tmp_path, HELD | {"bger_1C_2_2026"}, side)
    assert sc._known_dates == {"bger_2C_532_2025|2025-11-18"}
    assert sc._dated_ids == {"bger_2C_532_2025"}


def test_mark_date_appends_once_and_reloads(tmp_path):
    sc = scraper_with(tmp_path, HELD, SIDE)
    sc._mark_date("bger_2C_532_2025", "2026-07-21", "bger_2C_532_2025-D20260721")
    sc._mark_date("bger_2C_532_2025", "2026-07-21", "bger_2C_532_2025-D20260721")
    lines = (tmp_path / "bger.dates.txt").read_text().splitlines()
    assert lines.count(
        "bger_2C_532_2025\t2026-07-21\tbger_2C_532_2025-D20260721") == 1
    assert "bger_2C_532_2025|2026-07-21" in sc._known_dates
    # once the durable write marked state, a reload keeps the pair
    sc2 = scraper_with(tmp_path, HELD | {"bger_2C_532_2025-D20260721"}, None)
    assert "bger_2C_532_2025|2026-07-21" in sc2._known_dates


def test_date_iso_accepts_date_and_string_only():
    f = BgerScraper._date_iso
    assert f(date(2026, 7, 21)) == "2026-07-21"
    assert f("2026-07-21T00:00:00") == "2026-07-21"
    assert f("21.07.2026") is None
    assert f(None) is None


# ── discovery + fetch plumbing ───────────────────────────────────────────

NEUHEITEN = """
<html><body><table>
  <tr><td>03.09.2026</td><td>
    <a href="/ext/eurospider/live/de/php/aza/http/index.php?highlight_docid=aza://21-07-2026-2C_532-2025&amp;lang=de&amp;type=show_document">2C_532/2025</a>
  </td><td><cite>Ökologisches Gleichgewicht</cite></td></tr>
  <tr><td>03.09.2026</td><td>
    <a href="/ext/eurospider/live/de/php/aza/http/index.php?highlight_docid=aza://18-08-2026-4A_1-2026&amp;lang=de&amp;type=show_document">4A_1/2026</a>
  </td><td><cite>Vertragsrecht</cite></td></tr>
</table></body></html>
"""


class _Resp:
    def __init__(self, text):
        self.text = text
        self.ok = True


def test_neuheiten_discovery_yields_dated_stub(tmp_path, monkeypatch):
    sc = scraper_with(tmp_path, HELD, SIDE)
    sc._get_with_pow = lambda url: _Resp(NEUHEITEN)     # 14 daily pages
    stubs = list(sc._discover_via_neuheiten())
    ids = sorted(s["decision_id"] for s in stubs)
    assert ids == ["bger_2C_532_2025-D20260721", "bger_4A_1_2026"]
    coll = next(s for s in stubs if "-D" in s["decision_id"])
    assert coll["decision_date"] == date(2026, 7, 21)
    assert coll["publication_date"] == date.today()
    assert coll["marked_for_publication"] is False


def test_neuheiten_discovery_legacy_still_skips(tmp_path):
    """Legacy mode is the old behaviour byte-for-byte: the held docket is
    skipped, and a new docket is re-yielded per daily page (run_scraper's
    written_ids makes that harmless; date-aware mode dedups via claims)."""
    sc = scraper_with(tmp_path, HELD, None)
    sc._get_with_pow = lambda url: _Resp(NEUHEITEN)
    ids = [s["decision_id"] for s in sc._discover_via_neuheiten()]
    assert set(ids) == {"bger_4A_1_2026"}
    assert len(ids) == 14


def test_parse_decision_html_uses_stub_id(tmp_path, monkeypatch):
    sc = scraper_with(tmp_path, HELD, SIDE)
    body = ("Urteil vom 21. Juli 2026 " + "Die Beschwerde wird abgewiesen. " * 20)
    monkeypatch.setattr(sc, "_extract_full_text", lambda soup: body)
    monkeypatch.setattr(sc, "_extract_metadata", lambda soup, t: {})
    st = stub("2C_532/2025", date(2026, 7, 21))
    st["decision_id"] = "bger_2C_532_2025-D20260721"
    dec = sc._parse_decision_html("<html><body>x</body></html>", st, "https://x")
    assert dec.decision_id == "bger_2C_532_2025-D20260721"
    assert dec.docket_number == "2C_532/2025"
    assert dec.decision_date == date(2026, 7, 21)
    # a stub without an id (recovery scripts) keeps the docket-keyed id
    st2 = stub("2C_9/2026", date(2026, 7, 21)); st2.pop("decision_id")
    assert sc._parse_decision_html("<html/>", st2, "").decision_id == "bger_2C_9_2026"


def test_fetch_decision_marks_sidecar_only_in_dated_mode(tmp_path, monkeypatch):
    from models import Decision
    st = stub("2C_532/2025", date(2026, 7, 21))
    st["decision_id"] = "bger_2C_532_2025-D20260721"
    dec = Decision(decision_id=st["decision_id"], court="bger", canton="CH",
                   docket_number="2C_532/2025", decision_date=date(2026, 7, 21),
                   language="fr", full_text="x" * 100, source_url="https://x")
    for mode, expect_line in ((SIDE, True), (None, False)):
        sc = scraper_with(tmp_path, HELD, mode)
        monkeypatch.setattr(sc, "_make_jump_url", lambda s: None)
        monkeypatch.setattr(sc, "_get_with_pow", lambda url: _Resp("<html>" + "x" * 600))
        monkeypatch.setattr(sc, "_parse_decision_html", lambda h, s, u: dec)
        assert sc.fetch_decision(st) is dec
        side = tmp_path / "bger.dates.txt"
        text = side.read_text() if side.exists() else ""
        assert ("bger_2C_532_2025\t2026-07-21\tbger_2C_532_2025-D20260721"
                in text) is expect_line
        if side.exists():
            side.unlink()


# ── AZA search path ──────────────────────────────────────────────────────

SEARCH = """
<div class="ranklist_content"><ol>
  <li><span><a href="/ext/eurospider/live/de/php/aza/http/index.php?highlight_docid=aza://21-07-2026-2C_532-2025&amp;lang=de">21.07.2026 2C_532/2025</a></span>
      <div><div>II. öffentlich-rechtliche Abteilung</div><div>Ökologie</div><div>Titel</div></div></li>
  <li><span><a href="/ext/eurospider/live/de/php/aza/http/index.php?highlight_docid=aza://18-11-2025-2C_532-2025&amp;lang=de">18.11.2025 2C_532/2025</a></span></li>
</ol></div>
"""


def test_search_results_apply_dated_identity(tmp_path):
    sc = scraper_with(tmp_path, HELD, SIDE)
    soup = BeautifulSoup(SEARCH, "html.parser")
    stubs = list(sc._parse_search_results(soup, "de"))
    assert [s["decision_id"] for s in stubs] == ["bger_2C_532_2025-D20260721"]
    assert stubs[0]["vkammer"] == "II. öffentlich-rechtliche Abteilung"


def test_search_results_fallback_date_never_collides(tmp_path):
    """Rows without a parsable date get the window date — a held docket
    there must stay skipped (legacy), never become a bogus -D row."""
    sc = scraper_with(tmp_path, HELD, SIDE)
    html = ('<div class="ranklist_content"><ol><li><span>'
            '<a href="/x?highlight_docid=aza://21-07-2026-2C_532-2025">2C_532/2025</a>'
            '</span></li></ol></div>')
    soup = BeautifulSoup(html, "html.parser")
    assert list(sc._parse_search_results(soup, "de", fallback_date=date(2026, 7, 21))) == []
