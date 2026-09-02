"""Per-fiche identity for the NE scrapers (2026-09-02 gap forensics,
hardened per the same-day 10-agent review). Offline."""
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scrapers.cantonal.ne_gerichte import NEGerichteScraper  # noqa: E402
from models import make_decision_id  # noqa: E402


class FakeState:
    def __init__(self, known, state_file):
        self._seen = set(known)
        self.state_file = str(state_file)

    def is_known(self, decision_id):
        return decision_id in self._seen


def scraper_with(tmp_path, known, sidecar_lines):
    sc = NEGerichteScraper.__new__(NEGerichteScraper)  # no network init
    sc.state = FakeState(known, tmp_path / "ne_gerichte.jsonl")
    if sidecar_lines is not None:
        (tmp_path / "ne_gerichte.nf30.txt").write_text(
            "\n".join(sidecar_lines) + ("\n" if sidecar_lines else ""))
    sc._load_known_nf30()
    return sc


def stub(docket, nf30, date="2023-05-01"):
    return {
        "decision_id": make_decision_id("ne_gerichte", docket),
        "docket_number": docket,
        "nf30_key": nf30,
        "decision_date": date,
    }


def pair(nf30, docket):
    return f"{nf30}\t{make_decision_id('ne_gerichte', docket)}"


def test_legacy_mode_without_sidecar_behaves_as_before(tmp_path):
    sc = scraper_with(tmp_path, {"ne_gerichte_CDP.2023.197"}, None)
    assert sc._known_nf30 is None
    assert sc._stub_filter(stub("CDP.2023.197", "6011")) is None
    s = sc._stub_filter(stub("CDP.2024.1", "7000"))
    assert s["decision_id"] == "ne_gerichte_CDP.2024.1"


def test_legacy_mode_mark_never_writes_sidecar(tmp_path):
    """Review F0 (critical): a half-seeded sidecar arms a refetch storm."""
    sc = scraper_with(tmp_path, set(), None)
    sc._mark_nf30("42", "ne_gerichte_X.1.1")
    assert not (tmp_path / "ne_gerichte.nf30.txt").exists()


def test_empty_sidecar_is_legacy(tmp_path):
    sc = scraper_with(tmp_path, {"ne_gerichte_A.1.1"}, [])
    assert sc._known_nf30 is None


def test_known_fiche_skipped(tmp_path):
    sc = scraper_with(tmp_path, {"ne_gerichte_CDP.2023.197"},
                      [pair("6011", "CDP.2023.197")])
    assert sc._stub_filter(stub("CDP.2023.197", "6011")) is None


def test_crash_window_pair_is_dropped_and_fiche_retries(tmp_path):
    """Pair whose decision_id never reached state (crash between fetch
    and durable write) must not count as held."""
    sc = scraper_with(tmp_path, {"ne_gerichte_CDP.2023.197"},
                      [pair("5000", "CDP.2023.197"),
                       pair("6011", "CDP.2023.197-F6011")])  # -F row NOT in state
    assert "5000" in sc._known_nf30
    assert "6011" not in sc._known_nf30
    s = sc._stub_filter(stub("CDP.2023.197", "6011"))
    assert s["decision_id"] == "ne_gerichte_CDP.2023.197-F6011"


def test_new_docket_yields_even_if_nf30_listed(tmp_path):
    """Review F1: plain-id check comes first — a stale pair must never
    suppress a new docket's only ruling."""
    sc = scraper_with(tmp_path, set(), [pair("7100", "CDP.2024.5")])
    s = sc._stub_filter(stub("CDP.2024.5", "7100"))
    assert s is not None and s["decision_id"] == "ne_gerichte_CDP.2024.5"


def test_collision_fiche_gets_suffixed_id_and_real_docket(tmp_path):
    sc = scraper_with(tmp_path, {"ne_gerichte_CDP.2023.197"},
                      [pair("5000", "CDP.2023.197")])
    s = sc._stub_filter(stub("CDP.2023.197", "6011"))
    assert s["decision_id"] == "ne_gerichte_CDP.2023.197-F6011"
    assert s["docket_number"] == "CDP.2023.197"


def test_dateless_collision_fiche_skipped(tmp_path):
    """Review F5: same-canonical dedup would silently drop it later."""
    sc = scraper_with(tmp_path, {"ne_gerichte_CDP.2023.197"},
                      [pair("5000", "CDP.2023.197")])
    assert sc._stub_filter(stub("CDP.2023.197", "6011", date=None)) is None


def test_half_seeded_sidecar_falls_back_to_legacy(tmp_path):
    """Review F4 defence: sidecar << state means partial seed."""
    known = {f"ne_gerichte_X.{i}.1" for i in range(300)}
    sc = scraper_with(tmp_path, known, [pair("1", "X.0.1")])
    assert sc._known_nf30 is None


def test_mark_appends_pair_and_dedups(tmp_path):
    sc = scraper_with(tmp_path, {"ne_gerichte_A.1.1"}, [pair("1", "A.1.1")])
    sc._mark_nf30("42", "ne_gerichte_B.2.2")
    sc._mark_nf30("42", "ne_gerichte_B.2.2")
    text = (tmp_path / "ne_gerichte.nf30.txt").read_text()
    assert text.count("42\tne_gerichte_B.2.2") == 1
    assert "42" in sc._known_nf30


def test_suffixed_known_not_reyielded(tmp_path):
    sc = scraper_with(
        tmp_path,
        {"ne_gerichte_CDP.2023.197", "ne_gerichte_CDP.2023.197-F6011"},
        [pair("5000", "CDP.2023.197")])
    assert sc._stub_filter(stub("CDP.2023.197", "6011")) is None


def test_pair_without_decision_id_is_dropped(tmp_path):
    """An id-less pair must not count as held (validation bypass)."""
    sc = scraper_with(tmp_path, {"ne_gerichte_CDP.2023.197"},
                      [pair("5000", "CDP.2023.197"), "6011\t", "7022"])
    assert "6011" not in sc._known_nf30
    assert "7022" not in sc._known_nf30


def test_same_batch_sibling_takes_suffix_path(tmp_path):
    """Batch callers mark state only at run end: the second fiche of a
    new docket in one discovery run must not reuse the plain id."""
    sc = scraper_with(tmp_path, set(), [pair("1", "A.1.1")])
    s1 = sc._stub_filter(stub("CDP.2026.9", "9001"))
    assert s1["decision_id"] == "ne_gerichte_CDP.2026.9"
    s2 = sc._stub_filter(stub("CDP.2026.9", "9002"))
    assert s2 is not None
    assert s2["decision_id"] == "ne_gerichte_CDP.2026.9-F9002"
