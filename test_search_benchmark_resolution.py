import sqlite3

from benchmarks.run_search_benchmark import (
    _candidate_dockets_from_relevant_id,
    _load_goldens,
    _lookup_decision_id_by_docket,
    _parse_tag_requirements,
    _resolve_relevant_id,
)


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE decisions (
            decision_id TEXT PRIMARY KEY,
            docket_number TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO decisions(decision_id, docket_number) VALUES (?, ?)",
        ("bger_8C_47_2011_local", "8C_47/2011"),
    )
    conn.execute(
        "INSERT INTO decisions(decision_id, docket_number) VALUES (?, ?)",
        ("bvger_E_7414_2015_local", "E-7414/2015"),
    )
    conn.commit()
    return conn


def test_candidate_docket_derivation_for_year_suffix():
    cands = _candidate_dockets_from_relevant_id("bger_8C_47_2011")
    assert "8C_47/2011" in cands


def test_candidate_docket_derivation_for_dash_format():
    cands = _candidate_dockets_from_relevant_id("bvger_E-7414_2015")
    assert "E-7414/2015" in cands


def test_lookup_decision_id_by_docket_normalized_match():
    conn = _make_conn()
    try:
        resolved = _lookup_decision_id_by_docket(conn, "E_7414_2015")
        assert resolved == "bvger_E_7414_2015_local"
    finally:
        conn.close()


def test_resolve_relevant_id_uses_docket_fallback():
    conn = _make_conn()
    try:
        existing = {"already_there"}
        cache: dict[str, str | None] = {}
        resolved = _resolve_relevant_id(
            conn,
            "bger_8C_47_2011",
            existing_ids=existing,
            cache=cache,
        )
        assert resolved == "bger_8C_47_2011_local"
    finally:
        conn.close()


def test_parse_tag_requirements():
    parsed = _parse_tag_requirements(["de:10", "nl:5"])
    assert parsed == {"de": 10, "nl": 5}


def test_load_goldens_deduplicates_ids(tmp_path):
    g1 = tmp_path / "g1.json"
    g2 = tmp_path / "g2.json"
    g1.write_text(
        '{"queries":[{"id":"q1","query":"a","relevant":[]}]}\n',
        encoding="utf-8",
    )
    g2.write_text(
        '{"queries":[{"id":"q1","query":"b","relevant":[]}]}\n',
        encoding="utf-8",
    )
    queries, sources = _load_goldens([g1, g2])
    ids = [q["id"] for q in queries]
    assert ids[0] == "q1"
    assert ids[1].startswith("q1__")
    assert len(sources) == 2
