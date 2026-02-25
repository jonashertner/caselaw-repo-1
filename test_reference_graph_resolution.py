import json
import sqlite3
from pathlib import Path

from search_stack.build_reference_graph import build_graph
from search_stack.reference_extraction import (
    _extract_dockets_from_paren,
    extract_prior_instance,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    path.write_text(payload, encoding="utf-8")


def test_build_graph_resolves_docket_to_multiple_targets(tmp_path: Path):
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d_old",
            "docket_number": "ZB.2016.28",
            "court": "bs_gerichte",
            "canton": "BS",
            "language": "de",
            "decision_date": "2017-04-13",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_new",
            "docket_number": "ZB.2016.28",
            "court": "bs_appellationsgericht",
            "canton": "BS",
            "language": "de",
            "decision_date": "2018-08-23",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_source",
            "docket_number": "4A_291/2017",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2018-06-11",
            "title": "",
            "regeste": "",
            "full_text": "Vgl. ZB.2016.28.",
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    stats = build_graph(input_dir=input_dir, db_path=db_path)
    assert stats["citations_resolved"] == 1
    assert stats["citation_target_links"] == 2

    conn = sqlite3.connect(db_path)
    links = conn.execute(
        """
        SELECT source_decision_id, target_ref, target_decision_id, confidence_score
        FROM citation_targets
        ORDER BY target_decision_id
        """
    ).fetchall()
    conn.close()

    assert len(links) == 2
    assert links[0][:3] == ("d_source", "ZB_2016_28", "d_new")
    assert links[1][:3] == ("d_source", "ZB_2016_28", "d_old")
    assert 0.0 < float(links[0][3]) <= 1.0
    assert 0.0 < float(links[1][3]) <= 1.0
    assert float(links[1][3]) > float(links[0][3])


def test_build_graph_is_idempotent_across_rebuilds(tmp_path: Path):
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d1",
            "docket_number": "1A.122/2005",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2005-01-01",
            "title": "",
            "regeste": "",
            "full_text": "Art. 8 EMRK. BGE 147 I 268.",
        }
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    build_graph(input_dir=input_dir, db_path=db_path)

    conn = sqlite3.connect(db_path)
    stat_mention = conn.execute("SELECT mention_count FROM decision_statutes").fetchone()[0]
    cit_mention = conn.execute("SELECT mention_count FROM decision_citations").fetchone()[0]
    conn.close()

    assert stat_mention == 1
    assert cit_mention == 1


def test_build_graph_preserves_existing_snapshot_on_source_error(tmp_path: Path):
    db_path = tmp_path / "reference_graph.db"

    seed = sqlite3.connect(db_path)
    seed.executescript(
        """
        CREATE TABLE decisions (
            decision_id TEXT PRIMARY KEY,
            docket_number TEXT,
            docket_norm TEXT,
            court TEXT,
            canton TEXT,
            language TEXT,
            decision_date TEXT
        );
        INSERT INTO decisions(decision_id) VALUES ('seed');
        """
    )
    seed.commit()
    seed.close()

    missing_source = tmp_path / "missing.db"
    try:
        build_graph(
            input_dir=tmp_path / "unused",
            db_path=db_path,
            source_db=missing_source,
        )
    except sqlite3.OperationalError:
        pass
    else:
        raise AssertionError("expected sqlite3.OperationalError for missing source_db")

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    conn.close()
    assert count == 1


def test_citation_confidence_prefers_bger_for_bger_docket_pattern(tmp_path: Path):
    """When a docket like '4A_291/2017' matches both BGer and a cantonal court,
    the BGer target should get higher confidence via docket-pattern inference."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d_bger",
            "docket_number": "4A_291/2017",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2018-06-11",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_ge",
            "docket_number": "4A_291/2017",
            "court": "ge_gerichte",
            "canton": "GE",
            "language": "fr",
            "decision_date": "2018-06-11",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_cantonal_source",
            "docket_number": "ZB.2016.28",
            "court": "bs_gerichte",
            "canton": "BS",
            "language": "de",
            "decision_date": "2019-01-01",
            "title": "",
            "regeste": "",
            "full_text": "Vgl. 4A_291/2017.",
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    stats = build_graph(input_dir=input_dir, db_path=db_path)
    assert stats["citation_target_links"] == 2

    conn = sqlite3.connect(db_path)
    links = conn.execute(
        """
        SELECT target_decision_id, confidence_score
        FROM citation_targets
        WHERE source_decision_id = 'd_cantonal_source'
        ORDER BY confidence_score DESC
        """
    ).fetchall()
    conn.close()

    assert len(links) == 2
    # BGer should win: docket pattern "4A_*" strongly implies bger
    assert links[0][0] == "d_bger"
    assert links[1][0] == "d_ge"
    assert float(links[0][1]) > float(links[1][1])


def test_citation_confidence_prefers_bvger_for_bvger_docket_pattern(tmp_path: Path):
    """Docket 'E-5783/2024' should strongly prefer BVGer over any other court."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d_bvger",
            "docket_number": "E-5783/2024",
            "court": "bvger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2024-06-01",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_other",
            "docket_number": "E-5783/2024",
            "court": "zh_gerichte",
            "canton": "ZH",
            "language": "de",
            "decision_date": "2024-06-01",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_citing",
            "docket_number": "1A.100/2025",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2025-01-01",
            "title": "",
            "regeste": "",
            "full_text": "Gemäss E-5783/2024.",
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    build_graph(input_dir=input_dir, db_path=db_path)

    conn = sqlite3.connect(db_path)
    links = conn.execute(
        """
        SELECT target_decision_id, confidence_score
        FROM citation_targets
        WHERE source_decision_id = 'd_citing'
        ORDER BY confidence_score DESC
        """
    ).fetchall()
    conn.close()

    assert len(links) == 2
    assert links[0][0] == "d_bvger"
    assert float(links[0][1]) > float(links[1][1])


def test_infer_court_from_docket():
    from search_stack.build_reference_graph import _infer_court_from_docket

    # BGer patterns
    assert _infer_court_from_docket("6B_1234_2025") == "bger"
    assert _infer_court_from_docket("4A_291_2017") == "bger"
    assert _infer_court_from_docket("1C_100_2024") == "bger"

    # BVGer patterns
    assert _infer_court_from_docket("E_5783_2024") == "bvger"
    assert _infer_court_from_docket("D_8226_2025") == "bvger"
    assert _infer_court_from_docket("A_1234_2025") == "bvger"

    # BStGer patterns
    assert _infer_court_from_docket("SK_2025_1234") == "bstger"
    assert _infer_court_from_docket("BB_2024_100") == "bstger"

    # Cantonal / unknown — no inference
    assert _infer_court_from_docket("ZB_2016_28") is None
    assert _infer_court_from_docket("VB_2018_00411") is None
    assert _infer_court_from_docket("") is None


def test_bge_citations_resolved_to_bge_decisions(tmp_path: Path):
    """Issue 4: BGE citations (target_type='bge') should resolve to BGE decisions."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "bge_147_I_268",
            "docket_number": "147 I 268",
            "court": "bge",
            "canton": "CH",
            "language": "de",
            "decision_date": "2021-01-01",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_citing",
            "docket_number": "6B_100/2022",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2022-06-01",
            "title": "",
            "regeste": "",
            "full_text": "Gemäss BGE 147 I 268 ist dies klar.",
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    build_graph(input_dir=input_dir, db_path=db_path)

    conn = sqlite3.connect(db_path)
    # Verify BGE citation was extracted
    bge_cit = conn.execute(
        "SELECT target_ref, target_type FROM decision_citations WHERE source_decision_id = 'd_citing' AND target_type = 'bge'"
    ).fetchone()
    assert bge_cit is not None
    assert bge_cit[0] == "BGE 147 I 268"

    # Verify it was resolved
    link = conn.execute(
        "SELECT target_decision_id, match_type, confidence_score FROM citation_targets WHERE source_decision_id = 'd_citing' AND target_ref = 'BGE 147 I 268'"
    ).fetchone()
    conn.close()

    assert link is not None, "BGE citation should be resolved to a target"
    assert link[0] == "bge_147_I_268"
    assert link[1] == "bge_norm"
    assert float(link[2]) > 0.5


def test_build_graph_normalizes_whitespace_dockets(tmp_path: Path):
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "target",
            "docket_number": " VB.2018.00411 ",
            "court": "bs_gerichte",
            "canton": "BS",
            "language": "de",
            "decision_date": "2018-01-01",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "source",
            "docket_number": "X.2019.1",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2019-01-01",
            "title": "",
            "regeste": "Vgl. VB.2018.00411.",
            "full_text": "",
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    stats = build_graph(input_dir=input_dir, db_path=db_path)
    assert stats["citations_resolved"] == 1
    assert stats["citation_target_links"] == 1


# ---------------------------------------------------------------------------
# Prior instance extraction tests
# ---------------------------------------------------------------------------


def test_extract_prior_instance_german():
    text = (
        "Gegenstand\n"
        "Nichtanhandnahme; Gegenstandslosigkeit,\n"
        "Beschwerde gegen den Entscheid des Obergerichts des Kantons Aargau, "
        "Beschwerdekammer in Strafsachen, vom 13. November 2025 (SBK.2025.285).\n"
        "Erwägungen:\n"
    )
    result = extract_prior_instance(text)
    assert result == ["SBK_2025_285"]


def test_extract_prior_instance_french():
    text = (
        "Objet\n"
        "Aide sociale (condition de recevabilité),\n"
        "recours contre l'arrêt de la Cour de justice de la République "
        "et canton de Genève du 6 août 2024 (A/1168/2024 AIDSO - ATA/917/2024).\n"
        "Considérant en fait et en droit:\n"
    )
    result = extract_prior_instance(text)
    assert "A_1168_2024" in result
    assert "ATA_917_2024" in result


def test_extract_prior_instance_italian():
    text = (
        "Oggetto\n"
        "Assicurazione contro gli infortuni\n"
        "(presupposto processuale),\n"
        "ricorso contro la sentenza del Tribunale delle assicurazioni "
        "del Cantone Ticino del 31 marzo 2025 (35.2024.77).\n"
        "Visto:\n"
    )
    result = extract_prior_instance(text)
    assert result == ["35_2024_77"]


def test_extract_prior_instance_berufung():
    text = (
        "Gegenstand\n"
        "Unterhalt\n"
        "Berufung gegen das Urteil des Einzelgerichts am Bezirksgericht Horgen "
        "vom 6. Oktober 2025 (FP240022-L).\n"
        "Erwägungen:\n"
    )
    result = extract_prior_instance(text)
    assert result == ["FP240022_L"]


def test_extract_prior_instance_none_when_no_appeal():
    text = "Gegenstand\nSteuerfestsetzung.\nErwägungen:\n"
    assert extract_prior_instance(text) == []


def test_extract_prior_instance_empty_text():
    assert extract_prior_instance("") == []
    assert extract_prior_instance(None) == []


def test_extract_dockets_from_paren_comma_separated():
    """Comma-separated dockets in parenthetical should all be captured."""
    result = _extract_dockets_from_paren("A/1168/2024, ATA/917/2024")
    assert len(result) == 2
    # _normalize_docket replaces / with _
    assert "A_1168_2024" in result
    assert "ATA_917_2024" in result


def test_extract_dockets_from_paren_semicolon_separated():
    """Semicolon-separated dockets should all be captured."""
    result = _extract_dockets_from_paren("4A_648/2024; 5A_203/2025")
    assert len(result) == 2
    assert "4A_648_2024" in result
    assert "5A_203_2025" in result


def test_extract_dockets_from_paren_dash_separated():
    """Dash-separated dockets (existing behavior) still works."""
    result = _extract_dockets_from_paren("A/1168/2024 AIDSO - ATA/917/2024")
    assert len(result) == 2


def test_extract_dockets_from_paren_single():
    """Single docket still works."""
    result = _extract_dockets_from_paren("SBK.2025.285")
    assert len(result) == 1
    assert "SBK_2025_285" in result


def test_build_graph_marks_prior_instance(tmp_path: Path):
    """Prior instance docket from header is flagged in decision_citations."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d_lower",
            "docket_number": "SBK.2025.285",
            "court": "ag_obergericht",
            "canton": "AG",
            "language": "de",
            "decision_date": "2025-11-13",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_bger",
            "docket_number": "7B_1266/2025",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2026-01-21",
            "title": "",
            "regeste": "",
            "full_text": (
                "Gegenstand\n"
                "Nichtanhandnahme; Gegenstandslosigkeit,\n"
                "Beschwerde gegen den Entscheid des Obergerichts des Kantons Aargau, "
                "Beschwerdekammer in Strafsachen, vom 13. November 2025 (SBK.2025.285).\n"
                "Erwägungen:\n"
                "1. Vgl. auch 4A_291/2017.\n"
            ),
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    stats = build_graph(input_dir=input_dir, db_path=db_path)
    assert stats["prior_instance_links"] >= 1

    conn = sqlite3.connect(db_path)
    # The SBK.2025.285 citation should be flagged as prior instance
    prior = conn.execute(
        "SELECT target_ref, is_prior_instance FROM decision_citations "
        "WHERE source_decision_id = 'd_bger' AND is_prior_instance = 1"
    ).fetchall()
    assert len(prior) == 1
    assert prior[0][0] == "SBK_2025_285"

    # The 4A_291/2017 citation should NOT be flagged as prior instance
    other = conn.execute(
        "SELECT is_prior_instance FROM decision_citations "
        "WHERE source_decision_id = 'd_bger' AND target_ref = '4A_291_2017'"
    ).fetchone()
    assert other is not None
    assert other[0] == 0

    # The prior instance citation should resolve to d_lower
    resolved = conn.execute(
        "SELECT target_decision_id FROM citation_targets "
        "WHERE source_decision_id = 'd_bger' AND target_ref = 'SBK_2025_285'"
    ).fetchone()
    conn.close()
    assert resolved is not None
    assert resolved[0] == "d_lower"


def test_build_graph_prior_instance_resolves_across_chain(tmp_path: Path):
    """Full appeal chain: Bezirksgericht → Obergericht → BGer."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d_bezirk",
            "docket_number": "FV.2024.100",
            "court": "zh_gerichte",
            "canton": "ZH",
            "language": "de",
            "decision_date": "2024-06-01",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_ober",
            "docket_number": "LZ250038",
            "court": "zh_obergericht",
            "canton": "ZH",
            "language": "de",
            "decision_date": "2025-10-01",
            "title": "",
            "regeste": "",
            "full_text": (
                "Gegenstand\n"
                "Unterhalt\n"
                "Berufung gegen das Urteil des Bezirksgerichts "
                "vom 1. Juni 2024 (FV.2024.100).\n"
                "Erwägungen:\n"
            ),
        },
        {
            "decision_id": "d_bger",
            "docket_number": "5A_900/2025",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2026-02-01",
            "title": "",
            "regeste": "",
            "full_text": (
                "Gegenstand\n"
                "Unterhalt,\n"
                "Beschwerde gegen den Entscheid des Obergerichts des Kantons Zürich "
                "vom 1. Oktober 2025 (LZ250038).\n"
                "Sachverhalt:\n"
            ),
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    stats = build_graph(input_dir=input_dir, db_path=db_path)
    assert stats["prior_instance_links"] == 2

    conn = sqlite3.connect(db_path)
    # d_ober → d_bezirk
    link1 = conn.execute(
        "SELECT target_decision_id FROM citation_targets "
        "WHERE source_decision_id = 'd_ober' AND target_ref = 'FV_2024_100'"
    ).fetchone()
    assert link1 is not None
    assert link1[0] == "d_bezirk"

    # d_bger → d_ober
    link2 = conn.execute(
        "SELECT target_decision_id FROM citation_targets "
        "WHERE source_decision_id = 'd_bger' AND target_ref = 'LZ250038'"
    ).fetchone()
    conn.close()
    assert link2 is not None
    assert link2[0] == "d_ober"
