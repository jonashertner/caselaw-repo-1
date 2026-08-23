"""The demand queue must not price extraction artifacts as missing decisions.

"Urk. N" is a Zurich exhibit reference the extractor misreads as a docket;
it topped the 2026-08-22 queue at 1,816 citations. Until the extractor fix
lands (pipeline-gated), build_demand_queue filters the artifact classes:
URK_-prefixed refs and refs carrying control characters. Plain spaces stay
legal — BGE-shaped refs carry them.
"""
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.build_demand_queue import _is_extraction_artifact, build  # noqa: E402


def test_predicate_classes():
    assert _is_extraction_artifact("URK_ 2")
    assert _is_extraction_artifact("URK_ \n2")
    assert _is_extraction_artifact("urk_ 14")          # case-insensitive
    assert _is_extraction_artifact("4C_310\n_1996")    # control char = artifact
    assert not _is_extraction_artifact("4C_310_1996")
    assert not _is_extraction_artifact("BGE 131 III 115")  # spaces are legal
    assert not _is_extraction_artifact("E_3431_2021")


def test_build_excludes_artifacts_and_keeps_real_demand(tmp_path):
    db = tmp_path / "reference_graph.db"
    con = sqlite3.connect(db)
    con.executescript(
        "CREATE TABLE citation_targets (target_ref TEXT, target_decision_id TEXT);"
        "CREATE TABLE decision_citations (source_decision_id TEXT,"
        " target_ref TEXT, target_type TEXT);"
    )
    con.execute("INSERT INTO citation_targets VALUES ('BGE 100 II 1', 'bge_100_II_1')")
    rows = (
        [("zh_a%d" % i, "URK_ 2", "docket") for i in range(5)]
        + [("zh_b%d" % i, "URK_ \n2", "docket") for i in range(4)]
        + [("bger_c%d" % i, "4C_310_1996", "docket") for i in range(3)]
        + [("x", "BGE 100 II 1", "bge")]  # resolved — not demand
    )
    con.executemany("INSERT INTO decision_citations VALUES (?,?,?)", rows)
    con.commit()
    con.close()

    out = build(db, min_citations=2)
    refs = [r["target_ref"] for r in out]
    assert refs == ["4C_310_1996"]
    assert out[0]["citing_count"] == 3
