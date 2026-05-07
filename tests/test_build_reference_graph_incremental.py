"""Tests for incremental reference-graph builder (shadow-mode v0.1).

Strategy
--------
1. Build a tiny in-memory `decisions.db` (5 rows, hand-crafted full_text
   so the regex extractors produce a known set of edges).
2. Bootstrap the graph via the canonical full builder.
3. Mutate the corpus: add 1 row, modify 1 row, delete 1 row.
4. Run incremental — assert it matches a fresh full rebuild on the same
   mutated corpus, edge for edge.

The contract being verified: incremental output is *equivalent* to a
full rebuild for any sequence of additions / modifications / deletions.
If this ever drifts, we'd be promoting a buggy graph to production —
catching it at unit-test time is the safety net before the planned
shadow-run-on-real-corpus phase.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from search_stack.build_reference_graph import build_graph  # noqa: E402
from search_stack.build_reference_graph_incremental import (  # noqa: E402
    EXTRACTOR_VERSION,
    _diff_state,
    _ensure_state_tables,
    _extractor_hash,
    _get_meta,
    build_graph_incremental,
)


# ── fixture corpus ────────────────────────────────────────────────────


def _make_decisions_db(tmp_path: Path, rows: list[dict]) -> Path:
    """Build a stub decisions.db with the columns the reference-graph
    builder consumes. Schema mirrors the production FTS5 DB at the
    column level.
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "decisions.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE decisions (
            decision_id TEXT PRIMARY KEY,
            docket_number TEXT,
            court TEXT,
            canton TEXT,
            language TEXT,
            decision_date TEXT,
            title TEXT,
            regeste TEXT,
            full_text TEXT
        )
        """
    )
    for r in rows:
        conn.execute(
            """
            INSERT INTO decisions(decision_id, docket_number, court, canton,
                                  language, decision_date, title, regeste, full_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                r["decision_id"],
                r.get("docket_number"),
                r.get("court"),
                r.get("canton"),
                r.get("language"),
                r.get("decision_date"),
                r.get("title"),
                r.get("regeste"),
                r.get("full_text"),
            ),
        )
    conn.commit()
    conn.close()
    return db_path


# 5 rows hand-crafted so we know exactly which edges should appear.
# - BGE_147_I_268 (target of cites in row 3)
# - BGE_140_III_86 (target of cites in rows 3, 4)
# - 4A_1_2024 (cites BGE 147 I 268 + Art. 41 OR)
# - 9C_5_2024 (cites BGE 140 III 86 + Art. 8 EMRK)
# - SK_2024_99 (cites Art. 19 StGB only)
BASE_ROWS = [
    {
        "decision_id": "BGE_147_I_268",
        "docket_number": "BGE 147 I 268",
        "court": "bge",
        "canton": "CH",
        "language": "de",
        "decision_date": "2021-05-01",
        "title": "Verfassungsbeschwerde",
        "regeste": "Art. 8 EMRK; Eigentumsgarantie.",
        "full_text": "Sachverhalt zum Eigentumsschutz, BGE 134 V 231 wird zitiert.",
    },
    {
        "decision_id": "BGE_140_III_86",
        "docket_number": "BGE 140 III 86",
        "court": "bge",
        "canton": "CH",
        "language": "de",
        "decision_date": "2014-02-15",
        "title": "Vertrauensschutz",
        "regeste": "Art. 41 OR; Vertrauen.",
        "full_text": "Sachverhalt zum Vertrauensschutz im Vertragsrecht.",
    },
    {
        "decision_id": "BGER_4A_1_2024",
        "docket_number": "4A_1/2024",
        "court": "bger",
        "canton": "CH",
        "language": "de",
        "decision_date": "2024-06-12",
        "title": "Schadenersatzklage",
        "regeste": "Art. 41 OR; Adäquanz.",
        "full_text": "Vgl. BGE 147 I 268 betreffend Eigentum. Art. 41 OR ist anwendbar. Auch BGE 140 III 86 wird beigezogen.",
    },
    {
        "decision_id": "BGER_9C_5_2024",
        "docket_number": "9C_5/2024",
        "court": "bger",
        "canton": "CH",
        "language": "de",
        "decision_date": "2024-08-01",
        "title": "IV-Rente",
        "regeste": "Art. 8 EMRK; Diskriminierung.",
        "full_text": "Es wird auf BGE 140 III 86 verwiesen. Art. 8 EMRK ist verletzt.",
    },
    {
        "decision_id": "BSTGER_SK_2024_99",
        "docket_number": "SK_2024_99",
        "court": "bstger",
        "canton": "CH",
        "language": "de",
        "decision_date": "2024-09-09",
        "title": "Anklage",
        "regeste": "Art. 19 StGB; Schuldfähigkeit.",
        "full_text": "Art. 19 StGB ist zentral.",
    },
]


# ── _extractor_hash ───────────────────────────────────────────────────


def test_extractor_hash_deterministic() -> None:
    h1 = _extractor_hash(BASE_ROWS[0])
    h2 = _extractor_hash(dict(BASE_ROWS[0]))
    assert h1 == h2
    assert len(h1) == 64


def test_extractor_hash_changes_for_each_field() -> None:
    base_h = _extractor_hash(BASE_ROWS[0])
    for field in (
        "decision_id",
        "docket_number",
        "court",
        "canton",
        "language",
        "decision_date",
        "title",
        "regeste",
        "full_text",
    ):
        mutated = dict(BASE_ROWS[0])
        mutated[field] = (mutated.get(field) or "x") + "_changed"
        assert _extractor_hash(mutated) != base_h, f"hash unchanged for {field}"


def test_extractor_hash_treats_none_as_empty() -> None:
    a = _extractor_hash({"decision_id": "X"})
    b = _extractor_hash({k: None for k in ("decision_id",)} | {"decision_id": "X"})
    assert a == b


# ── _diff_state ───────────────────────────────────────────────────────


def test_diff_state_classifies_new_changed_deleted(tmp_path: Path) -> None:
    decisions_db = _make_decisions_db(tmp_path, BASE_ROWS)

    # Bootstrap the graph DB so processed_decisions is seeded.
    graph_db = tmp_path / "graph.db"
    build_graph_incremental(
        decisions_db=decisions_db,
        graph_db=tmp_path / "nonexistent.db",
        output_path=graph_db,
        force_full=True,
    )

    # Mutate corpus: add row, change row, delete row.
    mutated = [dict(r) for r in BASE_ROWS]
    mutated[2]["full_text"] = mutated[2]["full_text"] + " ZUSATZ."  # change
    mutated.pop(4)  # delete BSTGER_SK_2024_99
    mutated.append(  # add new
        {
            "decision_id": "BGER_5A_99_2026",
            "docket_number": "5A_99/2026",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2026-01-01",
            "title": "Erbrecht",
            "regeste": "Art. 560 ZGB; Universalsukzession.",
            "full_text": "Vgl. BGE 140 III 86 betreffend Vertrauen.",
        }
    )
    decisions_db_v2 = _make_decisions_db(tmp_path / "v2", mutated)

    conn = sqlite3.connect(graph_db)
    new_ids, changed_ids, deleted_ids, hashes_by_id, _ = _diff_state(
        decisions_db_v2, conn
    )
    conn.close()

    assert new_ids == {"BGER_5A_99_2026"}
    assert changed_ids == {"BGER_4A_1_2024"}
    assert deleted_ids == {"BSTGER_SK_2024_99"}
    assert len(hashes_by_id) == 5  # one per row in v2


def test_diff_state_empty_processed_means_all_new(tmp_path: Path) -> None:
    decisions_db = _make_decisions_db(tmp_path, BASE_ROWS)
    graph_db = tmp_path / "graph.db"
    # Create just the schema, NO processed_decisions seeding
    conn = sqlite3.connect(graph_db)
    _ensure_state_tables(conn)
    conn.commit()

    new_ids, changed_ids, deleted_ids, hashes_by_id, _ = _diff_state(
        decisions_db, conn
    )
    conn.close()

    assert new_ids == {r["decision_id"] for r in BASE_ROWS}
    assert changed_ids == set()
    assert deleted_ids == set()


# ── end-to-end: incremental matches full rebuild ──────────────────────


def _graph_signature(graph_db: Path) -> dict:
    """Stable summary of a graph DB's content — used to compare two
    graph DBs for equivalence regardless of insertion order.
    """
    conn = sqlite3.connect(graph_db)
    conn.row_factory = sqlite3.Row
    try:
        decisions = sorted(
            tuple(r) for r in conn.execute(
                "SELECT decision_id, docket_norm, court, canton, language, decision_date FROM decisions"
            )
        )
        statutes = sorted(
            tuple(r) for r in conn.execute(
                "SELECT statute_id, law_code, article, paragraph FROM statutes"
            )
        )
        decision_statutes = sorted(
            tuple(r) for r in conn.execute(
                "SELECT decision_id, statute_id, mention_count FROM decision_statutes"
            )
        )
        decision_citations = sorted(
            tuple(r) for r in conn.execute(
                """
                SELECT source_decision_id, target_ref, target_type, mention_count, is_prior_instance
                FROM decision_citations
                """
            )
        )
        citation_targets = sorted(
            (r[0], r[1], r[2], r[3])  # source, target_ref, target_id, match_type
            for r in conn.execute(
                """
                SELECT source_decision_id, target_ref, target_decision_id, match_type
                FROM citation_targets
                """
            )
        )
        return {
            "decisions": decisions,
            "statutes": statutes,
            "decision_statutes": decision_statutes,
            "decision_citations": decision_citations,
            "citation_targets": citation_targets,
        }
    finally:
        conn.close()


def test_incremental_matches_full_rebuild_after_add_change_delete(
    tmp_path: Path,
) -> None:
    # 1. Build baseline corpus + bootstrap graph
    base_db = _make_decisions_db(tmp_path / "v1", BASE_ROWS)
    graph_db = tmp_path / "graph.db"
    build_graph_incremental(
        decisions_db=base_db,
        graph_db=tmp_path / "nonexistent.db",
        output_path=graph_db,
        force_full=True,
    )

    # 2. Mutate corpus
    mutated = [dict(r) for r in BASE_ROWS]
    mutated[2]["full_text"] = (
        # add a new docket citation so we know an edge appears
        mutated[2]["full_text"] + " Vgl. auch 4A_77/2023."
    )
    mutated.pop(4)  # delete BSTGER_SK_2024_99
    mutated.append(
        {
            "decision_id": "BGER_5A_99_2026",
            "docket_number": "5A_99/2026",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2026-01-01",
            "title": "Erbrecht",
            "regeste": "Art. 560 ZGB; Universalsukzession.",
            "full_text": "Vgl. BGE 140 III 86 betreffend Vertrauen. Art. 560 ZGB.",
        }
    )
    mutated_db = _make_decisions_db(tmp_path / "v2", mutated)

    # 3. Run incremental on top of the baseline graph
    incr_out = tmp_path / "graph_incremental.db"
    incr_stats = build_graph_incremental(
        decisions_db=mutated_db,
        graph_db=graph_db,
        output_path=incr_out,
    )
    assert incr_stats["mode"] == "incremental"
    assert incr_stats["counts"]["new"] == 1
    assert incr_stats["counts"]["changed"] == 1
    assert incr_stats["counts"]["deleted"] == 1

    # 4. Run a fresh full rebuild on the same mutated corpus
    full_out = tmp_path / "graph_full.db"
    build_graph(
        input_dir=Path("/nonexistent"),  # ignored when source_db given
        db_path=full_out,
        source_db=mutated_db,
    )

    # 5. Signatures must match exactly
    incr_sig = _graph_signature(incr_out)
    full_sig = _graph_signature(full_out)
    for table_name in incr_sig:
        assert incr_sig[table_name] == full_sig[table_name], (
            f"divergence in {table_name}: "
            f"incremental has {len(incr_sig[table_name])} rows, "
            f"full has {len(full_sig[table_name])} rows"
        )


def test_incremental_with_no_changes_is_a_no_op(tmp_path: Path) -> None:
    base_db = _make_decisions_db(tmp_path / "v1", BASE_ROWS)
    graph_db = tmp_path / "graph.db"
    build_graph_incremental(
        decisions_db=base_db,
        graph_db=tmp_path / "nonexistent.db",
        output_path=graph_db,
        force_full=True,
    )
    sig_before = _graph_signature(graph_db)

    # Run incremental with the SAME corpus
    incr_out = tmp_path / "graph_incremental.db"
    stats = build_graph_incremental(
        decisions_db=base_db,
        graph_db=graph_db,
        output_path=incr_out,
    )
    assert stats["mode"] == "incremental"
    assert stats["counts"] == {"new": 0, "changed": 0, "deleted": 0}

    sig_after = _graph_signature(incr_out)
    for table_name in sig_before:
        assert sig_before[table_name] == sig_after[table_name]


def test_default_output_is_sibling_not_in_place(tmp_path: Path) -> None:
    """Safety: passing no --output and no --in-place must NOT overwrite
    the live graph DB."""
    base_db = _make_decisions_db(tmp_path / "v1", BASE_ROWS)
    graph_db = tmp_path / "graph.db"
    build_graph_incremental(
        decisions_db=base_db,
        graph_db=tmp_path / "nonexistent.db",
        output_path=graph_db,
        force_full=True,
    )
    sig_before = _graph_signature(graph_db)
    mtime_before = graph_db.stat().st_mtime_ns

    # Run with no output_path → defaults to sibling _incremental
    stats = build_graph_incremental(
        decisions_db=base_db,
        graph_db=graph_db,
    )
    expected_sibling = graph_db.with_name(
        graph_db.stem + "_incremental" + graph_db.suffix
    )
    assert Path(stats["output_path"]).resolve() == expected_sibling.resolve()
    assert expected_sibling.exists()

    # Live DB unchanged
    assert graph_db.stat().st_mtime_ns == mtime_before
    assert _graph_signature(graph_db) == sig_before


def test_extractor_version_bump_forces_full_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_db = _make_decisions_db(tmp_path / "v1", BASE_ROWS)
    graph_db = tmp_path / "graph.db"
    build_graph_incremental(
        decisions_db=base_db,
        graph_db=tmp_path / "nonexistent.db",
        output_path=graph_db,
        force_full=True,
    )

    # Bump version → next incremental call should detect mismatch and
    # bootstrap (i.e. re-run full builder).
    import search_stack.build_reference_graph_incremental as mod

    monkeypatch.setattr(mod, "EXTRACTOR_VERSION", EXTRACTOR_VERSION + 1)

    incr_out = tmp_path / "graph_incremental.db"
    stats = build_graph_incremental(
        decisions_db=base_db,
        graph_db=graph_db,
        output_path=incr_out,
    )
    assert stats["mode"] == "full_bootstrap"
    assert "version_mismatch" in stats.get("bootstrap_reason", "")

    # And the new file should have updated meta
    conn = sqlite3.connect(incr_out)
    try:
        v = _get_meta(conn, "extractor_version")
        assert v == str(EXTRACTOR_VERSION + 1)
    finally:
        conn.close()


# ── cascade-delete cleanliness ────────────────────────────────────────


def test_cascade_delete_leaves_no_orphan_edges(tmp_path: Path) -> None:
    """When a row is deleted, *every* edge sourced from it must be
    cleared — including resolved citation_targets entries that the
    global resolver populated on a previous run."""
    base_db = _make_decisions_db(tmp_path / "v1", BASE_ROWS)
    graph_db = tmp_path / "graph.db"
    build_graph_incremental(
        decisions_db=base_db,
        graph_db=tmp_path / "nonexistent.db",
        output_path=graph_db,
        force_full=True,
    )
    # Sanity: BGER_4A_1_2024 has both statute and citation edges
    conn = sqlite3.connect(graph_db)
    try:
        n_stat = conn.execute(
            "SELECT COUNT(*) FROM decision_statutes WHERE decision_id = ?",
            ("BGER_4A_1_2024",),
        ).fetchone()[0]
        n_cit = conn.execute(
            "SELECT COUNT(*) FROM decision_citations WHERE source_decision_id = ?",
            ("BGER_4A_1_2024",),
        ).fetchone()[0]
        n_tgt = conn.execute(
            "SELECT COUNT(*) FROM citation_targets WHERE source_decision_id = ?",
            ("BGER_4A_1_2024",),
        ).fetchone()[0]
        assert n_stat > 0
        assert n_cit > 0
        assert n_tgt > 0
    finally:
        conn.close()

    # Delete that decision, run incremental
    mutated = [r for r in BASE_ROWS if r["decision_id"] != "BGER_4A_1_2024"]
    mutated_db = _make_decisions_db(tmp_path / "v2", mutated)

    incr_out = tmp_path / "graph_incremental.db"
    build_graph_incremental(
        decisions_db=mutated_db,
        graph_db=graph_db,
        output_path=incr_out,
    )

    conn = sqlite3.connect(incr_out)
    try:
        for query, table in [
            ("SELECT COUNT(*) FROM decision_statutes WHERE decision_id = ?", "decision_statutes"),
            ("SELECT COUNT(*) FROM decision_citations WHERE source_decision_id = ?", "decision_citations"),
            ("SELECT COUNT(*) FROM citation_targets WHERE source_decision_id = ?", "citation_targets"),
            ("SELECT COUNT(*) FROM processed_decisions WHERE decision_id = ?", "processed_decisions"),
            ("SELECT COUNT(*) FROM decisions WHERE decision_id = ?", "decisions"),
        ]:
            n = conn.execute(query, ("BGER_4A_1_2024",)).fetchone()[0]
            assert n == 0, f"orphan rows in {table}: {n}"
    finally:
        conn.close()


def test_deleting_a_cited_target_does_not_violate_fk(tmp_path: Path) -> None:
    """Regression for the citation_targets.target_decision_id FK bug.

    BGE 140 III 86 is cited by both BGER_4A_1_2024 and BGER_9C_5_2024 in
    BASE_ROWS. A naive incremental that deletes only source-side edges
    leaves citation_targets rows pointing at the deleted target — which
    raises ``sqlite3.IntegrityError: FOREIGN KEY constraint failed`` the
    moment we DELETE FROM decisions.
    """
    base_db = _make_decisions_db(tmp_path / "v1", BASE_ROWS)
    graph_db = tmp_path / "graph.db"
    build_graph_incremental(
        decisions_db=base_db,
        graph_db=tmp_path / "nonexistent.db",
        output_path=graph_db,
        force_full=True,
    )

    # Sanity: BGE_140_III_86 IS the resolved target of at least one row
    conn = sqlite3.connect(graph_db)
    try:
        n_as_target = conn.execute(
            "SELECT COUNT(*) FROM citation_targets WHERE target_decision_id = ?",
            ("BGE_140_III_86",),
        ).fetchone()[0]
        assert n_as_target > 0, (
            "fixture invariant: BGE 140 III 86 is supposed to be cited"
        )
    finally:
        conn.close()

    # Delete the cited target and run incremental — must succeed
    mutated = [r for r in BASE_ROWS if r["decision_id"] != "BGE_140_III_86"]
    mutated_db = _make_decisions_db(tmp_path / "v2", mutated)

    incr_out = tmp_path / "graph_incremental.db"
    stats = build_graph_incremental(
        decisions_db=mutated_db,
        graph_db=graph_db,
        output_path=incr_out,
    )
    assert stats["mode"] == "incremental"
    assert stats["counts"]["deleted"] == 1

    # And the deleted target must be gone from every table
    conn = sqlite3.connect(incr_out)
    try:
        for query, table in [
            ("SELECT COUNT(*) FROM decisions WHERE decision_id = ?", "decisions"),
            ("SELECT COUNT(*) FROM citation_targets WHERE target_decision_id = ?", "citation_targets"),
            ("SELECT COUNT(*) FROM processed_decisions WHERE decision_id = ?", "processed_decisions"),
        ]:
            n = conn.execute(query, ("BGE_140_III_86",)).fetchone()[0]
            assert n == 0, f"orphan rows in {table}: {n}"
    finally:
        conn.close()


def test_bootstrap_streams_rows_and_does_not_materialize(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression for the bootstrap OOM bug.

    The earlier code did ``rows = list(_iter_decision_rows(decisions_db))``
    which materialised every row (including ``full_text``) before seeding
    ``processed_decisions``. On the production corpus that is ~60 GB.

    We instrument ``_iter_decision_rows`` to wrap every yielded row so
    that the *first* attribute access on the row marks it as "consumed".
    A streaming consumer marks each row as consumed before requesting
    the next one, bounding ``rows-in-flight`` at 1. A ``list()`` consumer
    yields all N rows before any are consumed, so peak == N.
    """
    base_db = _make_decisions_db(tmp_path / "v1", BASE_ROWS)
    graph_db = tmp_path / "graph.db"

    import search_stack.build_reference_graph_incremental as mod

    real_iter = mod._iter_decision_rows
    counters = {"yielded": 0, "consumed": 0, "peak_in_flight": 0}

    class _ConsumeOnAccess(dict):
        __slots__ = ("_consumed",)

        def __init__(self, base: dict) -> None:
            super().__init__(base)
            self._consumed = False

        def get(self, key, default=None):  # type: ignore[override]
            if not self._consumed:
                self._consumed = True
                counters["consumed"] += 1
            return super().get(key, default)

    def _instrumented(db_path):
        for row in real_iter(db_path):
            counters["yielded"] += 1
            in_flight = counters["yielded"] - counters["consumed"]
            if in_flight > counters["peak_in_flight"]:
                counters["peak_in_flight"] = in_flight
            yield _ConsumeOnAccess(row)

    monkeypatch.setattr(mod, "_iter_decision_rows", _instrumented)

    build_graph_incremental(
        decisions_db=base_db,
        graph_db=tmp_path / "nonexistent.db",
        output_path=graph_db,
        force_full=True,
    )

    assert counters["yielded"] == len(BASE_ROWS), (
        f"expected to yield {len(BASE_ROWS)} rows, got {counters['yielded']}"
    )
    assert counters["consumed"] == len(BASE_ROWS), (
        f"expected to consume {len(BASE_ROWS)} rows, got {counters['consumed']}"
    )
    # Streaming: each row consumed before the next yields → peak == 1.
    # list(...): all rows yielded before first consumption → peak == N.
    assert counters["peak_in_flight"] <= 1, (
        f"bootstrap appears to have materialised the corpus: "
        f"peak rows-in-flight = {counters['peak_in_flight']} "
        f"(expected 1 for streaming consumer)"
    )
