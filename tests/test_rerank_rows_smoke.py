"""`_rerank_rows` runs on every search. Nothing called it until now.

On 2026-08-22 a helper added elsewhere in the module reused the name
`_term_coverage`, which `_rerank_rows` calls five times. Python keeps the last
binding, so every reranked search raised and returned 500 for ~70 minutes. The
full suite passed 2003 tests before and after: no test had ever invoked this
function, despite it sitting on the hottest path in the product.

`tests/test_no_shadowed_definitions.py` stops that specific collision from
recurring. This file closes the wider hole — it executes the function, so any
exception inside it fails the suite regardless of cause: a renamed helper, a
changed signature, a new column that isn't in the row.

Deliberately a smoke test, not a ranking-quality test. It asserts the contract
(runs, returns dicts, honours limit/offset, preserves ids) and says nothing
about the order, because ranking quality belongs in the search benchmark where
it can be measured at production scale rather than pinned to fixtures.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

# Every column _rerank_rows reads off a candidate row.
COLUMNS = [
    "decision_id", "bm25_score", "title", "regeste", "snippet", "court",
    "canton", "chamber", "language", "decision_date", "docket_number",
    "pdf_url", "source_url",
]

CANDIDATES = [
    ("bge_BGE_131_III_115", -8.5, "Tierhalterhaftung",
     "Art. 56 Abs. 1 OR; Tierhalterhaftung. Haftungsvoraussetzungen.",
     "…Tierhalterhaftung…", "bge", None, "II", "de", "2004-10-04",
     "4C.200/2004", None, "https://example.invalid/1"),
    ("bger_4A_372_2019", -6.2, "Tierhalterhaftung | Haftpflichtrecht",
     None, "…Haftpflicht…", "bger", None, "I", "de", "2019-11-19",
     "4A_372/2019", None, "https://example.invalid/2"),
    ("bger_4A_25_2021", -5.1, "Tierhalterhaftung,",
     None, "…Kausalhaftung…", "bger", None, "I", "de", "2021-08-24",
     "4A_25/2021", None, "https://example.invalid/3"),
    ("zh_og_2020_1", -3.0, "Werkeigentümerhaftung",
     None, "…Werkeigentümer…", "zh_gerichte", "ZH", None, "de", "2020-03-02",
     "LB200011", None, "https://example.invalid/4"),
]


@pytest.fixture
def rows() -> list[sqlite3.Row]:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(f"CREATE TABLE c ({', '.join(COLUMNS)})")
    conn.executemany(
        f"INSERT INTO c VALUES ({', '.join('?' * len(COLUMNS))})", CANDIDATES)
    return conn.execute("SELECT * FROM c").fetchall()


def test_it_runs_at_all(rows):
    """The assertion that would have caught the 2026-08-22 outage: any
    exception raised inside _rerank_rows fails here."""
    out = m._rerank_rows(rows, "Tierhalterhaftung", limit=10)
    assert isinstance(out, list) and out


def test_every_result_is_a_dict_with_its_id(rows):
    out = m._rerank_rows(rows, "Tierhalterhaftung", limit=10)
    assert all(isinstance(r, dict) for r in out)
    assert {r["decision_id"] for r in out} <= {c[0] for c in CANDIDATES}


def test_limit_is_honoured(rows):
    assert len(m._rerank_rows(rows, "Tierhalterhaftung", limit=2)) == 2


def test_offset_pages_without_repeating(rows):
    first = m._rerank_rows(rows, "Tierhalterhaftung", limit=2, offset=0)
    second = m._rerank_rows(rows, "Tierhalterhaftung", limit=2, offset=2)
    assert not ({r["decision_id"] for r in first}
                & {r["decision_id"] for r in second})


def test_empty_candidates_return_empty(rows):
    assert m._rerank_rows([], "anything", limit=10) == []


@pytest.mark.parametrize("query", [
    "Tierhalterhaftung",                       # single term
    "Werkeigentümerhaftung Kausalhaftung",     # multi-term, umlaut
    "4A_372/2019",                             # docket
    "Art. 56 OR",                              # statute reference
    "BGE 131 III 115",                         # citation
    "responsabilité du détenteur d'animaux",   # French, apostrophe
    "",                                        # empty
    "   ",                                     # whitespace
])
def test_query_shapes_do_not_raise(rows, query):
    """The reranker branches hard on query shape — docket, statute, citation,
    language. Each branch must at least execute."""
    assert isinstance(m._rerank_rows(rows, query, limit=5), list)


def test_docket_query_flag_path(rows):
    assert isinstance(
        m._rerank_rows(rows, "4A_372/2019", limit=5, is_docket_query=True), list)


def test_signal_sink_is_populated(rows):
    """The sink exists so scoring weights can be tuned against evidence; if it
    silently stopped filling, that capability would rot unnoticed."""
    sink: dict = {}
    m._rerank_rows(rows, "Tierhalterhaftung", limit=10, signal_sink=sink)
    assert sink, "signal_sink stayed empty — per-signal contributions lost"
