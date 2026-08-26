"""Characters that FTS5 rejects outright must never reach the engine.

Regression for 2026-08-26. `search_scholarship` logged 176 "fts5: syntax
error" in two hours of production traffic — 131 on ",", 30 on "%", 15 on the
backtick, 2 on "=". `_sanitize_fts5` stripped parens, braces, brackets, "^",
"~", "/" and "\\", but not these, so a query as ordinary as "Haftung, Vertrag"
returned a database error to the user.

`search_decisions` was insulated only because the Haiku query-parse normalises
text before the engine sees it; `search_scholarship` passes user input
straight through. That is why the errors were confined to one tool while the
defect lived in shared code (CLAUDE.md invariant #3).
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def _load_sanitizer():
    """Exec just the function — importing mcp_server.py is expensive."""
    src = (REPO / "mcp_server.py").read_text(encoding="utf-8")
    start = src.index("def _sanitize_fts5(")
    end = src.index("\ndef ", start + 10)
    ns: dict = {"re": re}
    exec(src[start:end], ns)
    return ns["_sanitize_fts5"]


@pytest.fixture(scope="module")
def sanitize():
    return _load_sanitizer()


@pytest.fixture()
def fts():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE VIRTUAL TABLE t USING fts5(body)")
    db.execute("INSERT INTO t VALUES ('Vertragsrecht Haftung Treu und Glauben Schaden')")
    yield db
    db.close()


# The full set, found by sweeping the punctuation range against FTS5.
# Only ", % ` =" appeared in the two-hour log sample, but "?" and "!" are just
# as natural in a user's query and were equally fatal.
SYNTAX_ERROR_CHARS = list(",%`=;!?&|<>@#$")


@pytest.mark.parametrize("ch", SYNTAX_ERROR_CHARS)
def test_character_does_not_reach_fts5(ch, sanitize, fts):
    query = sanitize(f"Haftung{ch} Vertrag")
    # Must not raise — that is the entire contract.
    fts.execute("SELECT count(*) FROM t WHERE t MATCH ?", (query,)).fetchone()
    assert ch not in query


@pytest.mark.parametrize("query", [
    "Haftung, Vertrag",
    "50% Invalidität",
    "Art. 41 OR = Schaden",
    "`Treu und Glauben`",
    "Rente 100%,",
    "Was gilt bei Kündigung?",
    "Haftung & Schaden",
    "Vertrag | Miete",
])
def test_real_world_queries_execute(query, sanitize, fts):
    """Shapes taken from the production error log."""
    fts.execute(
        "SELECT count(*) FROM t WHERE t MATCH ?", (sanitize(query),)
    ).fetchone()


def test_near_group_keeps_its_comma(sanitize, fts):
    """The stripping must run BEFORE NEAR groups are re-inserted.

    FTS5's own NEAR syntax contains a comma — NEAR(a b, 5). If the character
    class were applied after re-insertion it would destroy the operator it is
    supposed to protect, turning a working proximity search into a plain AND.
    """
    out = sanitize("Haftung NEAR/5 Schaden")
    assert out == "NEAR(Haftung Schaden, 5)"
    fts.execute("SELECT count(*) FROM t WHERE t MATCH ?", (out,)).fetchone()


def test_phrase_interior_is_untouched(sanitize, fts):
    """Stashed phrases are re-inserted after the strip, so they survive."""
    out = sanitize('"Treu und Glauben"')
    assert out == '"Treu und Glauben"'
    assert fts.execute(
        "SELECT count(*) FROM t WHERE t MATCH ?", (out,)
    ).fetchone()[0] == 1


def test_operators_and_prefix_still_work(sanitize, fts):
    """The new character class must not eat FTS5 syntax we rely on."""
    assert sanitize("Haftung*") == "Haftung*"          # prefix operator
    assert sanitize("OR Vertrag") == '"OR" Vertrag'    # Obligationenrecht
    assert sanitize("Vertrag AND Haftung") == "Vertrag AND Haftung"
    for q in ("Haftung*", "OR Vertrag", "Vertrag AND Haftung"):
        fts.execute("SELECT count(*) FROM t WHERE t MATCH ?", (sanitize(q),)).fetchone()


def test_query_of_only_punctuation_is_empty_not_broken(sanitize):
    """A query that reduces to nothing must return "", not a fragment."""
    assert sanitize(",,, %%% ???") == ""
