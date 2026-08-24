"""FTS3/4-style `a NEAR/5 b` translates to FTS5 `NEAR(a b, 5)` (#78).

Users porting queries from other engines type the slash form; FTS5
rejects it, and until this fix the resulting error surfaced as a silent
no-match on every search surface. The sanitizer now folds the chain into
one valid NEAR group (stash-and-restore, like phrases, because the
sanitizer's own transforms strip parens and slashes).
"""
import sqlite3

import pytest

import mcp_server


def s(q):
    return mcp_server._sanitize_fts5(q)


def test_simple_pair():
    assert s("Kündigung NEAR/5 Frist") == "NEAR(Kündigung Frist, 5)"


def test_case_insensitive_and_chain_folds_to_max_distance():
    assert s("a near/3 b NEAR/7 c") == "NEAR(a b c, 7)"


def test_quoted_phrase_operand_stays_a_phrase():
    assert s('"Treu und Glauben" NEAR/4 Verwirkung') == \
        'NEAR("Treu und Glauben" Verwirkung, 4)'


def test_punctuated_operand_becomes_inner_phrase():
    assert s("Art.5 NEAR/3 Miete") == 'NEAR("Art 5" Miete, 3)'


def test_bare_near_keeps_legacy_semantics():
    # No /N → the old operator rules apply (bare operator dropped).
    assert s("NEAR Zurich") == "Zurich"
    assert s("Bern NEAR Zurich") == "Bern NEAR Zurich"


def test_surrounding_terms_survive():
    assert s("Mietrecht Kündigung NEAR/5 Frist Zürich") == \
        "Mietrecht NEAR(Kündigung Frist, 5) Zürich"


@pytest.fixture()
def fts():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE VIRTUAL TABLE d USING fts5(t)")
    conn.execute("INSERT INTO d VALUES ('die fristlose Kündigung wahrt die Frist nicht')")
    conn.execute("INSERT INTO d VALUES ('Kündigung des Vertrags. Viele Worte später kommt die Frist zur Sprache und zwar hier')")
    return conn


def test_translated_query_is_valid_fts5_and_proximity_works(fts):
    q5 = s("Kündigung NEAR/5 Frist")
    q1 = s("Kündigung NEAR/2 Frist")
    n5 = fts.execute("SELECT COUNT(*) FROM d WHERE d MATCH ?", (q5,)).fetchone()[0]
    n1 = fts.execute("SELECT COUNT(*) FROM d WHERE d MATCH ?", (q1,)).fetchone()[0]
    assert n5 == 1          # close pair matches at distance 5
    assert n1 == 1          # and still at 2 (4 tokens apart? no — only doc 1)
    # the far pair (doc 2) matches only with a generous distance
    q20 = s("Kündigung NEAR/20 Frist")
    n20 = fts.execute("SELECT COUNT(*) FROM d WHERE d MATCH ?", (q20,)).fetchone()[0]
    assert n20 == 2


def test_old_form_would_have_errored(fts):
    with pytest.raises(sqlite3.OperationalError):
        fts.execute("SELECT COUNT(*) FROM d WHERE d MATCH ?",
                    ("Kündigung NEAR/5 Frist",)).fetchone()
