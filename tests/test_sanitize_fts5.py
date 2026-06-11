"""Critical invariant #3: every FTS5 query passes _sanitize_fts5, and its output
must be executable as an FTS5 MATCH without raising. Golden cases = the documented
historical production failures + the bread-and-butter Swiss legal query shapes that
search_botschaft / find_leading_cases now route through the sanitizer.

Until now the sanitizer (a 22k-line-file hotspot) had ZERO tests, so any refactor
could silently re-introduce the exact crashes it exists to prevent.
"""
import sqlite3

import pytest

import mcp_server


# Inputs that historically raised fts5 syntax errors when passed raw.
DOCUMENTED = [
    "Art. 64 StGB Verwahrung",            # trailing dot after Art.
    "öffentlich-rechtliche Körperschaft",  # hyphen -> "no such column" error
    "l'abus de droit",                    # French apostrophe
    'unbalanced " quote',                 # unterminated string
    "Obligationenrecht OR",               # OR as abbreviation, not operator
    "trailing OR",                        # bare trailing operator
    "Rechtsmissbrauch Art. 2 ZGB",
    "",                                    # empty
    '""',                                  # empty quoted phrase
    "NEAR(",                               # bare operator fragment
    "a AND b NOT c",                       # operator tokens mid-query
]


@pytest.fixture(scope="module")
def fts():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE VIRTUAL TABLE t USING fts5(body)")
    conn.execute(
        "INSERT INTO t(body) VALUES "
        "('Art 64 StGB Verwahrung öffentlich rechtliche OR Obligationenrecht abus droit Rechtsmissbrauch ZGB')"
    )
    conn.commit()
    yield conn
    conn.close()


@pytest.mark.parametrize("raw", DOCUMENTED)
def test_sanitized_query_is_fts5_executable(fts, raw):
    """The sanitizer's output must never raise an FTS5 syntax error. (Empty is
    allowed — callers guard on it; any non-empty output must be MATCH-safe.)"""
    safe = mcp_server._sanitize_fts5(raw)
    if safe:
        fts.execute("SELECT 1 FROM t WHERE t MATCH ? LIMIT 1", (safe,)).fetchall()


def test_sanitizer_is_load_bearing(fts):
    """Prove the raw inputs really do raise — otherwise the sanitizer (and the
    search_botschaft / find_leading_cases fixes) would be pointless."""
    raised = 0
    for raw in ["Art. 64 StGB", "öffentlich-rechtliche", "l'abus", 'x " y']:
        try:
            fts.execute("SELECT 1 FROM t WHERE t MATCH ? LIMIT 1", (raw,)).fetchall()
        except sqlite3.OperationalError:
            raised += 1
    assert raised >= 3, "raw inputs did not raise — sanitizer is not load-bearing"


def test_trailing_dot_stripped():
    assert mcp_server._sanitize_fts5("Art. 64 StGB Verwahrung") == "Art 64 StGB Verwahrung"


def test_hyphen_neutralized():
    assert "-" not in mcp_server._sanitize_fts5("öffentlich-rechtliche Körperschaft")


def test_bare_operator_tokens_quoted():
    assert mcp_server._sanitize_fts5("Obligationenrecht OR") == 'Obligationenrecht "OR"'
    assert mcp_server._sanitize_fts5("trailing OR") == 'trailing "OR"'


def test_empty_quoted_phrase_collapses():
    assert mcp_server._sanitize_fts5('""').strip() == ""
