"""Closing-audit tests for the four hallucination classes that
attest_response now defends against:

  case      — citation exists in corpus, pinpoint resolves
  statute   — Art. X LAW reference resolves in statutes.db
  quote     — "..." appears verbatim in a cited decision
  date      — "vom DD.MM.YYYY" matches the actual decision date

These tests exercise the parser/auditor helpers in isolation so they
do not depend on the full corpus DB. The case-citation existence path
is covered by the integration test that hits a live decisions.db.
"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture(scope="module")
def m():
    return importlib.import_module("mcp_server")


# ── Quote audit ────────────────────────────────────────────────────

def test_quote_in_source_passes(m):
    src = [{
        "regeste": "Der Vermieter haftet für Mängel an der Sache.",
        "full_text": "",
        "paragraphs": [],
    }]
    draft = (
        'Das Bundesgericht hielt fest: '
        '\u201eDer Vermieter haftet f\u00fcr M\u00e4ngel an der Sache.\u201c'
    )
    issues = m._audit_quotes(draft, src)
    assert issues == []


def test_quote_not_in_source_flagged(m):
    src = [{"regeste": "Etwas ganz anderes.", "full_text": "", "paragraphs": []}]
    draft = '\u201eDieser Satz steht so nicht im Entscheid und ist erfunden.\u201c'
    issues = m._audit_quotes(draft, src)
    assert len(issues) == 1
    assert issues[0]["category"] == "quote"
    assert issues[0]["problem"] == "quote_not_in_cited_sources"


def test_short_quote_skipped(m):
    """Quotes under 30 chars are noise (article labels, names) — not audited."""
    src = []
    draft = '\u201ekurz\u201c und ein anderes \u201eauch zu kurz\u201c'
    assert m._audit_quotes(draft, src) == []


def test_french_quotes_handled(m):
    src = [{
        "regeste": "Le bailleur est responsable des d\u00e9fauts de la chose lou\u00e9e.",
        "full_text": "", "paragraphs": [],
    }]
    draft = '\u00abLe bailleur est responsable des d\u00e9fauts de la chose lou\u00e9e.\u00bb'
    assert m._audit_quotes(draft, src) == []


def test_whitespace_normalisation_in_quote_match(m):
    """Source has line breaks; draft has the same text with single spaces."""
    src = [{
        "regeste": "Der Vermieter haftet \n\n  f\u00fcr M\u00e4ngel\n  an der Sache.",
        "full_text": "", "paragraphs": [],
    }]
    draft = '\u201eDer Vermieter haftet f\u00fcr M\u00e4ngel an der Sache.\u201c'
    assert m._audit_quotes(draft, src) == []


# ── Date audit ─────────────────────────────────────────────────────

def test_date_match_no_issue(m):
    cits = [{
        "span": (0, 14), "full_match": "BGer 4A_1/2024",
        "_decision_date": "2024-03-12",
    }]
    draft = "BGer 4A_1/2024 vom 12.03.2024 hielt fest"
    assert m._audit_dates(draft, cits) == []


def test_date_mismatch_flagged(m):
    cits = [{
        "span": (0, 14), "full_match": "BGer 4A_1/2024",
        "_decision_date": "2024-03-12",
    }]
    draft = "BGer 4A_1/2024 vom 15.03.2024 hielt fest"
    issues = m._audit_dates(draft, cits)
    assert len(issues) == 1
    assert issues[0]["category"] == "date"
    assert issues[0]["claimed_date"] == "2024-03-15"
    assert issues[0]["actual_date"] == "2024-03-12"


def test_no_adjacent_date_no_issue(m):
    cits = [{
        "span": (0, 14), "full_match": "BGer 4A_1/2024",
        "_decision_date": "2024-03-12",
    }]
    draft = "BGer 4A_1/2024 hielt fest, dass ..."
    assert m._audit_dates(draft, cits) == []


def test_unverified_citation_skipped_by_date_audit(m):
    """A citation without _decision_date (failed existence check) is not
    date-audited — we cannot know what the right date would have been."""
    cits = [{"span": (0, 14), "full_match": "BGer X", "_decision_date": ""}]
    draft = "BGer X vom 99.99.9999"
    assert m._audit_dates(draft, cits) == []


# ── Statute audit guard ────────────────────────────────────────────

def test_statute_audit_noop_without_db(m, monkeypatch, tmp_path):
    """When statutes.db is missing, the audit must NOT flag every Art. ref."""
    monkeypatch.setattr(m, "STATUTES_DB_PATH", tmp_path / "nope.db")
    draft = "Siehe Art. 41 OR und Art. 256 OR sowie Art. 999 ZZZ."
    assert m._audit_statutes(draft) == []


# ── Top-level handler shape ────────────────────────────────────────

def test_attest_empty_draft_clean(m):
    res = m._handle_attest_response(draft_text="Pure prose, no claims.")
    assert res["ok"] is True
    assert res["citations_found"] == 0
    assert res["issues_count"] == 0
    assert res["issues_by_category"] == {
        "case": 0, "statute": 0, "quote": 0, "date": 0,
    }
    # New shape promises both rails even on the no-citation path
    assert "annotated_text" in res
    assert "linked_text" in res


def test_attest_no_case_but_unsourced_quote_flagged(m):
    draft = '\u201eDies ist ein erfundenes Zitat von mehr als drei\u00dfig Zeichen.\u201c'
    res = m._handle_attest_response(draft_text=draft)
    assert res["ok"] is False
    assert res["issues_by_category"]["quote"] == 1


def test_attest_returns_required_keys(m):
    res = m._handle_attest_response(draft_text="Nothing to audit.")
    for key in (
        "ok", "citations_found", "citations_ok", "issues_count",
        "issues_by_category", "annotated_text", "linked_text",
        "issues", "_note",
    ):
        assert key in res, f"missing key: {key}"


# ── Quote-normalisation helper (unit) ──────────────────────────────

def test_normalise_collapses_whitespace_and_quotes(m):
    raw = '  \u00abHello\u00bb   \nworld\u2014today\u2019s  '
    out = m._normalise_for_quote_match(raw)
    # « » → "; — → -; ’ → '; whitespace collapsed; lowercased
    assert out == '"hello" world-today\'s'


# ── Regression: regex must NOT eat connector words as law slot ────

@pytest.mark.parametrize("connector", ["und", "oder", "et", "ou", "bzw"])
def test_connector_after_absatz_not_flagged_as_law(m, connector, monkeypatch, tmp_path):
    """Bug fix: regex used re.IGNORECASE on the law slot, so "Art. 18 Abs. 1 und"
    parsed as law='und'. Connector words are now case-sensitive-rejected via
    invalid-laws guard AND can't match the (uppercase-required) law slot.
    """
    # Force the audit to run by pretending statutes.db exists; the body
    # itself never calls the DB because no Art. <num> <UPPERCASE-LAW>
    # match should fire.
    fake_db = tmp_path / "fake.db"
    fake_db.touch()
    monkeypatch.setattr(m, "STATUTES_DB_PATH", fake_db)
    monkeypatch.setattr(m, "_fetch_statute_text",
                        lambda **kw: {"sr_number": "x", "text_de": "y"})
    draft = f"Art. 18 Abs. 1 {connector} Art. 32 Abs. 1 OR sind klar."
    issues = m._audit_statutes(draft)
    flagged_laws = [i.get("law_code") for i in issues]
    assert connector not in [(s or "").lower() for s in flagged_laws]


# ── Regression: pinpoint full-text fallback ────────────────────────

@pytest.mark.parametrize("body,pinpoint,expected", [
    ("Erwägungen: ... E. 2.3. Wie das BGer hielt fest", "2.3", True),
    ("siehe consid. 4.1 hierzu", "4.1", True),
    ("siehe Erw. 5.2 hierzu", "5.2", True),
    ("(E. 6) ist klar", "6", True),
    ("nichts Erwägendes hier", "2.3", False),
    ("Erwägung 99.99 wird nicht erwähnt", "99.99", False),
])
def test_pinpoint_in_text_authoritative(m, body, pinpoint, expected):
    assert m._pinpoint_in_text(body, pinpoint) is expected


# ── Regression: strict resolver must not run LIKE %x% scan ─────────

def test_strict_resolver_returns_none_on_miss(m, monkeypatch):
    """The whole point of _resolve_decision_id_strict is to return None
    fast on a miss. Confirm it doesn't fall through to LIKE."""
    class FakeRow:
        def __init__(self, val): self.val = val
        def __getitem__(self, idx): return self.val

    calls = []
    class FakeConn:
        def execute(self, sql, params=()):
            calls.append((sql, params))
            class _Cur:
                def fetchone(self_): return None
            return _Cur()
        def close(self): pass

    monkeypatch.setattr(m, "get_db", lambda: FakeConn())
    out = m._resolve_decision_id_strict("bge_BGE_999_IV_999")
    assert out is None
    # Every SQL should be exact-match — none must contain LIKE
    for sql, _ in calls:
        assert "LIKE" not in sql.upper(), f"strict resolver leaked LIKE: {sql}"
