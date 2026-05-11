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
    """Quote present verbatim in the cited source — no issue. The
    citation anchor (BGE 140 III 86) qualifies the quote for the
    audit; the source-pool match then clears it."""
    src = [{
        "regeste": "Der Vermieter haftet für Mängel an der Sache, soweit dies vereinbart wurde.",
        "full_text": "",
        "paragraphs": [],
    }]
    draft = (
        'BGE 140 III 86 E. 2.3: '
        '\u201eDer Vermieter haftet f\u00fcr M\u00e4ngel an der Sache, '
        'soweit dies vereinbart wurde.\u201c'
    )
    issues = m._audit_quotes(draft, src)
    assert issues == []


def test_quote_not_in_source_flagged(m):
    """Quote that's NEAR a Swiss case citation but NOT in the cited
    source must be flagged — that's the original hallucination case."""
    src = [{"regeste": "Etwas ganz anderes.", "full_text": "", "paragraphs": []}]
    # Embed a BGE citation near the quote so the audit qualifies it
    # as a verifiable-source claim. Without this, after the
    # 2026-05-11 scoping refinement, a standalone quote is left alone.
    draft = ('BGE 140 III 86 E. 2.3: '
             '\u201eDieser Satz steht so nicht im Entscheid und ist '
             'tats\u00e4chlich frei erfunden.\u201c')
    issues = m._audit_quotes(draft, src)
    assert len(issues) == 1
    assert issues[0]["category"] == "quote"
    assert issues[0]["problem"] == "quote_not_in_cited_sources"


def test_standalone_quote_without_authority_context_skipped(m):
    """After 2026-05-11: a quote that has NO nearby Swiss-case citation
    or statute reference is left alone — the writer isn't claiming a
    legal source, so the audit doesn't fire (party narrative,
    defined terms, idioms, dialogue all live here)."""
    src = []  # no cited decisions
    draft = ('Der Beklagte sagte am Verhandlungstermin: '
             '\u201eIch werde liefern, sobald die Zahlung eingeht.\u201c '
             'Diese Aussage wurde protokolliert.')
    assert m._audit_quotes(draft, src) == []


def test_short_quote_skipped(m):
    """Quotes under 60 chars (raised from 30 on 2026-05-11) are noise:
    defined legal terms ('Treuepflicht', 'guter Glaube'), article
    labels, names — not actual verbatim source quotations."""
    src = []
    draft = ('BGE 140 III 86: \u201eTreuepflicht\u201c und '
             '\u201eguter Glaube\u201c')  # short defined terms near a citation
    assert m._audit_quotes(draft, src) == []


def test_french_quotes_handled(m):
    """French «...» quotes with adjacent ATF/BGer citation behave the
    same as German „..." quotes — match against the cited source."""
    src = [{
        "regeste": "Le bailleur est responsable des d\u00e9fauts de la chose lou\u00e9e, sauf clause contraire.",
        "full_text": "", "paragraphs": [],
    }]
    draft = ('ATF 140 III 86 c. 2.3: '
             '\u00abLe bailleur est responsable des d\u00e9fauts de la chose '
             'lou\u00e9e, sauf clause contraire.\u00bb')
    assert m._audit_quotes(draft, src) == []


def test_whitespace_normalisation_in_quote_match(m):
    """Source has line breaks; draft has the same text with single
    spaces, alongside a citation anchor."""
    src = [{
        "regeste": "Der Vermieter haftet \n\n  f\u00fcr M\u00e4ngel\n  an der Sache, soweit dies vereinbart wurde.",
        "full_text": "", "paragraphs": [],
    }]
    draft = ('BGE 140 III 86 E. 2.3: '
             '\u201eDer Vermieter haftet f\u00fcr M\u00e4ngel an der Sache, '
             'soweit dies vereinbart wurde.\u201c')
    assert m._audit_quotes(draft, src) == []


def test_quote_anchored_to_statute_reference(m):
    """A quote near an Art. X LAW reference also qualifies (the
    statute audit's source pool ends up in the same path)."""
    src = [{
        "regeste": "Definierte Treuepflichten zwischen den Parteien greifen erst nach Vertragsabschluss.",
        "full_text": "", "paragraphs": [],
    }]
    draft = ('Im Anwendungsbereich von Art. 2 ZGB gilt: '
             '\u201eDefinierte Treuepflichten zwischen den Parteien greifen '
             'erst nach Vertragsabschluss.\u201c')
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
        "case": 0, "statute": 0, "quote": 0, "date": 0, "grounding": 0,
    }
    # New shape promises both rails even on the no-citation path
    assert "annotated_text" in res
    assert "linked_text" in res


def test_attest_no_case_but_unsourced_quote_flagged(m):
    """Quote near a citation anchor + not in any cited source → flagged.

    Updated 2026-05-11: previously this used a standalone quote, but
    the refined quote-audit scope now leaves standalone quotes alone
    (they could be party narrative, dialogue, defined terms — the
    writer isn't claiming a verifiable source). Add a citation
    anchor to keep the test exercising the unsourced-quote path."""
    draft = ('BGE 140 III 86 E. 2: '
             '\u201eDies ist ein erfundenes Zitat von mehr als sechzig '
             'Zeichen, das so im Urteil gar nicht vorkommt.\u201c')
    res = m._handle_attest_response(draft_text=draft)
    assert res["ok"] is False
    assert res["issues_by_category"]["quote"] == 1


def test_attest_standalone_unsourced_quote_NOT_flagged(m):
    """New 2026-05-11: a long quote with NO citation/statute anchor in
    its vicinity stays unflagged. The user didn't claim it comes from
    a Swiss legal source, so we don't audit it."""
    draft = ('Der Zeuge sagte aus: '
             '\u201eIch war an jenem Abend nicht zu Hause, sondern '
             'unterwegs im Tessin bei meiner Schwester.\u201c '
             'Diese Aussage wurde protokolliert.')
    res = m._handle_attest_response(draft_text=draft)
    assert res["issues_by_category"]["quote"] == 0


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

@pytest.mark.parametrize("draft,citation_start,expected_starts_with", [
    ("Der Vermieter haftet für Mängel an der Mietsache (BGE 140 III 86).",
     len("Der Vermieter haftet für Mängel an der Mietsache ("),
     "Der Vermieter haftet"),
    ("Erstens. Zweitens haftet der Vermieter (BGE 140 III 86).",
     len("Erstens. Zweitens haftet der Vermieter ("),
     "Zweitens haftet"),
    # See-cite — should return None
    ("Vgl. BGE 140 III 86", 5, None),
    # Too short — None
    ("Ja (BGE 140 III 86)", 4, None),
])
def test_extract_preceding_claim(m, draft, citation_start, expected_starts_with):
    out = m._extract_preceding_claim(draft, citation_start)
    if expected_starts_with is None:
        assert out is None
    else:
        assert out is not None
        assert out.startswith(expected_starts_with)


def test_grounding_audit_unavailable_without_api_key(m, monkeypatch):
    monkeypatch.setattr(m, "ANTHROPIC_API_KEY", None)
    out, meta = m._audit_grounding("draft", [], [])
    assert out == []
    assert meta["available"] is False
    assert meta.get("error") == "anthropic_api_key_missing"


def test_grounding_audit_calls_judge_with_pairs(m, monkeypatch):
    """End-to-end stub: synthesize verified citations + cited sources,
    monkeypatch the Sonnet call, confirm pairs are built and verdicts
    flow through to issues."""
    monkeypatch.setattr(m, "ANTHROPIC_API_KEY", "sk-test")
    captured: dict = {}

    def fake_judge(pairs):
        captured["pairs"] = pairs
        # Verdicts: pair 0 = grounded, pair 1 = unrelated
        return [
            {"index": 0, "supports": "yes", "confidence": 0.9, "reasoning": "matches"},
            {"index": 1, "supports": "unrelated", "confidence": 0.8,
             "reasoning": "off-topic"},
        ]

    monkeypatch.setattr(m, "_judge_grounding_batched", fake_judge)

    claim_a = "Der Vermieter haftet f\u00fcr M\u00e4ngel an der Mietsache "
    cit_a = "BGE 140 III 86"
    sep = ". "
    claim_b = "Schadenersatz nach Art. 41 OR setzt Verschulden voraus "
    cit_b = "BGer 4A_747/2012"
    draft = claim_a + cit_a + sep + claim_b + cit_b + "."
    span_a = (len(claim_a), len(claim_a) + len(cit_a))
    pre_b = len(claim_a) + len(cit_a) + len(sep)
    span_b = (pre_b + len(claim_b), pre_b + len(claim_b) + len(cit_b))
    citations = [
        {"span": span_a, "full_match": cit_a, "_status": "OK",
         "_resolved_id": "bge_BGE_140_III_86", "pinpoint": None},
        {"span": span_b, "full_match": cit_b, "_status": "OK",
         "_resolved_id": "bger_4A_747_2012", "pinpoint": None},
    ]
    sources = [
        {"decision_id": "bge_BGE_140_III_86",
         "regeste": "Der Vermieter haftet für Mängel an der Sache.",
         "full_text": "", "paragraphs": []},
        {"decision_id": "bger_4A_747_2012",
         "regeste": "Verfahrensrechtliche Grundsätze zur Beschwerde.",
         "full_text": "", "paragraphs": []},
    ]
    issues, meta = m._audit_grounding(draft, citations, sources)
    assert meta["checked"] == 2, f"expected 2 pairs, meta={meta}"
    assert meta.get("error") is None
    # Pair 1 (unrelated) → flagged; pair 0 (yes) → not flagged
    assert len(issues) == 1
    assert issues[0]["category"] == "grounding"
    assert issues[0]["supports"] == "unrelated"
    assert "Schadenersatz" in issues[0]["claim"]


def test_grounding_audit_judge_failure_is_soft(m, monkeypatch):
    """If the Sonnet API errors, the audit must NOT throw — return []
    and surface the failure in meta."""
    monkeypatch.setattr(m, "ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setattr(m, "_judge_grounding_batched", lambda pairs: None)
    claim = "Eine ausreichend lange Behauptung mit Substanz "
    cit_str = "BGE 140 III 86"
    draft_full = claim + cit_str + "."
    span = (len(claim), len(claim) + len(cit_str))
    citations = [{
        "span": span, "full_match": cit_str,
        "_status": "OK", "_resolved_id": "bge_BGE_140_III_86", "pinpoint": None,
    }]
    sources = [{"decision_id": "bge_BGE_140_III_86",
                "regeste": "x" * 80, "full_text": "", "paragraphs": []}]
    out, meta = m._audit_grounding(draft_full, citations, sources)
    assert out == []
    assert meta.get("error") == "judge_unavailable"


def test_attest_skips_grounding_when_not_requested(m):
    res = m._handle_attest_response(draft_text="Pure prose.")
    assert res["grounding_meta"]["requested"] is False
    assert "grounding" in res["issues_by_category"]


def test_canonical_id_prefix_skips_like(m):
    """get_decision_by_id and _resolve_decision_id must NOT fall through
    to LIKE %x% scan when the input looks like a canonical decision_id
    (has a known court prefix). Such inputs either hit the exact-match
    path or are fabricated; the LIKE scan costs ~2 s on the live 970k
    table and produces nothing useful."""
    for cid in (
        "bge_BGE_999_IV_999",       # fabricated BGE
        "bger_4A_99999/9999",        # fabricated BGer
        "bvger_X-1234/2099",         # fabricated BVGer
        "zh_obergericht_NONE",       # fabricated cantonal
    ):
        assert m._CANONICAL_ID_PREFIX_RE.match(cid), \
            f"prefix regex should match canonical id {cid!r}"
    # Non-canonical inputs should NOT match (so LIKE fallback runs)
    for raw in ("4A_747/2012", "ABC.123/2099", "random text"):
        assert not m._CANONICAL_ID_PREFIX_RE.match(raw), \
            f"prefix regex should NOT match raw input {raw!r}"


def test_pragma_helper_rejects_bad_identifier(m, monkeypatch):
    """_sqlite_has_column must reject anything that isn't a clean
    identifier — defence in depth against future callers."""
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (a INTEGER)")
    assert m._sqlite_has_column(conn, "t", "a") is True
    assert m._sqlite_has_column(conn, "t; DROP TABLE t", "a") is False
    assert m._sqlite_has_column(conn, "1bad", "a") is False
    assert m._sqlite_has_column(conn, "", "a") is False
    conn.close()


def test_statute_text_cache_hits(m, monkeypatch, tmp_path):
    """Second call with the same args must be served from cache (no DB
    round-trip). Verified by counting connection opens."""
    monkeypatch.setattr(m, "_statute_text_cache", {})
    calls = {"n": 0}

    class FakeRow(dict):
        def __getitem__(self, k):
            return super().__getitem__(k)

    class FakeConn:
        def execute(self, sql, params=()):
            calls["n"] += 1
            class _C:
                def fetchone(_self):
                    if "FROM laws" in sql:
                        return FakeRow({"sr_number": "220"})
                    return FakeRow({"article_num": "41", "text": "TEXT", "lang": "de"})
            return _C()
        def close(self): pass

    monkeypatch.setattr(m, "_get_statutes_conn", lambda: FakeConn())
    r1 = m._fetch_statute_text(law_code="OR", article="41")
    r2 = m._fetch_statute_text(law_code="OR", article="41")
    assert r1 == r2
    assert r1.get("sr_number") == "220"
    # Two queries on first call, ZERO on the cached second call
    assert calls["n"] == 2, f"expected 2 DB calls (uncached only), got {calls['n']}"


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
