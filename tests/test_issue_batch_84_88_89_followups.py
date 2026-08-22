"""Follow-ups to the #81–#90 batch: the three defects left open after the
first pass, all fixed on the serving side.

* **#50 / #88** — `search_laws("Art. 60 OR")` named one article and got the
  articles that *mention* it. A provision never repeats its own number in its
  body, so it cannot win its own query; the abbreviation pre-match could not
  help because it rejects any query containing a space.
* **#84 (internal #49)** — `rule_statement` fell back to the first paragraph
  of `full_text`, which on OCR'd volumes is the previous decision's tail
  behind a printed running head. `cite()` offers that field as a verbatim
  quotable excerpt, so the citation contract passed while the words belonged
  to another case.
* **#84 (internal #48)** — BGE volumes are annual (vol. N = year N + 1874), and
  a stored `decision_date` can contradict its own volume by years. The audit
  then reported a *correct* draft date as the hallucination.
* **#89** — the stopword filter measured a no-op at production scale and stays
  off. What the reporter needed was a way to tell a thin corpus from a ranking
  failure, which is a signal, not a rewrite.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


# ── #50 / #88: an explicit article reference is a lookup, not a topic ────────

def test_reference_forms_are_recognised():
    for q, num, abbr in [
        ("Art. 60 OR", "60", "OR"),
        ("art. 190 al. 2 let. e LDIP", "190", "LDIP"),
        ("Art. 336c Abs. 1 OR", "336c", "OR"),
        ("Artikel 8 BV", "8", "BV"),
        ("articolo 41 CO", "41", "CO"),
        ("Art 41a BV", "41a", "BV"),
    ]:
        got = m._ARTICLE_REF_RE.match(q)
        assert got, f"{q!r} should parse as an article reference"
        assert (got.group("num"), got.group("abbr")) == (num, abbr)


def test_ordinal_past_sexies_survives():
    """The ordinal list must reach `decies` — truncating at `sexies` is what
    makes "Art. 322decies" resolve to Art. 322, a wrong answer rather than
    no answer."""
    got = m._ARTICLE_REF_RE.match("Art. 322decies StGB")
    assert got and got.group("num") == "322decies"


def test_topical_and_operator_queries_are_left_alone():
    for q in [
        "Verjährung unerlaubte Handlung",
        "Miete OR Pacht",
        "Kündigung AND Frist",
        "Art. 60",            # no abbreviation: nothing to resolve against
        "Hundehaltung",
    ]:
        assert m._ARTICLE_REF_RE.match(q) is None, q


def _statutes_fixture() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE laws (sr_number TEXT, abbr_de TEXT, abbr_fr TEXT,
                           abbr_it TEXT, title_de TEXT, title_fr TEXT,
                           title_it TEXT);
        CREATE TABLE articles (sr_number TEXT, article_num TEXT, lang TEXT,
                               heading TEXT, text TEXT);
        INSERT INTO laws VALUES
            ('220','OR','CO','CO','Obligationenrecht','Code des obligations',
             'Codice delle obbligazioni'),
            ('311.0','StGB','CP','CP','Strafgesetzbuch','Code pénal',
             'Codice penale');
        INSERT INTO articles VALUES
            ('220','60','de','Verjährung',
             'Der Anspruch auf Schadenersatz oder Genugtuung verjährt.'),
            ('220','601','de','Anderes', 'Ein ganz anderer Artikel.'),
            ('311.0','322d','de','Nicht gebührende Vorteile',
             'Keine nicht gebührenden Vorteile sind: ...');
        """
    )
    return conn


def test_named_article_is_resolved_not_merely_mentioned():
    conn = _statutes_fixture()
    out = m._article_reference_lookup_federal("Art. 60 OR", "de", conn=conn)
    assert len(out) == 1
    assert (out[0]["sr_number"], out[0]["article_num"]) == ("220", "60")
    assert out[0]["matched_as"] == "article_reference"


def test_article_60_does_not_resolve_to_601():
    """A LIKE-prefix fallback would answer "Art. 60" with Art. 601."""
    conn = _statutes_fixture()
    out = m._article_reference_lookup_federal("Art. 60 OR", "de", conn=conn)
    assert [r["article_num"] for r in out] == ["60"]


def test_truncated_ordinal_alias_is_reused_not_reimplemented():
    """#87's alias table is the single place that knows 322decies is stored
    as 322d; this resolver must inherit it rather than carry its own list."""
    conn = _statutes_fixture()
    out = m._article_reference_lookup_federal(
        "Art. 322decies StGB", "de", conn=conn)
    assert len(out) == 1
    assert out[0]["article_num"] == "322decies"          # relabelled
    assert out[0]["article_number_alias"]["stored_as"] == "322d"


def test_unknown_abbreviation_resolves_to_nothing():
    conn = _statutes_fixture()
    assert m._article_reference_lookup_federal(
        "Art. 60 NOTALAW", "de", conn=conn) == []


def test_absent_article_in_a_known_law_resolves_to_nothing():
    conn = _statutes_fixture()
    assert m._article_reference_lookup_federal(
        "Art. 9999 OR", "de", conn=conn) == []


# ── #84 / internal #49: page-bleed in rule_statement ─────────────────────────

BLEED = (
    "346 \nObligationenrecht. N° 49. \ndevait vraisemblablement accompagner "
    "cette valeur. Il releve an outre le defaut de concordance des "
    "declarations faites par dame Pasquier et par son frere sur le but de la "
    "visite qu'ils firent au caissier de la Banque populaire de la Gruyere, "
    "en signalant que la version de dame Pasquier etait fort sujette a "
    "caution et qu'il etait plus vraisemblable que cette visite avait pour "
    "but de tenter la realisation de certains titres."
)
REAL_DE = (
    "Das Kantonsgericht und mit ihm der Beklagte sind der Auffassung, ein "
    "Vertrag sei auch dann nichtig, wenn er auf Machenschaften beruhe, die "
    "gegen die guten Sitten verstiessen. Ob ein Vertrag gegen die guten "
    "Sitten verstoesst, ist nur anhand seines Inhaltes abzuwaegen."
)
REAL_FR = (
    "L'art. 190 al. 2 LDIP prevoit qu'une sentence peut etre attaquee "
    "lorsqu'elle est incompatible avec l'ordre public. Notion juridique "
    "indeterminee, l'ordre public est difficile a cerner et ne se prete "
    "guere a une definition passe-partout."
)


def test_page_bleed_is_detected():
    assert m._looks_like_page_bleed(BLEED, {"language": "de"}) is True


def test_real_openings_are_not_suppressed():
    assert m._looks_like_page_bleed(REAL_DE, {"language": "de"}) is False
    assert m._looks_like_page_bleed(REAL_FR, {"language": "fr"}) is False


def test_german_text_quoting_french_is_not_suppressed():
    """The guard must not fire on the very common case of a German decision
    quoting the French wording of a provision."""
    mixed = (
        "Das ergibt sich deutlich aus dem franzoesischen und dem "
        "italienischen Wortlaut des Art. 20 Abs. 1 OR: 'Le contrat est nul "
        "s'il a pour objet une chose impossible, illicite ou contraire aux "
        "moeurs', und ist deshalb nach der Rechtsprechung nicht zu "
        "beanstanden, wie das Bundesgericht wiederholt festgehalten hat."
    )
    assert m._looks_like_page_bleed(mixed, {"language": "de"}) is False


def test_short_openings_are_not_judged():
    assert m._looks_like_page_bleed(
        "A.- Der Beklagte hat den Vertrag unterzeichnet.",
        {"language": "de"}) is False


def test_rule_statement_prefers_regeste_over_bleeding_full_text():
    decision = {"language": "de", "regeste": "Regeste: Sittenwidrigkeit.",
                "full_text": BLEED}
    assert m._rule_statement(decision) == "Regeste: Sittenwidrigkeit."


def test_rule_statement_returns_nothing_rather_than_another_case():
    decision = {"language": "de", "regeste": None, "full_text": BLEED}
    assert m._rule_statement(decision) is None


def test_regeste_is_never_language_screened():
    """BGE Regesten are legitimately trilingual — screening them would strip
    the field from every BGE."""
    trilingual = (
        "Regeste Internationale Schiedsgerichtsbarkeit; Ordre public. "
        "Regeste Arbitrage international; ordre public; droit de la "
        "concurrence. Notion d'ordre public au sens de l'art. 190 al. 2 "
        "let. e LDIP. Regesto Arbitrato internazionale; ordine pubblico; "
        "le norme del diritto della concorrenza non rientrano fra i valori."
    )
    decision = {"language": "de", "regeste": trilingual, "full_text": ""}
    assert m._rule_statement(decision) is not None


# ── #84 / internal #48: BGE volume-year sanity ───────────────────────────────

def test_volume_year_mismatch_is_flagged():
    out = m._bge_volume_year_mismatch("BGE 76 II 346", "1937-12-30")
    assert out and out["expected_year"] == 1950 and out["volume"] == 76


def test_consistent_volume_year_is_silent():
    # vol. 119 → 1993, vol. 132 → 2006, vol. 84 → 1958, vol. 150 → 2024
    for cit, iso in [("BGE 119 II 380", "1993-09-02"),
                     ("BGE 132 III 389", "2006-03-08"),
                     ("BGE 84 II 13", "1958-01-21"),
                     ("BGE 150 III 1", "2024-02-02")]:
        assert m._bge_volume_year_mismatch(cit, iso) is None, cit


def test_year_end_tolerance():
    """A decision taken late in one year prints in the next volume."""
    assert m._bge_volume_year_mismatch("BGE 140 III 86", "2013-12-19") is None


def test_lettered_parts_are_checked():
    """BGE parts Ia/Ib ran roughly 1975-1994. A bare [IVX]+ part matcher skips
    twenty years of the collection without failing visibly."""
    assert m._bge_volume_year_mismatch("BGE 105 Ia 349", "1962-04-11")
    assert m._bge_volume_year_mismatch("BGE 120 Ib 224", "1971-06-02")
    # …and stays silent when those volumes check out (105 → 1979, 120 → 1994).
    assert m._bge_volume_year_mismatch("BGE 105 Ia 349", "1979-10-03") is None
    assert m._bge_volume_year_mismatch("BGE 120 Ib 224", "1994-05-18") is None


def test_french_and_italian_forms_are_checked_too():
    assert m._bge_volume_year_mismatch("ATF 76 II 346", "1937-12-30")
    assert m._bge_volume_year_mismatch("DTF 76 II 346", "1937-12-30")


def test_non_bge_references_are_not_checked():
    assert m._bge_volume_year_mismatch("BGer 4A_231/2014", "2014-09-23") is None
    assert m._bge_volume_year_mismatch("", "2014-09-23") is None
    assert m._bge_volume_year_mismatch("BGE 76 II 346", None) is None


def _cit(full_match, draft, iso):
    start = draft.index(full_match)
    return [{"full_match": full_match, "span": (start, start + len(full_match)),
             "_decision_date": iso}]


def test_audit_blames_the_corpus_not_the_draft_when_the_volume_agrees():
    """A draft dating BGE 76 II 346 to 1950 is right and the stored 1937 is
    wrong. Reporting that as the draft's error is how a correct citation gets
    called a hallucination."""
    draft = "So hielt das Bundesgericht fest (BGE 76 II 346 vom 12. Mai 1950)."
    issues = m._audit_dates(draft, _cit("BGE 76 II 346", draft, "1937-12-30"))
    assert len(issues) == 1
    assert issues[0]["problem"] == "stored_date_suspect"
    assert issues[0]["volume_year"] == 1950
    assert "STORED date is the unreliable one" in issues[0]["suggestion"]


def test_a_genuinely_wrong_draft_date_is_still_the_drafts_error():
    """The softening must not swallow real date hallucinations: a draft year
    that matches neither the record nor the volume stays an error."""
    draft = "So hielt das Bundesgericht fest (BGE 76 II 346 vom 12. Mai 1993)."
    issues = m._audit_dates(draft, _cit("BGE 76 II 346", draft, "1937-12-30"))
    assert len(issues) == 1
    assert issues[0]["problem"] == "date_does_not_match_decision"


def test_sound_records_keep_the_ordinary_mismatch_message():
    draft = "Vgl. (BGE 119 II 380 vom 2. September 1999)."
    issues = m._audit_dates(draft, _cit("BGE 119 II 380", draft, "1993-09-02"))
    assert len(issues) == 1
    assert issues[0]["problem"] == "date_does_not_match_decision"


# ── #89: a weak-match signal, not a query rewrite ────────────────────────────

def test_reported_query_is_reported_as_weak():
    """The reported symptom: an introduction to law for schoolchildren as the
    top hit for a corruption/nullity query."""
    matched, total, missing = m._term_coverage(
        "corruption pot-de-vin nullité du contrat droit suisse",
        "Le droit pour les lycéens — Müller, Christoph",
    )
    assert total >= 4
    assert matched / total < 0.5
    assert "corruption" in missing


def test_a_genuine_hit_is_not_flagged_weak():
    matched, total, _ = m._term_coverage(
        "corruption nullité contrat",
        "La corruption et la nullité du contrat en droit suisse",
    )
    assert matched == total


def test_coverage_is_accent_and_inflection_tolerant():
    """'nullité' must match 'nullite', and 'corruption' must match
    'corruptions' — the exact-match stopword bug's sibling."""
    matched, total, _ = m._term_coverage(
        "nullité corruption", "Les corruptions et la nullite du contrat")
    assert (matched, total) == (2, 2)


def test_empty_query_is_not_a_weak_match():
    assert m._term_coverage("", "anything") == (0, 0, [])
