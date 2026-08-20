"""Datasets derived from the corpus rather than from traffic.

The telemetry has a ceiling this traffic shape cannot lift. The corpus
does not: it is CC0, it scales with the collection instead of with
requests, and it carries no privacy dimension at all.

The valuable and delicate one is the parallel Regeste. The Federal
Supreme Court publishes the same holding in German, French and Italian,
so the alignment is human-made — but all three land in ONE `regeste`
field, and German and French share the heading word "Regeste". Telling
them apart is the whole job, and a mislabelled pair is worse than a
missing one in a translation set.

The fixture below is the real Regeste of BGE 137 III 193, abbreviated
but with its markers intact.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.build_corpus_datasets import (  # noqa: E402
    build_parallel, build_summary, detect_language, split_regeste)

# Real text, shortened. Note German and French share the "Regeste"
# heading; only Italian announces itself as "Regesto".
BGE_137_III_193 = """<br/> Regeste <br/>Regeste a
 Art. 72 Abs. 2 lit. b, Art. 74 Abs. 1 lit. b BGG; Rechtsnatur eines
Entscheides über eine Schuldneranweisung gemäss Art. 291 ZGB. Die
Schuldneranweisung stellt eine privilegierte Zwangsvollstreckungsmassnahme
sui generis dar (E. 1.1). Das Urteil ist grundsätzlich ein materielles
Endurteil und keine vorsorgliche Massnahme (E. 1.2).
Regeste a
 Art. 72 al. 2 let. b, art. 74 al. 1 let. b LTF; nature juridique d'une
décision d'avis aux débiteurs selon l'art. 291 CC. L'avis aux débiteurs
constitue une mesure d'exécution privilégiée sui generis (consid. 1.1).
Le jugement est en principe un jugement final sur le fond (consid. 1.2).
Regesto a
 Art. 72 cpv. 2 lett. b, art. 74 cpv. 1 lett. b LTF; natura giuridica di
una decisione concernente la diffida ai debitori giusta l'art. 291 CC.
La diffida costituisce una misura d'esecuzione privilegiata sui generis
(consid. 1.1). La decisione è una decisione finale di merito (consid. 1.2).
"""


def test_the_three_languages_are_separated():
    parts = split_regeste(BGE_137_III_193)
    assert set(parts) == {"de", "fr", "it"}
    assert "Zwangsvollstreckungsmassnahme" in parts["de"]
    assert "avis aux débiteurs" in parts["fr"]
    assert "diffida ai debitori" in parts["it"]


def test_no_language_leaks_into_another():
    parts = split_regeste(BGE_137_III_193)
    assert "diffida" not in parts["fr"], "Italian must not land in French"
    assert "Rechtsnatur" not in parts["fr"]
    assert "avis aux" not in parts["de"]


def test_italian_is_not_mistaken_for_french():
    """They share `consid.`, so a bare marker count calls Italian French.
    The distinctly-Italian forms have to win."""
    it = ("Art. 72 cpv. 2 lett. b LTF; natura giuridica della decisione "
          "concernente la diffida ai debitori (consid. 1.1).")
    assert detect_language(it) == "it"


def test_german_and_french_are_told_apart_without_a_heading():
    de = ("Art. 289 Abs. 2 und Art. 291 ZGB; Subrogation des Gemeinwesens "
          "in das Recht, eine Schuldneranweisung zu verlangen (E. 2 und 3).")
    fr = ("Art. 289 al. 2 et art. 291 CC; subrogation de la collectivité "
          "publique dans le droit de requérir un avis (consid. 2 et 3).")
    assert detect_language(de) == "de"
    assert detect_language(fr) == "fr"


def test_an_undecidable_block_is_dropped_not_guessed():
    """A wrong pair poisons a translation set; a missing one costs little."""
    assert detect_language("") is None
    assert detect_language("Regeste") is None
    assert detect_language("   ") is None


def test_a_single_language_regeste_is_not_parallel_text(tmp_path):
    db = _fixture_db(tmp_path, [
        ("bge_a", "bge", "de", "2011-03-16",
         "<br/> Regeste <br/>Art. 8 Abs. 2 BV; Gleichbehandlung im Sinne "
         "der Rechtsprechung, siehe dazu die Erwägungen (E. 3.1).",
         "x" * 5000),
    ])
    assert build_parallel(db) == []


def test_parallel_rows_carry_the_languages_they_hold(tmp_path):
    db = _fixture_db(tmp_path, [
        ("bge_137_III_193", "bge", "de", "2011-03-16",
         BGE_137_III_193, "x" * 9000),
    ])
    [row] = build_parallel(db)
    assert row["languages"] == ["de", "fr", "it"]
    assert row["n_languages"] == 3
    assert set(row["text"]) == {"de", "fr", "it"}


def test_the_summary_pair_uses_the_decisions_own_language(tmp_path):
    """A trilingual Regeste against a German body would otherwise teach
    summarising across languages by accident."""
    db = _fixture_db(tmp_path, [
        ("bge_137_III_193", "bge", "fr", "2011-03-16",
         BGE_137_III_193, "corps" * 2000),
    ])
    [row] = build_summary(db)
    assert "avis aux débiteurs" in row["summary"]
    assert "Zwangsvollstreckungsmassnahme" not in row["summary"]
    assert row["compression"] > 1


def test_a_docket_title_is_not_a_summary(tmp_path):
    """Not every court writes a Regeste; where none exists the field holds
    the subject line. Measured on the first full run: 94% of `bger` and
    90% of `bvger` rows were under 200 characters against 6% for `bge`.
    Pairing a title with a 30,000-char body teaches a model to emit
    titles."""
    db = _fixture_db(tmp_path, [
        ("bvger_x", "bvger", "de", "2020-08-27",
         "Asyl und Wegweisung (Mehrfachgesuch)", "x" * 30000),
    ])
    assert build_summary(db) == []


def test_a_title_repeated_either_side_of_a_pipe_is_rejected(tmp_path):
    """2,415 rows on the first run looked like 'X | X'."""
    t = "Asyl und Wegweisung (Mehrfachgesuch/Wiedererwägung)"
    db = _fixture_db(tmp_path, [
        ("bvger_y", "bvger", "de", "2020-08-27", f"{t} | {t}", "x" * 30000),
    ])
    assert build_summary(db) == []


def test_html_entities_are_decoded(tmp_path):
    """The bvger regeste arrive with &auml; intact — 38% of that court's
    rows on the first run."""
    reg = ("Art. 8 Abs. 2 BV; Wiedererw&auml;gung einer Verf&uuml;gung des "
           "SEM. Die Voraussetzungen der Wiedererw&auml;gung sind nach "
           "st&auml;ndiger Rechtsprechung eng zu fassen, siehe dazu die "
           "Erw&auml;gungen des Gerichts (E. 3.1 und E. 3.2 hiernach).")
    db = _fixture_db(tmp_path, [
        ("bvger_z", "bvger", "de", "2020-08-27", reg, "x" * 30000),
    ])
    [row] = build_summary(db)
    assert "&auml;" not in row["summary"]
    assert "Wiedererwägung" in row["summary"]


def test_a_short_body_is_not_a_summarisation_pair(tmp_path):
    db = _fixture_db(tmp_path, [
        ("bge_b", "bge", "de", "2011-01-01", BGE_137_III_193, "kurz"),
    ])
    assert build_summary(db) == []


def _fixture_db(tmp_path: Path, rows) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE decisions (decision_id TEXT, court TEXT, "
                "language TEXT, decision_date TEXT, regeste TEXT, "
                "full_text TEXT)")
    con.executemany("INSERT INTO decisions VALUES (?,?,?,?,?,?)", rows)
    return con
