"""Canonical identity: the cases production handed us as ground truth.

Every fixture here is a real pattern measured on 2026-08-18, not an
invented example.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from versioning.identity import (  # noqa: E402
    appeal_metadata, bge_channel, channels, crossref_channel,
    display_representation, docket_channel, entity_view, fed_channel,
    group, representation_role, url_channel,
)


# ── the Luzern twins: same EnId, different docket, different id ──────────
# 3,932 judgments stored twice this way. No string rule over dockets can
# join them; only the court's own URL id can.

LU_DIRECT = {"decision_id": "lu_gerichte_10", "court": "lu_gerichte",
             "docket_number": "10", "decision_date": "1999-05-04",
             "source_url": "https://gerichte.lu.ch/recht_sprechung/lgve/Ajax?EnId=10"}
LU_ES = {"decision_id": "lu_gerichte_22_99_123", "court": "lu_gerichte",
         "docket_number": "22 99 123", "decision_date": "1999-05-04",
         "source_url": "https://gerichte.lu.ch/recht_sprechung/lgve/Ajax?EnId=10"}


def test_lu_twins_group_via_url():
    g = group([LU_DIRECT, LU_ES])
    assert g["lu_gerichte_10"] == g["lu_gerichte_22_99_123"]


def test_lu_twins_do_not_share_a_docket_channel():
    """Documents why the stored canonical_key cannot work here."""
    assert docket_channel("lu_gerichte", "10") != \
        docket_channel("lu_gerichte", "22 99 123")


# ── BGE dual identifiers (found by the P2 scan) ──────────────────────────

def test_bge_dual_format_groups():
    a = {"decision_id": "bge_BGE_151_I_137", "court": "bge",
         "docket_number": "BGE 151 I 137", "source_url": None}
    b = {"decision_id": "bge_151 I 137", "court": "bge",
         "docket_number": "151 I 137", "source_url": None}
    g = group([a, b])
    assert g["bge_BGE_151_I_137"] == g["bge_151 I 137"]


def test_bge_ia_ib_family_collapses():
    assert bge_channel("BGE 120 Ia 45") == bge_channel("120 I 45")


def test_bge_prefixes_equivalent():
    for v in ("BGE 125 III 231", "ATF 125 III 231", "DTF 125 III 231",
              "CH_BGE_125_III_231", "125 III 231"):
        assert bge_channel(v) == "bge:125|III|231", v


# ── federal dockets: the 66,456 space rows (#76) and the EVG era ─────────

def test_space_and_underscore_dockets_are_one_judgment():
    assert fed_channel("6B 267/2012") == fed_channel("6B_267/2012")


def test_dot_era_and_two_digit_years():
    assert fed_channel("4C.355/2004") == "fed:4C_355/2004"
    assert fed_channel("U 49/98") == "fed:U_49/1998"
    assert fed_channel("C 1/07") == "fed:C_1/07".replace("/07", "/2007")


def test_federal_channel_only_for_federal_courts():
    rec = {"decision_id": "x", "court": "bl_gerichte",
           "docket_number": "410 2024 329", "source_url": None}
    assert not any(c.startswith("fed:") for c in channels(rec))


# ── the date must NOT split an entity (the P1 Wave-1 blocker) ────────────

def test_corrected_date_does_not_split():
    a = {"decision_id": "x_1", "court": "zh_obergericht",
         "docket_number": "LB230012", "decision_date": "2024-03-15",
         "source_url": None}
    b = {"decision_id": "x_2", "court": "zh_obergericht",
         "docket_number": "LB230012", "decision_date": "2024-03-18",
         "source_url": None}
    g = group([a, b])
    assert g["x_1"] == g["x_2"]


# ── unrelated decisions must NOT merge ───────────────────────────────────

def test_different_judgments_stay_apart():
    a = {"decision_id": "a", "court": "zh_obergericht",
         "docket_number": "LB230012", "source_url": None}
    b = {"decision_id": "b", "court": "zh_obergericht",
         "docket_number": "LB230013", "source_url": None}
    assert group([a, b])["a"] != group([a, b])["b"]


def test_same_docket_different_courts_stay_apart():
    a = {"decision_id": "a", "court": "zh_obergericht",
         "docket_number": "PBG 1", "source_url": None}
    b = {"decision_id": "b", "court": "be_verwaltungsgericht",
         "docket_number": "PBG 1", "source_url": None}
    assert group([a, b])["a"] != group([a, b])["b"]


def test_short_dockets_do_not_create_a_channel():
    assert docket_channel("x_court", "1") is None


# ── Geneva/Vaud/Schaffhausen: two representations, one decision ──────────
# The judgment copy (ACJC/…) is the frozen text; the case-number copy
# (A/…) is the living portal page that later gains the appeal. Both carry
# the SAME source_url. Neither is deleted.

JUDGMENT = {"decision_id": "ge_1", "court": "ge_gerichte",
            "docket_number": "ACJC/123/2024", "decision_date": "2025-08-05",
            "regeste": "", "full_text": "Le tribunal considere que " * 40,
            "source_url": "https://justice.ge.ch/apps/decis/fr/pjdoc/x?id=99",
            "pdf_url": None}
PUBPAGE = {"decision_id": "ge_2", "court": "ge_gerichte",
           "docket_number": "A/136/2024", "decision_date": "2025-08-05",
           "regeste": "Resume",
           "full_text": ("ACJC/123/2024 - Descripteurs, normes. "
                         "Cet arret est entre en force. "
                         "Recours au Tribunal federal 1C_511/2025."),
           "source_url": "https://justice.ge.ch/apps/decis/fr/pjdoc/x?id=99",
           "pdf_url": None}


def test_two_representations_are_one_decision():
    g = group([JUDGMENT, PUBPAGE])
    assert g["ge_1"] == g["ge_2"]


def test_crossref_links_them_without_the_url():
    """~89% of case numbers name the judgment number in the header, so the
    link survives even where the URL does not match."""
    a = dict(JUDGMENT, source_url=None)
    b = dict(PUBPAGE, source_url=None)
    assert group([a, b])["ge_1"] == group([a, b])["ge_2"]


def test_crossref_is_one_directional():
    """A judgment must not claim to be its own publication page."""
    assert crossref_channel("ge_gerichte", JUDGMENT) is None
    assert crossref_channel("ge_gerichte", PUBPAGE) is not None


def test_roles_are_distinguished():
    assert representation_role(JUDGMENT) == "judgment"
    assert representation_role(PUBPAGE) == "publication_page"


def test_appeal_filed_after_judgment_is_harvested():
    """The appeal postdates the judgment, so only the portal page can
    carry it - this is the treatment-graph payload."""
    m = appeal_metadata(PUBPAGE)
    assert "1C_511/2025" in m["federal_references"]
    assert m["has_force_statement"] is True


def test_entity_keeps_both_representations():
    v = entity_view([JUDGMENT, PUBPAGE])
    assert {r["decision_id"] for r in v["representations"]} == {"ge_1", "ge_2"}
    assert v["federal_references"] == ["1C_511/2025"]
    assert v["has_force_statement"] is True


def test_search_surfaces_one_representation():
    assert display_representation([JUDGMENT, PUBPAGE])["decision_id"] == "ge_2"
    assert display_representation([PUBPAGE, JUDGMENT])["decision_id"] == "ge_2"


def test_vaud_second_number_is_a_channel():
    """Vaud already stores the second number in docket_number_2."""
    a = {"decision_id": "vd_a", "court": "vd_gerichte",
         "docket_number": "HC/2024/511", "source_url": None}
    b = {"decision_id": "vd_b", "court": "vd_gerichte",
         "docket_number": "JI21.099", "docket_number_2": "HC/2024/511",
         "source_url": None}
    assert group([a, b])["vd_a"] == group([a, b])["vd_b"]


def test_url_channel_extracts_court_record_id():
    assert url_channel("https://gerichte.lu.ch/x/Ajax?EnId=747") == \
        "url:gerichte.lu.ch|enid=747"


def test_transitive_grouping():
    """A links to B by url, B links to C by docket -> one entity."""
    a = {"decision_id": "a", "court": "c", "docket_number": "AAA111",
         "source_url": "https://x.ch/d?id=5"}
    b = {"decision_id": "b", "court": "c", "docket_number": "BBB222",
         "source_url": "https://x.ch/d?id=5"}
    c = {"decision_id": "c", "court": "c", "docket_number": "BBB222",
         "source_url": None}
    g = group([a, b, c])
    assert g["a"] == g["b"] == g["c"]


def test_cited_judgment_numbers_do_not_merge_entities():
    """A publication page citing OTHER judgments must not fuse them into
    its entity. A 4,000-char window did exactly that in production and
    produced entities of twelve unrelated JTAPI decisions."""
    page = {"decision_id": "ge_p", "court": "ge_gerichte",
            "docket_number": "A/4229/2016", "decision_date": "2017-06-13",
            "regeste": "ATA/655/2017 - descripteurs",
            "full_text": ("ATA/655/2017 en la cause X. " + "texte " * 200
                          + " voir aussi JTAPI/718/2025 et JTAPI/460/2025 "
                            "et JTAPI/343/2024")}
    own = {"decision_id": "ge_own", "court": "ge_gerichte",
           "docket_number": "ATA/655/2017", "source_url": None}
    cited = {"decision_id": "ge_cited", "court": "ge_gerichte",
             "docket_number": "JTAPI/718/2025", "source_url": None}
    g = group([page, own, cited])
    assert g["ge_p"] == g["ge_own"]        # its own judgment: merged
    assert g["ge_p"] != g["ge_cited"]      # merely cited: separate
