"""How references are parsed for identity checks: nothing here prints a citation."""
import pytest
from opencaselaw_cli.references import (docket_in_reference, docket_variants, label_key, normalise_pinpoint,
                                        parse_reference, pinpoint_parent)


@pytest.mark.parametrize("text, expected", [
    ("BGE 136 III 513 E. 2.3", dict(bge_label="BGE 136 III 513", pinpoint="2.3", core="BGE 136 III 513")),
    ("ATF 137 III 303 consid. 2 p. 305", dict(bge_label="BGE 137 III 303", pinpoint="2", pages=["p. 305"])),
    ("BGE 125 II 633 E. 2 S. 636", dict(pinpoint="2", pages=["S. 636"], core="BGE 125 II 633")),
    ("BGE 121 V 240 E. 3c/aa", dict(pinpoint="3c/aa")),
    ("BGE 134 III 354 ff.", dict(bge_label="BGE 134 III 354", core="BGE 134 III 354")),
    ("(BGE 136 III 510)", dict(core="BGE 136 III 510")),
    ("BGE 136 III 510.", dict(core="BGE 136 III 510")),
    ("BGE 134 III 354 (4A_45/2008)", dict(bge_label="BGE 134 III 354", dockets=["4A_45/2008"])),
    ("BGer 4A_255/2012", dict(dockets=["4A_255/2012"], courts={"bger", "bge"}, court_words=True)),
    ("Urteil des Bundesgerichts 4A 87/2019 vom 2. September 2019, E. 4.2.1",
     dict(dockets=["4A 87/2019"], date="2019-09-02", pinpoint="4.2.1")),
    ("arrêt du TF 4A_485/2015 du 15 février 2016 consid. 3", dict(dockets=["4A_485/2015"], date="2016-02-15", pinpoint="3")),
    ("sentenza del TF 9C_313/2016 del 22 dicembre 2016", dict(dockets=["9C_313/2016"], date="2016-12-22")),
    ("arrêt du Tribunal fédéral 4A_89/2021 du 1er avril 2022", dict(date="2022-04-01")),
    ("Obergericht ZH LA210005 vom 15. Juni 2021", dict(dockets=["LA210005"], canton="ZH", date="2021-06-15")),
    ("Urteil des Verwaltungsgerichts des Kantons Aargau WBE.2026.33", dict(dockets=["WBE.2026.33"], canton="AG")),
    ("Gericht GE C/11532/2013 vom 8. November 2016", dict(dockets=["C/11532/2013"], canton="GE")),
    ("arrêt de la Cour de justice de Genève ACJC/1234/2024 du 5 mars 2024", dict(dockets=["ACJC/1234/2024"], canton="GE")),
    ("Tribunal VD HC / 2020 / 38 du 6 mai 2020", dict(dockets=["HC/2020/38"], canton="VD")),
    ("BVGer A-4843/2020 vom 1. April 2021", dict(dockets=["A-4843/2020"], courts={"bvger"})),
    ("Gericht BL 810 16 9 vom 10. August 2016", dict(dockets=["810 16 9"], canton="BL")),
    ("Verwaltungsgericht SG K 2015/3, K 2017/3 vom 18. November 2020", dict(dockets=["K 2015/3", "K 2017/3"], canton="SG")),
    ("4C.230/2006", dict(dockets=["4C.230/2006"], long_form=False)),
    ("OGer ZH, LA210005, 15.6.2021", dict(dockets=["LA210005"], date="2021-06-15")),
    ("1/2020", dict(dockets=[], core="1/2020", long_form=False)),
    ("WBE.2026.33", dict(dockets=["WBE.2026.33"], pinpoint=None, long_form=False)),
    ("Bundesgericht, Urteil vom 5. April 2013", dict(dockets=[], date="2013-04-05", long_form=True)),
    ("BGE 136 III 513", dict(long_form=False, courts={"bge"}, court_words=False)),
    ("bge_BGE_125_II_633", dict(long_form=False, court_words=False, dockets=[])),
])
def test_references_are_parsed_as_written(text, expected):
    parsed = parse_reference(text)
    for key, value in expected.items():
        assert getattr(parsed, key) == value, (text, key, getattr(parsed, key))


def test_queries_ask_for_the_label_not_the_prose():
    assert parse_reference("BGer 4A 535/2018").queries() == ["4A_535/2018", "4A.535/2018", "4A 535/2018"]
    assert parse_reference("BGE 134 III 354 (4A_45/2008)").queries() == ["BGE 134 III 354"]
    assert parse_reference("Bundesgericht, Urteil vom 5. April 2013").queries() == ["Bundesgericht, Urteil vom 5. April 2013"]
    assert parse_reference("1/2020").queries() == ["1/2020"]
    assert docket_variants("4C.230/2006") == ["4C_230/2006", "4C.230/2006"] and docket_variants("LA210005") == ["LA210005"]


def test_label_key_folds_only_for_comparison():
    assert label_key("BGE 136 III 513, E. 2.3") == label_key("ATF 136 III 513") == label_key("136 III 513") == "bge136iii513"
    assert label_key("BGE 134 III 354 ff.") == label_key("BGE 134 III 354 S. 357") == label_key("BGE 134 III 354, E. 2.1, S. 357")
    assert label_key("4A_747/2012") == label_key("4A 747/2012") == label_key("4A.747/2012")
    assert label_key("BGer 4A_747/2012 vom 5. April 2013") == label_key("BGer 4A 747/2012 vom 5. April 2013")
    assert label_key("140 III 86") != label_key("140 III 860") and label_key(None) is None


def test_docket_must_appear_whole_in_the_reference():
    assert docket_in_reference("BGer 4A_255/2012", "4A 255/2012")
    assert docket_in_reference("Obergericht ZH LA210005 vom 15. Juni 2021", "LA210005")
    assert docket_in_reference("Tribunal VD HC / 2020 / 38 du 6 mai 2020", "HC/2020/38")
    assert docket_in_reference("4C_230/2006", "4C.230/2006")
    assert not docket_in_reference("100/2015", "D-1100/2015")
    assert not docket_in_reference("BGE 134 III 354", "4A_45/2008")
    assert not docket_in_reference("1/2020", "11/2020") and not docket_in_reference("x", None)


def test_pinpoints_accept_the_authors_spelling_and_fail_only_themselves():
    assert normalise_pinpoint("consid. 2.3") == "2.3" and normalise_pinpoint("E. 3b") == "3b"
    assert normalise_pinpoint("3c/aa") == "3c/aa" and normalise_pinpoint(" 4.2.1 ") == "4.2.1" and normalise_pinpoint("") is None
    assert pinpoint_parent("3c/aa") == "3" and pinpoint_parent("2a") == "2" and pinpoint_parent("4.2.1") is None
    with pytest.raises(ValueError):
        normalise_pinpoint("foo")
    with pytest.raises(ValueError):
        normalise_pinpoint(12)


def test_court_scope_filters_candidates_but_keeps_unknown_courts():
    federal = parse_reference("BGer 4A_191/2019 vom 5. November 2019")
    assert federal.in_scope({"court": "bger"}) and not federal.in_scope({"court": "ge_gerichte", "canton": "GE"})
    assert federal.in_scope({})  # a candidate without court metadata cannot be ruled out
    geneva = parse_reference("arrêt de la Cour de justice de Genève ACJC/1234/2024")
    assert geneva.in_scope({"court": "ge_gerichte"}) and geneva.in_scope({"canton": "GE"}) and not geneva.in_scope({"court": "bger", "canton": "CH"})
    assert parse_reference("4A_191/2019").in_scope({"court": "ge_gerichte"})
