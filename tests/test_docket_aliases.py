"""Pure-helper tests for docket_aliases (issue #41 joined-docket extraction)."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import docket_aliases as da  # noqa: E402


# Realistic caption reproducing the stored full_text head of bger_1B_242_2022
# (3 joined dockets, DE, comma + "und"). The metadata header line repeats the
# lead docket 3x before the clean joined-docket line.
CAPTION_A = (
    "Bundesgericht I. Öffentlich-rechtliche Abteilung 30.05.2022 "
    "1B 242/2022 (1B_242/2022)\n"
    "Tribunal fédéral Ire Cour de droit public 30.05.2022 1B 242/2022 (1B_242/2022)\n"
    "Tribunale federale I Corte di diritto pubblico 30.05.2022 1B 242/2022 (1B_242/2022)\n\n"
    "Strafverfahren; Verletzung des Verbots der doppelten Strafverfolgung | Strafprozess\n\n"
    "Bundesgericht\nTribunal fédéral\nTribunale federale\nTribunal federal\n"
    "1B_242/2022, 1B_243/2022 und 1B_244/2022\n"
    "Urteil vom 30. Mai 2022\n"
    "I. öffentlich-rechtliche Abteilung\n"
    "Besetzung\nBundesrichterin Jametti\n"
    # A genuine citation to an UNRELATED case, AFTER the date line (body):
    "Verfahren 1B_242/2022\nVgl. zum Ganzen Urteil 6B_999/2019 vom 1. Januar 2020.\n"
)

CAPTION_B = (
    "Bundesgericht I. Strafrechtliche Abteilung 31.01.2022 6B 1518/2021 (6B_1518/2021)\n"
    "Bundesgericht\nTribunal fédéral\nTribunale federale\n"
    "6B_1518/2021, 6B_1519/2021\n"
    "Urteil vom 31. Januar 2022\n"
)

# French causes jointes: semicolon + "et", "Arrêt du"
CAPTION_FR = (
    "Tribunal fédéral IIe Cour de droit civil 16.07.2013 5D 117/2013 (5D_117/2013)\n"
    "5D_117/2013; 5D_118/2013 et 5D_119/2013\n"
    "Arrêt du 16 juillet 2013\n"
)

# Italian: "Sentenza del", connector "e"
CAPTION_IT = (
    "Tribunale federale 12.09.2024 2C 503/2012 (2C_503/2012)\n"
    "2C_503/2012 e 2C_504/2012\n"
    "Sentenza del 12 settembre 2024\n"
)


def test_extract_three_joined_de():
    got = da.extract_joined_dockets(CAPTION_A, "1B 242/2022")
    assert got == ["1B_243/2022", "1B_244/2022"], got


def test_extract_two_joined_de():
    got = da.extract_joined_dockets(CAPTION_B, "6B 1518/2021")
    assert got == ["6B_1519/2021"], got


def test_extract_french_causes_jointes():
    got = da.extract_joined_dockets(CAPTION_FR, "5D 117/2013")
    assert got == ["5D_118/2013", "5D_119/2013"], got


def test_extract_italian():
    got = da.extract_joined_dockets(CAPTION_IT, "2C 503/2012")
    assert got == ["2C_504/2012"], got


def test_body_citation_excluded():
    # The unrelated 6B_999/2019 appears AFTER "Urteil vom" and must NOT become
    # an alias.
    got = da.extract_joined_dockets(CAPTION_A, "1B 242/2022")
    assert "6B_999/2019" not in got


def test_single_docket_no_alias():
    text = (
        "Bundesgericht 30.05.2022 1B 242/2022 (1B_242/2022)\n"
        "1B_242/2022\nUrteil vom 30. Mai 2022\n"
    )
    assert da.extract_joined_dockets(text, "1B 242/2022") == []


def test_revision_reference_not_extracted():
    # Real false-positive class: 1A.104/2005 is a REVISION of the separate
    # decision 1A.278/2004, named in a prose subject line — NOT a joined docket.
    # The two dockets are not in one separator-joined run, so nothing is
    # extracted (verified against production data for bger_1A.104_2005).
    text = (
        "Bundesgericht I. öffentlich-rechtliche Abteilung 28.04.2005 1A.104/2005\n"
        "Tribunal fédéral Ire Cour de droit public 28.04.2005 1A.104/2005\n\n"
        "Revision des bundesgerichtlichen Urteils vom 17. März 2005 (1A.278/2004) "
        "| Ökologisches Gleichgewicht\n\n"
        "{T 0/2}\n1A.104/2005 /ggs\nUrteil vom 28. April 2005\n"
    )
    assert da.extract_joined_dockets(text, "1A.104/2005") == []


def test_lead_not_first_skips():
    # If the first docket in the head is NOT the stored lead, we skip rather
    # than mis-map (conservative).
    text = "9C_999/2020, 1B_242/2022 und 1B_243/2022\nUrteil vom 1. Januar 2022\n"
    assert da.extract_joined_dockets(text, "1B 242/2022") == []


def test_too_many_dockets_skips():
    dockets = ", ".join(f"6B_{100 + i}/2020" for i in range(15))
    text = f"6B_100/2020\n{dockets}\nUrteil vom 1. Januar 2020\n"
    assert da.extract_joined_dockets(text, "6B 100/2020") == []


def test_normalize_docket_key_forms():
    forms = ["1B_243/2022", "1B 243/2022", "1B.243/2022",
             "bger_1B_243_2022", "  1B_243/2022  "]
    keys = {da.normalize_docket_key(f) for f in forms}
    assert keys == {"1B_243/2022"}, keys


def test_normalize_docket_key_rejects_non_docket():
    assert da.normalize_docket_key("151 III 481") is None
    assert da.normalize_docket_key("") is None
    assert da.normalize_docket_key(None) is None
