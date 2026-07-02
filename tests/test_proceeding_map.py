"""#56/P0.1: fine proceeding-type classification — register codes with
fixed meanings only, NULL over guess."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from proceeding_map import derive_proceeding  # noqa: E402


def test_bgg_era_letters():
    f = lambda dk: derive_proceeding("bger", None, dk)
    assert f("4A_576/2024") == ("bgg_beschwerde_zivil", "bgg")
    assert f("1C_146/2025") == ("bgg_beschwerde_oeff", "bgg")
    assert f("6B_1/2024") == ("bgg_beschwerde_straf", "bgg")
    assert f("9C_652/2025") == ("bgg_beschwerde_soz", "bgg")
    assert f("5D_12/2020") == ("bgg_verfassungsbeschwerde", "bgg")
    assert f("2E_1/2018") == ("bgg_klage", "bgg")
    assert f("1F_9/2023") == ("bgg_revision", "bgg")
    assert f("1G_1/2026") == ("bgg_erlaeuterung", "bgg")
    assert f("1B_55/2019") == ("bgg_beschwerde_straf", "bgg")  # Zwangsmassnahmen


def test_og_era_and_evg():
    f = lambda dk: derive_proceeding("bger", None, dk)
    assert f("2A.123/2005") == ("og_verwaltungsgerichtsbeschwerde", "og")
    assert f("4P.17/2004") == ("og_staatsrechtliche_beschwerde", "og")
    assert f("4C.234/2003") == ("og_berufung", "og")
    assert f("6S.44/2002") == ("og_nichtigkeitsbeschwerde", "og")
    assert f("I_350/1999") == ("evg_verwaltungsgerichtsbeschwerde", "og")
    # BGG-era letter with pre-2007 year in underscore form -> OG mapping
    assert f("XX")[0] is None


def test_cantonal_registers():
    assert derive_proceeding("zh_obergericht", None, "RT250043") == ("schkg_rechtsoeffnung", "schkg")
    assert derive_proceeding("zh_obergericht", None, "LB200012") == ("zpo_berufung", "zpo")
    assert derive_proceeding("ge_gerichte", None, "DCSO/55/2020") == ("schkg_aufsichtsbeschwerde", "schkg")
    assert derive_proceeding("ge_gerichte", None, "ATAS/1001/2007") == ("sozialversicherungsbeschwerde", "atsg")
    assert derive_proceeding("vd_findinfo", None, "ML / 2010 / 55") == ("schkg_rechtsoeffnung", "schkg")
    assert derive_proceeding("vd_gerichte", None, "CACI 15/2019") == ("zpo_berufung", "zpo")
    # chamber field fallback (VD stores codes there too)
    assert derive_proceeding("vd_omni", "CDAP", "02/2014") == ("vwv_beschwerde", "vrg_vd")
    assert derive_proceeding("be_zivilstraf", None, "ABS 2021 12") == ("schkg_aufsichtsbeschwerde", "schkg")


def test_null_over_guess():
    assert derive_proceeding("lu_gerichte", None, "1A 12 34") == (None, None)
    assert derive_proceeding("bge", None, "150 II 1") == (None, None)  # volume, not a proceeding
    assert derive_proceeding(None) == (None, None)
    assert derive_proceeding("bger", None, "3X_1/2020") == (None, None)


def test_single_proceeding_courts():
    assert derive_proceeding("bvger", None, "A-1/2020") == ("vwvg_beschwerde", "vwvg")
    assert derive_proceeding("zh_sozialversicherungsgericht", None, "IV.2021.00016") == ("sozialversicherungsbeschwerde", "atsg")
    assert derive_proceeding("zh_handelsgericht", None, "HG200012") == ("zpo_ordentlich", "zpo")
    assert derive_proceeding("zh_steuerrekursgericht", None, "ST.2020.1") == ("vwv_rekurs", "vrg_zh")
