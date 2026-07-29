"""ECtHR material must not ship under the CC0 dedication (2026-07-29).

Verified live on 2026-07-27: data/ecthr_chamber.parquet,
data/ecthr_committee.parquet, data/ecthr_grand_chamber.parquet,
data/hudoc_ch.parquet and data/bge_egmr.parquet were on
voilaj/swiss-caselaw, a repo tagged license: cc0-1.0. CC0 is a rights
WAIVER; these are © ECHR-CEDH and not ours to waive. The export now
excludes them at source and the upload step refuses stale copies.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from export_parquet import EXCLUDED_COURTS  # noqa: E402

ECTHR = {"ecthr_chamber", "ecthr_committee", "ecthr_grand_chamber",
         "hudoc_ch", "bge_egmr"}


def test_all_ecthr_courts_excluded():
    assert ECTHR <= EXCLUDED_COURTS


def test_swiss_courts_not_excluded():
    for c in ("bger", "bge", "bvger", "bstger", "mkg", "ch_vb", "zh_obergericht"):
        assert c not in EXCLUDED_COURTS, c


def test_export_filters_at_source():
    src = (REPO / "export_parquet.py").read_text(encoding="utf-8")
    assert "c for c in courts if c not in EXCLUDED_COURTS" in src
    assert "held_back" in src          # and logs what it withheld


def test_upload_step_refuses_stale_excluded_parquet():
    """DATASET_DIR is never cleaned — a stale file from before the exclusion
    would still be globbed. The step must FAIL, not silently skip."""
    src = (REPO / "publish.py").read_text(encoding="utf-8")
    assert "from export_parquet import EXCLUDED_COURTS" in src
    assert "Refusing to upload non-CC0 court(s)" in src
    i = src.index("Refusing to upload non-CC0 court(s)")
    assert "return False" in src[i:i + 400]


def test_dataset_card_declares_the_exclusion():
    card = (REPO / "dataset_card.md").read_text(encoding="utf-8")
    assert "license: cc0-1.0" in card
    assert "Not included: European Court of Human Rights" in card
    assert "ECHR-CEDH" in card
    # the ECtHR court codes must no longer appear as included tables
    for row in ("| `ecthr_chamber` |", "| `hudoc_ch` |", "| `bge_egmr` |"):
        assert row not in card, row
