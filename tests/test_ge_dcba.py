"""Verify the dcba section is registered in GE scraper."""
import sys
sys.path.insert(0, ".")

def test_dcba_section_registered():
    from scrapers.cantonal.ge_gerichte import SECTIONS
    assert "dcba" in SECTIONS, "dcba section missing from SECTIONS dict"
    assert SECTIONS["dcba"] == "GE_DCBA_001"

def test_dcba_section_count():
    from scrapers.cantonal.ge_gerichte import SECTIONS
    # 18 original + 1 new = 19
    assert len(SECTIONS) == 19
