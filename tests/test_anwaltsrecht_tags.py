"""Tests for Anwaltsrecht tagging pipeline."""
import sys
sys.path.insert(0, ".")


def test_docket_regex():
    """Verify regex extracts BGer docket numbers from SAV PDF text."""
    from search_stack.build_anwaltsrecht_tags import DOCKET_PATTERNS
    sample = """
    2C_345/2023, arrêt du 15.3.2024
    Urteil 2C_100/2020 vom 5. Mai 2021
    Arrêt 2P.100/2005 du 10 janvier 2006
    5A_123/2019
    """
    all_matches = set()
    for pattern in DOCKET_PATTERNS:
        for m in pattern.finditer(sample):
            all_matches.add(m.group(0))
    assert "2C_345/2023" in all_matches
    assert "2C_100/2020" in all_matches
    assert "2P.100/2005" in all_matches
    assert "5A_123/2019" in all_matches


def test_bge_regex():
    from search_stack.build_anwaltsrecht_tags import BGE_PATTERN
    sample = "BGE 130 II 270 und ATF 140 II 102"
    matches = [m.group(0) for m in BGE_PATTERN.finditer(sample)]
    assert "BGE 130 II 270" in matches
    assert "ATF 140 II 102" in matches


def test_extract_article_from_filename():
    from search_stack.build_anwaltsrecht_tags import _extract_bgfa_article
    assert _extract_bgfa_article("Art12.pdf") == "Art. 12"
    assert _extract_bgfa_article("Art3.pdf") == "Art. 3"
    assert _extract_bgfa_article("Art36.pdf") == "Art. 36"
    assert _extract_bgfa_article("Rechtsprechung_Bund_2024-2025.pdf") is None
