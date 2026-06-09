"""R1 defense-in-depth: stray HTML in a docket (entscheidsuche feed artifacts —
e.g. ch_vb's '<td class="metadataCell">90000048</td>', 2,212 rows) must never leak
into a citation_string_*. _build_citation_strings sanitizes the docket so the
citation contract holds even before the source data is cleaned."""
import mcp_server


def test_clean_docket_strips_html_keeps_content():
    assert mcp_server._clean_docket('<td class="metadataCell">90000048</td>') == "90000048"
    assert mcp_server._clean_docket('<span style="x">4A_1/2020</span>') == "4A_1/2020"
    assert mcp_server._clean_docket("4A_1/2020") == "4A_1/2020"      # clean -> unchanged
    assert mcp_server._clean_docket("  9C_5/2021  ") == "9C_5/2021"  # whitespace trimmed
    assert mcp_server._clean_docket("") == ""
    assert mcp_server._clean_docket(None) == ""


def test_citation_has_no_html_for_ch_vb():
    decision = {
        "court": "ch_vb",
        "docket_number": '<td class="metadataCell">90000048</td>',
        "decision_id": "ch_vb__td_class__metadataCell__90000048__td_",
        "decision_date": "2005-01-01",
    }
    cites = mcp_server._build_citation_strings(decision)
    for k in ("citation_string_de", "citation_string_fr", "citation_string_it"):
        assert "<" not in cites[k] and "metadataCell" not in cites[k], f"{k} still HTML: {cites[k]!r}"
        assert "90000048" in cites[k], f"{k} lost the docket content: {cites[k]!r}"


def test_citation_unchanged_for_clean_bger():
    decision = {
        "court": "bger", "docket_number": "4A_1/2020",
        "decision_id": "bger_4A_1_2020", "decision_date": "2020-03-15",
    }
    cites = mcp_server._build_citation_strings(decision)
    assert cites["citation_string_de"].startswith("BGer 4A_1/2020")
