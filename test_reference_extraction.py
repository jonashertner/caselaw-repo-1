from search_stack.reference_extraction import extract_case_citations, extract_references, extract_statute_references


def test_extract_statute_references_basic():
    text = "Nach Art. 8 EMRK und Art. 34 Abs. 2 BV ist der Anspruch zu prüfen."
    refs = extract_statute_references(text)
    normalized = {r.normalized for r in refs}
    assert "ART.8.EMRK" in normalized
    assert "ART.34.ABS.2.BV" in normalized


def test_extract_case_citations_bge_and_docket():
    text = "Vgl. BGE 147 I 268 sowie Urteil 1A.122/2005 und VB.2018.00411."
    refs = extract_case_citations(text)
    normalized = {r.normalized for r in refs}
    assert "BGE 147 I 268" in normalized
    assert "1A_122_2005" in normalized
    assert "VB_2018_00411" in normalized


def test_extract_references_combined_schema():
    text = "Art. 8 EMRK; BGE 147 I 268."
    payload = extract_references(text)
    assert "statutes" in payload
    assert "citations" in payload
    assert payload["statutes"]
    assert payload["citations"]


def test_extract_statute_references_multilingual_paragraph_markers():
    text = "Selon art. 8 al. 1 CEDH et Art. 3 cpv. 2 LPGA."
    refs = extract_statute_references(text)
    normalized = {r.normalized for r in refs}
    assert "ART.8.ABS.1.CEDH" in normalized
    assert "ART.3.ABS.2.LPGA" in normalized
    assert all(not n.endswith(".AL") for n in normalized)


def test_extract_case_citations_does_not_duplicate_bge_as_docket():
    text = "Voir BGE 147 I 268."
    refs = extract_case_citations(text)
    normalized = [r.normalized for r in refs]
    assert normalized == ["BGE 147 I 268"]


def test_extract_statute_references_handles_ordinal_suffixes():
    text = "Art. 8bis BV, Art. 34ter BV und Art. 8 Abs. 2bis BV."
    refs = extract_statute_references(text)
    normalized = {r.normalized for r in refs}
    assert "ART.8bis.BV" in normalized
    assert "ART.34ter.BV" in normalized
    assert "ART.8.ABS.2bis.BV" in normalized
    assert "ART.8b.IS" not in normalized
    assert "ART.34t.ER" not in normalized
