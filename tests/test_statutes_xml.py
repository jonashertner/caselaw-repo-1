"""Issue #22 — structured (Akoma Ntoso XML) form for statute articles.

The builder must keep the verbatim per-article AN XML subtree (with
enumerations/<blockList> and footnotes/<authorialNote>) alongside the
flattened text, so /laws/{abbr} can serve structure, not just plain text.
"""
from search_stack.build_statutes_db import parse_xml

AKN = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

FIXTURE = f'''<?xml version="1.0" encoding="UTF-8"?>
<akomaNtoso xmlns="{AKN}">
  <act>
    <body>
      <article eId="art_41">
        <num>Art. 41</num>
        <heading>Schadenersatz<authorialNote><p>Fassung gemaess AS 2020 4525</p></authorialNote></heading>
        <paragraph eId="art_41-para_1">
          <num>1</num>
          <content>
            <p>Wer einem andern widerrechtlich Schaden zufuegt, wird ihm zum Ersatze verpflichtet.</p>
            <blockList>
              <item><num>a.</num><p>vorsaetzlich;</p></item>
              <item><num>b.</num><p>fahrlaessig.</p></item>
            </blockList>
          </content>
        </paragraph>
      </article>
    </body>
  </act>
</akomaNtoso>'''


def test_article_xml_fragment_preserves_structure(tmp_path):
    p = tmp_path / "de.xml"
    p.write_text(FIXTURE, encoding="utf-8")

    arts = parse_xml(p)
    assert len(arts) == 1
    a = arts[0]

    # existing behaviour unchanged (backward compatible)
    assert a["article_num"] == "41"
    assert "Schadenersatz" in (a["heading"] or "")
    assert "widerrechtlich" in a["text"]

    # new: a verbatim Akoma Ntoso XML subtree for the article
    assert "xml" in a, "article dict must carry an 'xml' fragment"
    xml = a["xml"]
    assert xml and xml.lstrip().startswith("<article"), "fragment is the <article> subtree"
    assert "art_41" in xml, "article identity preserved"
    # structure that plain text loses:
    assert "<blockList" in xml, "enumeration structure preserved (no ns prefix)"
    assert "<authorialNote" in xml, "footnote structure preserved (no ns prefix)"
    assert "<item" in xml and "vorsaetzlich" in xml
