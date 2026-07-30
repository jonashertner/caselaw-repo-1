"""Issue #59: decision ids with literal spaces broke every /entscheid/ link.

~58,500 stored ids (5.6% of the corpus) carry spaces — 'bge_152 V 60',
'fr_gerichte_604 2026 30'. Interpolated raw into Markdown, the URL ends at
the first space, and a client that derives the id from the link (the only
place the canonical id appears) gets 'bge_152' — truncated and NON-unique.
The reporter had three different BGE overwrite each other in one directory.

Fix: every /entscheid/ URL construction goes through _decision_path()
(percent-encoding, no safe chars). A source scan pins the invariant.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def test_decision_path_encodes_spaces():
    assert m._decision_path("bge_152 V 60") == "bge_152%20V%2060"
    assert m._decision_path("fr_gerichte_604 2026 30") == "fr_gerichte_604%202026%2030"
    # ids without spaces are untouched (unreserved chars stay literal)
    assert m._decision_path("bge_BGE_150_III_63") == "bge_BGE_150_III_63"
    assert m._decision_path("") == ""


def test_canonical_url_is_markdown_safe():
    url = m._canonical_decision_url("bge_152 V 60")
    assert url.endswith("/entscheid/bge_152%20V%2060")
    assert " " not in url
    # pinpoint anchor rides after the encoded id
    url = m._canonical_decision_url("bge_152 V 60", "2.3")
    assert url.endswith("/entscheid/bge_152%20V%2060#e-2-3")


def test_citation_builder_url_is_encoded_and_citation_untouched():
    cit = m._build_citation_strings({
        "decision_id": "bge_152 V 60", "court": "bge",
        "docket_number": "152 V 60", "decision_date": "2026-01-15",
    })
    assert " " not in cit["canonical_url"]
    assert cit["canonical_url"].endswith("/entscheid/bge_152%20V%2060")
    # the CITATION string keeps its spaces — only the URL is encoded
    assert cit["citation_string_de"] == "BGE 152 V 60"


def test_structured_decision_payload_carries_encoded_url():
    p = m._decision_hits_structured([{
        "decision_id": "bge_152 V 60", "court": "bge",
        "docket_number": "152 V 60", "decision_date": "2026-01-15",
        "language": "de", "title": "X.", "snippet": "s",
    }], "q", "de")
    assert " " not in p["decisions"][0]["canonical_url"]


def test_md_link_roundtrip_survives_markdown_parsing():
    """The exact failure mode: a Markdown parser must recover the FULL id."""
    link = m._md_link("152 V 60", m._canonical_decision_url("bge_152 V 60"))
    mm = re.match(r"\[(.+)\]\((\S+)\)$", link)
    assert mm, link
    url = mm.group(2)
    got = url.rsplit("/entscheid/", 1)[1]
    import urllib.parse
    assert urllib.parse.unquote(got) == "bge_152 V 60"


def test_every_entscheid_url_builder_uses_decision_path():
    """Source-scan: an f-string interpolating into /entscheid/ must go through
    _decision_path. The Starlette route pattern (the receiver) is exempt."""
    src = Path(REPO, "mcp_server.py").read_text(encoding="utf-8")
    bad = []
    for i, line in enumerate(src.splitlines(), 1):
        if "/entscheid/{" not in line:
            continue
        if "Route(" in line or "_decision_path(" in line:
            continue
        bad.append(f"{i}: {line.strip()[:100]}")
    assert not bad, "raw id interpolated into /entscheid/ URL:\n" + "\n".join(bad)
