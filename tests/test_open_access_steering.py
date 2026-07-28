"""Open-access steering (user directive 2026-07-28).

Observed failure: a research answer ended with "Was ich als Nächstes tun
würde: Volltextsuche in Swisslex/Legalis mit: …" — search strings drafted
for commercial databases. The platform exists to make Swiss law openly
accessible; every LLM surface we control must steer follow-up research to
this corpus or to free official sources, never to a paywall.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402
from web_api.providers.base import SYSTEM_PROMPT  # noqa: E402


def test_mcp_instructions_forbid_commercial_referrals():
    ins = m.server.instructions or ""
    assert "OPEN ACCESS ONLY" in ins
    for provider in ("Swisslex", "Legalis", "Weblaw", "beck-online"):
        assert provider in ins  # named so the model recognises them as banned
    assert "weiterführende Recherche" in ins  # the observed failure slot
    # the positive alternative is spelled out
    for free in ("Fedlex", "LexFind", "entscheidsuche.ch"):
        assert free in ins


def test_web_api_prompt_forbids_commercial_referrals():
    assert "NEVER direct the user to" in SYSTEM_PROMPT
    for provider in ("Swisslex", "Legalis", "Weblaw"):
        assert provider in SYSTEM_PROMPT
    # follow-up directions are anchored to THIS database
    assert "searches in" in SYSTEM_PROMPT and "THIS database" in SYSTEM_PROMPT


def test_web_api_quality_standards_carry_the_rule():
    i = SYSTEM_PROMPT.index("## Quality standards")
    tail = SYSTEM_PROMPT[i:]
    assert "Open access only" in tail
