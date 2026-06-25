"""robots.txt crawl policy.

HTML pages (/entscheid/, /search/, /, sitemaps) stay crawlable; the per-decision
API subtree /api/decisions/ (the export renderers + redundant JSON) is disallowed
for EVERY crawler. A crawler obeys only its most-specific User-agent group, so the
disallow must appear in each named AI-bot group or those bots ignore it — this is
the gotcha these tests pin. A plain prefix (no wildcard) is used so urllib's parser
and every real crawler honour it identically.
"""
from __future__ import annotations

import sys
from pathlib import Path
from urllib.robotparser import RobotFileParser

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

BASE = "https://opencaselaw.ch"
TXT = m._build_robots_txt(BASE)


def _parser() -> RobotFileParser:
    rp = RobotFileParser()
    rp.parse(TXT.splitlines())
    return rp


def test_structure_disallow_in_every_group():
    for ua in m._ROBOTS_AGENTS:
        assert "User-agent: %s\n" % ua in TXT
    # the per-decision disallow is repeated once per group (the precedence fix)
    assert TXT.count("Disallow: /api/decisions/") == len(m._ROBOTS_AGENTS)
    assert "Allow: /" in TXT
    assert "Sitemap: %s/sitemap.xml" % BASE in TXT
    # never block the whole site
    assert "Disallow: /\n" not in TXT


def test_every_bot_blocks_exports_and_per_decision_api():
    rp = _parser()
    export = BASE + "/api/decisions/bge_BGE_140_III_86/export.pdf"
    api_json = BASE + "/api/decisions/bge_BGE_140_III_86"
    for ua in ("*", "GPTBot", "ClaudeBot", "CCBot", "PerplexityBot",
               "Googlebot", "Bingbot", "Bytespider", "Applebot", "anthropic-ai"):
        assert rp.can_fetch(ua, export) is False, "%s must NOT crawl exports" % ua
        assert rp.can_fetch(ua, api_json) is False, "%s must NOT crawl /api/decisions" % ua


def test_every_bot_still_crawls_html_content():
    rp = _parser()
    html = BASE + "/entscheid/bge_BGE_140_III_86"
    for ua in ("*", "GPTBot", "ClaudeBot", "CCBot", "PerplexityBot",
               "Googlebot", "Bytespider", "Applebot"):
        assert rp.can_fetch(ua, html) is True, "%s must crawl /entscheid HTML" % ua
        assert rp.can_fetch(ua, BASE + "/") is True
        assert rp.can_fetch(ua, BASE + "/search/") is True
        assert rp.can_fetch(ua, BASE + "/sitemap.xml") is True
        # the rest of /api stays crawlable (only /api/decisions/ is closed)
        assert rp.can_fetch(ua, BASE + "/api/openapi.json") is True


def test_metrics_and_dev_now_honored_by_named_ai_bots():
    # the latent gap this also fixes: named bots used to ignore /metrics + /dev
    rp = _parser()
    for ua in ("GPTBot", "ClaudeBot", "PerplexityBot"):
        assert rp.can_fetch(ua, BASE + "/metrics") is False
        assert rp.can_fetch(ua, BASE + "/dev/anything") is False
