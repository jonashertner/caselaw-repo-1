"""TLS verification semantics for scrapers (offline).

Regression for 2026-08-26. The CA-bundle systemd drop-in shipped on 2026-08-25
set REQUESTS_CA_BUNDLE for every scraper unit. That silently re-enabled
certificate verification for the six scrapers that declared
``VERIFY_SSL = False``, because requests promotes REQUESTS_CA_BUNDLE over
``session.verify`` whenever the per-request ``verify`` is None
(Session.merge_environment_settings -> merge_setting returns the request-level
value). fr_gerichte broke on its very next scheduled run — its portal serves a
leaf-only chain it had been ignoring for ten months.

Two things are pinned here:
  1. an explicit VERIFY_SSL = False is actually honoured (the declaration must
     not lie), and
  2. scrapers that DO verify still pick up the CA bundle, so the bger fix that
     depends on it cannot regress.
"""
from __future__ import annotations

import sys
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def _effective_verify(session: requests.Session, explicit) -> object:
    """What requests would actually use for this request."""
    return session.merge_environment_settings(
        "https://example.invalid/", {}, None, explicit, None
    )["verify"]


def test_requests_promotes_ca_bundle_over_session_verify_false(monkeypatch, tmp_path):
    """The upstream behaviour this whole fix exists because of.

    If requests ever changes this, the setdefault in get()/post() becomes
    unnecessary — but until then it is load-bearing.
    """
    bundle = tmp_path / "ca-bundle.pem"
    bundle.write_text("")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(bundle))

    s = requests.Session()
    s.verify = False
    # verify=None is what you get when the caller does not pass one.
    assert _effective_verify(s, None) == str(bundle), (
        "session.verify=False was expected to be overridden by REQUESTS_CA_BUNDLE"
    )
    # Passing it explicitly is what defeats the promotion.
    assert _effective_verify(s, False) is False


def test_explicit_verify_true_still_resolves_to_the_bundle(monkeypatch, tmp_path):
    """Guard the bger fix: verifying scrapers must keep using the bundle."""
    bundle = tmp_path / "ca-bundle.pem"
    bundle.write_text("")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(bundle))

    s = requests.Session()          # default verify=True
    assert _effective_verify(s, True) == str(bundle)


def test_get_and_post_pass_verify_explicitly(monkeypatch, tmp_path):
    """BaseScraper.get/post must pin verify so the env cannot override it."""
    from base_scraper import BaseScraper

    bundle = tmp_path / "ca-bundle.pem"
    bundle.write_text("")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(bundle))

    class _Probe(BaseScraper):
        VERIFY_SSL = False
        REQUEST_DELAY = 0

        @property
        def court_code(self) -> str:
            return "probe"

        def discover_new(self, since_date=None):
            return iter(())

        def fetch_decision(self, stub):
            return None

    seen = {}

    def _fake(method):
        def _call(url, **kwargs):
            seen[method] = kwargs
            r = requests.Response()
            r.status_code = 200
            return r
        return _call

    s = _Probe(state_dir=tmp_path / "state")

    assert s.session.verify is False, "VERIFY_SSL=False must reach session.verify"

    monkeypatch.setattr(s.session, "get", _fake("get"))
    monkeypatch.setattr(s.session, "post", _fake("post"))
    s.get("https://example.invalid/")
    s.post("https://example.invalid/")

    for method in ("get", "post"):
        assert "verify" in seen[method], (
            f"{method}() did not pass verify explicitly; REQUESTS_CA_BUNDLE "
            f"would silently re-enable verification"
        )
        assert seen[method]["verify"] is False


def test_no_scraper_still_declares_verify_ssl_false():
    """All six portals verified against the CA bundle on 2026-08-26.

    Keeping the opt-out would now genuinely disable verification (it finally
    works), which is the opposite of what we want. If a portal's chain breaks
    again, add its intermediate to deploy/certs/extra/ rather than restoring
    this flag.
    """
    offenders = []
    for path in (REPO / "scrapers").rglob("*.py"):
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("VERIFY_SSL") and "False" in stripped:
                offenders.append(f"{path.relative_to(REPO)}:{i}")
    assert not offenders, (
        "VERIFY_SSL = False reintroduced at: " + ", ".join(offenders)
        + " — prefer adding the missing intermediate to deploy/certs/extra/"
    )
