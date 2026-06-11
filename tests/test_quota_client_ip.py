"""X-Forwarded-For spoofing must not defeat the per-IP LLM quota.

Behind a single trusted nginx hop, the only trustworthy client identity is
``X-Real-IP`` (nginx sets it to ``$remote_addr``) or the LAST ``X-Forwarded-For``
hop (the one nginx appended). Earlier XFF hops are fully attacker-controlled and
must never gate the quota — otherwise rotating the first hop yields unbounded
LLM-backed calls. Regression test for the spoofable-first-hop bug.
"""
import mcp_server


class _Req:
    def __init__(self, headers, client_host="203.0.113.9"):
        # FastAPI Request.headers.get is case-insensitive; the helper queries
        # lowercase keys, so a plain dict with lowercase keys is faithful.
        self.headers = headers
        self.client = type("C", (), {"host": client_host})() if client_host else None


def test_xreal_ip_wins_over_spoofed_forwarded_for():
    # Attacker prepends 1.2.3.4; nginx's X-Real-IP is the truth.
    req = _Req({"x-real-ip": "198.51.100.7",
                "x-forwarded-for": "1.2.3.4, 198.51.100.7"})
    assert mcp_server._trusted_client_ip(req) == "198.51.100.7"


def test_forwarded_for_uses_last_hop_not_first():
    # No X-Real-IP: trust only the last (nginx-appended) hop.
    req = _Req({"x-forwarded-for": "1.2.3.4, 5.6.7.8, 198.51.100.7"})
    assert mcp_server._trusted_client_ip(req) == "198.51.100.7"


def test_rotating_first_hop_cannot_multiply_quota():
    a = _Req({"x-forwarded-for": "9.9.9.1, 198.51.100.7"})
    b = _Req({"x-forwarded-for": "9.9.9.2, 198.51.100.7"})
    assert (mcp_server._trusted_client_ip(a)
            == mcp_server._trusted_client_ip(b) == "198.51.100.7")


def test_falls_back_to_socket_peer_when_no_headers():
    assert mcp_server._trusted_client_ip(_Req({})) == "203.0.113.9"


def test_no_client_yields_sentinel():
    assert mcp_server._trusted_client_ip(_Req({}, client_host=None)) == "0.0.0.0"
