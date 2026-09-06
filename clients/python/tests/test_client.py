import io
import json
from urllib.error import HTTPError, URLError

import pytest
from opencaselaw_cli.client import APIError, Client


class Clock:
    now = 0.0

    def sleep(self, seconds):
        self.now += seconds

    def monotonic(self):
        return self.now


def transport(sequence):
    clock = Clock()
    requests = []

    def opener(request, timeout):
        requests.append((request, timeout, clock.now))
        item = sequence.pop(0)
        if isinstance(item, Exception):
            raise item
        return io.BytesIO(item if isinstance(item, bytes) else json.dumps(item).encode())

    client = Client(opener=opener, sleep=clock.sleep, monotonic=clock.monotonic, wall_time=lambda: 0)
    return client, requests


def http_error(status, headers=None, body=None):
    return HTTPError("https://example.test/api/decisions", status, "failure", headers or {},
                     io.BytesIO(json.dumps(body or {"detail": "offline fixture"}).encode()))


def test_encoded_query_boolean_and_request_pacing():
    client, requests = transport([{"results": []}, {}])
    client.get("/api/decisions", {"query": "Art. 41 OR & ü", "full_text": False, "unused": None})
    client.get("/api/laws/OR")
    request, timeout, started = requests[0]
    assert request.full_url == "https://mcp.opencaselaw.ch/api/decisions?query=Art.+41+OR+%26+%C3%BC&full_text=false"
    assert timeout == 30
    assert requests[1][2] - started >= 0.2
    assert request.get_method() == "GET"


def test_retry_after_is_respected_and_retry_count_bounded():
    client, requests = transport([http_error(429, {"Retry-After": "3"}), {}])
    assert client.get("/api/decisions") == {}
    assert requests[1][2] == 3
    client, requests = transport([URLError("offline")] * 3)
    with pytest.raises(APIError, match="offline") as error:
        client.get("/api/decisions")
    assert error.value.status is None
    assert len(requests) == 3


def test_http_date_retry_after():
    client, requests = transport([http_error(503, {"Retry-After": "Thu, 01 Jan 1970 00:00:04 GMT"}), {}])
    client.get("/api/decisions")
    assert requests[1][2] == 4


@pytest.mark.parametrize("status,headers", [(404, {}), (429, {"Retry-After": "60"})])
def test_no_retry_for_permanent_errors_or_excessive_server_delay(status, headers):
    client, requests = transport([http_error(status, headers, {"detail": "specific reason"})])
    with pytest.raises(APIError) as error:
        client.get("/api/decisions")
    assert error.value.to_dict() == {"status": status, "message": "specific reason"}
    assert len(requests) == 1


def test_list_success_response_is_wrapped():
    client = Client(base_url="https://x", retries=0)
    assert client._parse(b'[{"court": "bger"}]', "application/json") == {"items": [{"court": "bger"}]}


@pytest.mark.parametrize("body", [b"<html>failure</html>", b"null"])
def test_invalid_success_response_is_explicit(body):
    client, requests = transport([body])
    with pytest.raises(APIError):
        client.get("/api/decisions")
    assert len(requests) == 1


@pytest.mark.parametrize("kwargs", [{"base_url": "https://user:pw@example.test"}, {"base_url": "https://example.test/api"},
                                        {"timeout": float("nan")}, {"timeout": 0}, {"retries": 6}])
def test_invalid_client_configuration(kwargs):
    with pytest.raises(ValueError):
        Client(**kwargs)


def test_paths_do_not_accept_arbitrary_url_or_query():
    client, requests = transport([])
    for path in ("https://example.test", "/api/decisions?secret=x", "/api/decisions#x", "/billing"):
        with pytest.raises(ValueError):
            client.get(path)
    assert not requests


def test_broken_response_is_retried_then_reported_as_api_error():
    import http.client
    client, requests = transport([http.client.IncompleteRead(b"partial"), http.client.BadStatusLine("garbage"),
                                  http.client.IncompleteRead(b"partial")])
    with pytest.raises(APIError) as failure:
        client.get("/api/decisions")
    assert failure.value.status is None and "Request failed" in failure.value.message
    assert len(requests) == 3
