"""Small stdlib HTTP transport; every operation is a paced, bounded GET."""

from __future__ import annotations

import email.utils
import http.client
import json
import math
import ssl
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from ._version import __version__


class APIError(Exception):
    def __init__(self, status: int | None, message: str):
        super().__init__(message)
        self.status = status
        self.message = message

    def to_dict(self) -> dict:
        return {"status": self.status, "message": self.message}


class Client:
    """Read-only client. Request starts are at least 200 ms apart per instance.

    Retry-After is respected for up to 30 seconds. A longer requested delay
    fails the request instead of retrying earlier than the server permits.
    HTTP 429/502/503/504 and network failures receive at most ``retries`` retries.
    """

    def __init__(self, base_url: str = "https://mcp.opencaselaw.ch",
                 timeout: float = 30, retries: int = 2, *, opener=None,
                 sleep=None, monotonic=None, wall_time=None, log=None):
        parsed = urlsplit(base_url)
        if (parsed.scheme not in {"http", "https"} or not parsed.netloc
                or parsed.username or parsed.password or parsed.query
                or parsed.fragment or parsed.path not in {"", "/"}):
            raise ValueError("base URL must be an HTTP(S) origin without credentials or a path")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a finite positive number")
        if not isinstance(retries, int) or not 0 <= retries <= 5:
            raise ValueError("retries must be between 0 and 5")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self._open = opener or urlopen
        self._sleep = sleep or time.sleep
        self._monotonic = monotonic or time.monotonic
        self._wall_time = wall_time or time.time
        self._last_start: float | None = None
        self._lock = threading.Lock()  # pacing is shared by concurrent workers
        self._log = log  # callable(str) for --verbose request lines, or None
        self.requests = 0  # every request start, retries included

    def _pace(self):
        with self._lock:
            if self._last_start is not None:
                delay = 0.2 - (self._monotonic() - self._last_start)
                if delay > 0:
                    self._sleep(delay)
            self._last_start = self._monotonic()

    def _retry_delay(self, headers, attempt: int) -> float:
        value = headers.get("Retry-After") if headers else None
        if value is not None:
            try:
                delay = float(value)
            except (ValueError, TypeError):
                try:
                    delay = email.utils.parsedate_to_datetime(value).timestamp() - self._wall_time()
                except (ValueError, TypeError, OverflowError):
                    delay = 2 ** attempt
            if math.isfinite(delay):
                return max(0, delay)
        return min(2 ** attempt, 8)

    def get(self, path: str, params: dict | None = None) -> dict:
        if (not path.startswith("/api/") and path != "/health") or "?" in path or "#" in path:
            raise ValueError("GET path must start with /api/; pass query parameters separately")
        query = {k: str(v).lower() if isinstance(v, bool) else v
                 for k, v in (params or {}).items() if v is not None}
        url = self.base_url + path + ("?" + urlencode(query, doseq=True) if query else "")
        request = Request(url, headers={"Accept": "application/json", "User-Agent": f"opencaselaw-cli/{__version__}"})
        for attempt in range(self.retries + 1):
            self._pace()
            started = self._monotonic()
            with self._lock:
                self.requests += 1
            try:
                with self._open(request, timeout=self.timeout) as response:
                    raw = response.read()
                if self._log:
                    self._log(f"GET {url} 200 {len(raw)}B {1000 * (self._monotonic() - started):.0f}ms")
                try:
                    result = json.loads(raw)
                except (ValueError, UnicodeError) as exc:
                    raise APIError(None, "API returned invalid JSON") from exc
                if not isinstance(result, dict):
                    raise APIError(None, "API returned JSON that is not an object")
                return result
            except HTTPError as exc:
                try:
                    body = json.loads(exc.read())
                    message = body.get("detail", body.get("error", exc.reason)) if isinstance(body, dict) else exc.reason
                    if not isinstance(message, str):
                        message = json.dumps(message, ensure_ascii=False)
                except (ValueError, UnicodeError):
                    message = str(exc.reason)
                finally:
                    exc.close()
                error = APIError(exc.code, str(message))
                if self._log:
                    self._log(f"GET {url} {exc.code} {1000 * (self._monotonic() - started):.0f}ms attempt {attempt + 1}")
                delay = self._retry_delay(exc.headers, attempt)
                if exc.code not in {429, 502, 503, 504} or attempt == self.retries or delay > 30:
                    raise error from exc
                self._sleep(delay)
            except (URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                # A certificate failure is deterministic: retrying only delays the answer.
                reason = getattr(exc, "reason", exc)
                if isinstance(exc, ssl.SSLCertVerificationError) or isinstance(reason, ssl.SSLCertVerificationError):
                    raise APIError(None, "TLS certificate verification failed: " + str(reason)) from exc
                # HTTPException covers IncompleteRead, BadStatusLine and LineTooLong,
                # which urllib raises from a broken response rather than as URLError.
                if attempt == self.retries:
                    raise APIError(None, "Request failed: " + str(getattr(exc, "reason", exc))) from exc
                self._sleep(min(2 ** attempt, 8))
        raise AssertionError("unreachable")
