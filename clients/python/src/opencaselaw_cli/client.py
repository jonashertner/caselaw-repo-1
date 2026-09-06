"""Small stdlib HTTP transport: paced, bounded GETs to the REST API and JSON-RPC
calls to the service's MCP tools over the same origin. An optional on-disk cache
keyed by the server's database generation makes repeated calls free."""

from __future__ import annotations

import email.utils
import hashlib
import http.client
import json
import math
import os
import ssl
import threading
import time
from pathlib import Path
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
                 sleep=None, monotonic=None, wall_time=None, log=None, cache_dir=None):
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
        # Response cache: opt-in, keyed by URL (+ body for tool calls) and the
        # server's database generation, so a nightly rebuild invalidates it.
        self.cache_dir = Path(cache_dir).expanduser() if cache_dir else None
        self.cache_hits = 0
        self._generation: str | None = None

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
        return self._cached(url, None, lambda: self._request(request, url, "GET"), cacheable=path != "/health")

    def post_json(self, path: str, payload: dict) -> dict:
        """POST a JSON body: the MCP endpoint at the origin root, or an /api/ route."""
        if path not in ("/", "") and not path.startswith("/api/"):
            raise ValueError("POST goes to / (MCP) or an /api/ route")
        url = self.base_url + (path if path.startswith("/api/") else "/")
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(url, data=body, method="POST",
                          headers={"Accept": "application/json, text/event-stream", "Content-Type": "application/json",
                                   "User-Agent": f"opencaselaw-cli/{__version__}"})
        return self._cached(url, body, lambda: self._request(request, url, "POST"), cacheable=True)

    # ── MCP tools over the same origin ──────────────────────────────────

    def mcp_tools(self) -> list[dict]:
        """Every tool the service advertises, with input and output schemas."""
        response = self.post_json("/", {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
        tools = (response.get("result") or {}).get("tools")
        if not isinstance(tools, list):
            raise APIError(None, "tools/list returned no tool list")
        return tools

    def tool_json(self, name: str, arguments: dict | None = None) -> dict:
        """The tool's own dict from POST /api/tool/{name} (servers from 2026-09-06); on an
        older server (404 for the route) the MCP call's structured content or text."""
        if not name or not isinstance(name, str):
            raise ValueError("tool name required")
        try:
            return self.post_json("/api/tool/" + name, dict(arguments or {}))
        except APIError as error:
            if error.status != 404 or "Unknown tool" in (error.message or ""):
                raise
        result = self.mcp_call(name, arguments)
        structured = result.get("structuredContent")
        if isinstance(structured, dict):
            value = dict(structured)
        else:
            texts = [c.get("text") for c in result.get("content", []) if isinstance(c, dict) and c.get("type") == "text"]
            value = {"text": "\n".join(t for t in texts if isinstance(t, str))}
        value["_tool"] = name
        if result.get("isError"):
            value["_is_error"] = True
            value.setdefault("error", value.get("text") or f"{name} reported an error")
        return value

    def mcp_call(self, name: str, arguments: dict | None = None) -> dict:
        """One tools/call. Returns the JSON-RPC result: content, structuredContent, isError.
        A JSON-RPC error (unknown tool, invalid params) is an APIError with status 400."""
        if not name or not isinstance(name, str):
            raise ValueError("tool name required")
        response = self.post_json("/", {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                        "params": {"name": name, "arguments": dict(arguments or {})}})
        if response.get("error"):
            error = response["error"]
            message = error.get("message") if isinstance(error, dict) else str(error)
            raise APIError(400, f"{name}: {message}")
        result = response.get("result")
        if not isinstance(result, dict):
            raise APIError(None, "tools/call returned no result object")
        return result

    # ── cache ───────────────────────────────────────────────────────────

    def generation(self) -> str:
        """The server's database generation (one /health per client, uncached)."""
        if self._generation is None:
            url = self.base_url + "/health"
            request = Request(url, headers={"Accept": "application/json", "User-Agent": f"opencaselaw-cli/{__version__}"})
            health = self._request(request, url, "GET")
            self._generation = str(health.get("db_generation") or "")
        return self._generation

    def _cached(self, url: str, body: bytes | None, fetch, *, cacheable: bool) -> dict:
        if not (self.cache_dir and cacheable):
            return fetch()
        generation = self.generation()
        digest = hashlib.sha256(url.encode("utf-8") + b"\n" + (body or b"")).hexdigest()
        path = self.cache_dir / digest[:2] / (digest + ".json")
        try:
            cached = json.loads(path.read_text(encoding="utf-8"))
            if cached.get("generation") == generation and isinstance(cached.get("body"), dict):
                with self._lock:
                    self.cache_hits += 1
                if self._log:
                    self._log(f"cache {url}")
                return cached["body"]
        except (OSError, ValueError):
            pass
        result = fetch()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp." + str(os.getpid()))
            tmp.write_text(json.dumps({"generation": generation, "url": url, "body": result}, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, path)
        except OSError:
            pass
        return result

    # ── transport ───────────────────────────────────────────────────────

    @staticmethod
    def _parse(raw: bytes, content_type: str) -> dict:
        text = raw.decode("utf-8")
        if "text/event-stream" in (content_type or "") or text.lstrip().startswith(("event:", "data:")):
            # Streamable HTTP: the JSON-RPC message rides in data: lines
            data = "".join(line[5:].strip() for line in text.splitlines() if line.startswith("data:"))
            text = data or text
        result = json.loads(text)
        if isinstance(result, list):
            return {"items": result}  # list endpoints such as /api/courts; callers read "items"
        if not isinstance(result, dict):
            raise APIError(None, "API returned JSON that is not an object")
        return result

    def _request(self, request: Request, url: str, method: str) -> dict:
        for attempt in range(self.retries + 1):
            self._pace()
            started = self._monotonic()
            with self._lock:
                self.requests += 1
            try:
                with self._open(request, timeout=self.timeout) as response:
                    raw = response.read()
                    content_type = ""
                    try:
                        content_type = response.headers.get("Content-Type", "") or ""
                    except AttributeError:
                        pass
                if self._log:
                    self._log(f"{method} {url} 200 {len(raw)}B {1000 * (self._monotonic() - started):.0f}ms")
                try:
                    return self._parse(raw, content_type)
                except (ValueError, UnicodeError) as exc:
                    raise APIError(None, "API returned invalid JSON") from exc
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
                    self._log(f"{method} {url} {exc.code} {1000 * (self._monotonic() - started):.0f}ms attempt {attempt + 1}")
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
