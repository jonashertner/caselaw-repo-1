"""Offline mode: the verification pack (one SQLite file) answers the endpoints the
verification commands use, so `citations resolve`, `cite`, `decisions passage`,
`quotes check` and bundles run without the service and without a memo's
citations leaving the machine.

The pack carries the service's own citation strings and its indexed Erwägungen;
it carries no full texts and no search index. Anything else answers with a
clear "not available offline" error (status 200, exit 4).

`pull()` fetches the gzip in a resumable way, verifies it against the published
`.sha256` sidecar before unpacking, installs the pack atomically and records the
verification next to it (`<pack>.json`, read by `ocl pack verify` and
`ocl pack info`). A pack whose schema major version is newer than
SUPPORTED_SCHEMA_MAJOR is refused on open.

Thread safety: the batch commands resolve rows on a thread pool, so every
thread opens its own read-only connection (sqlite3 connections and cursors are
not shareable). Every failure inside the pack is raised as APIError so the
workflows record it in the row instead of dying with a traceback.
"""
from __future__ import annotations

import gzip
import hashlib
import http.client
import json
import os
import re
import sqlite3
import sys
import threading
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from ._version import __version__
from .client import APIError
from .references import docket_variants, fold_docket, label_key, parse_reference
from .statutes import local_law

PACK_URL = "https://huggingface.co/datasets/voilaj/swiss-caselaw/resolve/main/artifacts/verification_pack/latest.sqlite.gz"
SUPPORTED_SCHEMA_MAJOR = 2      # packs of schema 1 and 2 open; a newer major version is refused with an upgrade hint
PROGRESS_EVERY = 50 << 20       # bytes between progress lines while pulling
READ_TIMEOUT = 120.0            # seconds without data before a pull gives up (resumable); there is no overall limit
_CHUNK = 1 << 20                # bytes per read while downloading, hashing and unpacking
_BGE_ID = re.compile(r"^bge_(?:BGE_)?(\d{1,3})[ _]([IVab]+)[ _](\d{1,4})$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


def default_pack_dir(platform: str | None = None, environ: dict | None = None) -> Path:
    """Where `ocl pack pull` stores the pack: %LOCALAPPDATA%\\ocl on Windows, else
    $XDG_DATA_HOME/ocl (default ~/.local/share/ocl)."""
    platform = platform or sys.platform
    environ = os.environ if environ is None else environ
    if platform == "win32":
        return Path(environ.get("LOCALAPPDATA") or (Path.home() / "AppData" / "Local")) / "ocl"
    return Path(environ.get("XDG_DATA_HOME") or (Path.home() / ".local" / "share")) / "ocl"


DEFAULT_PACK_DIR = default_pack_dir()
PACK_FILENAME = "verification_pack.sqlite"


def default_pack_path() -> Path:
    return default_pack_dir() / PACK_FILENAME


def pack_uri(path: str | Path) -> str:
    """The read-only, immutable SQLite URI for a pack file (spaces, `%`, `#` and `?` survive)."""
    return _sqlite_uri(Path(path).expanduser())


def _missing(path: Path) -> ValueError:
    return ValueError(f"verification pack not found: {path}. Run `ocl pack pull` to download it "
                      "(several GB), or point --pack PATH (or OCL_PACK) at an existing pack file.")


def _sqlite_uri(path: Path, query: str = "mode=ro&immutable=1") -> str:
    """A `file:` URI SQLite opens read-only on every platform: spaces and `%` are
    percent-encoded, C:\\dir becomes /C:/dir, \\\\server\\share stays a UNC path."""
    text = str(Path(path).resolve())
    if os.name == "nt":
        text = text.replace("\\", "/")
    if not text.startswith("/"):
        text = "/" + text
    return "file://" + quote(text, safe="/:") + "?" + query


def _check_schema(meta: dict, path: Path) -> None:
    """Refuse a pack whose schema major version is above SUPPORTED_SCHEMA_MAJOR,
    naming both versions; tables a newer minor version adds are simply unused."""
    version = meta.get("schema_version")
    if version is None and "decisions" in meta:
        version = "1"  # the first packs carried the key; a pack without it is read as schema 1
    try:
        major = int(str(version).strip().split(".")[0])
    except (TypeError, ValueError):
        raise APIError(200, f"{path} is not a verification pack (meta.schema_version is {version!r}); run `ocl pack pull`") from None
    if major > SUPPORTED_SCHEMA_MAJOR:
        raise APIError(200, f"verification pack {path} has schema {version}; opencaselaw-cli {__version__} reads schema 1 to "
                            f"{SUPPORTED_SCHEMA_MAJOR}. Upgrade the client (pipx upgrade opencaselaw-cli) or install a pack built for it.")


def _read_meta(path: Path) -> dict:
    """The meta table of a pack, read-only through a connection of its own; APIError
    when the file is not a pack or is too new for this client."""
    try:
        con = sqlite3.connect(_sqlite_uri(path), uri=True)
        try:
            meta = {r[0]: r[1] for r in con.execute("SELECT key, value FROM meta")}
        finally:
            con.close()
    except sqlite3.DatabaseError as exc:
        raise APIError(200, f"{path} is not a verification pack ({exc}); run `ocl pack pull`") from None
    _check_schema(meta, path)
    return meta


class LocalClient:
    """Duck-types Client.get() for the pack; keeps the request counter and base_url."""

    offline = True

    def __init__(self, pack: str | Path, *, log=None):
        self.pack_path = Path(pack).expanduser()
        if not self.pack_path.is_file():
            raise _missing(self.pack_path)
        self.pack_path = self.pack_path.resolve()
        self._uri = pack_uri(self.pack_path)
        self._threads = threading.local()
        self._lock = threading.Lock()
        self.requests = 0
        self.cache_dir = None
        self.cache_hits = 0
        self._log = log
        try:
            self.meta = {r["key"]: r["value"] for r in self._connection().execute("SELECT key, value FROM meta")}
        except sqlite3.Error as exc:
            raise ValueError(f"{self.pack_path} is not a verification pack ({exc}); run `ocl pack pull` again") from exc
        self.base_url = self.pack_path.as_uri()
        _check_schema(self.meta, self.pack_path)

    # ── connections ─────────────────────────────────────────────────────
    def _connection(self) -> sqlite3.Connection:
        """This thread's connection, opened on first use. A pool thread that is
        reused keeps its connection; the pack is immutable, so no state is shared."""
        con = getattr(self._threads, "con", None)
        if con is None:
            con = sqlite3.connect(self._uri, uri=True)
            con.row_factory = sqlite3.Row
            self._threads.con = con
        return con

    @property
    def _con(self) -> sqlite3.Connection:  # compatibility with 0.6/0.7 callers
        return self._connection()

    def close(self) -> None:
        con = getattr(self._threads, "con", None)
        if con is not None:
            con.close()
            self._threads.con = None

    # ── dispatch ────────────────────────────────────────────────────────
    def get(self, path: str, params: dict | None = None) -> dict:
        with self._lock:
            self.requests += 1
        params = {k: v for k, v in (params or {}).items() if v is not None}
        if self._log:
            self._log(f"local {path} {params}")
        try:
            return self._dispatch(path, params)
        except APIError:
            raise
        except Exception as exc:  # noqa: BLE001 — a pack failure is a row error, never a traceback
            raise APIError(None, f"offline pack error on {path}: {type(exc).__name__}: {exc} (pack {self.pack_path}; "
                                 "if it persists, run `ocl pack pull` again)") from exc

    def _dispatch(self, path: str, params: dict) -> dict:
        if path == "/health":
            return {"status": "ok", "decisions": int(self.meta.get("decisions") or 0), "db_generation": self.meta.get("db_generation"),
                    "offline": True, "pack": str(self.pack_path), "built_at": self.meta.get("built_at")}
        if path == "/api/cite":
            return self._cite(params.get("reference") or "", params.get("language") or "de")
        if path == "/api/lookup":
            return self._lookup(params.get("q") or params.get("query") or "", int(params.get("limit") or 25))
        if path.startswith("/api/decisions/"):
            return self._decision(unquote(path[len("/api/decisions/"):]), params)
        if path.startswith("/api/erwaegung/"):
            rest = path[len("/api/erwaegung/"):]
            decision_id, _, number = rest.rpartition("/")
            return self._passage(unquote(decision_id), unquote(number))
        if path.startswith("/api/laws/"):
            return local_law(self.pack_path, unquote(path[len("/api/laws/"):]), params)  # statutes.sqlite next to the pack, see statutes.py
        raise APIError(200, f"{path} is not available offline; the verification pack holds decision metadata, "
                            "citation strings and indexed Erwägungen only. Run without --local for search, laws and tools.")

    def mcp_tools(self):
        raise APIError(200, "tools are not available offline")

    def mcp_call(self, name, arguments=None):
        raise APIError(200, f"tool {name!r} is not available offline")

    # ── records ─────────────────────────────────────────────────────────
    def _row(self, decision_id: str):
        return self._connection().execute("SELECT * FROM decisions WHERE decision_id = ?", (decision_id,)).fetchone()

    def _record(self, row, full_text: bool = False) -> dict:
        record = {k: row[k] for k in row.keys() if k != "has_full_text"}
        record["citation_string"] = record.get("citation_string_de")
        record["is_canonical"] = record.get("canonical_decision_id") in (None, record["decision_id"])
        record["offline"] = True
        if full_text:
            record["full_text"] = None
            record["note"] = "The verification pack carries no full texts; `decisions passage` serves the indexed Erwägungen."
        aliases = [r[0] for r in self._connection().execute(
            "SELECT alias_docket_norm FROM aliases WHERE canonical_decision_id = ?", (row["decision_id"],))]
        if aliases:
            record["joined_dockets"] = aliases
        return record

    def _decision(self, decision_id: str, params: dict) -> dict:
        row = self._row(decision_id)
        if row is None:
            raise APIError(404, f"Decision not found in the pack: {decision_id}")
        wants_text = str(params.get("full_text", "false")).lower() == "true"
        return self._record(row, full_text=wants_text)

    def _passage(self, decision_id: str, number: str) -> dict:
        con = self._connection()
        row = con.execute("SELECT depth, parent, text_z FROM paragraphs WHERE decision_id = ? AND e_number = ?",
                          (decision_id, number)).fetchone()
        if row is None:
            available = [r[0] for r in con.execute("SELECT e_number FROM paragraphs WHERE decision_id = ? ORDER BY e_number LIMIT 60", (decision_id,))]
            if not available:
                return {"error": f"No structured Erwägungen found for {decision_id!r} in the pack.", "text_source": "none"}
            return {"error": f"E. {number!r} not found in {decision_id!r}.", "available_e_numbers": available, "text_source": "none"}
        text = zlib.decompress(row["text_z"]).decode("utf-8")
        record = self._row(decision_id)
        strings = {}
        if record is not None:
            # decision-level strings, as the service built them; the pack holds
            # no pinpointed strings and the client composes none
            strings = {f"decision_citation_string_{lang}": record[f"citation_string_{lang}"] for lang in ("de", "fr", "it") if record[f"citation_string_{lang}"]}
        return {"decision_id": decision_id, "e_number": number, "depth": row["depth"], "parent": row["parent"], "text": text,
                "text_source": "structure_index", "offline": True, **strings,
                "_note": "Offline: the decision-level citation strings are the service's; a pinpointed string is not in the pack."}

    # ── identity ────────────────────────────────────────────────────────
    def _candidates_for(self, reference: str) -> list:
        con = self._connection()
        parsed = parse_reference(reference)
        core = parsed.core
        found = []
        row = self._row(core)
        if row:
            return [row]
        if parsed.bge_label:
            m = re.match(r"^BGE (\d{1,3}) ([IVab]+) (\d{1,4})$", parsed.bge_label)
            if m:
                vol, part, page = m.groups()
                for cid in (f"bge_BGE_{vol}_{part}_{page}", f"bge_{vol} {part} {page}", f"bge_{vol}_{part}_{page}"):
                    row = self._row(cid)
                    if row:
                        found.append(row)
                if not found:
                    for row in con.execute("SELECT * FROM decisions WHERE court IN ('bge','bge_egmr') AND docket_number = ?", (f"{vol} {part} {page}",)):
                        found.append(row)
            if found or (parsed.bge_first or not parsed.dockets):
                return found
        if parsed.primary_docket:
            for variant in docket_variants(parsed.primary_docket):
                rows = con.execute("SELECT * FROM decisions WHERE docket_number = ? OR docket_number_2 = ? ORDER BY decision_date DESC LIMIT 8",
                                   (variant, variant)).fetchall()
                found.extend(r for r in rows if r["decision_id"] not in {f["decision_id"] for f in found})
            if not found:
                key = fold_docket(parsed.primary_docket).replace(" ", "_").upper()
                for r in con.execute("SELECT canonical_decision_id FROM aliases WHERE alias_docket_norm = ? LIMIT 8", (key,)).fetchall():
                    row = self._row(r[0])
                    if row:
                        found.append(row)
            scoped = [r for r in found if parsed.in_scope({"court": r["court"], "canton": r["canton"]})]
            return scoped or ([] if (parsed.courts or parsed.canton) else found)
        # a citation string of the service, verbatim
        key = label_key(core)
        for r in con.execute("SELECT * FROM decisions WHERE citation_string_de = ? OR citation_string_fr = ? OR citation_string_it = ? LIMIT 8",
                             (core, core, core)):
            found.append(r)
        return [r for r in found if key in {label_key(r["citation_string_de"]), label_key(r["citation_string_fr"]), label_key(r["citation_string_it"])}] or found

    def _cite(self, reference: str, language: str) -> dict:
        reference = reference.strip()
        if not reference:
            return {"error": "Provide a reference."}
        rows = self._candidates_for(reference)
        if not rows:
            return {"exists": False, "queried": reference, "close_matches": [], "offline": True,
                    "_note": "Not in the verification pack. The pack is a snapshot; check online before treating this as absent."}
        row = rows[0]
        record = self._record(row)
        return {"exists": True, "decision_id": row["decision_id"], "court": row["court"], "language": row["language"],
                "decision_date": row["decision_date"], "citation_string": record.get(f"citation_string_{language}") or record.get("citation_string_de"),
                "citation_string_de": record.get("citation_string_de"), "citation_string_fr": record.get("citation_string_fr"),
                "citation_string_it": record.get("citation_string_it"), "canonical_url": record.get("canonical_url"),
                "canonical_decision_id": record.get("canonical_decision_id"), "is_canonical": record.get("is_canonical"),
                "joined_dockets": record.get("joined_dockets"), "offline": True,
                "ambiguous_candidates": [r["decision_id"] for r in rows[1:]] or None}

    def _lookup(self, q: str, limit: int) -> dict:
        rows = self._candidates_for(q)
        results = [{"decision_id": r["decision_id"], "docket_number": r["docket_number"], "court": r["court"], "canton": r["canton"],
                    "decision_date": r["decision_date"], "citation": r["citation_string_de"]} for r in rows[:limit]]
        return {"query": q, "is_case_number": bool(parse_reference(q).dockets), "exact": True, "total": len(results), "results": results, "offline": True}


# ── pull, verify ────────────────────────────────────────────────────────
class PackIntegrityError(ValueError):
    """The download cannot be trusted: no published checksum, a malformed one, a
    digest mismatch or a broken gzip. Exit 2; the partial file is kept."""


_HEADERS = {"User-Agent": f"opencaselaw-cli/{__version__}", "Accept-Encoding": "identity"}


def local_source_path(url: str, platform: str | None = None) -> Path | None:
    """The local file a `file:` URL, a drive path or a UNC path names; None for http(s).

    Accepted: file:///C:/packs/x.gz, file:///home/u/x.gz, the share spellings
    file://server/share/x.gz and file:////server/share/x.gz (Windows only:
    \\\\server\\share\\x.gz), and plain paths such as D:\\packs\\x.gz or
    \\\\server\\share\\x.gz. urllib's own file handler refuses non-local hosts and
    cannot resume, so local sources are opened directly.
    """
    platform = platform or sys.platform
    parts = urlsplit(url)
    if parts.scheme in ("http", "https"):
        return None
    if parts.scheme == "file":
        host, path = parts.netloc, unquote(parts.path)
        unc = None
        if host and host.lower() != "localhost":
            unc = f"//{host}{path}"
        elif path.startswith("//") and not re.match(r"^/+[A-Za-z]:", path):
            unc = "//" + path.lstrip("/")
        if unc is not None:
            if platform != "win32":
                raise ValueError(f"{url} names a Windows share; on this system mount the share and pass the mounted path "
                                 "(file:///mnt/share/latest.sqlite.gz)")
            return Path(unc.replace("/", "\\"))
        if platform == "win32":
            if re.match(r"^/+[A-Za-z]:", path):
                path = path.lstrip("/")
            return Path(path.replace("/", "\\"))
        return Path(path)
    if parts.scheme == "" or len(parts.scheme) == 1:      # a plain path; a one-letter scheme is a drive letter
        return Path(url)
    raise ValueError(f"unsupported pack source {url!r}: use https://, file:// or a path to a .sqlite.gz")


def checksum_url(url: str) -> str:
    """Where the sha256sum-format sidecar of a pack URL lives: <url>.sha256 (before any query string)."""
    parts = urlsplit(url)
    if parts.scheme in ("http", "https"):
        return urlunsplit((parts.scheme, parts.netloc, parts.path + ".sha256", parts.query, ""))
    return url + ".sha256"


def _parse_digest(text: str, where: str) -> str:
    """The hex digest of a sha256sum line ("<hex>  <name>"); a bare digest is accepted too."""
    token = text.split()[0].lower() if text.split() else ""
    if not _HEX64.match(token):
        raise PackIntegrityError(f"malformed checksum file at {where}: expected a sha256sum line (64 hex characters, two spaces, file name)")
    return token


def _published_digest(url: str, *, opener, timeout: float) -> tuple[str | None, str]:
    """(digest, checksum url); digest None when nothing is published there (HTTP 404, missing file)."""
    where = checksum_url(url)
    local = local_source_path(where)
    if local is not None:
        try:
            text = local.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            return None, where
        return _parse_digest(text, where), where
    request = Request(where, headers=_HEADERS)
    try:
        with (opener or urlopen)(request, timeout=timeout) as response:
            text = response.read(4096).decode("utf-8", "replace")
    except HTTPError as exc:
        if exc.code == 404:
            return None, where
        raise APIError(exc.code, f"checksum fetch failed: HTTP {exc.code} for {where}") from None
    except URLError as exc:
        raise APIError(None, f"checksum fetch failed: {exc.reason} ({where})") from None
    except (OSError, http.client.HTTPException) as exc:  # a stalled or reset connection before any headers
        raise APIError(None, f"checksum fetch failed: {type(exc).__name__}: {exc} ({where})") from None
    return _parse_digest(text, where), where


class _FileSource:
    """A local-file source with the surface the download loop uses: status, headers, read, context manager."""

    def __init__(self, path: Path, offset: int):
        try:
            size = path.stat().st_size
            self._fp = open(path, "rb")
        except FileNotFoundError:
            raise ValueError(f"no pack at {path}") from None
        self._fp.seek(offset)
        self.status = 206 if offset else 200
        self.headers = {"Content-Length": str(size - offset)}
        if offset:
            self.headers["Content-Range"] = f"bytes {offset}-{size - 1}/{size}"

    def read(self, n: int) -> bytes:
        return self._fp.read(n)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._fp.close()
        return False


def _open_download(url: str, offset: int, validator: str | None, *, opener, timeout: float):
    """The response for `url` from byte `offset`; None when the source has nothing beyond it (HTTP 416)."""
    local = local_source_path(url)
    if local is not None:
        if offset and (not local.is_file() or offset >= local.stat().st_size):
            return None
        return _FileSource(local, offset)
    request = Request(url, headers=_HEADERS)
    if offset:
        request.add_header("Range", f"bytes={offset}-")
        if validator:
            request.add_header("If-Range", validator)
    try:
        return (opener or urlopen)(request, timeout=timeout)
    except HTTPError as exc:
        if exc.code == 416 and offset:
            return None
        if exc.code == 404:
            raise ValueError(f"no pack at {url} (HTTP 404)") from None
        raise APIError(exc.code, f"download failed: HTTP {exc.code} for {url}") from None
    except URLError as exc:
        raise APIError(None, f"download failed: {exc.reason} ({url})") from None
    except (OSError, http.client.HTTPException) as exc:  # a stalled or reset connection before any headers
        raise APIError(None, f"download failed: {type(exc).__name__}: {exc} ({url}); run `ocl pack pull` again to resume") from None


def _content_range_start(value) -> int | None:
    m = re.match(r"^\s*bytes\s+(\d+)-(\d+)?/(\d+|\*)\s*$", str(value or ""))
    return int(m.group(1)) if m else None


def _total_size(headers, status: int, offset: int) -> int | None:
    m = re.match(r"^\s*bytes\s+\d+-\d+/(\d+)\s*$", str(headers.get("Content-Range") or ""))
    if status == 206 and m:
        return int(m.group(1))
    length = str(headers.get("Content-Length") or "")
    if length.isdigit():
        return (offset if status == 206 else 0) + int(length)
    return None


def _human(n: int) -> str:
    return f"{n / 1e9:.2f} GB" if n >= 1e9 else f"{n / 1e6:.1f} MB" if n >= 1e6 else f"{n / 1e3:.0f} kB"


def _progress(done: int, total: int | None) -> str:
    if total:
        return f"downloaded {_human(done)} of {_human(total)} ({min(100, 100 * done // total)}%)"
    return f"downloaded {_human(done)}"


def _read_json(path: Path) -> dict | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _write_json(path: Path, value: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fp:
        while True:
            chunk = fp.read(_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _download(url: str, part: Path, offset: int, state: dict, state_path: Path, *, opener, timeout: float, log) -> tuple[int, int | None]:
    """Append the rest of `url` to `part` from byte `offset`; returns (bytes received now, total size or None).
    A full (200) answer to a resume request means the source ignored Range or the file changed: start over."""
    response = _open_download(url, offset, state.get("validator") if offset else None, opener=opener, timeout=timeout)
    if response is None:                    # 416: the part already holds everything the source has
        return 0, offset
    with response:
        status = getattr(response, "status", None) or 200
        headers = response.headers
        if offset and status == 206 and _content_range_start(headers.get("Content-Range")) != offset:
            if log:
                log("the source resumed from a different offset; starting over")
            return _download(url, part, 0, state, state_path, opener=opener, timeout=timeout, log=log)
        if offset and status != 206:
            if log:
                log("the source did not resume (no Range support, or the pack changed); starting over")
            offset = 0
        total = _total_size(headers, status, offset)
        etag = headers.get("ETag") or headers.get("Etag")
        validator = etag if etag and etag.startswith('"') else headers.get("Last-Modified")
        state.update(validator=validator, total=total)
        _write_json(state_path, state)
        if log and not offset:
            log(f"downloading {_human(total)}" if total else "downloading (size unknown)")
        received = 0
        next_mark = PROGRESS_EVERY
        with open(part, "ab" if offset else "wb") as out:
            try:
                while True:
                    chunk = response.read(_CHUNK)
                    if not chunk:
                        break
                    out.write(chunk)
                    received += len(chunk)
                    if log and received >= next_mark:
                        next_mark += PROGRESS_EVERY
                        log(_progress(offset + received, total))
            except (OSError, http.client.HTTPException) as exc:
                raise APIError(None, f"download interrupted ({exc.__class__.__name__}: {exc}); {offset + received:,} bytes kept in {part}; "
                                     "run `ocl pack pull` again to resume") from exc
        if total and offset + received < total:
            raise APIError(None, f"download ended early ({offset + received:,} of {total:,} bytes kept in {part}); run `ocl pack pull` again to resume")
        if log and received:
            log(_progress(offset + received, total))
        return received, total


def _unpack(part: Path, tmp_db: Path) -> str:
    """gunzip `part` into `tmp_db`; returns the sha256 of the unpacked bytes. A broken gzip raises PackIntegrityError."""
    digest = hashlib.sha256()
    try:
        with gzip.open(part, "rb") as src, open(tmp_db, "wb") as dst:
            while True:
                chunk = src.read(_CHUNK)
                if not chunk:
                    break
                dst.write(chunk)
                digest.update(chunk)
    except (OSError, EOFError, zlib.error) as exc:
        tmp_db.unlink(missing_ok=True)
        raise PackIntegrityError(f"{part} is not a complete gzip file ({exc}); the next `ocl pack pull` starts over") from None
    return digest.hexdigest()


def pull(destination: Path | None = None, *, url: str = PACK_URL, opener=None, log=None, insecure: bool = False,
         read_timeout: float = READ_TIMEOUT) -> dict:
    """Download the pack gzip to <destination>.gz.part (resuming an earlier attempt), verify it
    against the published .sha256, unpack it to <destination>.tmp, install it with os.replace and
    record the verification in <destination>.json.

    Without a published checksum the pull fails (exit 2) unless `insecure` is set. A digest
    mismatch keeps the .part for inspection and marks it so the next pull starts over.
    `read_timeout` bounds each socket read, not the whole transfer.
    """
    destination = Path(destination).expanduser() if destination else default_pack_dir() / "verification_pack.sqlite"
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".gz.part")
    state_path = part.with_name(part.name + ".json")
    tmp_db = destination.with_name(destination.name + ".tmp")
    record_path = destination.with_name(destination.name + ".json")
    started = time.monotonic()
    expected, where = _published_digest(url, opener=opener, timeout=read_timeout)
    if expected is None:
        if not insecure:
            raise PackIntegrityError(f"no checksum published at {where}; refusing to install an unverified pack. Pass --insecure to continue "
                                     "without verification, or point --url at a mirror that publishes the .sha256 sidecar.")
        if log:
            log(f"warning: no checksum published at {where}; continuing unverified (--insecure)")
    earlier = _read_json(state_path) or {}
    offset = part.stat().st_size if part.is_file() else 0
    if offset:
        reason = ("it came from another source" if earlier.get("url") != url else
                  "its checksum failed" if earlier.get("restart") else
                  "the pack was republished" if expected and earlier.get("expected") and earlier["expected"] != expected else None)
        if reason:
            if log:
                log(f"discarding the earlier partial download ({reason})")
            offset = 0
        elif log:
            log(f"resuming from {_human(offset)} in {part.name}")
    state = {"url": url, "expected": expected, "validator": earlier.get("validator") if offset else None,
             "total": earlier.get("total") if offset else None, "restart": False, "started_at": datetime.now(timezone.utc).isoformat()}
    _write_json(state_path, state)
    if log:
        log(f"source {url}")
    received, total = _download(url, part, offset, state, state_path, opener=opener, timeout=read_timeout, log=log)
    size = part.stat().st_size
    if log:
        log(f"downloaded {_human(size)} in {time.monotonic() - started:.0f} s; computing sha256")
    actual = _sha256_file(part)
    if expected is not None and actual != expected:
        state.update(restart=True, reason="checksum mismatch")
        _write_json(state_path, state)
        raise PackIntegrityError(f"checksum mismatch: {where} says {expected}, the download is {actual}. Kept {part} for inspection; "
                                 "the next `ocl pack pull` starts over (the pack may have been republished during the download).")
    if log:
        log(("checksum verified" if expected else "checksum not verified (--insecure)") + "; unpacking")
    try:
        pack_digest = _unpack(part, tmp_db)
    except PackIntegrityError:
        state.update(restart=True, reason="broken gzip")
        _write_json(state_path, state)
        raise
    try:
        meta = _read_meta(tmp_db)           # refuse a pack this client cannot read before touching the installed one
    except APIError:
        tmp_db.unlink(missing_ok=True)
        raise
    try:
        os.replace(tmp_db, destination)
    except PermissionError as exc:
        tmp_db.unlink(missing_ok=True)
        raise OSError(f"cannot replace {destination} ({exc}); close other ocl processes that use the pack and run `ocl pack pull` again: "
                      f"the verified download is kept in {part}") from None
    except OSError as exc:
        tmp_db.unlink(missing_ok=True)
        raise OSError(f"cannot install the pack at {destination} ({exc}); the verified download is kept in {part}") from None
    size_db = destination.stat().st_size
    record = {"pack": destination.name, "bytes": size_db, "pack_sha256": pack_digest, "gzip_sha256": actual, "gzip_bytes": size,
              "verified": expected is not None, "checksum_url": where if expected is not None else None, "source_url": url,
              "pulled_at": datetime.now(timezone.utc).isoformat(), "client_version": __version__,
              **{k: meta.get(k) for k in ("schema_version", "built_at", "db_generation", "decisions", "paragraphs")}}
    _write_json(record_path, record)
    part.unlink(missing_ok=True)
    state_path.unlink(missing_ok=True)
    if log:
        log(f"installed {destination} ({_human(size_db)})")
    return {"pack": str(destination), "bytes": size_db, "downloaded_bytes": received, "gzip_sha256": actual, "verified": expected is not None,
            "source_url": url, "seconds": round(time.monotonic() - started, 1),
            **{k: meta.get(k) for k in ("db_generation", "built_at", "decisions", "paragraphs", "schema_version")}}


def pack_report(path: Path | str) -> dict:
    """What `ocl pack info` and `ocl pack verify` print: the pack's meta (schema version,
    build date, counts) and the verification pull() recorded in <pack>.json."""
    path = Path(path).expanduser()
    if not path.is_file():
        return {"pack": str(path), "installed": False, "hint": "run `ocl pack pull`"}
    meta = _read_meta(path)
    size = path.stat().st_size
    report = {"pack": str(path), "installed": True, "bytes": size, **meta}
    record = _read_json(path.with_name(path.name + ".json"))
    if not record:
        report.update(verified=None, verification=f"no pull record ({path.name}.json) next to the pack: pulled by an older client or copied "
                                                  "by hand; run `ocl pack pull` to verify against the published checksum")
        return report
    report.update({k: record.get(k) for k in ("verified", "gzip_sha256", "pack_sha256", "source_url", "checksum_url", "pulled_at", "client_version")})
    if record.get("bytes") not in (None, size):
        report.update(verified=False, verification=f"the pack changed since the pull ({record['bytes']:,} bytes then, {size:,} now)")
    elif record.get("verified"):
        report["verification"] = f"gzip sha256 matched {record.get('checksum_url')} when pulled"
    else:
        report["verification"] = "pulled with --insecure: no published checksum was compared"
    return report
