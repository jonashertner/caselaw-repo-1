"""Offline mode: the verification pack (one SQLite file) answers the endpoints the
verification commands use, so `citations resolve`, `cite`, `decisions passage`,
`quotes check` and bundles run without the service and without a memo's
citations leaving the machine.

The pack carries the service's own citation strings and its indexed Erwägungen;
it carries no full texts and no search index. Anything else answers with a
clear "not available offline" error (status 200, exit 4).
"""
from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import sqlite3
import sys
import time
import zlib
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
from urllib.request import Request, urlopen

from ._version import __version__
from .client import APIError
from .references import docket_variants, fold_docket, label_key, parse_reference

PACK_URL = "https://huggingface.co/datasets/voilaj/swiss-caselaw/resolve/main/artifacts/verification_pack/latest.sqlite.gz"
DEFAULT_PACK_DIR = Path(os.environ.get("XDG_DATA_HOME") or (Path.home() / ".local" / "share")) / "ocl"
_BGE_ID = re.compile(r"^bge_(?:BGE_)?(\d{1,3})[ _]([IVab]+)[ _](\d{1,4})$")


class LocalClient:
    """Duck-types Client.get() for the pack; keeps the request counter and base_url."""

    def __init__(self, pack: str | Path, *, log=None):
        self.pack_path = Path(pack).expanduser()
        if not self.pack_path.is_file():
            raise ValueError(f"verification pack not found: {self.pack_path} (run `ocl pack pull`)")
        self._con = sqlite3.connect(f"file:{self.pack_path}?mode=ro&immutable=1", uri=True, check_same_thread=False)
        self._con.row_factory = sqlite3.Row
        self.meta = {r["key"]: r["value"] for r in self._con.execute("SELECT key, value FROM meta")}
        self.base_url = f"file://{self.pack_path}"
        self.requests = 0
        self.cache_dir = None
        self.cache_hits = 0
        self._log = log

    # ── dispatch ────────────────────────────────────────────────────────
    def get(self, path: str, params: dict | None = None) -> dict:
        self.requests += 1
        params = {k: v for k, v in (params or {}).items() if v is not None}
        if self._log:
            self._log(f"local {path} {params}")
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
        raise APIError(200, f"{path} is not available offline; the verification pack holds decision metadata, "
                            "citation strings and indexed Erwägungen only. Run without --local for search, laws and tools.")

    def mcp_tools(self):
        raise APIError(200, "tools are not available offline")

    def mcp_call(self, name, arguments=None):
        raise APIError(200, f"tool {name!r} is not available offline")

    # ── records ─────────────────────────────────────────────────────────
    def _row(self, decision_id: str):
        return self._con.execute("SELECT * FROM decisions WHERE decision_id = ?", (decision_id,)).fetchone()

    def _record(self, row, full_text: bool = False) -> dict:
        record = {k: row[k] for k in row.keys() if k != "has_full_text"}
        record["citation_string"] = record.get("citation_string_de")
        record["is_canonical"] = record.get("canonical_decision_id") in (None, record["decision_id"])
        record["offline"] = True
        if full_text:
            record["full_text"] = None
            record["note"] = "The verification pack carries no full texts; `decisions passage` serves the indexed Erwägungen."
        aliases = [r[0] for r in self._con.execute(
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
        row = self._con.execute("SELECT depth, parent, text_z FROM paragraphs WHERE decision_id = ? AND e_number = ?",
                                (decision_id, number)).fetchone()
        if row is None:
            available = [r[0] for r in self._con.execute("SELECT e_number FROM paragraphs WHERE decision_id = ? ORDER BY e_number LIMIT 60", (decision_id,))]
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
                    for row in self._con.execute("SELECT * FROM decisions WHERE court IN ('bge','bge_egmr') AND docket_number = ?", (f"{vol} {part} {page}",)):
                        found.append(row)
            if found or (parsed.bge_first or not parsed.dockets):
                return found
        if parsed.primary_docket:
            for variant in docket_variants(parsed.primary_docket):
                rows = self._con.execute("SELECT * FROM decisions WHERE docket_number = ? OR docket_number_2 = ? ORDER BY decision_date DESC LIMIT 8",
                                         (variant, variant)).fetchall()
                found.extend(r for r in rows if r["decision_id"] not in {f["decision_id"] for f in found})
            if not found:
                key = fold_docket(parsed.primary_docket).replace(" ", "_").upper()
                for r in self._con.execute("SELECT canonical_decision_id FROM aliases WHERE alias_docket_norm = ? LIMIT 8", (key,)):
                    row = self._row(r[0])
                    if row:
                        found.append(row)
            scoped = [r for r in found if parsed.in_scope({"court": r["court"], "canton": r["canton"]})]
            return scoped or ([] if (parsed.courts or parsed.canton) else found)
        # a citation string of the service, verbatim
        key = label_key(core)
        for r in self._con.execute("SELECT * FROM decisions WHERE citation_string_de = ? OR citation_string_fr = ? OR citation_string_it = ? LIMIT 8",
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


def pull(destination: Path | None = None, *, url: str = PACK_URL, opener=None, log=None) -> dict:
    """Download the pack (gzip) and unpack it to the destination file."""
    destination = Path(destination).expanduser() if destination else DEFAULT_PACK_DIR / "verification_pack.sqlite"
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_gz = destination.with_name(destination.name + ".gz.part")
    tmp_db = destination.with_name(destination.name + ".part")
    request = Request(url, headers={"User-Agent": f"opencaselaw-cli/{__version__}"})
    started = time.monotonic()
    total = 0
    with (opener or urlopen)(request, timeout=120) as response, open(tmp_gz, "wb") as out:
        length = response.headers.get("Content-Length") if hasattr(response, "headers") else None
        expected = int(length) if length and str(length).isdigit() else None
        while True:
            chunk = response.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)
            total += len(chunk)
            if log and total % (64 << 20) < (1 << 20):
                log(f"downloaded {total / 1e9:.2f} GB" + (f" of {expected / 1e9:.2f} GB" if expected else ""))
    with gzip.open(tmp_gz, "rb") as src, open(tmp_db, "wb") as dst:
        shutil.copyfileobj(src, dst, 1 << 20)
    tmp_gz.unlink()
    os.replace(tmp_db, destination)
    client = LocalClient(destination)
    return {"pack": str(destination), "bytes": destination.stat().st_size, "downloaded_bytes": total,
            "seconds": round(time.monotonic() - started, 1), **{k: client.meta.get(k) for k in ("db_generation", "built_at", "decisions", "paragraphs", "schema_version")}}
