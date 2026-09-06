"""Portable evidence collections and conservative citation resolution.

Only public read operations are used. JSON files preserve service results, not
original court response bytes. A saved bundle is replayable evidence; it is
not a promise that a future query against the live corpus will return it.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

from . import __version__
from .client import APIError
from .references import (docket_in_reference, docket_variants, fold_docket, label_key, normalise_pinpoint,
                         parse_reference, pinpoint_parent)

SCHEMA_VERSION = "1.0.0"
# The server ranks a text query over one bounded candidate pool sized from the
# requested window; pages of such a query are not composable. 800 is the
# server's compact page cap, so one request covers the whole bound.
RANKED_MAX_RESULTS = 800
LOOKUP_WINDOW = 25
_LAW_LANGUAGES = ("de", "fr", "it")
DEFAULT_JOBS = 4
# Five transport failures in a row (no HTTP status) mean the service or the
# network is down; stop instead of burning the retry budget on every item.
BREAKER_THRESHOLD = 5
_LAW_SPEC = re.compile(r"^(?:(?P<canton>[A-Za-z]{2})/)?(?P<abbr>[^:/]+):(?P<article>[^:]+)$")


def _now():
    return datetime.now(timezone.utc).isoformat()


def _progress(message: str | None) -> None:
    """One updating status line on stderr for a person watching; nothing when piped."""
    try:
        if not sys.stderr.isatty():
            return
    except (AttributeError, ValueError):
        return
    if message is None:
        sys.stderr.write("\r\x1b[2K")
    else:
        sys.stderr.write("\r\x1b[2K" + message[:120])
    sys.stderr.flush()


def _bytes(value):
    return (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")


def _slug(identifier):
    """Readable, filesystem-safe file stem: the identifier itself where possible."""
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", identifier).strip("._-")[:80]
    return slug or "item"


def _safe_file(directory, relative):
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ValueError("Invalid bundle artifact path")
    candidate = directory / path
    if candidate.is_symlink() or not candidate.resolve().is_relative_to(directory.resolve()):
        raise ValueError("Bundle artifacts must be regular files inside the bundle")
    return candidate


def _artifact(directory, relative, value, *, text=False):
    path = _safe_file(directory, relative)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = value.encode("utf-8") if text else _bytes(value)
    # Existing evidence is immutable; failed attempts do not create artifacts.
    with path.open("xb") as stream:
        stream.write(raw)
    return {"path": relative, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
            "recorded_at": _now()}


def _checkpoint(directory, manifest):
    manifest["updated_at"] = _now()
    temporary = directory / (".manifest-" + uuid4().hex + ".json")
    with temporary.open("xb") as stream:
        stream.write(_bytes(manifest))
    temporary.replace(directory / "manifest.json")


def _validate_saved(directory, manifest):
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("kind") != "opencaselaw-research-bundle":
        raise ValueError("Unsupported bundle manifest; create a new output directory")
    for item in manifest.get("artifacts", []):
        path = _safe_file(directory, item["path"])
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError("Saved evidence changed or is missing: " + item["path"])


def _jobs(args) -> int:
    value = getattr(args, "jobs", None) or DEFAULT_JOBS
    return max(1, min(int(value), 8))


def _snapshot(client) -> dict:
    """What the service reported about the corpus when this run started. The
    database generation changes with every nightly rebuild; it identifies the
    corpus state, it is not an immutable snapshot anyone can fetch later."""
    try:
        health = client.get("/health")
    except APIError as error:
        return {"captured_at": _now(), "base_url": client.base_url, "error": error.to_dict()}
    return {"captured_at": _now(), "base_url": client.base_url,
            "db_generation": health.get("db_generation"), "decisions": health.get("decisions")}


def _get(client, path, params=None):
    result = client.get(path, params)
    if not isinstance(result, dict):
        raise APIError(None, "Expected a JSON object from the research API")
    if result.get("error"):
        # The service answered: the item does not exist or is not indexed.
        # Status 200 tells callers apart from a transport failure (None).
        error = APIError(200, str(result["error"]))
        error.response = result
        raise error
    return result


def _request(args, client):
    maximum = args.max_results
    if not 1 <= maximum <= 1000:
        raise ValueError("--max-results must be between 1 and 1000")
    ranked = bool((args.query or "").strip())
    if ranked and maximum > RANKED_MAX_RESULTS:
        raise ValueError(
            f"a text query selects from one ranked candidate pool; --max-results is limited to "
            f"{RANKED_MAX_RESULTS} for text queries (filter-only selections without query text enumerate further)")
    filters = {name: getattr(args, name, None) for name in
               ("court", "canton", "language", "date_from", "date_to")}
    filters = {key: value for key, value in filters.items() if value is not None}
    # Statutes exist in de/fr/it only; a search language such as en or rm
    # would make every statute request fail.
    law_language = filters.get("language") if filters.get("language") in _LAW_LANGUAGES else "de"
    passages = []
    for passage in getattr(args, "passage", None) or []:
        number = normalise_pinpoint(passage)
        if not number:
            raise ValueError("--passage must be an Erwägung number such as 2.3 or 3b")
        if number not in passages:
            passages.append(number)
    laws = []
    for item in getattr(args, "law", None) or []:
        match = _LAW_SPEC.match(item.strip())
        if not match or not match.group("abbr").strip() or not match.group("article").strip():
            raise ValueError("--law requires ABBREVIATION:ARTICLE for federal law or CANTON/ABBREVIATION:ARTICLE "
                             "for cantonal law, for example OR:41 or ZH/StG:1")
        law = {"abbreviation": match.group("abbr").strip(), "article": match.group("article").strip(),
               "canton": (match.group("canton") or "CH").upper()}
        if law not in laws:
            laws.append(law)
    return {"base_url": client.base_url, "query": args.query,
            "filters": filters, "max_results": maximum, "ranked_single_request": ranked,
            "passages": passages, "laws": laws, "law_language": law_language}


def _select(directory, manifest, client):
    selection = manifest["selection"]
    request = manifest["request"]
    ranked = bool(request.get("ranked_single_request"))
    while not selection["finished"]:
        offset = selection["next_offset"]
        remaining = request["max_results"] - len(selection["decision_ids"])
        params = {"q": request["query"], **request["filters"],
                  "limit": min(RANKED_MAX_RESULTS, remaining) if ranked else min(50, remaining),
                  "offset": offset, "fields": "compact", "include_pinpoint": False}
        try:
            page = _get(client, "/api/decisions", params)
            rows = page.get("results")
            if (not isinstance(rows, list) or not isinstance(page.get("has_more"), bool)
                    or not isinstance(page.get("total_is_lower_bound"), bool)):
                raise APIError(None, "Search response lacks results or pagination metadata")
            if len(rows) > params["limit"]:
                raise APIError(None, "Search response exceeds the requested page size")
            new_ids = []
            for row in rows:
                decision_id = row.get("decision_id") if isinstance(row, dict) else None
                if not isinstance(decision_id, str) or not decision_id:
                    raise APIError(None, "Search returned a result without a decision_id")
                if decision_id not in selection["decision_ids"] and decision_id not in new_ids:
                    new_ids.append(decision_id)
            next_offset = page.get("next_offset")
            if page["has_more"] and (not rows or type(next_offset) is not int or next_offset != offset + len(rows)):
                raise APIError(None, "Search pagination did not advance")
            if len(selection["pages"]) >= 1000:
                raise APIError(None, "Search exceeded the bounded page limit; narrow the query")
            artifact = _artifact(directory, f"search/page-{len(selection['pages']):04d}-{uuid4().hex}.json", page)
            manifest["artifacts"].append(artifact)
            selection["pages"].append({"request": params, "retrieved_at": _now(), **artifact})
            selection["decision_ids"].extend(new_ids[:remaining])
            selection["last_page"] = {k: v for k, v in page.items() if k != "results"}
            selection["next_offset"] = next_offset
            selection["error"] = None
            selection["max_results_reached"] = len(selection["decision_ids"]) >= request["max_results"]
            selection["finished"] = ranked or selection["max_results_reached"] or not page["has_more"]
        except APIError as error:
            selection["error"] = error.to_dict()
            manifest["attempt_errors"].append({"stage": "search", "offset": offset,
                                                "at": _now(), **error.to_dict()})
            _checkpoint(directory, manifest)
            break
        _checkpoint(directory, manifest)


def _provenance(result):
    # A server content_hash is kept as supplied, separate from local SHA-256s.
    fields = ("decision_id", "canonical_key", "canonical_url", "source_url", "pdf_url",
              "court", "decision_date", "publication_date", "scraped_at", "content_hash",
              "date_provenance", "date_is_estimated", "citation_string", "citation_string_de",
              "citation_string_fr", "citation_string_it", "sr_number", "abbreviation",
              "consolidation_date", "canton", "language", "license", "license_url", "copyright")
    return {key: result[key] for key in fields if key in result}


def _validate_passage(result, decision_id, number):
    if result.get("decision_id") != decision_id or str(result.get("e_number")) != number:
        raise APIError(None, "Returned passage does not match the requested decision and Erwägung")
    if not isinstance(result.get("text"), str) or not result["text"].strip():
        raise APIError(None, "No passage text returned")


def _fetch(client, *, kind, identifier, path, params=None):
    """Network half of an item: returns (result, None) or (None, error). No manifest access,
    so several fetches can run at once."""
    try:
        result = _get(client, path, params)
        if kind == "decision" and result.get("decision_id") != identifier:
            raise APIError(None, "Returned decision identifier does not match the selected decision")
        if kind == "passage":
            decision_id, number = identifier.rsplit(":", 1)
            _validate_passage(result, decision_id, number)
        if kind == "law":
            articles = result.get("articles")
            if not isinstance(articles, list) or not any(
                isinstance(article, dict) and str(article.get("article_num")) == params["article"]
                and isinstance(article.get("text"), str) and article["text"].strip()
                for article in articles
            ):
                raise APIError(None, "No text returned for the requested statute article")
        return result, None
    except APIError as error:
        return None, error


def _record(directory, manifest, *, kind, identifier, path, params, result, error):
    """Manifest half of an item: runs sequentially, one checkpoint per item."""
    job_key = kind + ":" + identifier
    item = {"kind": kind, "identifier": identifier, "request": {"path": path, "params": params}}
    manifest["items"][job_key] = item
    if error is not None:
        # "unavailable": the service answered that the item is not there (a
        # passage the index lacks, an unknown article); "failed": transport or
        # validation failure that a retry may fix.
        item.update(status="unavailable" if error.status in (200, 404) else "failed", error=error.to_dict())
        manifest["attempt_errors"].append({"stage": kind, "identifier": identifier,
                                            "at": _now(), **error.to_dict()})
        _checkpoint(directory, manifest)
        return
    # A resumed attempt may return a different record. Keep the earlier
    # saved response rather than overwriting it or colliding with its name.
    name = f"{kind}s/{_slug(identifier)}-{uuid4().hex[:8]}.json"
    artifact = _artifact(directory, name, result)
    manifest["artifacts"].append(artifact)
    item.update(status="saved", retrieved_at=_now(), artifact=artifact, provenance=_provenance(result))
    # Preserve exactly the served full_text/text string, including line breaks.
    text = result.get("full_text") if kind == "decision" else result.get("text") if kind == "passage" else None
    if isinstance(text, str):
        text_artifact = _artifact(directory, name[:-5] + ".txt", text, text=True)
        manifest["artifacts"].append(text_artifact)
        item["text_artifact"] = text_artifact
    if kind == "decision" and (not isinstance(result.get("full_text"), str) or not result["full_text"].strip()):
        item["status"] = "missing_text"
        item["error"] = {"status": None, "message": "Decision metadata returned without full text"}
    if kind == "passage":
        item["pinpoint"] = result.get("e_number")
        item["composed_of"] = result.get("composed_of")
    _checkpoint(directory, manifest)


def _collect_all(directory, manifest, client, specs, jobs):
    """Fetch pending items concurrently, record them in order with a checkpoint each."""
    pending = [spec for spec in specs
               if (manifest["items"].get(spec["kind"] + ":" + spec["identifier"]) or {}).get("status") not in ("saved", "unavailable")]
    if not pending:
        return
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        fetched = pool.map(lambda spec: _fetch(client, **spec), pending)
        for index, (spec, (result, error)) in enumerate(zip(pending, fetched), 1):
            _progress(f"saving {index}/{len(pending)}: {spec['identifier']}")
            _record(directory, manifest, kind=spec["kind"], identifier=spec["identifier"], path=spec["path"],
                    params=spec.get("params"), result=result, error=error)
    _progress(None)


def _item_specs(request, decision_ids, passages=None):
    specs = []
    for decision_id in decision_ids:
        encoded = quote(decision_id, safe="")
        specs.append({"kind": "decision", "identifier": decision_id, "path": "/api/decisions/" + encoded})
        for passage in (passages if passages is not None else request["passages"]):
            specs.append({"kind": "passage", "identifier": decision_id + ":" + passage,
                          "path": f"/api/erwaegung/{encoded}/{quote(passage, safe='')}"})
    return specs


def _law_specs(request):
    specs = []
    for law in request["laws"]:
        params = {"article": law["article"], "language": request.get("law_language", "de")}
        if law.get("canton", "CH") != "CH":
            params["canton"] = law["canton"]
        identifier = (f"{law['canton']}/" if law.get("canton", "CH") != "CH" else "") + law["abbreviation"] + ":" + law["article"]
        specs.append({"kind": "law", "identifier": identifier,
                      "path": "/api/laws/" + quote(law["abbreviation"], safe=""), "params": params})
    return specs


def create_bundle(args, client):
    request = _request(args, client)
    directory = Path(args.out).expanduser()
    if directory.is_symlink():
        raise ValueError("Use a regular output directory for the bundle")
    if getattr(args, "resume", False):
        try:
            manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise ValueError("--resume requires a readable existing bundle manifest") from error
        _validate_saved(directory, manifest)
        if manifest.get("request") != request:
            raise ValueError("Resume must use the original query, filters and options; use a new directory for a new collection")
    else:
        if directory.exists():
            raise ValueError("Output directory already exists; use --resume or choose a new directory")
        directory.mkdir(parents=True)
        manifest = {"schema_version": SCHEMA_VERSION, "kind": "opencaselaw-research-bundle",
                    "client_version": __version__, "created_at": _now(), "request": request,
                    "corpus_snapshot": _snapshot(client),
                    "status": "collecting", "artifacts": [], "items": {}, "attempt_errors": [],
                    "selection": {"finished": False, "pages": [], "decision_ids": [], "next_offset": 0,
                                  "error": None, "max_results_reached": False},
                    "evidence_contract": {
                        "representation": "UTF-8 JSON serialization of API responses and unmodified served text strings; not original court response bytes",
                        "artifact_hash_algorithm": "SHA-256 over the saved file bytes",
                        "server_content_hash": "Preserved as supplied; its algorithm and original-source equivalence are not asserted",
                        "corpus_snapshot": "corpus_snapshot records the service's database generation and decision count when the run started; it identifies the corpus state, it is not an immutable copy",
                        "passage_text": "Served /api/erwaegung text; cross-references inside it can carry Markdown links added by the service",
                        "replay": "Only manifest-listed artifacts form this collection. Saved artifacts can be inspected offline; a later live query can return different results",
                        "licensing": "Per-record licence metadata is preserved when supplied; absence does not grant reuse rights",
                        "legal_support": "Not assessed; retrieval or citation existence does not establish that a decision supports a legal proposition"}}
        _checkpoint(directory, manifest)
    if getattr(args, "resume", False):
        pending_before = (not manifest["selection"]["finished"]) or any(
            item.get("status") != "saved" for item in manifest["items"].values())
        expected = len(_item_specs(request, manifest["selection"]["decision_ids"])) + len(_law_specs(request))
        if pending_before or len(manifest["items"]) < expected:
            # Something is left to fetch: note which corpus state answers the rest.
            latest = _snapshot(client)
            manifest["corpus_snapshot_latest"] = latest
            if latest.get("db_generation") and manifest.get("corpus_snapshot", {}).get("db_generation") not in (None, latest["db_generation"]):
                manifest.setdefault("notes", []).append(
                    f"Resumed against database generation {latest['db_generation']}; the bundle started on "
                    f"{manifest['corpus_snapshot']['db_generation']}. Later responses can differ from earlier ones.")
    _select(directory, manifest, client)
    specs = _item_specs(request, manifest["selection"]["decision_ids"]) + _law_specs(request)
    for addition in manifest.get("additions") or []:
        # Decisions added with `bundle add` belong to the bundle; a resume retries theirs too.
        specs += _item_specs(request, addition.get("decision_ids") or [], addition.get("passages"))
    _collect_all(directory, manifest, client, specs, _jobs(args))
    _finalise(manifest, client)
    _checkpoint(directory, manifest)
    _write_index(directory, manifest)
    return {"schema_version": SCHEMA_VERSION, "status": manifest["status"],
            "bundle": str(directory.resolve()), "manifest": str((directory / "manifest.json").resolve()),
            "completeness": manifest["completeness"]}, 0 if manifest["status"] == "complete" else 4


def _finalise(manifest, client):
    """Status and completeness counters computed from the item statuses themselves."""
    items = list(manifest["items"].values())
    failed = [item for item in items if item["status"] == "failed"]
    unavailable = [item for item in items if item["status"] == "unavailable"]
    missing_text = [item for item in items if item["status"] == "missing_text"]
    selection = manifest["selection"]
    request = manifest["request"]
    complete = bool(selection["finished"] and not selection["error"] and not failed and not unavailable and not missing_text)
    manifest["status"] = "complete" if complete else "partial"
    manifest["requests"] = getattr(client, "requests", None)
    manifest["completeness"] = {"selected_items_saved": complete,
                                "exhaustive_legal_research": False,
                                "selected_decisions": len(selection["decision_ids"]),
                                "added_decisions": sum(len(entry.get("decision_ids") or []) for entry in manifest.get("additions") or []),
                                "items": len(items), "saved_items": sum(1 for item in items if item["status"] == "saved"),
                                "failed_items": len(failed), "unavailable_items": len(unavailable),
                                "missing_text_items": len(missing_text),
                                "search_error": selection["error"],
                                "max_results_reached": selection["max_results_reached"],
                                "ranked_single_request": bool(request.get("ranked_single_request")),
                                "corpus_generation": (manifest.get("corpus_snapshot") or {}).get("db_generation"),
                                "server_last_page": selection.get("last_page"),
                                "note": ("Complete means every requested item was saved; relevance-ranked search can use a capped "
                                         "candidate pool. failed_items are retried by --resume; unavailable_items are answers "
                                         "from the service that it does not have the item")}


def _write_index(directory, manifest):
    """Plain-language listing of the bundle. A convenience view regenerated on
    every run; manifest.json is the record and the only hashed inventory."""
    request = manifest["request"]
    lines = ["# Research bundle", "",
             f"Query: {request['query']!r}" + (f", filters {request['filters']}" if request.get("filters") else ""),
             f"Status: {manifest['status']} (created {manifest.get('created_at', '')[:19]} UTC, "
             f"updated {manifest.get('updated_at', '')[:19]} UTC, client {manifest.get('client_version')})",
             (f"Corpus: database generation {manifest['corpus_snapshot'].get('db_generation')} "
              f"({manifest['corpus_snapshot'].get('decisions')} decisions) at {manifest['corpus_snapshot'].get('captured_at', '')[:19]} UTC"
              if manifest.get("corpus_snapshot", {}).get("db_generation") else "Corpus: generation not recorded"),
             "", "Files are the service's responses saved as received (see manifest.json for hashes, "
             "timestamps and source links). Saved evidence is not an assessment of legal support.", ""]
    kinds = (("decision", "## Decisions"), ("passage", "## Passages (Erwägungen)"), ("law", "## Statute articles"))
    for kind, heading in kinds:
        items = [item for item in manifest["items"].values() if item["kind"] == kind]
        if not items:
            continue
        lines += [heading, ""]
        for item in items:
            provenance = item.get("provenance") or {}
            label = (provenance.get("citation_string_de") or provenance.get("citation_string")
                     or item["identifier"])
            if kind == "passage":
                # The service's passage citation already names the Erwägung;
                # a failed passage borrows its decision's label instead.
                decision_id, number = item["identifier"].rsplit(":", 1)
                parent = (manifest["items"].get("decision:" + decision_id) or {}).get("provenance") or {}
                label = (provenance.get("citation_string_de") or provenance.get("citation_string")
                         or f"{parent.get('citation_string_de') or parent.get('citation_string') or decision_id}, E. {number}")
            if kind == "law":
                label = f"{provenance.get('abbreviation') or item['identifier'].split(':')[0]} Art. {item['identifier'].rsplit(':', 1)[1]}"
                if provenance.get("consolidation_date"):
                    label += f" (consolidated {provenance['consolidation_date']})"
            files = [item["artifact"]["path"]] if item.get("artifact") else []
            if item.get("text_artifact"):
                files.append(item["text_artifact"]["path"])
            detail = f"{item['status']}"
            if provenance.get("decision_date"):
                detail += f", decided {provenance['decision_date']}"
            if item.get("error"):
                detail += f": {item['error'].get('message')}"
            lines.append(f"- {label}: {detail}" + (f" [{', '.join(files)}]" if files else ""))
        lines.append("")
    statuses = [item.get("status") for item in manifest["items"].values()]
    unavailable = statuses.count("unavailable")
    failed = statuses.count("failed")
    missing_text = statuses.count("missing_text")
    parts = []
    if unavailable:
        parts.append(f"{unavailable} item(s) the service does not have (for example a passage that is not "
                     "indexed for that decision, or an unknown decision); a rerun does not request them again")
    if failed:
        parts.append(f"{failed} item(s) failed to download; rerun with --resume")
    if missing_text:
        parts.append(f"{missing_text} decision(s) came without text; --resume requests them again")
    if parts:
        lines += ["; ".join(parts) + ".", ""]
    (directory / "INDEX.md").write_text("\n".join(lines), encoding="utf-8")


# ── identity ───────────────────────────────────────────────────────────────

_PLAIN_LINK = re.compile(r"\s*\[([^\]]+)\]\((?:https?://)[^)\s]+\)")
_BARE_NUMERIC_DOCKET = re.compile(r"^\d{1,5}/\d{4}$")

# Comparison key for labels (kept under its 0.2 name for callers).
reference_key = label_key
_reference_key = label_key


class ResolutionError(APIError):
    """A reference that cannot be tied to exactly one decision; not a transport failure."""

    def __init__(self, message: str, outcome: str, row: dict | None = None):
        super().__init__(None, message)
        self.outcome = outcome
        self.row = row or {}

    def to_dict(self) -> dict:
        return {"status": None, "kind": "resolution", "outcome": self.outcome, "message": self.message}


def extract_docket(reference: str) -> str | None:
    """The docket inside a longer reference, if any, else None."""
    parsed = parse_reference(reference or "")
    if not parsed.dockets:
        return None
    docket = parsed.dockets[0]
    return None if label_key(docket) == label_key(parsed.core) else docket


def plain_text(text):
    """Served passage text with the service's Markdown cross-reference links reduced to their labels.
    Derived for comparisons with the decision text; `text` stays the served string."""
    if not isinstance(text, str):
        return None
    return _PLAIN_LINK.sub(lambda m: " " + " ".join(m.group(1).split()), text)


def fetch_passage(client, decision_id: str, pinpoint: str):
    """One Erwägung by number. A lettered or slashed sub-number the index lacks (E. 2a,
    E. 3c/aa) falls back to its parent number so the reader can locate the letter inside.
    Returns (passage or None, status, error or None); status is retrieved, parent_retrieved
    or unavailable."""
    encoded = quote(decision_id, safe="")
    parent = pinpoint_parent(pinpoint)
    numbers = [parent] if "/" in pinpoint and parent else [pinpoint] + ([parent] if parent else [])
    error = None
    for number in numbers:
        try:
            passage = _get(client, f"/api/erwaegung/{encoded}/{quote(number, safe='')}")
            _validate_passage(passage, decision_id, number)
        except APIError as failure:
            error = failure
            if failure.status not in (200, 404):
                return None, "unavailable", failure
            continue
        if isinstance(passage.get("text"), str):
            passage["text_plain"] = plain_text(passage["text"])
        return passage, "retrieved" if number == pinpoint else "parent_retrieved", None
    return None, "unavailable", error or APIError(200, f"E. {pinpoint} is not addressable")


def _cite(client, query, language):
    response = _get(client, "/api/cite", {"reference": query, "language": language})
    if response.get("exists") not in (True, False):
        raise APIError(None, "Citation response does not say whether the decision exists")
    return response


def _labels(*records) -> set:
    keys = {label_key(record.get(name)) for record in records if isinstance(record, dict)
            for name in ("citation_string", "citation_string_de", "citation_string_fr", "citation_string_it")}
    keys.discard(None)
    return keys


def _candidate_summary(candidate: dict) -> dict:
    return {name: candidate.get(name) for name in ("decision_id", "docket_number", "court", "canton", "decision_date",
                                                   "citation", "citation_string_de", "joined_dockets", "canonical_decision_id")
            if candidate.get(name) is not None}


def _carried_labels(candidate: dict) -> set:
    """The labels a lookup hit carries: its id, its own docket, its citation and, for a
    consolidated proceeding, every joined docket the service lists for it."""
    keys = {label_key(candidate.get(name)) for name in ("decision_id", "docket_number", "citation")}
    if isinstance(candidate.get("joined_dockets"), list):
        keys.update(label_key(d) for d in candidate["joined_dockets"] if isinstance(d, str))
    keys.discard(None)
    return keys


def _carries_written_label(parsed, record) -> bool:
    """Whether a record's own docket is the label the reference writes first, or its
    citation string is the reference itself. A docket the reference only mentions
    later (a cross-reference, a joined file) does not count."""
    docket = record.get("docket_number")
    primary = parsed.primary_docket
    if primary is not None:
        return isinstance(docket, str) and fold_docket(docket) == fold_docket(primary)
    return docket_in_reference(parsed.core, docket)


def _close_match_candidates(parsed, responses):
    """Close matches that carry the label written in the reference: their own citation
    string equals it, or their docket is the one written first. The service's ranking
    plays no part."""
    core_key = label_key(parsed.core)
    found = {}
    for response in responses:
        for match in response.get("close_matches") or []:
            if not isinstance(match, dict) or not isinstance(match.get("decision_id"), str) or not match["decision_id"]:
                continue
            carried = core_key in _labels(match) or _carries_written_label(parsed, match)
            if carried and parsed.in_scope(match):
                found.setdefault(match["decision_id"], match)
    return list(found.values())


def _carriers_by_search(client, parsed, docket):
    """Decisions whose stored docket equals a bare numeric docket such as 1/2020, which /api/lookup does not index."""
    page = _get(client, "/api/decisions", {"q": f'"{docket}"', "limit": LOOKUP_WINDOW, "fields": "compact", "include_pinpoint": False})
    rows = page.get("results") if isinstance(page.get("results"), list) else []
    key = label_key(docket)
    return [_candidate_summary(row) for row in rows
            if isinstance(row, dict) and isinstance(row.get("decision_id"), str)
            and label_key(row.get("docket_number")) == key and parsed.in_scope(row)]


def _identify(client, reference, language, row, parsed=None):
    """Tie a reference to the one decision it names. Fills `row`; returns (parsed, decision),
    with decision None when the row reached a terminal status."""
    parsed = parsed or parse_reference(reference)
    responses, citation = [], None
    for query in parsed.queries():
        response = _cite(client, query, language)
        responses.append(response)
        if response["exists"] and isinstance(response.get("decision_id"), str) and response["decision_id"]:
            citation = response
            if query != reference:
                row["query"] = query
            break
    if citation is None and responses:
        candidates = _close_match_candidates(parsed, responses)
        ids = {c["decision_id"] for c in candidates}
        if len(ids) > 1:
            row.update(status="ambiguous", citation=responses[-1], candidates=[_candidate_summary(c) for c in candidates],
                       reason="More than one decision carries a label written in this reference; name the court or use an explicit decision_id")
            return parsed, None
        if len(ids) == 1:
            response = _cite(client, candidates[0]["decision_id"], language)
            if response["exists"] and response.get("decision_id") == candidates[0]["decision_id"]:
                citation = response
                row["query"] = candidates[0]["decision_id"]
                row["matched_via"] = "close_match_label"
    if citation is None:
        row["citation"] = responses[-1] if responses else None
        row.update(status="missing", note="No decision carries this label; close_matches are for the author to inspect, never substitutes")
        return parsed, None
    row["citation"] = citation
    decision_id = citation["decision_id"]
    decision = _get(client, "/api/decisions/" + quote(decision_id, safe=""), {"full_text": False})
    if decision.get("decision_id") != decision_id:
        raise APIError(None, "Decision retrieval disagrees with citation resolution")
    labels = _labels(decision, citation)
    dockets = [decision.get(name) for name in ("docket_number", "docket_number_2")
               if isinstance(decision.get(name), str) and decision.get(name).strip()]
    # A consolidated proceeding is filed under its lead docket; the record lists
    # the joined ones (joined_dockets), any of which the author may have cited.
    joined = [d for d in decision.get("joined_dockets") if isinstance(d, str) and d.strip()] \
        if isinstance(decision.get("joined_dockets"), list) else []
    core_key = label_key(parsed.core)
    primary = parsed.primary_docket
    if primary is not None:
        docket_hit = next((d for d in dockets if fold_docket(d) == fold_docket(primary)), None)
        joined_hit = next((d for d in joined if fold_docket(d) == fold_docket(primary)), None)
    else:
        docket_hit = next((d for d in dockets if docket_in_reference(parsed.core, d)), None)
        joined_hit = next((d for d in joined if docket_in_reference(parsed.core, d)), None)
    hit = docket_hit if docket_hit is not None else joined_hit
    if len(parsed.dockets) > 1:
        row["other_dockets"] = parsed.dockets[1:]
    proposed = {"decision_id": decision_id, "docket_number": decision.get("docket_number"), "court": decision.get("court"),
                **{n: citation.get(n) for n in ("citation_string_de", "citation_string_fr", "citation_string_it") if citation.get(n)}}
    if parsed.core == decision_id:
        method = "exact_canonical_id"
    elif core_key in labels or (parsed.bge_label and label_key(parsed.bge_label) in labels):
        method = "exact_server_citation"
    elif docket_hit is not None:
        method = "exact_server_docket"
    elif joined_hit is not None:
        method = "exact_server_joined_docket"
    elif primary is not None and any(fold_docket(d) in {fold_docket(o) for o in parsed.dockets[1:]} for d in dockets):
        row.update(status="unrecognized", service_candidate=proposed, identity_check={"method": "secondary_label"},
                   reason=f"The decision the service proposed carries a docket the reference only mentions after its main label {primary}; "
                          "nothing is guessed. Write that docket first if it is the one meant")
        return parsed, None
    else:
        # Last resort: the label may be a docket the service's lookup index
        # knows even though the record carries it in another form.
        method = "exact_candidate_label"
    if not parsed.in_scope(decision):
        row.update(status="unrecognized", service_candidate=proposed, identity_check={"method": "court_mismatch"},
                   reason=f"The reference names another court than {decision_id} ({decision.get('court')})")
        return parsed, None
    check = {"method": method}
    if method == "exact_server_joined_docket":
        check.update(joined_docket=joined_hit, lead_docket=decision.get("docket_number"))
    if method in ("exact_server_docket", "exact_server_joined_docket", "exact_candidate_label"):
        # A docket can be reused by another court, and the service also matches
        # fragments. exact=true returns only decisions carrying the label (older
        # servers pad the window with related cases; the label comparison filters
        # them out either way). Candidates at a court the reference does not name
        # are listed but do not make the reference ambiguous.
        label = hit if hit is not None else parsed.core
        lookup = _get(client, "/api/lookup", {"q": label, "limit": LOOKUP_WINDOW, "exact": True})
        candidates = lookup.get("results", [])
        if not isinstance(candidates, list) or any(
            not isinstance(c, dict) or not isinstance(c.get("decision_id"), str) or not c["decision_id"] for c in candidates
        ):
            raise APIError(None, "Lookup returned invalid candidate identifiers")
        label_k = label_key(label)
        carrying = [c for c in candidates if label_k in _carried_labels(c)]
        matching = [c for c in carrying if parsed.in_scope(c)]
        excluded = [c for c in carrying if not parsed.in_scope(c)]
        if not carrying and hit is not None and _BARE_NUMERIC_DOCKET.match(hit.strip()):
            matching = _carriers_by_search(client, parsed, hit)
        ids = {c["decision_id"] for c in matching} | ({decision_id} if hit is not None else set())
        row["lookup"] = lookup
        check.update(docket=label, matching_candidates=[_candidate_summary(c) for c in matching],
                     lookup_exact=bool(lookup.get("exact")), candidate_window_may_be_capped=len(candidates) >= LOOKUP_WINDOW,
                     uniqueness="verified" if matching else "unverified")
        if excluded:
            check["out_of_scope_candidates"] = [_candidate_summary(c) for c in excluded]
        if len(ids) > 1 or (ids and decision_id not in ids):
            listed = list(check["matching_candidates"])
            if hit is not None and decision_id not in {c.get("decision_id") for c in listed}:
                listed.insert(0, _candidate_summary({**decision, "citation": citation.get("citation_string")}))
            row.update(identity_check=check, status="ambiguous", candidates=listed,
                       reason="Several decisions carry this label; name the court or use an explicit decision_id")
            return parsed, None
        if len(matching) >= LOOKUP_WINDOW:
            row.update(identity_check=check, status="resolution_incomplete",
                       reason="The candidate window is full of exact matches; use a canonical citation or explicit decision_id")
            return parsed, None
        if hit is None and (not ids or not lookup.get("is_case_number")):
            row.update(status="unrecognized", service_candidate=proposed, identity_check={"method": "no_label_match", **{k: v for k, v in check.items() if k != "method"}},
                       reason="The decision the service proposed carries no label written in this reference; nothing is guessed. Write the docket or the canonical citation")
            return parsed, None
    canonical = decision.get("canonical_decision_id")
    if isinstance(canonical, str) and canonical and canonical != decision_id:
        # The same ruling is stored under another id as well, and the service
        # names that record as the canonical one. Reported, never substituted:
        # decision_id stays the record the reference resolved to.
        row["canonical_decision_id"] = canonical
    row.update(decision_id=decision_id, provenance=_provenance(decision), official_source_available=bool(decision.get("source_url")),
               identity_check=check, status="resolved")
    return parsed, decision


def identify_row(client, reference: str, language: str = "de") -> dict:
    """The resolved row (decision_id, citation, identity_check, ...) for a reference, or
    ResolutionError carrying the row that explains why there is not exactly one decision."""
    row = {}
    _identify(client, reference.strip(), language, row)
    if row.get("status") == "resolved":
        return row
    outcome = row.get("status", "error")
    message = row.get("reason") or row.get("note") or "Reference could not be resolved"
    if outcome == "missing":
        message = f"Reference not found in the corpus: {reference}"
    elif row.get("service_candidate"):
        candidate = row["service_candidate"]
        message = (f"{reference}: {message} (the service proposed {candidate['decision_id']}"
                   + (f", docket {candidate['docket_number']!r}" if candidate.get("docket_number") else "") + ")")
    raise ResolutionError(message, outcome, row)


def identify(client, reference: str, language: str = "de") -> str:
    """The decision_id a reference names, or ResolutionError explaining why there is not exactly one."""
    return identify_row(client, reference, language)["decision_id"]


def _record_carries(decision, docket) -> bool:
    """Whether a decision record itself names a docket: its own docket fields or the ECLI in canonical_key."""
    key = fold_docket(docket)
    for name in ("docket_number", "docket_number_2"):
        if isinstance(decision.get(name), str) and fold_docket(decision[name]) == key:
            return True
    canonical = decision.get("canonical_key")
    if isinstance(canonical, str) and canonical.upper().startswith("ECLI:"):
        tail = canonical.rsplit(":", 1)[-1].replace(".", "/")
        return fold_docket(tail) == key.replace(".", "/")
    return False


def _discrepancies(client, parsed, decision, language, row):
    """What the author wrote about an identified decision that the record contradicts."""
    found = []
    decided = decision.get("decision_date")
    if parsed.date and isinstance(decided, str) and decided[:10] != parsed.date and not decision.get("date_is_estimated"):
        found.append({"kind": "date", "written": parsed.date, "decision": decided[:10]})
    if parsed.bge_label and parsed.dockets:
        docket = parsed.dockets[0]
        other = None
        for variant in docket_variants(docket):
            response = _cite(client, variant, language)
            if response["exists"] and isinstance(response.get("decision_id"), str) and response["decision_id"]:
                other = response
                break
        if _record_carries(decision, docket):
            row["related_docket"] = {"docket": docket, "decision_id": decision.get("decision_id"), "verified": True,
                                     "note": "the BGE record itself names this docket"}
        elif other is None:
            row.setdefault("notes", []).append(f"the docket {docket} written with the BGE label is not in the corpus and was not checked")
        elif other["decision_id"] != decision.get("decision_id"):
            record = _get(client, "/api/decisions/" + quote(other["decision_id"], safe=""), {"full_text": False})
            other_date = record.get("decision_date")
            if isinstance(other_date, str) and isinstance(decided, str) and other_date[:10] == decided[:10]:
                # Same day is consistent with one judgment published twice, but
                # it is not proof: say so instead of affirming it.
                row["related_docket"] = {"docket": docket, "decision_id": other["decision_id"], "verified": False,
                                         "note": "a ruling of the same day; whether it is this BGE was not verified"}
                row.setdefault("notes", []).append(f"the docket {docket} names {other['decision_id']}, decided the same day as this BGE; not verified as the same judgment")
            else:
                found.append({"kind": "docket", "written": docket, "resolves_to": other["decision_id"],
                              "decision_date": other_date, "bge_decision_date": decided})
    return found


def _resolve_one(client, item, language):
    reference = item["reference"].strip()
    row = {"reference": reference, "pinpoint": None, "checked_at": _now(), "legal_support_assessed": False}
    extra = {key: value for key, value in item.items() if key not in ("reference", "pinpoint")}
    if extra:
        row["input"] = extra
    try:
        parsed = parse_reference(reference)
        try:
            explicit = normalise_pinpoint(item.get("pinpoint"))
        except ValueError as error:
            row.update(status="error", error={"status": 400, "kind": "input", "message": str(error)})
            return row
        pinpoint = explicit or parsed.pinpoint
        if pinpoint:
            row["pinpoint"] = pinpoint
            row["pinpoint_source"] = "input" if explicit else "reference"
            if explicit and parsed.pinpoint and explicit != parsed.pinpoint:
                row["note"] = f"pinpoint {explicit} from the input row is checked; the reference itself names E. {parsed.pinpoint}"
        parsed, decision = _identify(client, reference, language, row, parsed)
        if decision is None:
            return row
        discrepancies = _discrepancies(client, parsed, decision, language, row)
        if discrepancies:
            row.update(status="discrepancy", discrepancies=discrepancies,
                       reason="The decision was identified, but what the reference says about it does not match the record: "
                              + ", ".join(d["kind"] for d in discrepancies))
        if isinstance(item.get("quote"), str) and item["quote"].strip():
            row["quote"] = item["quote"]
            checked = None
            for label, text, status in _quote_sources(client, decision["decision_id"], pinpoint):
                if not text:
                    continue
                found = match_quote(item["quote"], text)
                if found["quote_status"] == "unverifiable":
                    continue
                found["found_in"] = label
                if checked is None or found["quote_status"] == "exact" or (found["ratio"] > checked["ratio"] and checked["quote_status"] != "exact"):
                    checked = found
                if found["quote_status"] == "exact":
                    break
            # No served text at all (no indexed passage for the pinpoint, no full text
            # in this mode): nothing was compared, so the quotation is not "not found".
            row["quote_check"] = checked or {"quote_status": "unverifiable", "reason": "no served text"}
        if pinpoint:
            passage, pinpoint_status, error = fetch_passage(client, decision["decision_id"], pinpoint)
            row["pinpoint_status"] = pinpoint_status
            if passage is not None:
                row["passage"] = passage
                if passage.get("composed_of"):
                    row["composed_of"] = passage["composed_of"]
            if pinpoint_status != "retrieved":
                if error is not None and error.status not in (200, 404):
                    # A transport failure on the passage fetch is not "not indexed".
                    row.update(status="error", error=error.to_dict())
                    return row
                if row["status"] == "resolved":
                    row["status"] = "pinpoint_unavailable"
                if error is not None:
                    row["error"] = error.to_dict()
                    response = getattr(error, "response", None)
                    if isinstance(response, dict) and response.get("available_e_numbers"):
                        row["available_e_numbers"] = response["available_e_numbers"]
                if pinpoint_status == "parent_retrieved":
                    row["pinpoint_note"] = (f"E. {pinpoint} is not indexed as such; E. {passage.get('e_number')} was retrieved, "
                                            "locate the lettered part inside it before quoting")
    except APIError as error:
        row.update(status="error", error=error.to_dict())
    return row


def resolve_rows(client, inputs: list[dict], language: str = "de", jobs: int = DEFAULT_JOBS) -> dict:
    """Resolve prepared rows ({"reference", "pinpoint"?, "quote"?, ...}); the report shape of `citations resolve`."""
    if len(inputs) > 1000:
        raise ValueError("Resolve at most 1000 references per invocation")
    results = []
    jobs = max(1, min(int(jobs), 8))
    consecutive_transport_failures = 0
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        for start in range(0, len(inputs), jobs):
            chunk = inputs[start:start + jobs]
            _progress(f"resolving {start + 1}-{start + len(chunk)}/{len(inputs)}")
            for row in pool.map(lambda item: _resolve_one(client, item, language), chunk):
                results.append(row)
                if row.get("status") == "error" and (row.get("error") or {}).get("status") is None:
                    consecutive_transport_failures += 1
                else:
                    consecutive_transport_failures = 0
            if consecutive_transport_failures >= BREAKER_THRESHOLD and start + jobs < len(inputs):
                for item in inputs[start + jobs:]:
                    results.append({"reference": item["reference"].strip(), "pinpoint": item.get("pinpoint"),
                                    "status": "skipped", "legal_support_assessed": False,
                                    "reason": f"Stopped after {BREAKER_THRESHOLD} consecutive transport failures; rerun when the service is reachable"})
                break
    _progress(None)
    counts = {}
    for row in results:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    complete = all(row["status"] == "resolved" and (row.get("quote_check") or {}).get("quote_status", "exact") == "exact" for row in results)
    return {"schema_version": SCHEMA_VERSION, "kind": "opencaselaw-citation-resolution",
            "client_version": __version__, "base_url": client.base_url, "generated_at": _now(),
            "status": "complete" if complete else "partial", "results": results, "counts": counts,
            "requests": getattr(client, "requests", None),
            "scope": "Decision existence and requested pinpoint retrieval in the OpenCaseLaw corpus; no assessment of legal support or original-source accuracy"}


def resolve_citations(args, client):
    # Use the same strict JSONL/plain-line input grammar as core get commands.
    from .cli import read_inputs
    inputs = read_inputs(args, field="reference")
    if not inputs:
        raise ValueError("Provide references, --input FILE or --stdin")
    report = resolve_rows(client, inputs, args.language or "de", _jobs(args))
    return report, 0 if report["status"] == "complete" else 4


# ── coverage: what "not in the corpus" means for the court a reference names ──
# Court words as written, mapped to the stem of the corpus's court codes
# (zh_obergericht, be_verwaltungsgericht, sg_kantonsgericht, ...). A word with an
# empty stem names the canton's courts as a whole.
_COURT_WORD = re.compile(
    r"(?<![A-Za-zÀ-ÿ])(Bundesverwaltungsgericht\w*|Bundesstrafgericht\w*|Bundesgericht\w*|Bundespatentgericht\w*|"
    r"Sozialversicherungsgericht\w*|Steuerrekursgericht\w*|Steuerrekurskommission|Verwaltungsgericht\w*|Appellationsgericht\w*|"
    r"Handelsgericht\w*|Obergericht\w*|Kantonsgericht\w*|Bezirksgericht\w*|Arbeitsgericht\w*|Mietgericht\w*|Baurekursgericht\w*|"
    r"Versicherungsgericht\w*|Zivilgericht\w*|Strafgericht\w*|Kassationsgericht\w*|Gericht\w*|"
    r"Tribunal administratif fédéral|Tribunal pénal fédéral|Tribunal fédéral|Tribunal administratif|Tribunal cantonal|Cour de justice|Cour d'appel|Tribunal|"
    r"Tribunale amministrativo federale|Tribunale penale federale|Tribunale federale|Tribunale d'appello|Tribunale cantonale|Tribunale|"
    r"BVGer|BStGer|BPatGer|BGer|OGer|KGer|VGer|SozVGer|TAF|TPF|TF)(?![A-Za-zÀ-ÿ])")
_COURT_STEMS = {
    "bundesgericht": "bger", "bger": "bger", "tf": "bger", "tribunal fédéral": "bger", "tribunale federale": "bger",
    "bundesverwaltungsgericht": "bvger", "bvger": "bvger", "taf": "bvger", "tribunal administratif fédéral": "bvger", "tribunale amministrativo federale": "bvger",
    "bundesstrafgericht": "bstger", "bstger": "bstger", "tpf": "bstger", "tribunal pénal fédéral": "bstger", "tribunale penale federale": "bstger",
    "bundespatentgericht": "bpatger", "bpatger": "bpatger",
    "obergericht": "obergericht", "oger": "obergericht", "kantonsgericht": "kantonsgericht", "kger": "kantonsgericht",
    "tribunal cantonal": "kantonsgericht", "tribunale cantonale": "kantonsgericht", "tribunale d'appello": "appello",
    "verwaltungsgericht": "verwaltungsgericht", "vger": "verwaltungsgericht", "tribunal administratif": "verwaltungsgericht",
    "handelsgericht": "handelsgericht", "sozialversicherungsgericht": "sozialversicherungsgericht", "sozvger": "sozialversicherungsgericht",
    "steuerrekursgericht": "steuerrekurs", "steuerrekurskommission": "steuerrekurs", "appellationsgericht": "appellationsgericht",
    "bezirksgericht": "bezirksgericht", "arbeitsgericht": "arbeitsgericht", "mietgericht": "mietgericht", "baurekursgericht": "baurekursgericht",
    "versicherungsgericht": "versicherungsgericht", "zivilgericht": "zivilgericht", "strafgericht": "strafgericht", "kassationsgericht": "kassationsgericht",
}
_FEDERAL_STEMS = {"bger", "bvger", "bstger", "bpatger", "bge"}
_FEDERAL_DOCKET = re.compile(r"^\d[A-Z]{1,2}[ _.]\d{1,5}/\d{4}$")
_BVGER_DOCKET = re.compile(r"^[A-Z]{1,2}-\d{1,5}/\d{4}$")


def infer_court(reference: str) -> dict:
    """What a written reference says about where the decision comes from: the label
    written, the court word as written, the canton code, and the corpus court codes
    or stems it points at. Nothing here is looked up; it is read from the text."""
    parsed = parse_reference(reference)
    label = parsed.bge_label or parsed.primary_docket or parsed.core
    head = parsed.core
    word = None
    for m in _COURT_WORD.finditer(head):
        word = re.sub(r"(gericht)(?:s|es)$", r"\1", m.group(1))   # "des Obergerichts" names the Obergericht
        break
    stem = None
    if word:
        lowered = word.casefold()
        for name, value in _COURT_STEMS.items():
            if lowered == name or lowered.startswith(name):
                stem = value
                break
        if stem is None:
            stem = ""  # a court word without a known stem: the canton's collections
    courts: list[str] = []
    if parsed.bge_label and parsed.bge_first:
        courts.append("bge")
        word = word or "BGE"
    elif stem in _FEDERAL_STEMS:
        courts.append(stem)
    elif stem is None and parsed.primary_docket:
        if _FEDERAL_DOCKET.match(parsed.primary_docket):
            courts.append("bger"); word = word or "BGer"
        elif _BVGER_DOCKET.match(parsed.primary_docket):
            courts.append("bvger"); word = word or "BVGer"
    elif "bger" in parsed.courts and not parsed.canton:
        courts.append("bger")
    return {"label": label, "court_word": word, "canton": parsed.canton, "courts": courts,
            "stem": stem if not courts else None}


def _coverage_rows(value) -> list[dict]:
    """Rows {court, canton, decisions, first_year, last_year} from list_courts (any of its
    shapes) or from the pack's courts table."""
    rows = value
    if isinstance(value, dict):
        rows = next((value[k] for k in ("courts", "results", "items", "data") if isinstance(value.get(k), list)), None)
    if not isinstance(rows, list):
        return []
    out = []
    for r in rows:
        if not isinstance(r, dict) or not r.get("court"):
            continue
        n = next((r[k] for k in ("decision_count", "decisions", "total", "count") if r.get(k) is not None), None)
        first = next((r[k] for k in ("first_year", "earliest") if r.get(k)), None)
        last = next((r[k] for k in ("last_year", "latest") if r.get(k)), None)
        try:
            n = int(n) if n is not None else None
        except (TypeError, ValueError):
            n = None
        out.append({"court": str(r["court"]), "canton": (str(r.get("canton") or "")).upper() or None, "decisions": n,
                    "first_year": str(first)[:4] if first else None, "last_year": str(last)[:4] if last else None})
    return out


def load_coverage(client) -> tuple[list[dict], str | None]:
    """Per-court coverage of the corpus: online from the list_courts tool, offline from the
    pack's courts table (schema 2). ([], None) when neither is available; never raises."""
    tool_json = getattr(client, "tool_json", None)
    if callable(tool_json):
        try:
            rows = _coverage_rows(tool_json("list_courts", {}))
            if rows:
                return rows, "list_courts"
        except Exception:  # noqa: BLE001 - coverage is a qualification, never a reason to fail the check
            pass
    pack = getattr(client, "pack_path", None)
    if pack and Path(pack).is_file():
        try:
            con = sqlite3.connect(f"file:{Path(pack)}?mode=ro&immutable=1", uri=True)
            try:
                rows = [{"court": r[0], "canton": r[1], "decisions": r[2], "first_year": r[3], "last_year": r[4]}
                        for r in con.execute("SELECT court, canton, decisions, first_year, last_year FROM courts")]
            finally:
                con.close()
            rows = _coverage_rows(rows)
            if rows:
                return rows, "pack"
        except sqlite3.Error:
            pass
    return [], None


def coverage_for(reference: str, rows: list[dict], source: str | None) -> dict:
    """The coverage line's data for a missing reference: the inferred court, the matched
    corpus collections and their decision count and year span (None when unknown)."""
    inferred = infer_court(reference)
    out = {"inferred": inferred, "courts": [], "decisions": None, "first_year": None, "last_year": None, "source": source}
    rows = _coverage_rows(rows)
    if not rows:
        return out
    canton = (inferred.get("canton") or "").upper()
    matched: list[dict] = []
    if inferred["courts"]:
        matched = [r for r in rows if r["court"] in inferred["courts"]]
    elif canton:
        own = [r for r in rows if (r.get("canton") == canton or r["court"].lower().startswith(canton.lower() + "_"))]
        stem = inferred.get("stem")
        if stem:
            matched = [r for r in own if stem in r["court"].lower()]
        if not matched and own:
            matched = own
            out["canton_wide"] = True
    if not matched:
        return out
    counted = [r for r in matched if r.get("decisions") is not None]
    out["courts"] = [r["court"] for r in matched]
    out["decisions"] = sum(r["decisions"] for r in counted) if counted else None
    firsts = [r["first_year"] for r in matched if r.get("first_year")]
    lasts = [r["last_year"] for r in matched if r.get("last_year")]
    out["first_year"] = min(firsts) if firsts else None
    out["last_year"] = max(lasts) if lasts else None
    return out

def check_statute_rows(client, rows: list[dict], language: str = "de", jobs: int = DEFAULT_JOBS,
                       default_canton: str | None = None) -> list[dict]:
    """The statute references a draft cites (rows from `documents.find_statutes`): does the act
    exist, does it have the article, does the quotation next to it stand in the served text.
    One request per distinct (canton, act, article); every field reported is the answer's own.
    Offline, a missing statutes database makes rows `unverifiable`, never `error`."""
    from .statutes import classify_law_error, classify_law_response, law_request
    specs = [law_request(row, language, default_canton) for row in rows]
    keyed = {key: (path, params) for path, params, key in specs if path}

    def fetch(key):
        path, params = keyed[key]
        try:
            return _get(client, path, params), None
        except APIError as error:
            return None, error

    answers = {}
    if keyed:
        with ThreadPoolExecutor(max_workers=max(1, min(int(jobs), 8))) as pool:
            answers = dict(zip(list(keyed), pool.map(fetch, list(keyed))))
    results = []
    for row, (path, reason, key) in zip(rows, specs):
        out = {"reference": row["reference"], "law": row.get("law"), "article": row.get("article")}
        out.update({k: row[k] for k in ("paragraph", "letter", "canton", "sr_number", "paragraph_index", "context") if row.get(k) is not None})
        out.update(checked_at=_now(), legal_support_assessed=False)
        if path is None:
            out.update(status="unverifiable", reason=reason)
        else:
            if key[0] != "CH":
                out["canton"] = key[0]  # the canton the act was asked in (written, or the default for bare § references)
            result, error = answers[key]
            out.update(classify_law_error(error) if error is not None else classify_law_response(result, row.get("article")))
        if isinstance(row.get("quote"), str) and row["quote"].strip():
            out["quote"] = row["quote"]
            if out.get("article_text"):
                found = match_quote(row["quote"], out["article_text"])
                found["found_in"] = "article"
                out["quote_check"] = found
            else:
                out["quote_check"] = {"quote_status": "unverifiable", "ratio": 0.0, "reason": "no article text served"}
        results.append(out)
    return results


def check_document(args, client):
    """`ocl check DRAFT`: read the draft, find its citations and quotations, check them, write a report."""
    from .documents import find_citations, find_statutes, read_document, unparsed_candidates
    from .report import court_name, language_of, render_html, render_markdown, summarize
    source = Path(args.draft).expanduser()
    if not source.is_file():
        raise ValueError(f"draft not found: {source}")
    language = args.language or "de"
    paragraphs = read_document(source)
    found = find_citations(paragraphs)
    unparsed = unparsed_candidates(paragraphs, found)
    rows = [{"reference": f["reference"], **({"quote": f["quote"]} if f.get("quote") else {}), "paragraph": f["paragraph"]} for f in found]
    report = resolve_rows(client, rows, language, _jobs(args)) if rows else {
        "schema_version": SCHEMA_VERSION, "kind": "opencaselaw-citation-resolution", "client_version": __version__,
        "base_url": client.base_url, "generated_at": _now(), "status": "complete", "results": [], "counts": {}, "requests": getattr(client, "requests", None)}
    missing = [r for r in report["results"] if r.get("status") == "missing"]
    if missing:
        # "Not in the corpus" is qualified by what the corpus holds for the court the
        # reference names: an unpublished ruling or the decision under appeal is
        # expected to be absent, a wrong citation is not.
        coverage_rows, coverage_source = load_coverage(client)
        for r in missing:
            r["coverage"] = coverage_for(r["reference"], coverage_rows, coverage_source)
            r["coverage"]["name"] = court_name(r["coverage"], language)
    report["unparsed"] = unparsed
    # statutes: their own rows, checked after the decisions so the request counter covers both
    from os import environ
    statutes_found = find_statutes(paragraphs)
    report["statutes"] = check_statute_rows(client, statutes_found, args.language or "de", _jobs(args),
                                            getattr(args, "canton", None) or environ.get("OCL_CANTON") or None) if statutes_found else []
    report["requests"] = getattr(client, "requests", None)
    summary = summarize(report, source.name, language)
    report_path = None
    if not getattr(args, "no_report", False):
        target = Path(args.report).expanduser() if getattr(args, "report", None) else source.with_name(source.stem + ".check.html")
        text = render_markdown(report, source.name, found, language) if target.suffix.lower() in (".md", ".markdown") else render_html(report, source.name, found, language)
        target.write_text(text, encoding="utf-8")
        report_path = str(target)
    report.update(kind="opencaselaw-draft-check", source=str(source), paragraphs=len(paragraphs), found=found, statutes_found=statutes_found,
                  summary=summary, report_path=report_path, report_language=language_of(language))
    return report, 0 if summary["attention"] == 0 and not summary.get("statutes_attention") else 4


def _load_manifest(directory):
    directory = Path(directory).expanduser()
    try:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ValueError(f"{directory} is not a readable bundle (manifest.json missing or invalid)") from error
    if manifest.get("kind") != "opencaselaw-research-bundle":
        raise ValueError(f"{directory} is not an OpenCaseLaw research bundle")
    return directory, manifest


def verify_bundle(args):
    """Re-hash every listed file; report changed, missing and unlisted files."""
    directory, manifest = _load_manifest(args.bundle)
    report = {"kind": "opencaselaw-bundle-verification", "bundle": str(directory.resolve()),
              "checked_at": _now(), "client_version": __version__, "ok": [], "changed": [], "missing": [], "unlisted": []}
    listed = set()
    for item in manifest.get("artifacts", []):
        listed.add(item["path"])
        try:
            path = _safe_file(directory, item["path"])
        except ValueError:
            report["changed"].append(item["path"])
            continue
        if not path.is_file():
            report["missing"].append(item["path"])
        elif hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            report["changed"].append(item["path"])
        else:
            report["ok"].append(item["path"])
    for path in sorted(directory.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(directory).as_posix()
        if relative in listed or relative in ("manifest.json", "INDEX.md") or path.name.startswith(".manifest-"):
            continue
        report["unlisted"].append(relative)
    report["counts"] = {key: len(report[key]) for key in ("ok", "changed", "missing", "unlisted")}
    report["corpus_snapshot"] = manifest.get("corpus_snapshot")
    report["status"] = "verified" if not report["changed"] and not report["missing"] else "failed"
    report["scope"] = "File integrity against the manifest only; not a check of the original court publications"
    return report, 0 if report["status"] == "verified" else 4


def diff_bundles(args):
    """What changed between two bundles of the same question."""
    dir_a, a = _load_manifest(args.old)
    dir_b, b = _load_manifest(args.new)
    def decisions(manifest):
        return {item["identifier"]: item for item in manifest.get("items", {}).values() if item["kind"] == "decision"}
    da, db = decisions(a), decisions(b)
    added = sorted(set(db) - set(da)); removed = sorted(set(da) - set(db))
    changed_text, status_changes = [], []
    for decision_id in sorted(set(da) & set(db)):
        pa, pb = da[decision_id].get("provenance") or {}, db[decision_id].get("provenance") or {}
        if pa.get("content_hash") and pb.get("content_hash") and pa["content_hash"] != pb["content_hash"]:
            changed_text.append({"decision_id": decision_id, "old": pa["content_hash"], "new": pb["content_hash"]})
        if da[decision_id].get("status") != db[decision_id].get("status"):
            status_changes.append({"identifier": decision_id, "old": da[decision_id].get("status"), "new": db[decision_id].get("status")})
    for key in sorted(set(a.get("items", {})) & set(b.get("items", {}))):
        if not key.startswith("decision:") and a["items"][key].get("status") != b["items"][key].get("status"):
            status_changes.append({"identifier": key, "old": a["items"][key].get("status"), "new": b["items"][key].get("status")})
    request_changes = {key: {"old": a["request"].get(key), "new": b["request"].get(key)}
                       for key in sorted(set(a["request"]) | set(b["request"])) if a["request"].get(key) != b["request"].get(key)}
    ga = (a.get("corpus_snapshot") or {}).get("db_generation"); gb = (b.get("corpus_snapshot") or {}).get("db_generation")
    report = {"kind": "opencaselaw-bundle-diff", "old": str(dir_a.resolve()), "new": str(dir_b.resolve()),
              "compared_at": _now(), "request_changes": request_changes,
              "corpus_generation": {"old": ga, "new": gb, "changed": ga != gb},
              "decisions": {"added": added, "removed": removed, "unchanged": len(set(da) & set(db)),
                            "text_changed": changed_text},
              "status_changes": status_changes,
              "note": ("Added and removed follow the saved selections; a decision can appear because it was newly decided, "
                       "newly published or newly indexed, or because the ranking changed. The manifests record the "
                       "corpus generation, not the reason.")}
    return report, 0


def add_to_bundle(args, client):
    """Add decisions found elsewhere to an existing bundle, with the bundle's passages."""
    directory, manifest = _load_manifest(args.bundle)
    _validate_saved(directory, manifest)
    from .cli import read_inputs
    decision_ids = [row["decision_id"].strip() for row in read_inputs(args, "decision_id")]
    passages = list(manifest["request"].get("passages", []))
    for passage in getattr(args, "passage", None) or []:
        number = normalise_pinpoint(passage)
        if not number:
            raise ValueError("--passage must be an Erwägung number such as 2.3 or 3b")
        if number not in passages:
            passages.append(number)
    manifest.setdefault("additions", []).append({"at": _now(), "decision_ids": decision_ids, "passages": passages,
                                                 "corpus_snapshot": _snapshot(client)})
    _collect_all(directory, manifest, client, _item_specs(manifest["request"], decision_ids, passages), _jobs(args))
    _finalise(manifest, client)
    _checkpoint(directory, manifest)
    _write_index(directory, manifest)
    added = {key: item["status"] for key, item in manifest["items"].items()
             if item["kind"] in ("decision", "passage") and item["identifier"].split(":")[0] in decision_ids}
    return {"schema_version": SCHEMA_VERSION, "status": manifest["status"], "bundle": str(directory.resolve()),
            "manifest": str((directory / "manifest.json").resolve()), "added": added,
            "completeness": manifest["completeness"]}, 0 if all(status == "saved" for status in added.values()) else 4


def run(args, client):
    if args.command == "bundle" and args.action == "create":
        return create_bundle(args, client)
    if args.command == "bundle" and args.action == "verify":
        return verify_bundle(args)
    if args.command == "bundle" and args.action == "diff":
        return diff_bundles(args)
    if args.command == "bundle" and args.action == "add":
        return add_to_bundle(args, client)
    if args.command == "citations" and args.action == "resolve":
        return resolve_citations(args, client)
    if args.command == "quotes" and args.action == "check":
        return check_quotes(args, client)
    if args.command == "check":
        return check_document(args, client)
    raise ValueError("Unknown research workflow")


# ── quotations ─────────────────────────────────────────────────────────────

import difflib as _difflib
import unicodedata as _unicodedata

_QUOTE_TRANSLATE = str.maketrans({
    " ": " ", " ": " ", " ": " ", "­": "", "‘": "'", "’": "'", "‚": "'", "“": '"',
    "”": '"', "„": '"', "«": '"', "»": '"', "‹": "'", "›": "'", "–": "-", "—": "-",
    "…": "...", "ﬁ": "fi", "ﬂ": "fl",
})
_HYPHEN_BREAK = re.compile(r"(\w)-\s*\n\s*(?=[a-zäöüàéèêôûç])")
QUOTE_NEAR_RATIO = 0.9


def normalise_quote(text: str) -> str:
    """Comparison form of a quotation or of served text: typography, OCR line
    hyphenation, whitespace and the service's link markup folded. Never used to
    print anything; the served wording is what is reported."""
    text = plain_text(text) or ""
    text = _unicodedata.normalize("NFC", text)
    text = re.sub(r"[«‹]\s*", '"', text)   # French guillemets carry an inner space
    text = re.sub(r"\s*[»›]", '"', text)
    text = text.translate(_QUOTE_TRANSLATE)
    text = _HYPHEN_BREAK.sub(r"\1", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def match_quote(quote: str, text: str) -> dict:
    """Where a quotation stands in a served text.

    exact: the normalised quote is a substring of the normalised text.
    near: the best window of the text matches with a difflib ratio >= 0.9; the
    differing spans are listed (quote wording vs served wording).
    not_found: below that; the best window and its ratio are still reported.
    unverifiable: the served text is empty, so nothing was compared (never a verdict)."""
    q = normalise_quote(quote)
    t = normalise_quote(text)
    if not q:
        return {"quote_status": "not_found", "ratio": 0.0, "reason": "empty quotation"}
    idx = t.find(q)
    if idx >= 0:
        return {"quote_status": "exact", "ratio": 1.0, "offset": idx}
    lowered_idx = t.casefold().find(q.casefold())
    if lowered_idx >= 0:
        served = t[lowered_idx:lowered_idx + len(q)]
        return {"quote_status": "near", "ratio": 0.99, "offset": lowered_idx, "served": served,
                "differences": [{"quote": quote.strip(), "served": served, "kind": "case"}]}
    if not t:
        # Nothing to compare with: not a verdict on the quotation.
        return {"quote_status": "unverifiable", "reason": "no served text"}
    width = len(q)
    step = max(1, width // 8)
    best_ratio, best_start = 0.0, 0
    matcher = _difflib.SequenceMatcher(autojunk=False)
    matcher.set_seq2(q)
    for start in range(0, max(1, len(t) - width // 2), step):
        window = t[start:start + width + width // 5]
        matcher.set_seq1(window)
        if matcher.real_quick_ratio() < best_ratio or matcher.quick_ratio() < best_ratio:
            continue
        ratio = matcher.ratio()
        if ratio > best_ratio:
            best_ratio, best_start = ratio, start
    # tighten the window to the matched span
    window = t[best_start:best_start + width + width // 5]
    matcher.set_seq1(window)
    blocks = [b for b in matcher.get_matching_blocks() if b.size]
    if blocks:
        lo = blocks[0].a
        hi = blocks[-1].a + blocks[-1].size
        served = window[lo:hi]
    else:
        served = window
    matcher.set_seq1(served)
    ratio = round(matcher.ratio(), 3)
    differences = []
    for op, i1, i2, j1, j2 in matcher.get_opcodes():
        if op == "equal":
            continue
        differences.append({"kind": op, "served": served[i1:i2], "quote": q[j1:j2]})
    result = {"quote_status": "near" if ratio >= QUOTE_NEAR_RATIO else "not_found", "ratio": ratio,
              "offset": best_start + (blocks[0].a if blocks else 0), "served": served, "differences": differences[:12]}
    return result


def _quote_sources(client, decision_id: str, pinpoint: str | None):
    """(label, text) pairs to search, most specific first: the passage, then the decision text."""
    sources = []
    if pinpoint:
        passage, status, _ = fetch_passage(client, decision_id, pinpoint)
        if passage is not None and isinstance(passage.get("text"), str):
            sources.append((f"E. {passage.get('e_number')}", passage["text"], status))
    try:
        record = _get(client, "/api/decisions/" + quote(decision_id, safe=""), {"full_text": True})
        if isinstance(record.get("full_text"), str):
            sources.append(("full_text", record["full_text"], "retrieved"))
    except APIError as error:
        sources.append(("full_text", "", error.to_dict()))
    return sources


def check_one_quote(client, item, language):
    """Identify the reference, then look for the quotation in the passage, else in the decision text."""
    reference = item["reference"].strip()
    quotation = item.get("quote")
    row = {"reference": reference, "quote": quotation, "checked_at": _now(), "legal_support_assessed": False}
    if not isinstance(quotation, str) or not quotation.strip():
        row.update(status="error", quote_status="not_checked", error={"status": 400, "kind": "input", "message": "Each row needs a non-empty quote"})
        return row
    try:
        parsed = parse_reference(reference)
        try:
            explicit = normalise_pinpoint(item.get("pinpoint"))
        except ValueError as error:
            row.update(status="error", quote_status="not_checked", error={"status": 400, "kind": "input", "message": str(error)})
            return row
        pinpoint = explicit or parsed.pinpoint
        row["pinpoint"] = pinpoint
        parsed, decision = _identify(client, reference, language, row, parsed)
        if decision is None:
            row["quote_status"] = "not_checked"
            return row
        best = None
        for label, text, status in _quote_sources(client, decision["decision_id"], pinpoint):
            if not text:
                continue
            found = match_quote(quotation, text)
            if found["quote_status"] == "unverifiable":
                continue
            found["found_in"] = label
            if label != "full_text":
                found["pinpoint_status"] = status
            if best is None or found["quote_status"] == "exact" or (found["ratio"] > best["ratio"] and best["quote_status"] != "exact"):
                best = found
            if found["quote_status"] == "exact":
                break
        if best is None:
            # No served text was compared (no indexed passage, no full text in this
            # mode): the quotation is unverifiable here, not "not found".
            row.update(quote_status="unverifiable", reason="no served text")
        else:
            row.update(best)
        if row.get("status") == "resolved" and row["quote_status"] != "exact":
            row["status"] = "quote_" + row["quote_status"]
    except APIError as error:
        row.update(status="error", quote_status="not_checked", error=error.to_dict())
    return row


def check_quotes(args, client):
    from .cli import read_inputs
    inputs = read_inputs(args, field="reference")
    single = getattr(args, "quote", None)
    if single:
        if len(inputs) != 1:
            raise ValueError("--quote checks one reference; use --input/--stdin rows with a quote field for several")
        inputs[0]["quote"] = single
        if getattr(args, "pinpoint", None):
            inputs[0]["pinpoint"] = args.pinpoint
    if len(inputs) > 500:
        raise ValueError("Check at most 500 quotations per invocation")
    language = args.language or "de"
    jobs = _jobs(args)
    results = []
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        for start in range(0, len(inputs), jobs):
            chunk = inputs[start:start + jobs]
            _progress(f"checking {start + 1}-{start + len(chunk)}/{len(inputs)}")
            results.extend(pool.map(lambda item: check_one_quote(client, item, language), chunk))
    _progress(None)
    counts = {}
    for row in results:
        counts[row.get("quote_status", "not_checked")] = counts.get(row.get("quote_status", "not_checked"), 0) + 1
    complete = all(row.get("quote_status") == "exact" for row in results)
    return {"schema_version": SCHEMA_VERSION, "kind": "opencaselaw-quotation-check", "client_version": __version__,
            "base_url": client.base_url, "generated_at": _now(), "status": "complete" if complete else "partial",
            "results": results, "counts": counts, "requests": getattr(client, "requests", None),
            "scope": "Whether each quotation stands, verbatim or nearly, in the served text of the decision it is attributed to; the served wording is authoritative and is never rewritten"}, 0 if complete else 4
