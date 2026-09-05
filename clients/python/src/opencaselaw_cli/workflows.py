"""Portable evidence collections and conservative citation resolution.

Only public read operations are used. JSON files preserve service results, not
original court response bytes. A saved bundle is replayable evidence; it is
not a promise that a future query against the live corpus will return it.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

from . import __version__
from .client import APIError

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
# A docket embedded in a longer reference: the federal form the service itself
# prints ("BGer 4A_747/2012 vom 5. April 2013") and dotted cantonal files
# ("Verwaltungsgericht des Kantons Aargau WBE.2026.33").
_EMBEDDED_DOCKET = re.compile(r"\b(\d[A-Z]{1,2}[ _.]\d{1,5}/\d{4}|[A-Z]{2,6}\.\d{4}\.\d{1,5})\b")


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
        raise APIError(200, str(result["error"]))
    return result


def extract_docket(reference: str) -> str | None:
    """The docket inside a longer reference, if any, else None."""
    match = _EMBEDDED_DOCKET.search(reference or "")
    if not match:
        return None
    docket = match.group(1)
    return None if docket.strip() == (reference or "").strip() else docket


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
    passages = list(dict.fromkeys(getattr(args, "passage", None) or []))
    for passage in passages:
        if not passage or not all(part.isdigit() for part in passage.split(".")):
            raise ValueError("--passage must be an Erwägung number such as 2.3")
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
        item.update(status="unavailable" if error.status == 200 else "failed", error=error.to_dict())
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
               if (manifest["items"].get(spec["kind"] + ":" + spec["identifier"]) or {}).get("status") != "saved"]
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
    _collect_all(directory, manifest, client, specs, _jobs(args))
    failed = [item for item in manifest["items"].values() if item["status"] != "saved"]
    unavailable = [item for item in failed if item["status"] == "unavailable"]
    selection = manifest["selection"]
    complete = selection["finished"] and not selection["error"] and not failed
    manifest["status"] = "complete" if complete else "partial"
    manifest["requests"] = getattr(client, "requests", None)
    manifest["completeness"] = {"selected_items_saved": complete,
                                "exhaustive_legal_research": False,
                                "selected_decisions": len(selection["decision_ids"]),
                                "failed_items": len(failed), "unavailable_items": len(unavailable),
                                "search_error": selection["error"],
                                "max_results_reached": selection["max_results_reached"],
                                "ranked_single_request": bool(request.get("ranked_single_request")),
                                "corpus_generation": (manifest.get("corpus_snapshot") or {}).get("db_generation"),
                                "server_last_page": selection.get("last_page"),
                                "note": "Complete means the requested bounded collection was saved; relevance-ranked search can use a capped candidate pool"}
    _checkpoint(directory, manifest)
    _write_index(directory, manifest)
    return {"schema_version": SCHEMA_VERSION, "status": manifest["status"],
            "bundle": str(directory.resolve()), "manifest": str((directory / "manifest.json").resolve()),
            "completeness": manifest["completeness"]}, 0 if complete else 4


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
    completeness = manifest.get("completeness") or {}
    if completeness.get("failed_items"):
        unavailable = completeness.get("unavailable_items", 0)
        transport = completeness["failed_items"] - unavailable
        parts = []
        if unavailable:
            parts.append(f"{unavailable} item(s) the service does not have (for example a passage that is not "
                         "indexed for that decision); these will not appear on a rerun")
        if transport:
            parts.append(f"{transport} item(s) failed to download; rerun with --resume")
        lines += ["; ".join(parts) + ".", ""]
    (directory / "INDEX.md").write_text("\n".join(lines), encoding="utf-8")


# A pinpoint marker must follow a separator: the "e." inside a docket such
# as WBE.2026.33 is part of the label.
_PINPOINT_TAIL = re.compile(r"[\s,;]+(?:e\.|erw\.|erwägung|consid\.|cons\.|c\.)\s*\d+(?:\.\d+)*[a-z]?\s*$")
_BARE_BGE = re.compile(r"^\d{1,3}(?:ia|ib|iii|ii|iv|i|v)\d{1,4}$")


def reference_key(value):
    """Comparison key for source-supplied labels; never used to generate a citation.

    Folds case, whitespace and the federal docket separator (4A_747/2012 and
    4A 747/2012 name the same file), a trailing pinpoint (", E. 2.3") and the
    official French/Italian BGE collection labels (ATF/DTF).
    """
    if not isinstance(value, str):
        return None
    key = _PINPOINT_TAIL.sub("", value.casefold().strip())
    key = re.sub(r"[\s_]+", "", key).rstrip(".,;:")
    if key.startswith(("atf", "dtf")) and len(key) > 3 and key[3].isdigit():
        key = "bge" + key[3:]
    elif _BARE_BGE.match(key):
        key = "bge" + key
    return key


_reference_key = reference_key


def _resolve_one(client, item, language):
    reference = item["reference"].strip()
    pinpoint = item.get("pinpoint")
    row = {"reference": reference, "pinpoint": pinpoint, "checked_at": _now(),
           "legal_support_assessed": False}
    try:
        citation = _get(client, "/api/cite", {"reference": reference, "pinpoint": pinpoint, "language": language})
        row["citation"] = citation
        lookup_reference = reference
        if citation.get("exists") is False:
            # A long-form reference ("BGer 4A_747/2012 vom 5. April 2013") the
            # service does not parse: retry with the docket it contains. The
            # docket-label identity check below still has to pass.
            docket = extract_docket(reference)
            if docket:
                retry = _get(client, "/api/cite", {"reference": docket, "pinpoint": pinpoint, "language": language})
                if retry.get("exists") is True:
                    row["citation_as_written"] = citation
                    row["citation"] = citation = retry
                    row["docket_extracted"] = docket
                    lookup_reference = docket
        if citation.get("exists") is False:
            row["status"] = "missing"
            row["note"] = "No decision carries this label; close_matches are for the author to inspect, never substitutes"
            return row
        decision_id = citation.get("decision_id")
        if citation.get("exists") is not True or not isinstance(decision_id, str) or not decision_id:
            raise APIError(None, "Citation response does not identify an existing decision")
        decision = _get(client, "/api/decisions/" + quote(decision_id, safe=""), {"full_text": False})
        if decision.get("decision_id") != decision_id:
            raise APIError(None, "Decision retrieval disagrees with citation resolution")
        row.update(decision_id=decision_id, provenance=_provenance(decision))
        row["official_source_available"] = bool(decision.get("source_url"))
        key = reference_key(lookup_reference)
        canonical_labels = {reference_key(record.get(name))
                            for record in (decision, citation)
                            for name in ("citation_string", "citation_string_de", "citation_string_fr", "citation_string_it")}
        docket_match = key is not None and key == reference_key(decision.get("docket_number"))
        if lookup_reference == decision_id:
            row["identity_check"] = {"method": "exact_canonical_id"}
        elif key in canonical_labels:
            # /lookup can return topical hits that only cite the requested BGE.
            # The server's canonical citation plus fetched record is stronger
            # identity evidence than that relevance-ranked candidate window.
            row["identity_check"] = {"method": "exact_server_citation"}
        else:
            # A docket can be reused by another court, and the service also
            # matches fragments. The lookup window shows other decisions that
            # carry the same label; it is padded with related decisions, so
            # only label-matching rows count.
            # exact=true (servers from 2026-09-05) returns only decisions carrying the
            # label; older servers ignore it and pad the window with related cases,
            # which the label comparison below filters out either way.
            lookup = _get(client, "/api/lookup", {"q": lookup_reference, "limit": LOOKUP_WINDOW, "exact": True})
            candidates = lookup.get("results", [])
            if not isinstance(candidates, list) or any(
                not isinstance(candidate, dict) or not isinstance(candidate.get("decision_id"), str)
                or not candidate["decision_id"] for candidate in candidates
            ):
                raise APIError(None, "Lookup returned invalid candidate identifiers")
            matching = [candidate for candidate in candidates if key in {
                reference_key(candidate.get(name)) for name in ("decision_id", "docket_number", "citation")
            }]
            ids = {candidate["decision_id"] for candidate in matching}
            row["lookup"] = lookup
            row["identity_check"] = {"method": "exact_server_docket" if docket_match else "exact_candidate_label",
                                     "matching_candidates": matching,
                                     "lookup_exact": bool(lookup.get("exact")),
                                     "candidate_window_may_be_capped": len(candidates) >= LOOKUP_WINDOW}
            distinct = ids | ({decision_id} if docket_match else set())
            if len(distinct) > 1 or (ids and decision_id not in ids):
                row.update(status="ambiguous", reason="Multiple exact matches or disagreement between resolvers; select an explicit decision_id")
                return row
            if len(matching) >= LOOKUP_WINDOW:
                row.update(status="resolution_incomplete", reason="The candidate window is full of exact matches; use a canonical citation or explicit decision_id")
                return row
            if not docket_match and (not ids or not lookup.get("is_case_number")):
                row.update(status="unrecognized", reason="The input did not match a canonical citation or an exact source docket; use an explicit decision_id")
                return row
        row["status"] = "resolved"
        if pinpoint:
            try:
                passage = _get(client, f"/api/erwaegung/{quote(decision_id, safe='')}/{quote(str(pinpoint), safe='')}")
                _validate_passage(passage, decision_id, str(pinpoint))
                row["passage"] = passage
                row["pinpoint_status"] = "retrieved"
                if passage.get("composed_of"):
                    row["composed_of"] = passage["composed_of"]
            except APIError as error:
                row.update(status="pinpoint_unavailable", pinpoint_status="unavailable", error=error.to_dict())
    except APIError as error:
        row.update(status="error", error=error.to_dict())
    return row


def resolve_citations(args, client):
    # Use the same strict JSONL/plain-line input grammar as core get commands.
    from .cli import read_inputs
    inputs = read_inputs(args, field="reference")
    if not inputs:
        raise ValueError("Provide references, --input FILE or --stdin")
    if len(inputs) > 1000:
        raise ValueError("Resolve at most 1000 references per invocation")
    for item in inputs:
        pinpoint = item.get("pinpoint")
        if pinpoint is not None and (not isinstance(pinpoint, str) or not pinpoint
                                    or not all(part.isdigit() for part in pinpoint.split("."))):
            raise ValueError("A pinpoint must be an Erwägung number string such as 2.3")
    results = []
    language = args.language or "de"
    jobs = _jobs(args)
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
    complete = all(row["status"] == "resolved" for row in results)
    return {"schema_version": SCHEMA_VERSION, "kind": "opencaselaw-citation-resolution",
            "client_version": __version__, "base_url": client.base_url, "generated_at": _now(),
            "status": "complete" if complete else "partial", "results": results, "counts": counts,
            "requests": getattr(client, "requests", None),
            "scope": "Decision existence and requested pinpoint retrieval in the OpenCaseLaw corpus; no assessment of legal support or original-source accuracy"}, 0 if complete else 4


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
    passages = list(dict.fromkeys(list(manifest["request"].get("passages", [])) + list(getattr(args, "passage", None) or [])))
    for passage in passages:
        if not passage or not all(part.isdigit() for part in passage.split(".")):
            raise ValueError("--passage must be an Erwägung number such as 2.3")
    manifest.setdefault("additions", []).append({"at": _now(), "decision_ids": decision_ids, "passages": passages,
                                                 "corpus_snapshot": _snapshot(client)})
    _collect_all(directory, manifest, client, _item_specs(manifest["request"], decision_ids, passages), _jobs(args))
    failed = [item for item in manifest["items"].values() if item["status"] != "saved"]
    manifest["status"] = "complete" if not failed and not manifest["selection"].get("error") else "partial"
    manifest.setdefault("completeness", {})["failed_items"] = len(failed)
    manifest["completeness"]["added_decisions"] = sum(len(entry["decision_ids"]) for entry in manifest["additions"])
    _checkpoint(directory, manifest)
    _write_index(directory, manifest)
    added = {key: item["status"] for key, item in manifest["items"].items()
             if item["kind"] in ("decision", "passage") and item["identifier"].split(":")[0] in decision_ids}
    return {"schema_version": SCHEMA_VERSION, "status": manifest["status"], "bundle": str(directory.resolve()),
            "manifest": str((directory / "manifest.json").resolve()), "added": added}, 0 if not failed else 4


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
    raise ValueError("Unknown research workflow")
