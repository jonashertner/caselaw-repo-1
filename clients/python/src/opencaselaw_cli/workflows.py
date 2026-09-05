"""Portable evidence collections and conservative citation resolution.

Only public read operations are used. JSON files preserve service results, not
original court response bytes. A saved bundle is replayable evidence; it is
not a promise that a future query against the live corpus will return it.
"""
from __future__ import annotations

import hashlib
import json
import re
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


def _now():
    return datetime.now(timezone.utc).isoformat()


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


def _get(client, path, params=None):
    result = client.get(path, params)
    if not isinstance(result, dict):
        raise APIError(None, "Expected a JSON object from the research API")
    if result.get("error"):
        raise APIError(None, str(result["error"]))
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
    passages = list(dict.fromkeys(getattr(args, "passage", None) or []))
    for passage in passages:
        if not passage or not all(part.isdigit() for part in passage.split(".")):
            raise ValueError("--passage must be an Erwägung number such as 2.3")
    laws = []
    for item in getattr(args, "law", None) or []:
        abbreviation, separator, article = item.partition(":")
        if not separator or not abbreviation.strip() or not article.strip():
            raise ValueError("--law requires ABBREVIATION:ARTICLE, for example OR:41")
        law = {"abbreviation": abbreviation.strip(), "article": article.strip()}
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


def _collect(directory, manifest, client, *, kind, identifier, path, params=None):
    job_key = kind + ":" + identifier
    item = manifest["items"].get(job_key)
    if item and item["status"] == "saved":
        return
    item = {"kind": kind, "identifier": identifier, "request": {"path": path, "params": params}}
    manifest["items"][job_key] = item
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
    except APIError as error:
        item.update(status="failed", error=error.to_dict())
        manifest["attempt_errors"].append({"stage": kind, "identifier": identifier,
                                            "at": _now(), **error.to_dict()})
    _checkpoint(directory, manifest)


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
                    "status": "collecting", "artifacts": [], "items": {}, "attempt_errors": [],
                    "selection": {"finished": False, "pages": [], "decision_ids": [], "next_offset": 0,
                                  "error": None, "max_results_reached": False},
                    "evidence_contract": {
                        "representation": "UTF-8 JSON serialization of API responses and unmodified served text strings; not original court response bytes",
                        "artifact_hash_algorithm": "SHA-256 over the saved file bytes",
                        "server_content_hash": "Preserved as supplied; its algorithm and original-source equivalence are not asserted",
                        "corpus_snapshot": None,
                        "passage_text": "Served /api/erwaegung text; cross-references inside it can carry Markdown links added by the service",
                        "replay": "Only manifest-listed artifacts form this collection. Saved artifacts can be inspected offline; a later live query can return different results",
                        "licensing": "Per-record licence metadata is preserved when supplied; absence does not grant reuse rights",
                        "legal_support": "Not assessed; retrieval or citation existence does not establish that a decision supports a legal proposition"}}
        _checkpoint(directory, manifest)
    _select(directory, manifest, client)
    for decision_id in manifest["selection"]["decision_ids"]:
        encoded = quote(decision_id, safe="")
        _collect(directory, manifest, client, kind="decision", identifier=decision_id,
                 path="/api/decisions/" + encoded)
        for passage in request["passages"]:
            _collect(directory, manifest, client, kind="passage", identifier=decision_id + ":" + passage,
                     path=f"/api/erwaegung/{encoded}/{quote(passage, safe='')}")
    for law in request["laws"]:
        _collect(directory, manifest, client, kind="law", identifier=law["abbreviation"] + ":" + law["article"],
                 path="/api/laws/" + quote(law["abbreviation"], safe=""),
                 params={"article": law["article"], "language": request.get("law_language", "de")})
    failed = [item for item in manifest["items"].values() if item["status"] != "saved"]
    selection = manifest["selection"]
    complete = selection["finished"] and not selection["error"] and not failed
    manifest["status"] = "complete" if complete else "partial"
    manifest["completeness"] = {"selected_items_saved": complete,
                                "exhaustive_legal_research": False,
                                "selected_decisions": len(selection["decision_ids"]),
                                "failed_items": len(failed), "search_error": selection["error"],
                                "max_results_reached": selection["max_results_reached"],
                                "ranked_single_request": bool(request.get("ranked_single_request")),
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
        lines += [f"{completeness['failed_items']} requested item(s) could not be saved; rerun with --resume "
                  "after the cause is fixed, or keep this as a partial collection.", ""]
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
        if citation.get("exists") is False:
            row["status"] = "missing"
            return row
        decision_id = citation.get("decision_id")
        if citation.get("exists") is not True or not isinstance(decision_id, str) or not decision_id:
            raise APIError(None, "Citation response does not identify an existing decision")
        decision = _get(client, "/api/decisions/" + quote(decision_id, safe=""), {"full_text": False})
        if decision.get("decision_id") != decision_id:
            raise APIError(None, "Decision retrieval disagrees with citation resolution")
        row.update(decision_id=decision_id, provenance=_provenance(decision))
        row["official_source_available"] = bool(decision.get("source_url"))
        key = reference_key(reference)
        canonical_labels = {reference_key(record.get(name))
                            for record in (decision, citation)
                            for name in ("citation_string", "citation_string_de", "citation_string_fr", "citation_string_it")}
        docket_match = key is not None and key == reference_key(decision.get("docket_number"))
        if reference == decision_id:
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
            lookup = _get(client, "/api/lookup", {"q": reference, "limit": LOOKUP_WINDOW})
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
    results = [_resolve_one(client, item, args.language or "de") for item in inputs]
    counts = {}
    for row in results:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    complete = all(row["status"] == "resolved" for row in results)
    return {"schema_version": SCHEMA_VERSION, "kind": "opencaselaw-citation-resolution",
            "client_version": __version__, "base_url": client.base_url, "generated_at": _now(),
            "status": "complete" if complete else "partial", "results": results, "counts": counts,
            "scope": "Decision existence and requested pinpoint retrieval in the OpenCaseLaw corpus; no assessment of legal support or original-source accuracy"}, 0 if complete else 4


def run(args, client):
    if args.command == "bundle" and args.action == "create":
        return create_bundle(args, client)
    if args.command == "citations" and args.action == "resolve":
        return resolve_citations(args, client)
    raise ValueError("Unknown research workflow")
