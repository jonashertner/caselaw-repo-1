"""CLI grammar, streaming-friendly output, and bounded research operations."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path
from urllib.parse import quote

from .client import APIError, Client
from .workflows import RANKED_MAX_RESULTS, reference_key


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def nonnegative_int(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return number


def confidence(value):
    number = float(value)
    if not math.isfinite(number) or not 0 <= number <= 1:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return number


def _common():
    parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS)
    parser.add_argument("--base-url", help="API origin (default: https://mcp.opencaselaw.ch)")
    parser.add_argument("--timeout", type=float, help="per-request timeout in seconds (default: 30)")
    parser.add_argument("--retries", type=nonnegative_int, help="retries per request, 0-5 (default: 2)")
    parser.add_argument("--format", choices=["json", "jsonl"], help="output encoding (default: json)")
    parser.add_argument("--fields", help="comma-separated result fields; pagination/errors are always retained")
    return parser


_FILTER_HELP = {
    "court": "court code, e.g. bge, bger, zh_obergericht (see /api/courts)",
    "canton": "canton code, e.g. ZH, BE, GE; CH for federal",
    "language": "de, fr, it or rm (en reaches ECtHR judgments only)",
    "date-from": "earliest decision date, YYYY-MM-DD",
    "date-to": "latest decision date, YYYY-MM-DD",
}


def _filters(parser):
    for name in ("court", "canton", "language", "date-from", "date-to"):
        parser.add_argument("--" + name, help=_FILTER_HELP[name])


def _input(parser, name, what):
    parser.add_argument(name, nargs="*", help=f"{what}; alternatively use --stdin or --input")
    parser.add_argument("--stdin", action="store_true", help="read one item per line, or JSONL records, from stdin")
    parser.add_argument("--input", metavar="FILE", help="read one item per line, or JSONL records, from this UTF-8 file")


_EXAMPLES = """examples:
  ocl decisions search 'Rachekündigung Art. 336 OR' --max-results 5
  ocl decisions passage bge_BGE_136_III_513 2.3
  ocl laws get OR --article 336
  ocl citations resolve 'BGE 136 III 513' '4A_747/2012'
  ocl bundle create 'Rachekündigung Art. 336 OR' --max-results 5 --passage 2 --law OR:336 --out evidence

Output is JSON (or JSONL with --format jsonl) on stdout; messages go to stderr.
Exit codes: 0 complete, 2 invalid input, 3 API or network failure, 4 partial or unresolved.
Guide: https://github.com/jonashertner/opencaselaw/blob/main/docs/research-cli.md
"""


def build_parser():
    fmt = argparse.RawDescriptionHelpFormatter
    common = _common()
    parser = argparse.ArgumentParser(
        prog="ocl", parents=[_common()], formatter_class=fmt,
        description=("Research Swiss case law and statutes from the command line. Every command is a "
                     "read-only call to the public OpenCaseLaw API; identifiers, citation strings and "
                     "passage text come back from the service unchanged."),
        epilog=_EXAMPLES)
    parser.set_defaults(base_url="https://mcp.opencaselaw.ch", timeout=30, retries=2, format="json", fields=None)
    parser.add_argument("--version", action="version", version="ocl 0.1.0")
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    decisions = commands.add_parser("decisions", parents=[common], formatter_class=fmt,
                                    help="search decisions, fetch them, or fetch one exact Erwägung",
                                    description="Search court decisions, fetch full decisions, or fetch one numbered Erwägung verbatim.")
    actions = decisions.add_subparsers(dest="action", required=True, metavar="ACTION")
    search_parser = actions.add_parser(
        "search", parents=[common], formatter_class=fmt,
        help="find decisions by text and/or filters (bounded, with completeness metadata)",
        description=("Find decisions. A text query is ranked by relevance and sent as ONE request of "
                     "--max-results (at most 800). Without query text, the filters enumerate an exact set "
                     "page by page. The output keeps the service's counts (total, total_is_lower_bound, "
                     "has_more, next_offset) so you can see how complete the selection is."),
        epilog="example:\n  ocl decisions search 'Rachekündigung' --court bge --date-from 2015-01-01 --max-results 20 --format jsonl > hits.jsonl\n")
    search_parser.add_argument("query", nargs="?", default="", help="search text (FTS5 syntax: words, \"phrases\", AND/OR/NOT); omit to enumerate by filters only")
    _filters(search_parser)
    search_parser.add_argument("--chamber", help="chamber or division, substring match")
    search_parser.add_argument("--marked-for-publication", action="store_true", default=None, help="only BGer rulings flagged for the official BGE collection")
    search_parser.add_argument("--sort", choices=["relevance", "date_desc", "date_asc"], default="relevance", help="ordering (default: relevance)")
    search_parser.add_argument("--detail", choices=["full", "compact"], default="compact", help="record detail requested from the service (default: compact; full adds regeste, snippet and pinpoint)")
    search_parser.add_argument("--no-pinpoint", action="store_true", help="skip the per-result pinpoint lookup for full detail (faster)")
    search_parser.add_argument("--max-results", type=positive_int, default=50, help="bound for the selection (default: 50; a text query allows at most 800)")
    search_parser.add_argument("--page-size", type=positive_int, default=50, help="results per request for filter-only enumeration, at most 800 (default: 50)")
    search_parser.add_argument("--offset", type=nonnegative_int, default=0, help="skip this many results first (default: 0)")

    get = actions.add_parser("get", parents=[common], formatter_class=fmt,
                             help="fetch full decisions by ID or docket, singly or in batch",
                             description=("Fetch full decisions. Accepts decision IDs (bge_BGE_136_III_513), dockets "
                                          "(4A_747/2012) or the JSONL rows written by `decisions search`."),
                             epilog="example:\n  ocl decisions search 'Rachekündigung' --max-results 5 --format jsonl | ocl decisions get --stdin --format jsonl > decisions.jsonl\n")
    _input(get, "ids", "decision IDs or dockets")
    get.add_argument("--no-full-text", action="store_true", help="metadata only, without the decision text")
    passage = actions.add_parser("passage", parents=[common], formatter_class=fmt,
                                 help="fetch one numbered Erwägung verbatim",
                                 description="Fetch the exact text of one numbered Erwägung (consid.), the citable unit in Swiss practice.",
                                 epilog="example:\n  ocl decisions passage bge_BGE_136_III_513 2.3\n")
    passage.add_argument("decision_id", help="decision ID or docket, e.g. bge_BGE_136_III_513 or 4A_747/2012")
    passage.add_argument("number", help="Erwägung number, e.g. 2 or 2.3")

    laws = commands.add_parser("laws", parents=[common], formatter_class=fmt,
                               help="fetch statute text (federal and cantonal, current or historical)",
                               description="Fetch statute text from the Fedlex mirror (federal) or the cantonal mirrors.")
    law_actions = laws.add_subparsers(dest="action", required=True, metavar="ACTION")
    law = law_actions.add_parser("get", parents=[common], formatter_class=fmt,
                                 help="fetch one article, or a law's table of contents",
                                 description="Fetch one article (with --article) or the table of contents of a law.",
                                 epilog="examples:\n  ocl laws get OR --article 336\n  ocl laws get ZGB --article 8 --language fr\n  ocl laws get OR --article 41 --as-of 2015-01-01\n")
    law.add_argument("abbreviation", help="law abbreviation, e.g. OR, ZGB, StGB, BV (or _ with --sr-number)")
    law.add_argument("--article", help="article number, e.g. 41 or 41a")
    law.add_argument("--sr-number", help="SR number instead of an abbreviation, e.g. 220")
    law.add_argument("--as-of", help="historical federal edition in force on this date, YYYY-MM-DD")
    law.add_argument("--canton", default="CH", help="CH for federal law (default), or a canton code for cantonal law")
    law.add_argument("--language", choices=["de", "fr", "it"], default="de", help="text language (default: de)")

    citations = commands.add_parser("citations", parents=[common], formatter_class=fmt,
                                    help="follow the citation graph, or check a list of references",
                                    description="Follow what a decision cites and what cites it, or check a list of references from a draft.")
    citation_actions = citations.add_subparsers(dest="action", required=True, metavar="ACTION")
    listing = citation_actions.add_parser("list", parents=[common], formatter_class=fmt,
                                          help="what a decision cites and what cites it (one page)",
                                          description="List citations of a decision from the reference graph, one explicitly paged response per direction.",
                                          epilog="example:\n  ocl citations list bge_BGE_140_III_86 --direction incoming --limit 20\n")
    listing.add_argument("decision_id", help="decision ID or docket")
    listing.add_argument("--direction", choices=["both", "incoming", "outgoing"], default="both", help="incoming = decisions citing it; outgoing = decisions it cites (default: both)")
    listing.add_argument("--limit", type=positive_int, default=50, help="per direction, at most 200 (default: 50)")
    listing.add_argument("--offset", type=nonnegative_int, default=0, help="pagination offset per direction (default: 0)")
    listing.add_argument("--min-confidence", type=confidence, default=0.3, help="minimum resolution confidence 0-1 (default: 0.3)")
    resolve = citation_actions.add_parser(
        "resolve", parents=[common], formatter_class=fmt,
        help="check references from a draft: found, missing, ambiguous; verify pinpoints",
        description=("Check a list of references. Each comes back as resolved, missing, ambiguous, "
                     "resolution_incomplete, unrecognized or error, with the service's evidence. A "
                     "reference with a pinpoint is only 'resolved' if that Erwägung exists. Existence "
                     "is not legal support: the report never says a decision backs a proposition."),
        epilog="examples:\n  ocl citations resolve 'BGE 136 III 513' '4A_747/2012'\n  ocl citations resolve --input references.jsonl --format jsonl > resolution.jsonl\n\nreferences.jsonl lines look like {\"reference\": \"BGE 136 III 513\", \"pinpoint\": \"2.3\"}\n")
    _input(resolve, "references", "references such as 'BGE 136 III 513' or '4A_747/2012'")
    resolve.add_argument("--language", choices=["de", "fr", "it"], default="de", help="language of the returned citation string (default: de)")

    cite = commands.add_parser("cite", parents=[common], formatter_class=fmt,
                               help="get the canonical citation string for a reference",
                               description="Get the canonical Swiss citation string (DE/FR/IT) and link for a reference. Formatting only: use `citations resolve` or `decisions passage` to check that a pinpoint exists.",
                               epilog="example:\n  ocl cite 'BGE 136 III 513' --pinpoint 2.3 --language fr\n")
    _input(cite, "references", "references such as 'BGE 136 III 513' or '4A_747/2012'")
    cite.add_argument("--pinpoint", help="Erwägung number to append, e.g. 2.3")
    cite.add_argument("--language", choices=["de", "fr", "it"], default="de", help="primary language of citation_string (default: de)")

    bundle = commands.add_parser("bundle", parents=[common], formatter_class=fmt,
                                 help="save the evidence for a question into a folder you can keep or share",
                                 description="Save research evidence into a folder with a provenance manifest.")
    bundle_actions = bundle.add_subparsers(dest="action", required=True, metavar="ACTION")
    create = bundle_actions.add_parser(
        "create", parents=[common], formatter_class=fmt,
        help="run a search and save the decisions, passages and statutes it needs",
        description=("Run a search, then save each selected decision (JSON and plain text), the requested "
                     "Erwägungen and statute articles into --out. INDEX.md lists what was saved in plain "
                     "language; manifest.json records every request, timestamp, source link and file hash. "
                     "Nothing already saved is ever overwritten; use --resume to finish an interrupted run."),
        epilog="example:\n  ocl bundle create 'Rachekündigung Art. 336 OR' --max-results 10 --passage 2 --law OR:336 --out rachekuendigung-2026-09\n")
    create.add_argument("query", help="search text; an empty string with filters enumerates by filters only")
    create.add_argument("--out", required=True, metavar="DIR", help="new folder for the evidence (or an existing bundle with --resume)")
    create.add_argument("--max-results", type=positive_int, default=10, help="how many decisions to select (default: 10; a text query allows at most 800)")
    _filters(create)
    create.add_argument("--resume", action="store_true", help="continue an interrupted or partly failed bundle with the same options")
    create.add_argument("--passage", action="append", default=[], metavar="NUMBER", help="also save this Erwägung of every selected decision; repeatable")
    create.add_argument("--law", action="append", default=[], metavar="ABBR:ARTICLE", help="also save this federal statute article, e.g. OR:336; repeatable")
    return parser


def read_inputs(args, field="decision_id") -> list[dict]:
    """Validate every input before making requests; metadata lines are ignored."""
    inputs = [{field: value} for value in getattr(args, "ids", getattr(args, "references", []))]
    sources = []
    if getattr(args, "stdin", False):
        sources.append(("stdin", sys.stdin.read().lstrip("\ufeff")))
    if getattr(args, "input", None):
        sources.append((args.input, Path(args.input).read_text(encoding="utf-8-sig")))
    for name, content in sources:
        for line_number, line in enumerate(content.splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            if line[0] in '{["':
                try:
                    row = json.loads(line)
                except ValueError as exc:
                    raise ValueError(f"{name}:{line_number}: invalid JSON") from exc
                if isinstance(row, str):
                    row = {field: row}
                if isinstance(row, dict) and row.get("_type") == "pagination":
                    continue
                if not isinstance(row, dict) or not isinstance(row.get(field), str) or not row[field].strip():
                    raise ValueError(f"{name}:{line_number}: expected a string or object containing {field}")
                if row.get("pinpoint") is not None and not isinstance(row["pinpoint"], str):
                    raise ValueError(f"{name}:{line_number}: pinpoint must be a string")
                inputs.append(row)
            else:
                inputs.append({field: line})
    if not inputs or any(not row[field].strip() for row in inputs):
        raise ValueError("provide at least one reference/ID, --stdin or --input FILE")
    return inputs


def search(client, params: dict, max_results: int, page_size: int = 50) -> tuple[dict, int]:
    """Bounded retrieval that keeps every server page's metadata and partial results.

    A text query is ranked over one bounded candidate pool whose size depends
    on the requested window, so its pages are not composable: the selection
    is fetched as ONE request of ``max_results`` (server cap 800). Filter-only
    searches enumerate an exact, stably ordered set and are paged. Exhausting
    retrievable pages never proves corpus-wide exhaustiveness.
    """
    if max_results <= 0 or not 1 <= page_size <= 800:
        raise ValueError("max-results must be positive; page-size must be between 1 and 800")
    ranked = bool((params.get("query") or "").strip())
    if ranked and max_results > RANKED_MAX_RESULTS:
        raise ValueError(
            f"a text query is ranked over one bounded candidate pool; --max-results is limited to "
            f"{RANKED_MAX_RESULTS} for text queries (filter-only searches without query text enumerate further)")
    start = params.get("offset", 0)
    offset = start
    results, pages, errors = [], [], []
    seen: set = set()
    duplicates = 0
    last = {}
    while len(results) < max_results:
        limit = max_results if ranked else min(page_size, max_results - len(results))
        try:
            page = client.get("/api/decisions", {**params, "limit": limit, "offset": offset})
            rows = page.get("results")
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise APIError(None, "search response has no valid results array")
            if len(rows) > limit:
                raise APIError(None, "search response exceeds the requested page size")
            if not isinstance(page.get("has_more"), bool) or not isinstance(page.get("total_is_lower_bound"), bool):
                raise APIError(None, "search response lacks completeness metadata")
            metadata = {key: value for key, value in page.items() if key != "results"}
            pages.append(metadata)
            for row in rows:
                decision_id = row.get("decision_id")
                if isinstance(decision_id, str):
                    if decision_id in seen:
                        duplicates += 1
                        continue
                    seen.add(decision_id)
                results.append(row)
            last = metadata
            if ranked or not page["has_more"]:
                break
            next_offset = page.get("next_offset")
            if not rows or type(next_offset) is not int or next_offset != offset + len(rows):
                raise APIError(None, "search pagination did not advance consistently")
            offset = next_offset
        except APIError as exc:
            if not pages:
                raise
            errors.append({"offset": offset, **exc.to_dict()})
            break
    envelope = {**last, "results": results, "returned": len(results), "offset": start,
                "limit": max_results, "total_is_lower_bound": any(p["total_is_lower_bound"] for p in pages),
                "_client": {"pages": pages, "errors": errors,
                            "ranked_single_request": ranked,
                            "duplicates_dropped": duplicates,
                            "max_results_reached": len(results) >= max_results,
                            "retrieval_complete": not errors and last.get("has_more") is False}}
    return envelope, 4 if errors else 0


def _project(row, fields):
    if not fields or not isinstance(row, dict):
        return row
    selected = {key: row[key] for key in fields if key in row}
    # Errors and uncertainty must survive convenience projections.
    for key in ("error", "errors", "status", "ambiguous", "candidates", "_client",
                "exists", "pinpoint_exists", "close_matches", "warning", "warnings",
                "note", "_note", "hint", "recency_note", "completeness", "manifest",
                "bundle", "scope", "provenance", "official_source_available",
                "pinpoint_status", "legal_support_assessed", "reason", "available_e_numbers", "identity_check"):
        if key in row:
            selected[key] = row[key]
    for key in row:
        if key.endswith(("_warning", "_warnings", "_note")):
            selected[key] = row[key]
    if row.get("status") == "ambiguous" and "lookup" in row:
        selected["lookup"] = row["lookup"]
    if "articles" in row:
        for key in ("sr_number", "abbreviation", "language", "canton", "version", "as_of",
                    "consolidation_date", "snapshot_date", "source_url", "source_label", "pdf_url",
                    "fedlex_snapshot_uri", "work_uri", "work_entry_in_force", "work_no_longer_in_force",
                    "work_in_force_status", "verbatim_status", "pending_changes"):
            if key in row:
                selected[key] = row[key]
    return selected


def emit(value, args):
    fields = [field.strip() for field in args.fields.split(",") if field.strip()] if args.fields else None
    collections = [key for key in ("results", "incoming", "outgoing")
                   if isinstance(value, dict) and isinstance(value.get(key), list)]
    if args.format == "jsonl" and collections:
        for key in collections:
            for row in value[key]:
                row = _project(row, fields)
                if key in {"incoming", "outgoing"}:
                    row = {**row, "_direction": key}
                print(json.dumps(row, ensure_ascii=False, allow_nan=False))
        metadata = {key: item for key, item in value.items() if key not in collections}
        print(json.dumps({"_type": "pagination", **metadata}, ensure_ascii=False, allow_nan=False))
    else:
        if collections:
            value = {**value, **{key: [_project(row, fields) for row in value[key]] for key in collections}}
        else:
            value = _project(value, fields)
        print(json.dumps(value, ensure_ascii=False, allow_nan=False, indent=None if args.format == "jsonl" else 2))


def create_client(args):
    return Client(base_url=args.base_url, timeout=args.timeout, retries=args.retries)


def _decision_id(client, reference):
    # ASGI servers decode %2F before route matching. Resolve docket forms such
    # as 4A_747/2012 using the query-based citation endpoint first.
    if "/" not in reference:
        return reference
    citation = client.get("/api/cite", {"reference": reference})
    if citation.get("error"):
        raise APIError(200, str(citation["error"]))
    if citation.get("exists") is False:
        raise APIError(404, "Reference not found in the corpus: " + reference)
    decision_id = citation.get("decision_id")
    if citation.get("exists") is not True or not isinstance(decision_id, str) or not decision_id or "/" in decision_id:
        raise APIError(None, "Citation response does not identify a canonical decision ID")
    # The service also matches docket fragments by substring. Only operate on
    # a decision whose own docket or citation label is the given reference.
    decision = client.get("/api/decisions/" + quote(decision_id, safe=""), {"full_text": False})
    labels = {reference_key(record.get(name)) for record in (decision, citation)
              for name in ("docket_number", "citation_string", "citation_string_de",
                           "citation_string_fr", "citation_string_it")}
    if decision.get("decision_id") != decision_id or reference_key(reference) not in labels:
        raise APIError(None, (
            f"{reference} is not the exact docket of the resolved decision {decision_id} "
            f"(docket {decision.get('docket_number')!r}); use the canonical decision_id or `citations resolve`"))
    return decision_id


def _batch(args, client, kind):
    field = "decision_id" if kind == "get" else "reference"
    inputs = read_inputs(args, field)
    rows, errors = [], []
    for index, row in enumerate(inputs):
        try:
            if kind == "get":
                decision_id = _decision_id(client, row[field])
                result = client.get("/api/decisions/" + quote(decision_id, safe=""), {"full_text": not args.no_full_text})
            else:
                pinpoint = row["pinpoint"] if row.get("pinpoint") is not None else args.pinpoint
                result = client.get("/api/cite", {"reference": row[field], "pinpoint": pinpoint, "language": args.language})
            if result.get("error"):
                errors.append({"index": index, field: row[field], "status": 200,
                               "message": str(result["error"]), "response": result})
                continue
            rows.append(result)
        except APIError as exc:
            errors.append({"index": index, field: row[field], **exc.to_dict()})
    unresolved = any(row.get("exists") is False or row.get("pinpoint_exists") is False for row in rows)
    if len(inputs) == 1 and not errors:
        return rows[0], 4 if unresolved else 0
    code = ((4 if rows else 3) if errors else (4 if unresolved else 0))
    return {"results": rows, "errors": errors, "requested": len(inputs), "returned": len(rows)}, code


def _response(value):
    return value, 3 if value.get("error") else 0


def run(args, client):
    if args.command == "bundle" or (args.command == "citations" and args.action == "resolve"):
        from . import workflows
        return workflows.run(args, client)
    if args.command == "decisions":
        if args.action == "search":
            params = {name: getattr(args, name) for name in ("query", "court", "canton", "language", "date_from", "date_to", "chamber", "marked_for_publication", "sort", "offset")}
            params.update(fields=args.detail, include_pinpoint=not args.no_pinpoint)
            return search(client, params, args.max_results, args.page_size)
        if args.action == "get":
            return _batch(args, client, "get")
        decision_id = _decision_id(client, args.decision_id)
        return _response(client.get("/api/erwaegung/" + quote(decision_id, safe="") + "/" + quote(args.number, safe="")))
    if args.command == "laws":
        params = {name: getattr(args, name) for name in ("article", "sr_number", "as_of", "canton", "language")}
        result, code = _response(client.get("/api/laws/" + quote(args.abbreviation, safe=""), params))
        if not code and args.article and result.get("articles") == []:
            code = 4
        return result, code
    if args.command == "citations":
        if args.limit > 200:
            raise ValueError("citation limit must be between 1 and 200")
        params = {name: getattr(args, name) for name in ("direction", "limit", "offset", "min_confidence")}
        decision_id = _decision_id(client, args.decision_id)
        return _response(client.get("/api/citations/" + quote(decision_id, safe=""), params))
    return _batch(args, client, "cite")


def main(argv=None):
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        value, code = run(args, create_client(args))
        emit(value, args)
        if code:
            print("ocl: some requested items failed or did not resolve; see the structured output for details", file=sys.stderr)
        sys.stdout.flush()
        return code
    except APIError as exc:
        print(json.dumps({"error": exc.to_dict()}, ensure_ascii=False), file=sys.stderr)
        return 3
    except BrokenPipeError:
        # Prevent a second BrokenPipeError from Python's interpreter shutdown.
        try:
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, sys.stdout.fileno())
            os.close(devnull)
        except (OSError, AttributeError, ValueError):
            pass
        return 0
    except (ValueError, OSError) as exc:
        print("ocl: " + str(exc), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
