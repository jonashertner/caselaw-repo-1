"""Readable terminal rendering for humans. JSON/JSONL contracts are unchanged.

Text mode is chosen when stdout is a terminal (or with --format text). It never
rewrites evidence: citation strings, identifiers and passage text are shown as
served. Markdown link markup inside a passage is folded to its text for
display only; the JSON output keeps it.
"""
from __future__ import annotations

import json
import re
import shutil
import textwrap
from pathlib import Path

_LINK = re.compile(r"\[([^\]]+)\]\((?:https?://|/)[^)]+\)")
_MARK = re.compile(r"</?mark>")
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


class Style:
    """ANSI styling that degrades to plain text when colour is off."""

    def __init__(self, enabled: bool):
        self.enabled = enabled

    def _wrap(self, code: str, text: str) -> str:
        return f"\x1b[{code}m{text}\x1b[0m" if self.enabled and text else text

    def bold(self, t): return self._wrap("1", t)
    def dim(self, t): return self._wrap("2", t)
    def green(self, t): return self._wrap("32", t)
    def red(self, t): return self._wrap("31", t)
    def yellow(self, t): return self._wrap("33", t)
    def cyan(self, t): return self._wrap("36", t)
    def magenta(self, t): return self._wrap("35", t)


def terminal_width(default: int = 100, maximum: int = 110) -> int:
    return max(60, min(shutil.get_terminal_size((default, 24)).columns, maximum))


def visible_len(text: str) -> int:
    return len(_ANSI.sub("", text))


def _wrap(text: str, width: int, indent: str = "") -> list[str]:
    lines: list[str] = []
    for paragraph in (text or "").replace("\r", "").split("\n"):
        if not paragraph.strip():
            lines.append("")
            continue
        lines.extend(textwrap.wrap(paragraph, width, initial_indent=indent, subsequent_indent=indent,
                                   break_long_words=False, break_on_hyphens=False) or [""])
    while lines and not lines[-1]:
        lines.pop()
    return lines


_SINGLE_NL = re.compile(r"(?<!\n)\n(?!\n)")
_SPACE_BEFORE_PUNCT = re.compile(r"\s+([,.;:!?)\]])")
_SPACE_AFTER_OPEN = re.compile(r"([(\[])\s+")


def _display_text(text: str) -> str:
    """Served text for reading: link markup folded, hard-wrapped lines joined,
    stray spaces around punctuation (left by folded links) removed. Display only."""
    text = _MARK.sub("", _LINK.sub(r"\1", text or ""))
    text = _SINGLE_NL.sub(" ", text)
    text = _SPACE_BEFORE_PUNCT.sub(r"\1", text)
    text = _SPACE_AFTER_OPEN.sub(r"\1", text)
    text = re.sub(r"(['\u2019])\s+(?=\w)", r"\1", text)  # "l' art." -> "l'art."
    return re.sub(r"[ \t]{2,}", " ", text)


def _label(row: dict) -> str:
    return (row.get("citation_string_de") or row.get("citation_string") or row.get("citation")
            or row.get("decision_id") or "?")


_STATUS_COLOUR = {
    "resolved": "green", "complete": "green", "saved": "green", "verified": "green", "retrieved": "green",
    "missing": "red", "error": "red", "failed": "red", "changed": "red",
}


def _status(s: Style, status: str) -> str:
    colour = _STATUS_COLOUR.get(status, "yellow")
    return getattr(s, colour)(status)


# ── renderers ──────────────────────────────────────────────────────────────

def render_error(value: dict, s: Style, width: int) -> str:
    error = value.get("error")
    message = error.get("message") if isinstance(error, dict) else str(error)
    lines = [s.red("error: ") + str(message)]
    for key in ("hint", "message", "note", "_note"):
        if key != "message" and value.get(key):
            lines += _wrap(str(value[key]), width, "  ")
    if value.get("available_e_numbers"):
        lines.append("  " + s.dim("available: " + ", ".join(map(str, value["available_e_numbers"]))))
    return "\n".join(lines)


def render_search(value: dict, s: Style, width: int) -> str:
    rows = value.get("results") or []
    lines = []
    for number, row in enumerate(rows, 1):
        meta = " · ".join(str(x) for x in (row.get("court"), row.get("decision_date")) if x)
        lines.append(f"{s.dim(f'{number:>3}.')} {s.bold(_label(row))}" + (f"  {s.dim(meta)}" if meta else ""))
        lines.append(f"     {s.cyan(str(row.get('decision_id', '')))}")
        if row.get("title"):
            lines += _wrap(str(row["title"]), width, "     ")
        if row.get("snippet"):
            lines += [s.dim(l) for l in _wrap(_display_text(str(row["snippet"])), width, "     ")]
        if row.get("regeste"):
            lines += [s.dim(l) for l in _wrap(_display_text(str(row["regeste"]))[:600], width, "     ")]
    total = value.get("total")
    footer = f"{len(rows)} shown"
    if total is not None:
        footer += f" of {'at least ' if value.get('total_is_lower_bound') else ''}{total} matching"
    client = value.get("_client") or {}
    if value.get("has_more"):
        footer += f"; more retrievable from offset {value.get('next_offset')}"
    if client.get("ranked_single_request"):
        footer += ". Ranked search over a bounded pool; not an exhaustive list"
    if client.get("duplicates_dropped"):
        footer += f"; {client['duplicates_dropped']} duplicate row(s) dropped"
    if client.get("errors"):
        footer += f"; {len(client['errors'])} page request(s) failed"
    if value.get("note"):
        lines += [""] + _wrap(str(value["note"]), width)
    lines += ["", s.dim(footer + ".")]
    return "\n".join(lines)


def render_decision(value: dict, s: Style, width: int) -> str:
    lines = [s.bold(_label(value))]
    meta = " · ".join(str(x) for x in (value.get("decision_id"), value.get("court"), value.get("decision_date"),
                                       value.get("docket_number")) if x)
    lines.append(s.dim(meta))
    for key in ("citation_string_fr", "citation_string_it"):
        if value.get(key):
            lines.append(s.dim(str(value[key])))
    if value.get("source_url"):
        lines.append(s.cyan(str(value["source_url"])))
    if value.get("recency_note"):
        lines += [""] + [s.yellow(l) for l in _wrap(str(value["recency_note"]), width)]
    if value.get("regeste"):
        lines += ["", s.bold("Regeste")] + _wrap(_display_text(str(value["regeste"])), width)
    if value.get("full_text"):
        lines += ["", s.bold("Text")] + _wrap(_display_text(str(value["full_text"])), width)
        if value.get("full_text_truncated"):
            lines += ["", s.yellow(f"Text truncated at {value.get('full_text_returned_chars')} of "
                                   f"{value.get('full_text_total_chars')} characters; full text: {value.get('full_text_url')}")]
    return "\n".join(lines)


def render_passage(value: dict, s: Style, width: int) -> str:
    header = value.get("citation_string_de") or f"{value.get('decision_id')} E. {value.get('e_number')}"
    meta = " · ".join(str(x) for x in (value.get("decision_id"), f"E. {value.get('e_number')}",
                                       value.get("court"), value.get("language")) if x)
    lines = [s.bold(str(header)), s.dim(meta)]
    if value.get("composed_of"):
        lines.append(s.dim("composed of " + ", ".join(f"E. {e}" for e in value["composed_of"])))
    if value.get("note"):
        lines += [s.yellow(l) for l in _wrap(str(value["note"]), width)]
    lines += [""] + _wrap(_display_text(str(value.get("text") or "")), width)
    return "\n".join(lines)


def render_cite(value: dict, s: Style, width: int) -> str:
    if value.get("exists") is False:
        lines = [s.red("not found: ") + str(value.get("queried") or value.get("resolved_id") or "")]
        for match in value.get("close_matches") or []:
            if isinstance(match, dict):
                lines.append("  " + s.dim("close match: ") + _label(match) + s.dim(f"  {match.get('decision_id', '')}"))
        if value.get("_note"):
            lines += [""] + [s.dim(l) for l in _wrap(str(value["_note"]), width)]
        return "\n".join(lines)
    lines = [s.bold(str(value.get("citation_string") or value.get("citation_string_de") or ""))]
    for key in ("citation_string_de", "citation_string_fr", "citation_string_it"):
        if value.get(key) and value[key] != value.get("citation_string"):
            lines.append(value[key])
    if value.get("canonical_url"):
        lines.append(s.cyan(str(value["canonical_url"])))
    if value.get("pinpoint_note"):
        lines += [s.yellow(l) for l in _wrap(str(value["pinpoint_note"]), width)]
        if value.get("available_e_numbers"):
            lines.append("  " + s.dim("available: " + ", ".join(map(str, value["available_e_numbers"][:12]))))
    if value.get("rule_statement"):
        lines += ["", s.dim("Rule statement (verbatim excerpt):")] + _wrap(_display_text(str(value["rule_statement"])), width, "  ")
    return "\n".join(lines)


def render_law(value: dict, s: Style, width: int) -> str:
    head = " · ".join(str(x) for x in (value.get("abbreviation"), f"SR {value.get('sr_number')}" if value.get("sr_number") else None,
                                       value.get("canton"), value.get("language")) if x)
    lines = [s.bold(head)]
    if value.get("title"):
        lines += _wrap(str(value["title"]), width)
    edition = []
    if value.get("consolidation_date"):
        edition.append(f"consolidated {value['consolidation_date']}")
    if value.get("as_of"):
        edition.append(f"edition in force on {value['as_of']}")
    if value.get("snapshot_date"):
        edition.append(f"snapshot {value['snapshot_date']}")
    if value.get("source_label") or value.get("source_url"):
        edition.append(f"{value.get('source_label') or 'source'}: {value.get('source_url') or ''}".strip())
    if edition:
        lines.append(s.dim(" · ".join(edition)))
    if value.get("verbatim_quotation"):
        lines.append(s.yellow(f"verbatim quotation: {value['verbatim_quotation']}"))
    articles = value.get("articles") or []
    for article in articles:
        if not isinstance(article, dict):
            continue
        title = f"Art. {article.get('article_num')}"
        if article.get("heading"):
            title += f"  {article['heading']}"
        lines += ["", s.bold(title)]
        if article.get("section_heading"):
            lines.append(s.dim(str(article["section_heading"])))
        if article.get("text"):
            lines += _wrap(str(article["text"]), width)
    if value.get("pending_changes"):
        lines += ["", s.yellow(f"{len(value['pending_changes'])} pending change(s) recorded for this act.")]
    if not articles:
        lines += ["", s.dim("No articles in this response.")]
    return "\n".join(lines)


def _edge_label(edge: dict) -> str:
    return (edge.get("docket_number") or edge.get("target_ref") or edge.get("source_decision_id")
            or edge.get("target_decision_id") or "?")


def render_citations(value: dict, s: Style, width: int) -> str:
    lines = [s.bold(f"Citations for {value.get('decision_id')}")]
    for direction, title in (("incoming", "Cited by"), ("outgoing", "Cites")):
        if direction not in value:
            continue
        edges = value.get(direction) or []
        total = value.get(f"{direction}_total")
        head = f"{title}: {len(edges)}" + (f" of {total}" if total is not None else "")
        if value.get(f"{direction}_has_more"):
            head += f" (more from offset {value.get('next_offset')})"
        lines += ["", s.bold(head)]
        for edge in edges:
            meta = " · ".join(str(x) for x in (edge.get("court"), edge.get("decision_date")) if x)
            conf = edge.get("confidence_score")
            extra = f"  {s.dim(meta)}" if meta else ""
            if conf is not None:
                extra += s.dim(f"  conf {conf:.2f}")
            target = edge.get("source_decision_id") if direction == "incoming" else edge.get("target_decision_id")
            lines.append(f"  {_edge_label(edge)}{extra}")
            if target:
                lines.append(f"    {s.cyan(str(target))}")
    return "\n".join(lines)


def _candidate_line(candidate: dict) -> str:
    parts = [str(candidate.get("decision_id") or "")]
    meta = " · ".join(str(x) for x in (candidate.get("court"), candidate.get("decision_date")) if x)
    return parts[0] + (f" ({meta})" if meta else "")


def render_resolution(value: dict, s: Style, width: int) -> str:
    rows = value.get("results") or []
    ref_w = min(36, max([len(str(r.get("reference", ""))) for r in rows] + [9]))
    id_w = min(34, max([len(str(r.get("decision_id") or "")) for r in rows] + [0]))
    lines = []
    for row in rows:
        status = str(row.get("status", "?"))
        ref = str(row.get("reference", ""))
        decision_id = str(row.get("decision_id") or "")
        detail = []
        provenance = row.get("provenance") or {}
        label = provenance.get("citation_string_de") or provenance.get("citation_string")
        if label and reference_like(label) != reference_like(ref):
            detail.append(s.dim(label))
        if row.get("query") and row["query"] != ref and not decision_id.startswith(str(row["query"])):
            detail.append(s.dim(f"via {row['query']}"))
        for item in row.get("discrepancies") or []:
            if item.get("kind") == "date":
                detail.append(s.yellow(f"date written {item.get('written')}, decision dated {item.get('decision')}"))
            elif item.get("kind") == "docket":
                detail.append(s.yellow(f"docket {item.get('written')} names {item.get('resolves_to')}"
                                       + (f" ({item['decision_date']})" if item.get("decision_date") else "")))
        if row.get("pinpoint"):
            ps = row.get("pinpoint_status")
            origin = s.dim(" (from the reference)") if row.get("pinpoint_source") == "reference" else ""
            if ps == "retrieved":
                detail.append(s.green(f"E. {row['pinpoint']} retrieved") + origin)
            elif ps == "parent_retrieved":
                parent = (row.get("passage") or {}).get("e_number")
                detail.append(s.yellow(f"E. {row['pinpoint']} not indexed as such; E. {parent} retrieved, locate the letter inside") + origin)
            elif ps == "unavailable":
                available = row.get("available_e_numbers")
                detail.append(s.yellow(f"E. {row['pinpoint']} not in the index") + origin
                              + (s.dim(f"  available: {', '.join(map(str, available[:12]))}") if available else ""))
        if status == "missing":
            detail.append(s.dim("not in the corpus"))
        if status in ("unrecognized",) and row.get("service_candidate"):
            candidate = row["service_candidate"]
            detail.append(s.dim(f"service proposed {candidate.get('decision_id')} ({candidate.get('citation_string_de') or candidate.get('docket_number')}); label not in the reference"))
        if status == "ambiguous" and row.get("candidates"):
            detail.append(s.dim("candidates: " + "; ".join(_candidate_line(c) for c in row["candidates"][:6])))
        if row.get("related_docket"):
            detail.append(s.dim(f"docket {row['related_docket'].get('docket')} = {row['related_docket'].get('decision_id')}, same date"))
        if row.get("reason") and status not in ("discrepancy", "ambiguous", "unrecognized"):
            detail.append(s.dim(str(row["reason"])))
        elif status in ("ambiguous", "unrecognized") and not (row.get("candidates") or row.get("service_candidate")):
            detail.append(s.dim(str(row.get("reason") or "")))
        if row.get("error") and status == "error":
            detail.append(s.dim(str(row["error"].get("message"))))
        for note in row.get("notes") or []:
            detail.append(s.dim(str(note)))
        line = f"{_status(s, status)}{' ' * max(1, 22 - len(status))}{ref:<{ref_w}}  "
        line += f"{s.cyan(decision_id)}{' ' * max(0, id_w - len(decision_id))}  " if id_w else ""
        lines.append((line + "  ".join(d for d in detail if d)).rstrip())
    counts = value.get("counts") or {}
    summary = ", ".join(f"{n} {k}" for k, n in counts.items())
    lines += ["", _status(s, str(value.get("status", ""))) + s.dim(f": {summary}." if summary else ".")]
    lines.append(s.dim("Existence, identity and pinpoints only; no assessment of legal support."))
    return "\n".join(lines)


def reference_like(text: str) -> str:
    return re.sub(r"[\s_]+", "", (text or "").casefold())


def render_bundle(value: dict, s: Style, width: int) -> str:
    completeness = value.get("completeness") or {}
    lines = [_status(s, str(value.get("status", ""))) + "  " + s.bold(str(value.get("bundle", "")))]
    parts = [f"{completeness.get('selected_decisions', 0)} decision(s) selected"]
    page = completeness.get("server_last_page") or {}
    if page.get("total") is not None:
        parts.append(f"{'at least ' if page.get('total_is_lower_bound') else ''}{page['total']} matching")
    if completeness.get("unavailable_items"):
        parts.append(s.yellow(f"{completeness['unavailable_items']} item(s) the service does not have"))
    if completeness.get("failed_items"):
        parts.append(s.red(f"{completeness['failed_items']} item(s) failed to download"))
    lines.append("  " + s.dim(", ".join(parts)))
    manifest = None
    try:
        manifest = json.loads(Path(str(value.get("manifest"))).read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        pass
    if manifest:
        lines.append("")
        for item in manifest.get("items", {}).values():
            provenance = item.get("provenance") or {}
            label = provenance.get("citation_string_de") or provenance.get("citation_string") or item["identifier"]
            if item["kind"] == "passage":
                decision_id, number = item["identifier"].rsplit(":", 1)
                parent = (manifest["items"].get("decision:" + decision_id) or {}).get("provenance") or {}
                label = provenance.get("citation_string_de") or f"{parent.get('citation_string_de') or decision_id}, E. {number}"
            if item["kind"] == "law":
                label = f"{provenance.get('abbreviation') or item['identifier'].split(':')[0]} Art. {item['identifier'].rsplit(':', 1)[1]}"
            status = item.get("status", "?")
            line = f"  {_status(s, status)}{' ' * max(1, 14 - len(status))}{label}"
            if item.get("error"):
                line += s.dim(f"  {item['error'].get('message')}")
            elif item.get("text_artifact"):
                line += s.dim(f"  {item['text_artifact']['path']}")
            elif item.get("artifact"):
                line += s.dim(f"  {item['artifact']['path']}")
            lines.append(line)
    lines += ["", s.dim("INDEX.md lists the folder; manifest.json is the record with hashes and source links.")]
    if completeness.get("failed_items"):
        lines.append(s.dim("Rerun the same command with --resume to retry the failed items."))
    return "\n".join(lines)


def render_bundle_verification(value: dict, s: Style, width: int) -> str:
    counts = value.get("counts") or {}
    lines = [_status(s, str(value.get("status", ""))) + "  " + s.bold(str(value.get("bundle", ""))),
             "  " + s.dim(", ".join(f"{counts.get(key, 0)} {key}" for key in ("ok", "changed", "missing", "unlisted")))]
    for key, colour in (("changed", s.red), ("missing", s.red), ("unlisted", s.yellow)):
        for path in value.get(key) or []:
            lines.append(f"  {colour(key):<18}{path}")
    snapshot = value.get("corpus_snapshot") or {}
    if snapshot.get("db_generation"):
        lines.append("  " + s.dim(f"collected on database generation {snapshot['db_generation']}"))
    lines += ["", s.dim(str(value.get("scope") or "File integrity against the manifest only."))]
    return "\n".join(lines)


def render_bundle_diff(value: dict, s: Style, width: int) -> str:
    lines = [s.bold("old ") + str(value.get("old", "")), s.bold("new ") + str(value.get("new", ""))]
    added, removed = value.get("added") or [], value.get("removed") or []
    changed, statuses = value.get("changed_text") or [], value.get("status_changes") or []
    lines.append("  " + s.dim(f"{len(added)} added, {len(removed)} removed, {len(changed)} text changed, "
                              f"{len(statuses)} status change(s), {len(value.get('unchanged') or [])} unchanged"))
    for decision_id in added:
        lines.append(f"  {s.green('added'):<18}{decision_id}")
    for decision_id in removed:
        lines.append(f"  {s.red('removed'):<18}{decision_id}")
    for item in changed:
        lines.append(f"  {s.yellow('text changed'):<18}{item.get('decision_id')}  " + s.dim(f"{str(item.get('old'))[:12]} → {str(item.get('new'))[:12]}"))
    for item in statuses:
        lines.append(f"  {s.yellow('status'):<18}{item.get('identifier')}  " + s.dim(f"{item.get('old')} → {item.get('new')}"))
    for key, change in (value.get("request_changes") or {}).items():
        lines.append(f"  {s.yellow('request'):<18}{key}: " + s.dim(f"{change.get('old')} → {change.get('new')}"))
    generation = value.get("corpus_generation") or {}
    if isinstance(generation, dict) and generation.get("old") != generation.get("new"):
        lines.append("  " + s.dim(f"database generation {generation.get('old')} → {generation.get('new')}"))
    if value.get("note"):
        lines += ["", s.dim(str(value["note"]))]
    return "\n".join(lines)


def render_bundle_addition(value: dict, s: Style, width: int) -> str:
    lines = [_status(s, str(value.get("status", ""))) + "  " + s.bold(str(value.get("bundle", "")))]
    for key, status in (value.get("added") or {}).items():
        lines.append(f"  {_status(s, str(status))}{' ' * max(1, 14 - len(str(status)))}{key}")
    completeness = value.get("completeness") or {}
    parts = [f"{completeness.get('saved_items', 0)} saved"]
    if completeness.get("unavailable_items"):
        parts.append(s.yellow(f"{completeness['unavailable_items']} unavailable"))
    if completeness.get("failed_items"):
        parts.append(s.red(f"{completeness['failed_items']} failed"))
    lines += ["", "  " + s.dim(", ".join(parts) + " in the bundle")]
    return "\n".join(lines)


def render_batch(value: dict, s: Style, width: int, item_renderer) -> str:
    blocks = [item_renderer(row, s, width) for row in value.get("results") or []]
    errors = value.get("errors") or []
    if errors:
        block = [s.red(f"{len(errors)} of {value.get('requested')} item(s) failed")]
        for error in errors:
            key = error.get("decision_id") or error.get("reference") or ""
            block.append(f"  {s.yellow(str(key))}  {s.dim(str(error.get('message', '')))}")
        blocks.append("\n".join(block))
    rule = s.dim("─" * min(width, 60))
    return f"\n{rule}\n".join(blocks)


def render(value, args, s: Style, width: int) -> str:
    """Pick the renderer from the command; fall back to pretty JSON."""
    if not isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, indent=2)
    command, action = getattr(args, "command", None), getattr(args, "action", None)
    if value.get("error") and command != "citations" or (isinstance(value.get("error"), str) and not value.get("results")):
        return render_error(value, s, width)
    if command == "decisions" and action == "search":
        return render_search(value, s, width)
    if command == "decisions" and action == "get":
        return render_batch(value, s, width, render_decision) if "requested" in value else render_decision(value, s, width)
    if command == "decisions" and action == "passage":
        return render_passage(value, s, width)
    if command == "laws":
        return render_law(value, s, width)
    if command == "citations" and action == "list":
        return render_citations(value, s, width)
    if command == "citations" and action == "resolve":
        return render_resolution(value, s, width)
    if command == "cite":
        return render_batch(value, s, width, render_cite) if "requested" in value else render_cite(value, s, width)
    if command == "bundle":
        kind = value.get("kind")
        if kind == "opencaselaw-bundle-verification":
            return render_bundle_verification(value, s, width)
        if kind == "opencaselaw-bundle-diff":
            return render_bundle_diff(value, s, width)
        if "added" in value:
            return render_bundle_addition(value, s, width)
        return render_bundle(value, s, width)
    return json.dumps(value, ensure_ascii=False, indent=2)


# ── tabular formats: table, csv, md ────────────────────────────────────────

def tabular(value, args):
    """(columns, rows) for list-shaped results, else None. Cells are plain strings."""
    if not isinstance(value, dict):
        return None
    command, action = getattr(args, "command", None), getattr(args, "action", None)
    def cell(v):
        return "" if v is None else str(v)
    if command == "decisions" and action == "search":
        cols = ["decision_id", "citation", "court", "decision_date", "docket_number", "title"]
        rows = [[cell(r.get("decision_id")), cell(_label(r)), cell(r.get("court")), cell(r.get("decision_date")),
                 cell(r.get("docket_number")), cell(r.get("title"))] for r in value.get("results") or []]
        return cols, rows
    if command == "citations" and action == "resolve":
        cols = ["reference", "status", "decision_id", "pinpoint", "pinpoint_status", "decision", "identity", "detail"]
        rows = []
        for r in value.get("results") or []:
            # The decision-level label from provenance, never the pinpointed
            # string: a pinpoint the index lacks must not read as if it existed.
            provenance = r.get("provenance") or {}
            detail = []
            for item in r.get("discrepancies") or []:
                detail.append(f"{item.get('kind')}: written {item.get('written')}, record {item.get('decision') or item.get('resolves_to')}")
            if r.get("status") == "unrecognized" and r.get("service_candidate"):
                detail.append(f"service proposed {r['service_candidate'].get('decision_id')}")
            if r.get("status") == "ambiguous" and r.get("candidates"):
                detail.append("candidates: " + ", ".join(str(c.get("decision_id")) for c in r["candidates"][:5]))
            if r.get("pinpoint_status") == "parent_retrieved":
                detail.append(f"E. {(r.get('passage') or {}).get('e_number')} retrieved instead")
            if r.get("status") == "error" and r.get("error"):
                detail.append(str(r["error"].get("message")))
            rows.append([cell(r.get("reference")), cell(r.get("status")), cell(r.get("decision_id")), cell(r.get("pinpoint")),
                         cell(r.get("pinpoint_status")), cell(provenance.get("citation_string_de") or provenance.get("citation_string")),
                         cell((r.get("identity_check") or {}).get("method")), "; ".join(detail)])
        return cols, rows
    if command == "citations" and action == "list":
        cols = ["direction", "label", "decision_id", "court", "decision_date", "confidence"]
        rows = []
        for direction in ("incoming", "outgoing"):
            for e in value.get(direction) or []:
                target = e.get("source_decision_id") if direction == "incoming" else e.get("target_decision_id")
                conf = e.get("confidence_score")
                rows.append([direction, cell(_edge_label(e)), cell(target), cell(e.get("court")), cell(e.get("decision_date")),
                             f"{conf:.2f}" if isinstance(conf, (int, float)) else ""])
        return cols, rows
    if command == "laws":
        cols = ["article", "heading", "section", "text"]
        rows = [[cell(a.get("article_num")), cell(a.get("heading")), cell(a.get("section")), cell(a.get("text"))]
                for a in value.get("articles") or [] if isinstance(a, dict)]
        return cols, rows
    if command == "decisions" and action == "get" and "requested" in value:
        cols = ["decision_id", "citation", "court", "decision_date", "docket_number"]
        rows = [[cell(r.get("decision_id")), cell(_label(r)), cell(r.get("court")), cell(r.get("decision_date")), cell(r.get("docket_number"))]
                for r in value.get("results") or []]
        return cols, rows
    if command == "cite" and "requested" in value:
        cols = ["citation_string", "exists", "decision_id", "canonical_url"]
        rows = [[cell(r.get("citation_string")), cell(r.get("exists")), cell(r.get("decision_id")), cell(r.get("canonical_url"))]
                for r in value.get("results") or []]
        return cols, rows
    return None


def render_table(value, args, width: int) -> str:
    """Aligned plain-text table for list results; falls back to the text renderer."""
    shaped = tabular(value, args)
    if shaped is None:
        return render(value, args, Style(False), width)
    cols, rows = shaped
    if not rows:
        return "(no rows)"
    # drop columns that are empty everywhere, cap wide cells
    keep = [i for i, c in enumerate(cols) if any(r[i] for r in rows)]
    cols = [cols[i] for i in keep]; rows = [[r[i] for i in keep] for r in rows]
    cap = max(24, (width - 3 * len(cols)) // max(1, len(cols)))
    rows = [[(c if len(c) <= cap else c[:cap - 1] + "…").replace("\n", " ") for c in r] for r in rows]
    widths = [max(len(cols[i]), *(len(r[i]) for r in rows)) for i in range(len(cols))]
    line = lambda cells: "  ".join(c.ljust(widths[i]) for i, c in enumerate(cells)).rstrip()
    out = [line(cols), line(["-" * w for w in widths])] + [line(r) for r in rows]
    if isinstance(value, dict) and value.get("counts"):
        out += ["", f"{value.get('status')}: " + ", ".join(f"{n} {k}" for k, n in value["counts"].items())]
    if isinstance(value, dict) and value.get("total") is not None and getattr(args, "action", None) == "search":
        out += ["", f"{len(rows)} shown of {'at least ' if value.get('total_is_lower_bound') else ''}{value['total']} matching"
                + ("; more retrievable" if value.get("has_more") else "")]
    return "\n".join(out)


def render_csv(value, args) -> str:
    import csv, io
    shaped = tabular(value, args)
    if shaped is None:
        raise ValueError("--format csv needs a list result (search, get in batch, cite in batch, citations, laws)")
    cols, rows = shaped
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(cols); writer.writerows(rows)
    return buffer.getvalue().rstrip("\n")


def _md_escape(text: str) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def render_md(value, args, width: int) -> str:
    """Markdown for a memo appendix: tables for lists, headings and quotes for single records."""
    shaped = tabular(value, args)
    if shaped is not None:
        cols, rows = shaped
        keep = [i for i, c in enumerate(cols) if any(r[i] for r in rows)] or list(range(len(cols)))
        head = "| " + " | ".join(cols[i] for i in keep) + " |"
        sep = "|" + "|".join(" --- " for _ in keep) + "|"
        body = ["| " + " | ".join(_md_escape(r[i]) for i in keep) + " |" for r in rows]
        lines = [head, sep, *body]
        if isinstance(value, dict) and value.get("counts"):
            lines += ["", f"**{value.get('status')}**: " + ", ".join(f"{n} {k}" for k, n in value["counts"].items())
                      + ". Existence and pinpoints only; no assessment of legal support."]
        return "\n".join(lines)
    if not isinstance(value, dict):
        return "```json\n" + json.dumps(value, ensure_ascii=False, indent=2) + "\n```"
    command, action = getattr(args, "command", None), getattr(args, "action", None)
    if value.get("error"):
        return "**Error**: " + str(value["error"] if not isinstance(value["error"], dict) else value["error"].get("message"))
    if command == "decisions" and action == "passage":
        header = value.get("citation_string_de") or f"{value.get('decision_id')} E. {value.get('e_number')}"
        quote = "\n".join("> " + l for l in _display_text(str(value.get("text") or "")).split("\n"))
        note = f"\n\n*{value['note']}*" if value.get("note") else ""
        return f"**{header}**" + (f" ({value['canonical_url']})" if value.get("canonical_url") else "") + note + "\n\n" + quote
    if command == "cite":
        lines = [f"**{value.get('citation_string') or value.get('citation_string_de') or ''}**"]
        for key in ("citation_string_fr", "citation_string_it"):
            if value.get(key):
                lines.append(str(value[key]))
        if value.get("canonical_url"):
            lines.append(f"<{value['canonical_url']}>")
        if value.get("pinpoint_note"):
            lines.append(f"*{value['pinpoint_note']}*")
            if value.get("available_e_numbers"):
                lines.append("available: " + ", ".join(map(str, value["available_e_numbers"][:12])))
        return "  \n".join(lines)
    if command == "laws":
        return render_law(value, Style(False), width)
    if command == "decisions" and action == "get":
        lines = [f"## {_label(value)}", "", " · ".join(str(x) for x in (value.get("decision_id"), value.get("court"), value.get("decision_date")) if x)]
        if value.get("source_url"):
            lines.append(f"<{value['source_url']}>")
        if value.get("regeste"):
            lines += ["", "**Regeste**", "", _display_text(str(value["regeste"]))]
        if value.get("full_text"):
            lines += ["", _display_text(str(value["full_text"]))]
        return "\n".join(lines)
    if command == "bundle":
        try:
            return Path(str(value.get("bundle"))).joinpath("INDEX.md").read_text(encoding="utf-8")
        except (OSError, TypeError):
            pass
    return "```json\n" + json.dumps(value, ensure_ascii=False, indent=2) + "\n```"
