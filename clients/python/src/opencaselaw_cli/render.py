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
    "resolved": "green", "complete": "green", "saved": "green",
    "missing": "red", "error": "red", "failed": "red",
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
        citation = row.get("citation") or {}
        label = citation.get("citation_string_de") if isinstance(citation, dict) else None
        if label and reference_like(label) != reference_like(ref) and not row.get("pinpoint"):
            detail.append(s.dim(label))
        if row.get("pinpoint"):
            ps = row.get("pinpoint_status")
            detail.append(s.green(f"E. {row['pinpoint']} retrieved") if ps == "retrieved"
                          else s.yellow(f"E. {row['pinpoint']} not in the index") if ps == "unavailable" else "")
        if status == "missing":
            detail.append(s.dim("not in the corpus"))
        if row.get("reason"):
            detail.append(s.dim(str(row["reason"])))
        if row.get("error") and status == "error":
            detail.append(s.dim(str(row["error"].get("message"))))
        line = f"{_status(s, status)}{' ' * max(1, 22 - len(status))}{ref:<{ref_w}}  "
        line += f"{s.cyan(decision_id)}{' ' * max(0, id_w - len(decision_id))}  " if id_w else ""
        lines.append((line + "  ".join(d for d in detail if d)).rstrip())
    counts = value.get("counts") or {}
    summary = ", ".join(f"{n} {k}" for k, n in counts.items())
    lines += ["", _status(s, str(value.get("status", ""))) + s.dim(f": {summary}." if summary else ".")]
    lines.append(s.dim("Existence and pinpoints only; no assessment of legal support."))
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
    if completeness.get("failed_items"):
        parts.append(s.yellow(f"{completeness['failed_items']} requested item(s) failed"))
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
        return render_bundle(value, s, width)
    return json.dumps(value, ensure_ascii=False, indent=2)
