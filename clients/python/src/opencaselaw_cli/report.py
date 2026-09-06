"""The report a person reads after `ocl check`: what held, what needs attention, and
what to do about each item. HTML (opens anywhere) or Markdown. Wording comes from
the service's answers; the report never composes a citation."""
from __future__ import annotations

import html
from datetime import datetime, timezone

from ._version import __version__
from .statutes import statutes_html, statutes_markdown, summarize_statutes  # statute rows: their own table, see statutes.py

_LABELS = {
    "resolved": ("verified", "The decision exists and carries this label; the cited passage was retrieved."),
    "pinpoint_unavailable": ("passage not indexed", "The decision exists; the cited Erwägung is not in the index. Quote from the decision text, not from memory."),
    "discrepancy": ("detail wrong", "The decision exists, but the date or docket written next to it does not match the record."),
    "missing": ("not found", "No decision carries this label. Check the citation; a similar case is never substituted."),
    "ambiguous": ("ambiguous", "Several decisions carry this label. Name the court, or cite by decision id."),
    "unrecognized": ("not verifiable", "The service proposed a decision that does not carry the label as written. Not cited."),
    "resolution_incomplete": ("not verifiable", "Too many exact matches to decide. Cite by decision id."),
    "error": ("check failed", "The service or network did not answer for this item; run the check again."),
    "skipped": ("skipped", "Stopped after repeated failures; run the check again."),
    "quote_near": ("quotation differs", "The quotation differs from the served wording; the differences are listed."),
    "quote_not_found": ("quotation not found", "The quotation does not appear in the cited passage or the decision text."),
}


def _label(row: dict) -> tuple[str, str]:
    status = row.get("status", "error")
    quote = (row.get("quote_check") or {}).get("quote_status")
    if status == "resolved" and quote in ("near", "not_found"):
        status = "quote_" + quote
    return _LABELS.get(status, (status, ""))


def _detail(row: dict) -> str:
    bits = []
    for d in row.get("discrepancies") or []:
        if d.get("kind") == "date":
            bits.append(f"date written {d.get('written')}, decision dated {d.get('decision')}")
        elif d.get("kind") == "docket":
            bits.append(f"docket {d.get('written')} names {d.get('resolves_to')}")
    if row.get("pinpoint_status") == "parent_retrieved":
        bits.append(f"E. {row.get('pinpoint')} is not indexed as such; E. {(row.get('passage') or {}).get('e_number')} was retrieved instead")
    if row.get("available_e_numbers"):
        bits.append("indexed: " + ", ".join(map(str, row["available_e_numbers"][:12])))
    for c in row.get("candidates") or []:
        bits.append(f"candidate {c.get('decision_id')} ({c.get('court')}, {c.get('decision_date')})")
    if row.get("service_candidate"):
        bits.append(f"service proposed {row['service_candidate'].get('decision_id')}, label not carried")
    quote = row.get("quote_check") or {}
    if quote.get("quote_status") == "near":
        for d in (quote.get("differences") or [])[:3]:
            bits.append(f"written «{d.get('quote')}», served «{d.get('served')}»")
    elif quote.get("quote_status") == "not_found" and quote.get("served"):
        bits.append("closest served wording: «" + " ".join(str(quote["served"]).split())[:240] + "»")
    if row.get("error") and row.get("status") == "error":
        bits.append(str(row["error"].get("message")))
    return "; ".join(bits)


def _citation(row: dict) -> str:
    provenance = row.get("provenance") or {}
    return provenance.get("citation_string_de") or provenance.get("citation_string") or ""


def summarize(result: dict, source: str) -> dict:
    rows = result.get("results") or []
    attention = [r for r in rows if _label(r)[0] != "verified"]
    verified = [r for r in rows if _label(r)[0] == "verified"]
    return {"source": source, "checked": len(rows), "verified": len(verified), "attention": len(attention),
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "client_version": __version__,
            "base_url": result.get("base_url"), "requests": result.get("requests"),
            **summarize_statutes(result.get("statutes") or [])}


def render_markdown(result: dict, source: str, found: list[dict]) -> str:
    rows = result.get("results") or []
    summary = summarize(result, source)
    lines = [f"# Citation check: {source}", "",
             f"{summary['checked']} citations found, {summary['verified']} verified, {summary['attention']} need attention. "
             f"Checked {summary['generated_at']} against {summary['base_url']} (client {__version__}).", ""]
    attention = [r for r in rows if _label(r)[0] != "verified"]
    if attention:
        lines += ["## Needs attention", "", "| Citation as written | Finding | What to do | Detail |", "|---|---|---|---|"]
        for r in attention:
            label, advice = _label(r)
            lines.append(f"| {r.get('reference')} | {label} | {advice} | {_detail(r)} |")
        lines.append("")
    verified = [r for r in rows if _label(r)[0] == "verified"]
    if verified:
        lines += ["## Verified", ""]
        for r in verified:
            pin = f", E. {r['pinpoint']}" if r.get("pinpoint") else ""
            lines.append(f"- {r.get('reference')} → {_citation(r)}{pin}" + (" (quotation verbatim)" if (r.get('quote_check') or {}).get('quote_status') == 'exact' else ""))
        lines.append("")
    lines += statutes_markdown(result.get("statutes") or [])
    lines += ["## Scope", "", "Existence, identity and wording only: whether each cited decision exists and carries the label as written, "
              "whether the cited Erwägung exists, whether dates and dockets match the record, and whether quotations stand in the served text. "
              "For statutes: whether the act exists and has the article, and whether a quotation stands in the served article text. "
              "Not whether a decision supports the argument or is still good law.", ""]
    return "\n".join(lines)


def render_html(result: dict, source: str, found: list[dict]) -> str:
    rows = result.get("results") or []
    summary = summarize(result, source)
    e = html.escape
    attention = [r for r in rows if _label(r)[0] != "verified"]
    verified = [r for r in rows if _label(r)[0] == "verified"]
    parts = [f"<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Citation check: {e(source)}</title>",
             "<style>body{font:15px/1.5 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;max-width:1000px;margin:2rem auto;padding:0 1rem;color:#222}"
             "h1{font-size:1.4rem}h2{font-size:1.1rem;margin-top:2rem}table{border-collapse:collapse;width:100%}th,td{border-top:1px solid #ddd;padding:.5rem;vertical-align:top;text-align:left}"
             ".ok{color:#1a7f37}.warn{color:#9a6700}.bad{color:#b42318}.muted{color:#666;font-size:.9rem}code{font-family:ui-monospace,Menlo,monospace;font-size:.9em}</style></head><body>",
             f"<h1>Citation check: {e(source)}</h1>",
             f"<p><strong>{summary['checked']}</strong> citations found · <span class=\"ok\">{summary['verified']} verified</span> · "
             f"<span class=\"{'bad' if summary['attention'] else 'ok'}\">{summary['attention']} need attention</span></p>",
             f"<p class=\"muted\">Checked {e(summary['generated_at'])} against {e(str(summary['base_url']))}, client {__version__}. "
             "The citation strings shown are the service's; nothing was rewritten.</p>"]
    if attention:
        parts.append("<h2>Needs attention</h2><table><tr><th>Citation as written</th><th>Finding</th><th>What to do</th><th>Detail</th></tr>")
        for r in attention:
            label, advice = _label(r)
            cls = "bad" if label in ("not found", "detail wrong", "quotation not found", "not verifiable") else "warn"
            parts.append(f"<tr><td><code>{e(str(r.get('reference')))}</code>" + (f"<br><span class=\"muted\">§{r.get('input', {}).get('paragraph', '')}</span>" if (r.get('input') or {}).get('paragraph') else "") +
                         f"</td><td class=\"{cls}\">{e(label)}</td><td>{e(advice)}</td><td class=\"muted\">{e(_detail(r))}</td></tr>")
        parts.append("</table>")
    if verified:
        parts.append("<h2>Verified</h2><table><tr><th>Citation as written</th><th>Decision (service's citation)</th><th>Passage</th></tr>")
        for r in verified:
            pin = f"E. {r['pinpoint']} retrieved" if r.get("pinpoint") else ""
            quote = (r.get("quote_check") or {}).get("quote_status")
            parts.append(f"<tr><td><code>{e(str(r.get('reference')))}</code></td><td>{e(_citation(r))}</td><td class=\"ok\">{e(pin)}{' · quotation verbatim' if quote == 'exact' else ''}</td></tr>")
        parts.append("</table>")
    parts += statutes_html(result.get("statutes") or [], e)
    parts.append("<h2>Scope</h2><p class=\"muted\">Existence, identity and wording only: whether each cited decision exists and carries the label as written, whether the cited Erwägung exists, "
                 "whether dates and dockets match the record, and whether quotations stand in the served text. For statutes: whether the act exists and has the article, "
                 "and whether a quotation stands in the served article text. Not whether a decision supports the argument or is still good law.</p></body></html>")
    return "\n".join(parts)
