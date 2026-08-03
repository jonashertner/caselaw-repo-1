"""One-off back-scan: every provably-nonexistent BGE citation in decisions
decided since a given date, enriched and grouped per court for outreach.

The daily pipeline alerts on a rolling 90-day window (high precision, low
volume). For a correction campaign the operator needs the full recent
record, so this runs the SAME classifier over a wide window and produces
per-court dossiers instead of an alert.

Deliberately reuses quality.checks.citation_anomalies: the classifier is
the load-bearing part (five false-positive mechanisms closed on
2026-07-31), and a second copy would drift.

Output (default under OCL_BACKSCAN_OUT, else ./backscan-<since>):
  findings.json   every hit with enrichment
  by-court/<court>.md   dossier per court, German, ready to draft from
  SUMMARY.md      counts per court and per reason

Usage:
  python3 scripts/citation_anomaly_backscan.py --since 2024-01-01
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import quote

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from quality.checks import citation_anomalies as ca  # noqa: E402

SNIPPET_RADIUS = 260

REASON_DE = {
    "volume_out_of_range": "Bandnummer existiert nicht",
    "division_absent_for_volume": "Abteilung existiert in diesem Band nicht",
    "page_beyond_series": "Seitenzahl liegt ausserhalb des Bandes",
    "page_looks_like_year": "vermutlich Jahreszahl statt Seitenzahl",
}


def reason_de(reason: str) -> str:
    parts = reason.split()
    return "; ".join(REASON_DE.get(p, p) for p in parts)


def scan(since: str, rg_path: Path) -> list[dict]:
    rg = sqlite3.connect(f"file:{rg_path}?mode=ro&immutable=1", uri=True)
    rg.row_factory = sqlite3.Row
    try:
        idx, max_vol = ca._bge_series_index(rg)
        hits: list[dict] = []
        for r in ca._iter_window_tokens(rg, since, resolved=False):
            if r["tt"] != "bge":
                continue
            reason = ca._classify_bge(r["ref"], idx, max_vol)
            if reason:
                hits.append({"decision_id": r["sid"], "decided": r["sdate"],
                             "token": r["ref"], "reason": reason})
        return hits
    finally:
        rg.close()


def enrich(hits: list[dict], db_path: Path) -> None:
    c = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    c.row_factory = sqlite3.Row
    try:
        for h in hits:
            r = c.execute(
                "SELECT court, canton, docket_number, decision_date, language, "
                "full_text FROM decisions WHERE decision_id = ?",
                (h["decision_id"],)).fetchone()
            if not r:
                h["missing_from_db"] = True
                continue
            h.update(court=r["court"], canton=r["canton"],
                     docket=r["docket_number"], date=r["decision_date"],
                     language=r["language"],
                     url="https://mcp.opencaselaw.ch/entscheid/"
                         + quote(h["decision_id"], safe=""))
            ft = r["full_text"] or ""
            i = ft.find(h["token"])
            h["token_verbatim"] = i >= 0
            if i >= 0:
                lo = max(0, i - SNIPPET_RADIUS)
                hi = i + len(h["token"]) + SNIPPET_RADIUS
                h["snippet"] = " ".join(ft[lo:hi].split())
    finally:
        c.close()


def write_dossiers(hits: list[dict], out: Path, since: str) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "findings.json").write_text(
        json.dumps(hits, ensure_ascii=False, indent=1))

    by_court: dict[str, list[dict]] = defaultdict(list)
    for h in hits:
        by_court[h.get("court", "unbekannt")].append(h)

    court_dir = out / "by-court"
    court_dir.mkdir(exist_ok=True)
    for court, rows in sorted(by_court.items()):
        rows.sort(key=lambda x: (x.get("date") or "", x.get("docket") or ""))
        lines = [f"# {court} — {len(rows)} Befund(e) seit {since}", "",
                 "Grundlage: Abgleich aller Zitate gegen die vollständige "
                 "BGE-Serie (1875–heute). «Existiert nicht» heisst: die "
                 "Fundstelle kommt in der amtlichen Sammlung nicht vor — "
                 "Band, Abteilung oder Seite sind nachweislich unbelegt.", ""]
        for h in rows:
            lines += [f"## {h.get('docket', h['decision_id'])} "
                      f"vom {h.get('date', '?')}",
                      f"- zitiert: **{h['token']}**",
                      f"- Befund: {reason_de(h['reason'])}",
                      f"- Entscheid: {h.get('url', '–')}"]
            if not h.get("token_verbatim", True):
                lines.append("- ⚠ Token nicht wörtlich im Entscheidtext "
                             "gefunden (Extraktionsartefakt möglich — vor "
                             "Kontaktaufnahme prüfen)")
            if h.get("snippet"):
                lines.append(f"- Kontext: «…{h['snippet']}…»")
            lines.append("")
        (court_dir / f"{court}.md").write_text("\n".join(lines))

    reasons = Counter(h["reason"].split()[0] for h in hits)
    years = Counter((h.get("date") or "?")[:4] for h in hits)
    unverified = [h for h in hits if not h.get("token_verbatim", True)]
    summary = [f"# Rückwärts-Scan: BGE-Zitate seit {since}", "",
               f"- Befunde gesamt: **{len(hits)}**",
               f"- betroffene Gerichte: **{len(by_court)}**",
               f"- ohne wörtlichen Beleg im Text (vor Versand prüfen): "
               f"**{len(unverified)}**", "",
               "## Nach Gericht", ""]
    for court, rows in sorted(by_court.items(), key=lambda kv: -len(kv[1])):
        summary.append(f"- `{court}` — {len(rows)}")
    summary += ["", "## Nach Befundart", ""]
    for k, n in reasons.most_common():
        summary.append(f"- {REASON_DE.get(k, k)} — {n}")
    summary += ["", "## Nach Entscheidjahr", ""]
    for y, n in sorted(years.items()):
        summary.append(f"- {y} — {n}")
    (out / "SUMMARY.md").write_text("\n".join(summary) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2024-01-01")
    ap.add_argument("--out", default=None)
    ap.add_argument("--graph", default=os.environ.get(
        "SWISS_CASELAW_REFERENCE_GRAPH", "output/reference_graph.db"))
    ap.add_argument("--db", default=os.environ.get(
        "SWISS_CASELAW_DB", "output/decisions.db"))
    a = ap.parse_args()
    out = Path(a.out or os.environ.get("OCL_BACKSCAN_OUT",
                                       f"backscan-{a.since}"))
    t0 = datetime.datetime.now()
    hits = scan(a.since, Path(a.graph))
    enrich(hits, Path(a.db))
    write_dossiers(hits, out, a.since)
    print(f"{len(hits)} finding(s) since {a.since} -> {out} "
          f"({(datetime.datetime.now() - t0).seconds}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
