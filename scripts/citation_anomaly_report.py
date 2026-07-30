"""Confidential citation-anomaly reports for court correction outreach.

Runs daily (systemd timer, after the nightly dataset audit has refreshed
logs/citation_anomalies_latest.json). For each NEW anomaly — not seen in any
previous report — it enriches the finding from the serving decisions.db
(court, docket, date, canonical URL, and the verbatim text snippet around
the defective citation) and writes a German, per-court Markdown report into
the PRIVATE repository at /opt/caselaw/confidential-reports, which it
commits and pushes over a write-scoped deploy key.

Confidentiality: the detailed findings name specific decisions of specific
courts as containing defective citations — analysis the operator wants to
bring to the courts before it is public. Therefore: details go ONLY to the
private repo; the public ntfy topic receives a bare count ("N neue
Zitier-Anomalien"), never an identifier; the public quality.json already
carries counts only.

Regularity: a report is written whenever there are new findings, and every
Monday regardless (a "keine neuen Anomalien" digest), so silence is
distinguishable from breakage.

State: .seen.json inside the private repo — the dedup memory travels with
the channel and survives host rebuilds.
"""
from __future__ import annotations

import datetime
import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.request
from pathlib import Path

REPORT_JSON = Path(os.environ.get("OCL_CITATION_ANOMALY_REPORT",
                                  "logs/citation_anomalies_latest.json"))
PRIVATE_REPO = Path(os.environ.get("OCL_ANOMALY_PRIVATE_REPO",
                                   "/opt/caselaw/confidential-reports"))
DECISIONS_DB = Path(os.environ.get("SWISS_CASELAW_DB", "output/decisions.db"))
NTFY_URL = os.environ.get("OCL_ANOMALY_NTFY", "https://ntfy.sh/opencaselaw-publish")
DEPLOY_KEY = os.environ.get("OCL_ANOMALY_DEPLOY_KEY",
                            "/root/.ssh/anomaly_reports_ed25519")
SNIPPET_RADIUS = 240

REASON_DE = {
    "volume_out_of_range": "Bandnummer existiert nicht",
    "division_absent_for_volume": "Abteilung existiert in diesem Band nicht",
    "page_beyond_series": "Seitenzahl liegt ausserhalb des Bandes",
    "no_case_at_page": "an dieser Stelle beginnt kein Entscheid "
                       "(auch unter Berücksichtigung von Pinpoint-Zitaten)",
    "page_looks_like_year": "vermutlich Jahreszahl statt Seitenzahl",
}


def _reason_de(reason: str) -> str:
    parts = reason.split()
    if parts and all(p in REASON_DE for p in parts):
        return "; ".join(REASON_DE[p] for p in parts)
    return reason                       # free-text reason: leave intact


def _load_hits() -> list[dict]:
    if not REPORT_JSON.exists():
        return []
    d = json.loads(REPORT_JSON.read_text())
    hits = []
    for h in d.get("nonexistent_bge", []):
        hits.append({**h, "klass": "nonexistent_bge"})
    for h in d.get("anachronistic", []):
        hits.append({**h, "klass": "anachronistic",
                     "reason": f"zitiert Geschäftsjahr {h.get('cited_year')} "
                               f"nach dem eigenen Entscheiddatum"})
    return hits


def _key(h: dict) -> str:
    return f"{h['decision_id']}|{h['token']}"


def _enrich(hits: list[dict]) -> None:
    """court/docket/URL/snippet from the serving DB (read-only)."""
    if not DECISIONS_DB.exists():
        return
    c = sqlite3.connect(f"file:{DECISIONS_DB}?mode=ro&immutable=1", uri=True)
    c.row_factory = sqlite3.Row
    try:
        for h in hits:
            r = c.execute(
                "SELECT court, canton, docket_number, decision_date, language, "
                "full_text FROM decisions WHERE decision_id = ?",
                (h["decision_id"],),
            ).fetchone()
            if not r:
                continue
            h["court"] = r["court"]
            h["canton"] = r["canton"]
            h["docket"] = r["docket_number"]
            h["date"] = r["decision_date"]
            from urllib.parse import quote
            h["url"] = ("https://mcp.opencaselaw.ch/entscheid/"
                        + quote(h["decision_id"], safe=""))
            ft = r["full_text"] or ""
            i = ft.find(h["token"])
            if i >= 0:
                lo, hi = max(0, i - SNIPPET_RADIUS), i + len(h["token"]) + SNIPPET_RADIUS
                h["snippet"] = " ".join(ft[lo:hi].split())
    finally:
        c.close()


def _render(new_hits: list[dict], today: str) -> str:
    lines = [f"# Zitier-Anomalien — Meldung vom {today}", ""]
    if not new_hits:
        lines += ["Keine neuen Anomalien seit der letzten Meldung.", ""]
        return "\n".join(lines)
    lines += [f"{len(new_hits)} neue Befund(e). Grundlage: nächtliche Prüfung "
              "aller kürzlich ergangenen Entscheide gegen die vollständige "
              "BGE-Serie (1875–heute).", ""]
    by_court: dict[str, list[dict]] = {}
    for h in new_hits:
        by_court.setdefault(h.get("court", "unbekannt"), []).append(h)
    for court in sorted(by_court):
        lines.append(f"## {court}")
        for h in by_court[court]:
            date = h.get("date", h.get("decided", "?"))
            docket = h.get("docket", h.get("decision_id"))
            lines.append(f"- **{docket}** vom {date}  ")
            lines.append(f"  zitiert **{h['token']}** — {_reason_de(h.get('reason',''))}.  ")
            if h.get("url"):
                lines.append(f"  Entscheid: {h['url']}  ")
            if h.get("snippet"):
                lines.append(f"  Kontext: «…{h['snippet']}…»")
            lines.append("")
    lines += ["---",
              "Hinweis: «existiert nicht» heisst: die zitierte Fundstelle "
              "kommt in der vollständigen BGE-Serie nicht vor. Ob Tippfehler, "
              "OCR-Artefakt des Quellportals oder fehlerhafte Übernahme aus "
              "einer Rechtsschrift, ist je Fall zu prüfen — der Kontext-Auszug "
              "zeigt die Stelle im Entscheidtext.", ""]
    return "\n".join(lines)


def _git(args: list[str]) -> None:
    env = {**os.environ,
           "GIT_SSH_COMMAND": f"ssh -i {DEPLOY_KEY} -o StrictHostKeyChecking=accept-new"}
    subprocess.run(["git", "-C", str(PRIVATE_REPO), *args],
                   check=True, env=env, capture_output=True, text=True)


def _ntfy(n: int) -> None:
    try:
        req = urllib.request.Request(
            NTFY_URL,
            data=(f"{n} neue Zitier-Anomalie(n) im Entscheidkorpus — "
                  "Details im privaten Report-Repository.").encode(),
            headers={"Title": "OpenCaseLaw Zitier-Anomalien"})
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        print(f"ntfy ping failed (non-fatal): {e}", file=sys.stderr)


def main() -> int:
    today = datetime.date.today().isoformat()
    is_monday = datetime.date.today().weekday() == 0
    if not PRIVATE_REPO.exists():
        print(f"private repo missing: {PRIVATE_REPO}", file=sys.stderr)
        return 1
    try:
        _git(["pull", "--rebase", "-q"])
    except subprocess.CalledProcessError:
        pass                    # fresh/empty repo: no upstream branch yet

    seen_path = PRIVATE_REPO / ".seen.json"
    seen: dict = json.loads(seen_path.read_text()) if seen_path.exists() else {}

    hits = _load_hits()
    new_hits = [h for h in hits if _key(h) not in seen]
    if not new_hits and not is_monday:
        print("no new findings; not Monday — nothing to send")
        return 0

    _enrich(new_hits)
    report = _render(new_hits, today)
    out = PRIVATE_REPO / "reports" / f"{today}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report)

    for h in new_hits:
        seen[_key(h)] = {"first_reported": today,
                         "reason": h.get("reason", ""), "court": h.get("court")}
    seen_path.write_text(json.dumps(seen, ensure_ascii=False, indent=1))

    _git(["add", "-A"])
    _git(["-c", "user.name=OpenCaseLaw Anomaly Reporter",
          "-c", "user.email=noreply@opencaselaw.ch",
          "commit", "-q", "-m",
          f"Meldung {today}: {len(new_hits)} neue Anomalie(n)"])
    _git(["push", "-q", "-u", "origin", "HEAD"])
    print(f"report {out.name}: {len(new_hits)} new finding(s), pushed")
    if new_hits:
        _ntfy(len(new_hits))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
