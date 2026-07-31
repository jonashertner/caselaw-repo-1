"""Citation-integrity anomalies inside court decisions (periodic review).

Motivation (2026-07-31): court-confronted AI fabrications are tracked at
case level worldwide; the first place fabricated citations would surface in
the Swiss official record is decisions quoting party submissions. The BGE
series is complete in-corpus since 1875, which makes nonexistence of a
cited BGE *provable*, not merely "we failed to resolve it".

Three classes, in descending signal strength, each computed over a rolling
window of recently DECIDED decisions (default 90 days,
OCL_CITATION_ANOMALY_WINDOW_DAYS):

  nonexistent_bge   an unresolved BGE-form token whose (volume, division,
                    page) matches nothing in the complete series, even
                    allowing the 30-page pin-cite window. Sub-reasons:
                    volume_out_of_range / division_absent_for_volume /
                    page_beyond_series / no_case_at_page (+ a
                    page_looks_like_year tag for the classic OCR shape
                    "BGE 127 I 2002").
  anachronistic     a citation token whose parsed docket year postdates
                    the citing decision's own year (tolerance 1 year for
                    year-boundary drafting).
  unresolved_recent_bger  informational: unresolved federal-shaped dockets
                    (nA_n/YYYY etc.) from 2008+, where corpus coverage is
                    near-complete.

Deliberately NOT named hallucination detection: in court decisions a
nonexistent citation is more often clerical or OCR than AI-generated. The
checks report anomalies; a human classifies. Scope note from the 2026-07-31
recon: 600k unresolved docket tokens are dominated by cantonal internal
series (WBE_* etc.) — coverage gaps, not anomalies — hence the tight
federal-shape filter here.

All checks are alert-only (MODULE_NEVER_CRITICAL); a detail artifact with
per-case samples goes to logs/citation_anomalies_latest.json for review.
"""
from __future__ import annotations

import datetime
import json
import os
import re
import sqlite3
from pathlib import Path

from quality.types import CheckResult, Severity

MODULE_NEVER_CRITICAL = True  # advisory review queue, never blocks a publish

_BGE_TOKEN = re.compile(r"^BGE\s+(\d{1,3})\s+([IVX]+[ab]?)\s+(\d{1,4})$")
_BGE_SERIES = re.compile(r"^(\d{1,3})\s+([IVX]+[ab]?)\s+(\d{1,4})$")
# Federal-court docket shapes: 4A_123/2019, 9C_55/2024, 7B 1008/2023, 2C-64/2026
_FED_DOCKET = re.compile(r"^(\d{1,2}[A-Z])[ _\-](\d{1,4})/(\d{4})$")
# Year extraction for the anachronism check: docket-year positions only
# (federal shape, or a slash-year suffix). A bare _NNNN match reads
# file-number serials as years: STA_2026_2069 flagged "2069" because _
# is a word char, so _2026 has no trailing boundary and _2069 matched.
_ANY_YEAR = re.compile(r"/((?:19|20)\d\d)$")
PINCITE_WINDOW = 30


def _window_days() -> int:
    try:
        return max(1, int(os.environ.get("OCL_CITATION_ANOMALY_WINDOW_DAYS", "90")))
    except ValueError:
        return 90


def _report_path() -> Path:
    return Path(os.environ.get("OCL_CITATION_ANOMALY_REPORT",
                               "logs/citation_anomalies_latest.json"))


def _open_rg() -> sqlite3.Connection | None:
    p = Path(os.environ.get("SWISS_CASELAW_REFERENCE_GRAPH",
                            "output/reference_graph.db"))
    if not p.exists():
        return None
    rg = sqlite3.connect(f"file:{p}?mode=ro&immutable=1", uri=True)
    rg.row_factory = sqlite3.Row
    rg.execute("PRAGMA busy_timeout=30000")
    return rg


def _bge_series_index(rg) -> tuple[dict, int]:
    """(volume, division) -> sorted first pages, from the complete BGE series.
    Returns (index, max_volume)."""
    idx: dict[tuple[int, str], list[int]] = {}
    max_vol = 0
    for r in rg.execute(
        "SELECT docket_number FROM decisions WHERE court='bge'"
    ):
        m = _BGE_SERIES.match((r["docket_number"] or "").strip())
        if not m:
            continue
        vol, div, page = int(m.group(1)), m.group(2), int(m.group(3))
        idx.setdefault((vol, div), []).append(page)
        max_vol = max(max_vol, vol)
    for pages in idx.values():
        pages.sort()
    return idx, max_vol


def _classify_bge(token: str, idx: dict, max_vol: int) -> str | None:
    """Anomaly sub-reason for an unresolved BGE token, or None if plausibly
    a resolver gap rather than nonexistence."""
    m = _BGE_TOKEN.match(token.strip())
    if not m:
        return None                     # malformed beyond our grammar: skip
    vol, div, page = int(m.group(1)), m.group(2), int(m.group(3))
    year_tag = " page_looks_like_year" if 1875 <= page <= 2100 and page > 700 else ""
    if vol < 1 or vol > max_vol:
        return "volume_out_of_range" + year_tag
    if (vol, div) not in idx:
        return "division_absent_for_volume" + year_tag
    pages = idx[(vol, div)]
    if page > pages[-1] + PINCITE_WINDOW:
        return "page_beyond_series" + year_tag
    # inside range: does any first page sit within the pin-cite window?
    import bisect
    i = bisect.bisect_right(pages, page)
    if i > 0 and page - pages[i - 1] <= PINCITE_WINDOW:
        return None                     # a case plausibly covers this page
    return "no_case_at_page" + year_tag


def _iter_window_tokens(rg, since: str, resolved: bool | None):
    """(source_id, source_date, target_type, token) for decisions decided
    on/after `since`. resolved=False -> only unresolved tokens."""
    join = ("LEFT JOIN citation_targets ct ON "
            "ct.source_decision_id = dc.source_decision_id "
            "AND ct.target_ref = dc.target_ref")
    where = "d.decision_date >= ? AND d.decision_date <= ?"
    if resolved is False:
        where += " AND ct.target_ref IS NULL"
    # upper bound: today+1y guards the known future-dated-portal noise
    upper = (datetime.date.today() + datetime.timedelta(days=366)).isoformat()
    sql = f"""SELECT dc.source_decision_id AS sid, d.decision_date AS sdate,
                     dc.target_type AS tt, dc.target_ref AS ref
              FROM decision_citations dc
              JOIN decisions d ON d.decision_id = dc.source_decision_id
              {join}
              WHERE {where}"""
    yield from rg.execute(sql, (since, upper))


def _since() -> str:
    return (datetime.date.today()
            - datetime.timedelta(days=_window_days())).isoformat()


def _write_report(payload: dict) -> None:
    """Merge-write: each check contributes its keys to one report file, so
    the confidential alert dispatcher sees every class in a single artifact."""
    try:
        p = _report_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        existing: dict = {}
        if p.exists():
            try:
                existing = json.loads(p.read_text())
            except Exception:
                existing = {}
        existing.update(payload)
        p.write_text(json.dumps(existing, ensure_ascii=False, indent=1))
    except Exception:
        pass                            # the report is best-effort


def check_nonexistent_bge_citations(conn: sqlite3.Connection, **_) -> CheckResult:
    """Recently decided decisions citing a BGE that provably does not exist
    in the complete series (even allowing the 30-page pin-cite window)."""
    name = "citation_anomalies.nonexistent_bge"
    rg = _open_rg()
    if rg is None:
        return CheckResult(name=name, severity=Severity.INFO, passed=True,
                           metric_value=0, threshold=None,
                           message="reference_graph.db absent — skipped")
    try:
        idx, max_vol = _bge_series_index(rg)
        if not idx:
            return CheckResult(name=name, severity=Severity.INFO, passed=True,
                               metric_value=0, threshold=None,
                               message="no BGE series rows — skipped")
        # Two scopes, deliberately split (measured 2026-07-31): the
        # corpus-wide scan finds ~2,600 anomalies, but they concentrate in
        # 19th/early-20th-century citing decisions where the flagged token is
        # usually OUR OCR of Roman numerals, not the court's error — useful
        # as a corpus-QA metric, useless for court outreach. The recent
        # window (default 90d, decision date) is the high-precision alerting
        # and outreach surface; the corpus-wide total is reported as an INFO
        # metric so extraction-noise regressions stay visible.
        since = _since()
        recent: list[dict] = []
        all_by_reason: dict[str, int] = {}
        all_total = 0
        for r in _iter_window_tokens(rg, "1800-01-01", resolved=False):
            if r["tt"] != "bge":
                continue
            reason = _classify_bge(r["ref"], idx, max_vol)
            if not reason:
                continue
            all_total += 1
            key = reason.split()[0]
            all_by_reason[key] = all_by_reason.get(key, 0) + 1
            if (r["sdate"] or "") >= since:
                recent.append({"decision_id": r["sid"], "decided": r["sdate"],
                               "token": r["ref"], "reason": reason})
        threshold = 25
        _write_report({
            "generated": datetime.date.today().isoformat(),
            "window_days": _window_days(), "since": since,
            "nonexistent_bge": recent[:200],
            "nonexistent_bge_total": len(recent),
            "nonexistent_bge_corpus_total": all_total,
            "nonexistent_bge_corpus_by_reason": all_by_reason,
        })
        sev = Severity.WARNING if len(recent) > threshold else Severity.INFO
        return CheckResult(
            name=name, severity=sev, passed=len(recent) <= threshold,
            metric_value=len(recent), threshold=threshold,
            message=(f"{len(recent)} provably-nonexistent BGE citation(s) in "
                     f"decisions decided since {since}; corpus-wide total "
                     f"{all_total} (mostly historical OCR noise — QA metric, "
                     f"not outreach; detail: {_report_path()})"))
    finally:
        rg.close()


def check_anachronistic_citations(conn: sqlite3.Connection, **_) -> CheckResult:
    """Decisions citing a docket whose year postdates the decision's own
    year by more than 1 (rectification/consolidation tolerance)."""
    name = "citation_anomalies.anachronistic"
    rg = _open_rg()
    if rg is None:
        return CheckResult(name=name, severity=Severity.INFO, passed=True,
                           metric_value=0, threshold=None,
                           message="reference_graph.db absent — skipped")
    try:
        since = _since()
        hits: list[dict] = []
        for r in _iter_window_tokens(rg, since, resolved=None):
            sdate = r["sdate"] or ""
            if len(sdate) < 4 or not sdate[:4].isdigit():
                continue
            syear = int(sdate[:4])
            ref = (r["ref"] or "").strip()
            m = _FED_DOCKET.match(ref) or _ANY_YEAR.search(ref)
            if not m:
                continue
            cyear = int(m.group(m.lastindex or 1))
            if 1875 <= cyear <= 2100 and cyear > syear + 1:
                hits.append({"decision_id": r["sid"], "decided": sdate,
                             "token": r["ref"], "cited_year": cyear})
        _write_report({"anachronistic": hits[:200],
                       "anachronistic_total": len(hits)})
        threshold = 50
        sev = Severity.WARNING if len(hits) > threshold else Severity.INFO
        return CheckResult(
            name=name, severity=sev, passed=len(hits) <= threshold,
            metric_value=len(hits), threshold=threshold,
            message=(f"{len(hits)} citation token(s) with year > citing "
                     f"decision's year+1, decisions since {since}"))
    finally:
        rg.close()


def check_unresolved_recent_bger(conn: sqlite3.Connection, **_) -> CheckResult:
    """Informational: unresolved federal-shaped dockets from the
    near-complete post-2008 coverage window."""
    name = "citation_anomalies.unresolved_recent_bger"
    rg = _open_rg()
    if rg is None:
        return CheckResult(name=name, severity=Severity.INFO, passed=True,
                           metric_value=0, threshold=None,
                           message="reference_graph.db absent — skipped")
    try:
        since = _since()
        n = 0
        for r in _iter_window_tokens(rg, since, resolved=False):
            if r["tt"] != "docket":
                continue
            m = _FED_DOCKET.match((r["ref"] or "").strip())
            if not m or int(m.group(3)) < 2008:
                continue
            # anachronisms are the harder class and own their tokens —
            # keep this counter to plausible not-yet-ingested citations
            sdate = r["sdate"] or ""
            if sdate[:4].isdigit() and int(m.group(3)) > int(sdate[:4]) + 1:
                continue
            n += 1
        return CheckResult(
            name=name, severity=Severity.INFO, passed=True,
            metric_value=n, threshold=None,
            message=(f"{n} unresolved federal-shaped docket citation(s) in "
                     f"decisions since {since} (informational; includes "
                     "not-yet-ingested and non-public decisions)"))
    finally:
        rg.close()
