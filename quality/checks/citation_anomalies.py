"""Citation-integrity anomalies inside court decisions (periodic review).

Motivation (2026-07-31): court-confronted AI fabrications are tracked at
case level worldwide; the first place fabricated citations would surface in
the Swiss official record is decisions quoting party submissions. The BGE
series is complete in-corpus since 1875 for CLOSED volumes, which makes
nonexistence of a cited BGE *provable* there, not merely "we failed to
resolve it". The newest two volumes are still filling (BGE issues appear
with up to a year of lag) and are exempt from every page/division proof —
only a volume number beyond next-year's is flaggable that recently.

Three classes, in descending signal strength, each computed over a rolling
window of recently DECIDED decisions (default 90 days,
OCL_CITATION_ANOMALY_WINDOW_DAYS):

  nonexistent_bge   an unresolved BGE-form token whose (volume, division,
                    page) provably matches nothing in the series.
                    Sub-reasons: volume_out_of_range /
                    division_absent_for_volume / page_beyond_series (+ a
                    page_looks_like_year tag for the classic OCR shape
                    "BGE 127 I 2002"). A former no_case_at_page class was
                    retired 2026-07-31: BGE volumes are continuously
                    paginated, so any page between two consecutive case
                    starts lies WITHIN the earlier case — mid-volume
                    nonexistence is not provable ("BGE 141 V 312" sits 31
                    pages into BGE 141 V 281 and is a legitimate deep
                    pin-cite the old fixed window flagged wrongly).
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
# Bare series form as stored for FR/IT citations. The graph's extractor
# (search_stack/reference_extraction.py) recognises only the literal "BGE"
# prefix, so "ATF 143 III 666" / "DTF 143 III 666" survive solely via the
# bare-form docket pattern: target_type='docket', target_ref='143 III 666'.
# Measured 2026-08-06: decisions since 2024 yield 345,146 bge-typed tokens
# from DE sources but only 283 from FR — a thousandfold artefact of that
# prefix gap, which made FR/IT invisible to this scan. DE continuation
# citations ("BGE 143 III 666 E. 4; 144 II 5 E. 2") take the same path.
_BARE_BGE = re.compile(r"^(\d{1,3})\s+([IVX]{1,4}[ABab]?)\s+(\d{1,4})$")
# Divisions the series has ever used. Prefixed citations may misname a
# division (that is a reportable error); a bare token with a division
# outside this set (VI, VIII, ...) is far more likely a non-citation
# string, so bare tokens are gated to real divisions.
_REAL_DIVISIONS = {"I", "IA", "IB", "II", "III", "IV", "V"}
# The series appears in three docket formats across scrape eras:
# "142 II 590" (spaced), "1_I_26" (underscored, historical) and
# "BGE 136 II 120" (prefixed). Division letters are stored uppercase
# ("120 IA 1") while citations write "BGE 120 Ia 1" — both sides are
# normalized via .upper(). (2026-07-31: the previous spaced-only pattern
# silently dropped 70% of series rows — 35k of 50k — and absence proofs
# from that partial index were unsafe.)
_BGE_SERIES = re.compile(r"^(?:BGE[ _])?(\d{1,3})[ _]+([IVX]+[ABab]?)[ _]+(\d{1,4})$")
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
        vol, div, page = int(m.group(1)), m.group(2).upper(), int(m.group(3))
        idx.setdefault((vol, div), []).append(page)
        max_vol = max(max_vol, vol)
    for pages in idx.values():
        pages.sort()
    return idx, max_vol


_I_FAMILY = ("I", "IA", "IB")


def _family_pages(idx: dict, vol: int, div: str) -> list[int]:
    """Start pages for (vol, div). The I-family (I vs the 1955-1994 Ia/Ib
    split) is evaluated as a union: citations and dockets casefold the
    sub-letter in both directions, so an I-vs-Ia mismatch proves nothing."""
    if div in _I_FAMILY:
        merged: set[int] = set()
        for d in _I_FAMILY:
            merged.update(idx.get((vol, d), []))
        return sorted(merged)
    return idx.get((vol, div), [])


def _last_case_window(pages: list[int]) -> int:
    """How far beyond the last case START a page may plausibly lie: the
    longest case observed in this (vol, div) — no case is likely longer
    than the volume's own longest — with PINCITE_WINDOW as the floor
    (BGE 143 II holds the 53-page Gaba; BGE 145 V's longest is 18)."""
    longest_gap = max((b - a for a, b in zip(pages, pages[1:])), default=0)
    return max(PINCITE_WINDOW, longest_gap)


def _classify_bge(token: str, idx: dict, max_vol: int,
                  bare: bool = False) -> str | None:
    """Anomaly sub-reason for an unresolved BGE token, or None if plausibly
    a resolver gap rather than nonexistence.

    bare=True: the token arrived without a BGE/ATF/DTF prefix (the FR/IT
    and continuation-citation path). A prefixed token asserts "I am a BGE
    citation", so even a year-shaped page is a reportable error ("BGE 137
    V 2010"). A bare token asserts nothing: "31 III 2004" is how older
    French texts write 31 March 2004, and Roman months I-XII overlap the
    real divisions, so bare + year-shaped page is treated as a date, not
    an anomaly, and bare divisions outside the series' own set are
    treated as non-citations."""
    m = (_BARE_BGE if bare else _BGE_TOKEN).match(token.strip())
    if not m:
        return None                     # malformed beyond our grammar: skip
    vol, div, page = int(m.group(1)), m.group(2).upper(), int(m.group(3))
    if bare:
        if div not in _REAL_DIVISIONS:
            return None                 # VI+, XII, ...: not a citation
        if 1875 <= page <= 2100 and page > 700:
            return None                 # date shape: "31 III 2004"
    year_tag = " page_looks_like_year" if 1875 <= page <= 2100 and page > 700 else ""
    if vol < 1 or vol > max_vol + 1:    # +1: next-year volume may simply
        return "volume_out_of_range" + year_tag  # not be scraped yet
    if vol >= max_vol - 1:
        return None                     # open volumes: still filling, no proof
    pages = _family_pages(idx, vol, div)
    if not pages:
        return "division_absent_for_volume" + year_tag
    if page > pages[-1] + _last_case_window(pages):
        return "page_beyond_series" + year_tag
    # Volumes are continuously paginated: any page at/after the first case
    # start lies within some case, so a mid-volume page is always a
    # plausible deep pin-cite. Only pages beyond the last case (plus the
    # adaptive window for its unknown length) are provably outside.
    return None


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
    in the series (volume/division absent, or page beyond the volume end
    allowing the last case's unknown length)."""
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
            if r["tt"] == "bge":
                bare = False
            elif r["tt"] == "docket" and _BARE_BGE.match((r["ref"] or "").strip()):
                bare = True             # FR/IT ATF/DTF + continuation cites
            else:
                continue
            reason = _classify_bge(r["ref"], idx, max_vol, bare=bare)
            if not reason:
                continue
            all_total += 1
            key = reason.split()[0]
            all_by_reason[key] = all_by_reason.get(key, 0) + 1
            if (r["sdate"] or "") >= since:
                recent.append({"decision_id": r["sid"], "decided": r["sdate"],
                               "token": r["ref"], "reason": reason,
                               "form": "bare" if bare else "prefixed"})
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


def main() -> int:
    """Standalone daily run (opencaselaw-anomaly-audit.timer, 11:30 UTC).

    Decoupled from the nightly publish on purpose: the publish process
    imports this module at 03:30, so code deployed during the day only
    reaches the in-publish audit a build later (observed 2026-07-31, when
    the report stayed stale for a day). This entry point always runs the
    code on disk and refreshes the report ~45 min before the 12:15
    confidential reporter reads it. Always exits 0 — the checks are
    advisory (MODULE_NEVER_CRITICAL); breakage surfaces via the unit's
    OnFailure ntfy hook on crash, not via exit codes.
    """
    for check in (check_nonexistent_bge_citations,
                  check_anachronistic_citations,
                  check_unresolved_recent_bger):
        try:
            res = check(None)
            print(f"{res.name}: {res.message}")
        except Exception as e:                          # noqa: BLE001
            print(f"{check.__name__} crashed: {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
