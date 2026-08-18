"""Canonical identity: which records are the same judgment.

Why this exists
---------------
The stored `canonical_key` is inert: it is populated for 100% of
1,053,305 decisions and yields exactly ONE group with more than one
record. It declares the whole corpus unique, which is why three separate
dedup implementations exist at query time to paper over it — and why one
of them was silently shortening result pages (issue #77).

Two failure modes explain it, both confirmed against production:

  1. It is a single string of court|docket|date. Two records of the same
     judgment that disagree on ANY component cannot meet. Luzern stores
     3,932 judgments twice, once keyed by the portal's EnId and once by
     the real docket — the dockets differ, so no string rule can join
     them. Only the source URL does.
  2. The date participates. A judgment whose date is later corrected
     splits into two keys, which is the documented P1 Wave-1 blocker.

So identity here is a union over several INDEPENDENT court-assigned
channels rather than one composite string. Two records are the same
judgment if they agree on any channel:

  url     the record id the court's own URL carries (EnId=, id=, …)
  docket  court + docket normalised to alphanumerics, date excluded
  bge     volume/division/page, from any of the three stored forms
  fed     federal chamber docket, separator- and era-insensitive

One decision, several representations
-------------------------------------
Geneva, Vaud and Schaffhausen publish the same judgment under two
numbers, and the two are NOT byte-identical, so no (court, date,
text-hash) key can join them. They are also not simultaneous duplicates:

  * the Entscheidnummer copy (ACJC/…, ATA/…) is the FROZEN judgment text
  * the Verfahrensnummer copy (A/…, P/…) is the LIVING publication page
    of the portal, carrying descriptors, norms, résumé, lower instance
    and the later appeal

The second copy accrues over time. In Geneva 2,240 of 2,251 decisions
from 2026 are still single, while 2021-2025 are only ~12% single; the
judgment appears first and the publication page follows. Measured
duplication is 1.83x, not 2x: 168,950 rows resolve to 92,314 unique
decisions by source URL.

11.3% of Geneva publication pages carry a Federal Supreme Court appeal
that the judgment CANNOT contain, because the appeal was filed after the
judgment date (A/136/2024, judgment 05.08.2025, recours 1C_511/2025 of
15.09.2025). That metadata is the raw material of a treatment graph.

So neither copy is redundant and neither is deleted. A decision is one
canonical entity with several representations; both numbers stay
resolvable for users, search returns the decision once, and the appeal
metadata is extracted from the publication page BEFORE any merge.

Because the second representation appears later, linking is a build-time
computation, not a one-off migration.
"""
from __future__ import annotations

import re
from urllib.parse import parse_qs, urlsplit

# ── channels ─────────────────────────────────────────────────────────────

_URL_ID_PARAMS = ("enid", "id", "docid", "targetid", "guid", "documentid",
                  "showdoc", "nr", "num")
_BGE = re.compile(r"(?:CH)?(?:BGE|ATF|DTF)?[ _]*(\d{1,3})[ _]+"
                  r"([IVX]{1,4}[AB]?)[ _]+(\d{1,4})", re.I)
# Federal chamber dockets across eras: 6B_267/2012, 6B 267/2012,
# 4C.355/2004, U 49/98, I 350/99.
_FED = re.compile(r"^([0-9]{1,2}[A-Z]{0,2}|[A-Z])[ _.\-]?([0-9]+)/([0-9]{2,4})$")
_FEDERAL_COURTS = {"bger", "bge", "bvger", "bstger", "bpatger"}


def _norm(s: str | None) -> str:
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())


def url_channel(url: str | None) -> str | None:
    """The record id the COURT assigns, from its own URL."""
    if not url:
        return None
    try:
        u = urlsplit(url)
    except ValueError:
        return None
    host = (u.netloc or "").lower()
    q = {k.lower(): v for k, v in parse_qs(u.query).items()}
    for p in _URL_ID_PARAMS:
        if q.get(p) and q[p][0].strip():
            return f"url:{host}|{p}={q[p][0].strip().lower()}"
    seg = [s for s in (u.path or "").split("/") if s]
    if seg and re.search(r"[0-9a-f]{6,}|\d{2,}", seg[-1]):
        return f"url:{host}|{seg[-1].lower()}"
    return None


def bge_channel(*values: str | None) -> str | None:
    """Volume/division/page, however the reporter reference is written."""
    for v in values:
        if not v:
            continue
        m = _BGE.search(str(v).replace("_", " "))
        if m:
            div = m.group(2).upper()
            # The 1955-1994 Ia/Ib split casefolds in citations both ways.
            div = "I" if div in ("IA", "IB") else div
            return f"bge:{int(m.group(1))}|{div}|{int(m.group(3))}"
    return None


def fed_channel(*values: str | None) -> str | None:
    """Federal chamber docket, separator- and century-insensitive."""
    for v in values:
        d = (v or "").strip()
        m = _FED.match(d)
        if not m:
            continue
        year = m.group(3)
        if len(year) == 2:
            year = ("20" if int(year) < 40 else "19") + year
        return f"fed:{m.group(1).upper()}_{int(m.group(2))}/{year}"
    return None


def docket_channel(court: str | None, docket: str | None) -> str | None:
    """Court + docket, WITHOUT the date.

    The date is deliberately excluded twice over: a corrected
    decision_date must not split a judgment (the defect that made the
    stored canonical_key inert), and sh_gerichte currently carries the
    publication date in that field, so it is not even the same quantity
    across courts.
    """
    d = _norm(docket)
    if len(d) < 3:
        return None
    return f"dk:{(court or '').lower()}|{d}"


# The portal's case number (Verfahrensnummer) and the judgment number
# (Entscheidnummer). The publication page names the judgment number in
# its header — ~89% of 57,000 Geneva case numbers do — which links the
# two representations even where the URL does not.
_CASE_NO = re.compile(r"^[AP]/\d+/\d{4}$", re.I)
_JUDGMENT_NO = re.compile(
    r"\b(ACJC|AARP|ATAS|ATA|DAS|DCSO|ACPR|JTPI|CAPJ|AJP|CAPH)/\d+/\d{4}\b",
    re.I)


def crossref_channel(court: str | None, rec: dict) -> str | None:
    """Judgment number declared inside a publication page.

    Only emitted FROM a case-number record: the publication page points
    at the judgment, not the reverse, so this is a one-directional link
    that meets the judgment's own docket channel.
    """
    if not _CASE_NO.match((rec.get("docket_number") or "").strip()):
        return None
    head = ((rec.get("regeste") or "") + " "
            + (rec.get("full_text") or ""))[:4000]
    m = _JUDGMENT_NO.search(head)
    if not m:
        return None
    return f"dk:{(court or '').lower()}|{_norm(m.group(0))}"


def channels(rec: dict) -> set[str]:
    """Every identity channel a record participates in."""
    court = (rec.get("court") or "").lower()
    out: set[str] = set()
    # Shared portal URL — the strongest link, and the one that makes the
    # Geneva pair meet (both copies carry the same source_url and pdf_url).
    for u in (url_channel(rec.get("source_url")),
              url_channel(rec.get("pdf_url"))):
        if u:
            out.add(u)
    dk = docket_channel(court, rec.get("docket_number"))
    if dk:
        out.add(dk)
    # Vaud already stores the second number, so it is a channel outright.
    dk2 = docket_channel(court, rec.get("docket_number_2"))
    if dk2:
        out.add(dk2)
    x = crossref_channel(court, rec)
    if x:
        out.add(x)
    if court == "bge" or str(rec.get("decision_id", "")).startswith("bge"):
        b = bge_channel(rec.get("docket_number"), rec.get("decision_id"))
        if b:
            out.add(b)
    if court in _FEDERAL_COURTS:
        f = fed_channel(rec.get("docket_number"), rec.get("docket_number_2"))
        if f:
            out.add(f)
    return out


# ── grouping ─────────────────────────────────────────────────────────────

class UnionFind:
    def __init__(self) -> None:
        self.parent: dict = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def group(records) -> dict:
    """-> {decision_id: entity_id}. Records sharing ANY channel merge."""
    uf = UnionFind()
    for rec in records:
        did = rec["decision_id"]
        uf.find(did)
        for ch in channels(rec):
            uf.union(ch, did)
    return {rec["decision_id"]: uf.find(rec["decision_id"])
            for rec in records}


# ── representation roles ─────────────────────────────────────────────────
# The two copies are not redundant: they have different roles and
# different lifecycles. Nothing here deletes either one.

_APPEAL = re.compile(
    r"entr[ée]e?\s+en\s+force|force de chose jug[ée]e|rechtskr[äa]ftig|"
    r"in Rechtskraft|passato in giudicato|"
    r"recours.{0,40}Tribunal f[ée]d[ée]ral|"
    r"Beschwerde.{0,40}Bundesgericht",
    re.I)
_FED_REF = re.compile(r"\b\d{1,2}[A-Z]{0,2}[_ ]\d+/\d{4}\b")


def representation_role(rec: dict) -> str:
    """'publication_page' (living portal page) or 'judgment' (frozen text).

    The distinction matters because only the publication page can carry
    an appeal filed AFTER the judgment date, and that is precisely the
    information a treatment graph needs.
    """
    if _CASE_NO.match((rec.get("docket_number") or "").strip()):
        return "publication_page"
    return "judgment"


def appeal_metadata(rec: dict) -> dict:
    """Weiterzug / force information to harvest BEFORE any merge."""
    text = (rec.get("regeste") or "") + " " + (rec.get("full_text") or "")
    refs = sorted(set(_FED_REF.findall(text)))
    return {
        "role": representation_role(rec),
        "has_force_statement": bool(_APPEAL.search(text)),
        "federal_references": refs,
        "source_decision_id": rec.get("decision_id"),
    }


def display_representation(records: list) -> dict:
    """Which representation search should surface. Discards nothing.

    Search must return a decision once, so one representation is chosen
    for display; the others stay resolvable by their own number.
    """
    def rank(r):
        text = (r.get("regeste") or "") + (r.get("full_text") or "")
        return (
            1 if _APPEAL.search(text) else 0,
            1 if (r.get("regeste") or "").strip() else 0,
            len(text),
            r.get("decision_date") or "",
        )
    return max(records, key=rank)


def entity_view(records: list) -> dict:
    """A canonical decision: every representation kept and labelled."""
    reps = [{"decision_id": r.get("decision_id"),
             "docket_number": r.get("docket_number"),
             "role": representation_role(r)} for r in records]
    harvested = [appeal_metadata(r) for r in records
                 if representation_role(r) == "publication_page"]
    fed_refs = sorted({x for h in harvested for x in h["federal_references"]})
    return {
        "representations": reps,
        "display": display_representation(records).get("decision_id"),
        "federal_references": fed_refs,
        "has_force_statement": any(h["has_force_statement"]
                                   for h in harvested),
    }
