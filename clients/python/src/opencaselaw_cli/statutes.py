"""Statute references in a draft: finding them in the prose (German, French,
Italian; federal and cantonal), reading what the service or a local statutes
database answers about each, and the rows the report shows.

The grammar mirrors the server's statute audit (`_STATUTE_AUDIT_PATTERN` and
`_STATUTE_AUDIT_INVALID_LAWS` in mcp_server.py): the same article slot, the
same subdivision markers, the same case-sensitive law slot, the same words
that are never a law. On top of that: chains ("Art. 8, 9 und 10 ZGB"),
"ff."/"ss." runs, the paragraph sign with a canton ("§ 12 Abs. 2 StG/ZH",
"§ 18 VRG (ZH)") and SR numbers ("SR 210").

Nothing here composes a citation or an article text. The abbreviation, the SR
number, the title and the wording in every row are the fields the service (or
the statutes database) served, shown as they came.
"""
from __future__ import annotations

import os
import re
import sqlite3
import threading
from pathlib import Path

CANTONS = frozenset("ZH BE LU UR SZ OW NW GL ZG FR SO BS BL SH AR AI SG GR AG TG TI VD VS NE GE JU".split())

# ── grammar (server parity, see the module docstring) ─────────────────────
_ORDINALS = r"bis|ter|quater|quinquies|sexies|septies|octies|novies|decies"
# An article number: digits, an optional Latin ordinal ("8bis") or a letter
# suffix ("41a", "85 a"). A space-separated letter is never e or o, which are
# the Italian/French connectives in a chain ("art. 8 e 9 CC").
_NUM = rf"\d{{1,4}}(?:\s?(?:{_ORDINALS})\b|(?:[a-z]|\s[a-df-np-z])(?![a-zA-Z.]))?"
_CONN = r"(?:,|/|[-–]|\b(?:und|et|e|oder|ou|o|and|sowie|bzw\.)\s)"
_ARTICLES = rf"(?P<articles>{_NUM}(?:\s*{_CONN}\s*{_NUM})*)"
_FF = r"(?:\s?(?:ff\.?|f\.|ss\.?|s\.|segg?\.?|e\s?segg?\.?|et\s?ss?\.))?"
_MARKER = (r"Abs|Absatz|al|alin(?:ea|éa)?|cpv|capoverso|co|para|par"
           r"|Bst|Buchstabe|lit|let|lettre|lett|lettera"
           r"|Ziff|Ziffer|ch|chiffre|Nr|Nummer|num|numero|n"
           r"|Satz|phrase|frase|sent")
_VALUE = r"(?:\d+[a-z]?|[ivxIVX]{1,5}|[a-z])"
_VALUES = rf"{_VALUE}\b(?:\s*(?:,|[-–]|\b(?:und|et|e|oder|ou|o|and|sowie|bzw\.)\s)\s*{_VALUE}\b)*"
_SUB = rf"(?P<sub>(?:\s*\b(?i:{_MARKER})\.?\s*{_VALUES})*)"
# Law slot as the server accepts it (uppercase first, hyphenated chunks, an
# optional /XX suffix), plus "EG SchKG"-style introductory acts and a
# bracketed canton.
_LAW = (r"(?P<law>(?:EG\s+(?=[A-Z][a-z]*[A-Z]))?[A-Z][A-Za-zÄÖÜ0-9]{1,11}(?:-[A-Z0-9][A-Za-zÄÖÜ0-9]{0,11})*)"
        r"(?:/(?P<suffix>[A-Z0-9]{2,6}))?(?![A-Za-z0-9])(?:\s*\((?P<bracket>[A-Z]{2})\))?")
_ART_RE = re.compile(rf"(?<![A-Za-z])(?i:Artt?\.?|Artikel|articles?|articol[oi])\s*{_ARTICLES}{_FF}{_SUB}{_FF}\s+{_LAW}")
_SECTION_RE = re.compile(rf"§§?\s*{_ARTICLES}{_FF}{_SUB}{_FF}\s+{_LAW}")
_SR_RE = re.compile(r"(?<![A-Za-z0-9])(?:SR|RS)\s?(?P<sr>(?:0|[1-9]\d{0,2})(?:\.\d{1,4})*)(?!\.?\d)")
_NUM_RE = re.compile(_NUM)
_SUB_RE = re.compile(rf"\b(?i:({_MARKER}))\.?\s*({_VALUE})\b")
_PARAGRAPH_MARKERS = {"abs", "absatz", "al", "alin", "alinea", "alinéa", "cpv", "capoverso", "co", "para", "par"}
_LETTER_MARKERS = {"bst", "buchstabe", "lit", "let", "lettre", "lett", "lettera"}

# The server's list: all-caps words that look like a law slot and never are.
INVALID_LAWS = frozenset({
    "ABS", "ABSATZ", "AL", "ALIN", "ALINEA", "CPV", "CAPOVERSO", "PARA", "PAR",
    "BST", "BUCHSTABE", "LIT", "LET", "LETTRE", "LETT", "LETTERA",
    "ZIFF", "ZIFFER", "CHIFFRE", "NUMMER", "NUMERO", "NR",
    "SATZ", "PHRASE", "FRASE", "SENT",
    "BIS", "TER", "QUATER", "QUINQUIES", "SEXIES",
    "OG", "OGER", "BG", "BGE", "BGER", "BGB", "EG", "IG", "VG", "RR",
    "EN", "DE", "FR", "IT",
    "UND", "ODER", "BZW", "USW", "ET", "OU", "EE", "OD",
})


def normalise_article(number: str | None) -> str:
    """The comparison form of an article number: "85 a" -> "85a", "8 bis" -> "8bis"."""
    return re.sub(r"\s+", "", str(number or "")).lower()


def _subdivisions(sub: str) -> dict:
    fields: dict = {}
    for m in _SUB_RE.finditer(sub or ""):
        marker = m.group(1).lower()
        if marker in _PARAGRAPH_MARKERS and "paragraph" not in fields:
            fields["paragraph"] = m.group(2)
        elif marker in _LETTER_MARKERS and "letter" not in fields:
            fields["letter"] = m.group(2)
    return fields


def _rows_from_match(kind: str, m: re.Match, text: str) -> list[dict]:
    if kind == "sr":
        return [{"reference": text, "law": text, "sr_number": m.group("sr"), "article": None}]
    law = m.group("law")
    canton = None
    suffix = m.group("suffix")
    if suffix:
        if suffix in CANTONS:
            canton = suffix
        else:
            law = f"{law}/{suffix}"
    if m.group("bracket") in CANTONS:
        canton = m.group("bracket")
    if law.upper() in INVALID_LAWS:
        return []
    fields = _subdivisions(m.group("sub"))
    rows = []
    for number in _NUM_RE.findall(m.group("articles")):
        row = {"reference": text, "law": law, "article": normalise_article(number), **fields}
        if canton:
            row["canton"] = canton
        if kind == "section":
            row["section_sign"] = True
        rows.append(row)
    return rows


def find_statute_references(paragraphs: list[str], quote_pattern=None) -> list[dict]:
    """Statute references in document order, one row per article named, deduplicated by
    written form; the quotation within 300 characters is attached as `quote`."""
    found: list[dict] = []
    seen: dict = {}
    for index, paragraph in enumerate(paragraphs):
        spans = []
        for kind, pattern in (("art", _ART_RE), ("section", _SECTION_RE), ("sr", _SR_RE)):
            for m in pattern.finditer(paragraph):
                spans.append((m.start(), -m.end(), kind, m))
        spans.sort(key=lambda s: (s[0], s[1]))
        quotes = [(q.start(), q.end(), (q.group(1) or (q.group(2) if q.re.groups > 1 else "") or "").strip())
                  for q in quote_pattern.finditer(paragraph)] if quote_pattern else []
        last_end = -1
        for start, neg_end, kind, m in spans:
            end = -neg_end
            if start < last_end:
                continue
            last_end = end
            text = m.group(0).strip().rstrip(",;")
            quote = None
            for qs, qe, qtext in quotes:
                if abs(qs - end) <= 300 or abs(start - qe) <= 300:
                    quote = qtext
                    break
            for row in _rows_from_match(kind, m, text):
                key = (text, row["article"])
                if key in seen:
                    if quote and not found[seen[key]].get("quote"):
                        found[seen[key]]["quote"] = quote
                    continue
                seen[key] = len(found)
                row.update(paragraph_index=index + 1, context=paragraph[max(0, start - 60):end + 60].strip())
                if quote:
                    row["quote"] = quote
                found.append(row)
    return found


# ── the request a row needs, and what the answer means ────────────────────
def law_request(row: dict, language: str = "de", default_canton: str | None = None):
    """(path, params, key) for a found row, or (None, reason, None) when it cannot be asked."""
    if row.get("sr_number") and not row.get("article"):
        params = {"sr_number": row["sr_number"], "language": language}
        return "/api/laws/_", params, ("CH", "SR " + row["sr_number"], None, language)
    canton = (row.get("canton") or (default_canton if row.get("section_sign") else None) or "CH").upper()
    if row.get("section_sign") and canton == "CH":
        return None, "cantonal act without a canton: write the canton (StG/ZH or VRG (ZH)) or set OCL_CANTON", None
    params = {"article": row["article"], "language": language}
    if canton != "CH":
        params["canton"] = canton
    from urllib.parse import quote
    return "/api/laws/" + quote(row["law"], safe=""), params, (canton, row["law"].upper(), row["article"], language)


def classify_law_response(result: dict, article: str | None) -> dict:
    """What a /api/laws answer (service or local) establishes about one reference."""
    provenance = {k: result.get(k) for k in ("sr_number", "abbreviation", "title", "canton", "level", "consolidation_date",
                                              "source_url", "source_label", "language") if result.get(k) not in (None, "")}
    if isinstance(result.get("article_language_fallback"), dict):
        provenance["language_served"] = result["article_language_fallback"].get("served")
    if result.get("offline"):
        provenance["offline"] = True
    if not article:
        return {"status": "law_found", "provenance": provenance}
    articles = result.get("articles")
    if not isinstance(articles, list):
        return {"status": "error", "provenance": provenance, "error": {"status": None, "message": "the answer carries no articles list"}}
    wanted = normalise_article(article)
    exact = sorted((a for a in articles if isinstance(a, dict) and normalise_article(a.get("article_num")) == wanted),
                   key=lambda a: bool(a.get("section")))
    if not exact:
        offered = [str(a.get("article_num")) for a in articles if isinstance(a, dict) and a.get("article_num")][:8]
        reason = f"the act has no Art. {article}"
        if offered:
            reason += "; the service served instead: " + ", ".join(offered)
        return {"status": "article_missing", "provenance": provenance, "reason": reason}
    art = exact[0]
    served = {k: art.get(k) for k in ("article_num", "heading", "footnote", "section", "section_heading") if art.get(k) not in (None, "")}
    text = art.get("text") if isinstance(art.get("text"), str) else ""
    out = {"provenance": provenance, "article_served": served}
    if result.get("article_section_note"):
        out["note"] = result["article_section_note"]
    if art.get("empty_body") or not text.strip():
        out.update(status="article_empty", reason=str(art.get("footnote") or "no text in the current edition"))
        return out
    out.update(status="statute_found", article_text=text)
    return out


def classify_law_error(error) -> dict:
    """What an APIError from the law request means: the service answered (law unknown,
    or the statutes are not available), or it did not answer (error)."""
    response = getattr(error, "response", None)
    response = response if isinstance(response, dict) else {}
    message = str(getattr(error, "message", error))
    if getattr(error, "status", None) in (200, 404):
        if response.get("unavailable") or "not available" in message.lower():
            return {"status": "unverifiable", "reason": message}
        out = {"status": "law_unknown", "reason": message}
        if isinstance(response.get("candidates"), list) and response["candidates"]:
            out["candidates"] = response["candidates"][:6]
        return out
    return {"status": "error", "error": error.to_dict() if hasattr(error, "to_dict") else {"status": None, "message": message}}


# ── the report ─────────────────────────────────────────────────────────────
LABELS = {
    "statute_found": ("article retrieved", "The act exists and has this article; its text is shown as served."),
    "law_found": ("act found", "An act carries this SR number."),
    "article_missing": ("article not in the act", "The act exists but has no article with this number. Check the number and its suffix (41a, 41bis), or whether it belongs to another act."),
    "article_empty": ("article has no text", "The number exists but carries no text in the current edition (repealed or left empty; the note says why). Check the edition in force at the relevant date."),
    "law_unknown": ("act not found", "No act carries this abbreviation. Check it; for a cantonal act name the canton (StG/ZH)."),
    "unverifiable": ("not checked", "Could not be checked in this run; check it by hand or run the check online."),
    "error": ("check failed", "The service or network did not answer for this item; run the check again."),
    "quote_near": ("quotation differs", "The quotation differs from the served article text; the differences are listed."),
    "quote_not_found": ("quotation not found", "The quotation does not appear in the served article text."),
}
ATTENTION = {"article_missing", "article_empty", "law_unknown", "error", "quote_near", "quote_not_found"}


def statute_label(row: dict) -> tuple[str, str]:
    status = row.get("status", "error")
    quote = (row.get("quote_check") or {}).get("quote_status")
    if status == "statute_found" and quote in ("near", "not_found"):
        status = "quote_" + quote
    return LABELS.get(status, (status, ""))


def statute_state(row: dict) -> str:
    """verified, attention or unverifiable."""
    status = row.get("status", "error")
    quote = (row.get("quote_check") or {}).get("quote_status")
    if status == "statute_found" and quote in ("near", "not_found"):
        status = "quote_" + quote
    if status in ATTENTION:
        return "attention"
    if status == "unverifiable":
        return "unverifiable"
    return "verified"


def statute_detail(row: dict) -> str:
    bits = []
    provenance = row.get("provenance") or {}
    if provenance.get("title"):
        bits.append(f"{provenance.get('abbreviation') or provenance.get('sr_number')}: {provenance['title']}"
                    + (f" (SR {provenance['sr_number']})" if provenance.get("sr_number") and provenance.get("abbreviation") else ""))
    if provenance.get("language_served"):
        bits.append(f"text served in {provenance['language_served']}")
    if row.get("reason") and row.get("status") not in ("statute_found",):
        bits.append(str(row["reason"]))
    if row.get("note"):
        bits.append(str(row["note"]))
    for c in (row.get("candidates") or [])[:4]:
        if isinstance(c, dict):
            bits.append(f"candidate {c.get('sr_number') or ''} {c.get('title') or c.get('abbreviation') or ''}".strip())
    quote = row.get("quote_check") or {}
    if quote.get("quote_status") == "near":
        for d in (quote.get("differences") or [])[:3]:
            bits.append(f"written «{d.get('quote')}», served «{d.get('served')}»")
    elif quote.get("quote_status") == "not_found" and quote.get("served"):
        bits.append("closest served wording: «" + " ".join(str(quote["served"]).split())[:240] + "»")
    elif quote.get("quote_status") == "exact":
        bits.append("quotation verbatim")
    if row.get("error") and row.get("status") == "error":
        bits.append(str((row["error"] or {}).get("message")))
    return "; ".join(b for b in bits if b)


def statute_excerpt(row: dict, limit: int = 200) -> str:
    """The first characters of the served article text, whitespace collapsed; never rewritten."""
    text = " ".join(str(row.get("article_text") or "").split())
    if len(text) <= limit:
        return text
    return text[:limit - 1].rstrip() + "…"


def summarize_statutes(rows: list[dict]) -> dict:
    states = [statute_state(r) for r in rows]
    return {"statutes_checked": len(rows), "statutes_verified": states.count("verified"),
            "statutes_attention": states.count("attention"), "statutes_unverifiable": states.count("unverifiable")}


def _summary_sentence(rows: list[dict]) -> str:
    s = summarize_statutes(rows)
    parts = [f"{s['statutes_checked']} statute references found", f"{s['statutes_verified']} retrieved", f"{s['statutes_attention']} need attention"]
    if s["statutes_unverifiable"]:
        parts.append(f"{s['statutes_unverifiable']} not checked")
    return ", ".join(parts) + "."


def statutes_markdown(rows: list[dict]) -> list[str]:
    if not rows:
        return []
    lines = ["## Statutes", "", _summary_sentence(rows), "", "| As written | Finding | What to do | Article text (excerpt) |", "|---|---|---|---|"]
    for r in rows:
        label, advice = statute_label(r)
        detail = statute_detail(r)
        cell = advice + (f" ({detail})" if detail else "")
        excerpt = statute_excerpt(r)
        lines.append(f"| {r.get('reference')} | {label} | {cell.replace('|', '/')} | {excerpt.replace('|', '/')} |")
    lines.append("")
    return lines


def statutes_html(rows: list[dict], e) -> list[str]:
    if not rows:
        return []
    parts = ["<h2>Statutes</h2>", f"<p class=\"muted\">{e(_summary_sentence(rows))}</p>",
             "<table><tr><th>As written</th><th>Finding</th><th>What to do</th><th>Article text (excerpt)</th></tr>"]
    for r in rows:
        label, advice = statute_label(r)
        state = statute_state(r)
        cls = "ok" if state == "verified" else "muted" if state == "unverifiable" else ("bad" if label in ("act not found", "article not in the act", "quotation not found") else "warn")
        detail = statute_detail(r)
        parts.append(f"<tr><td><code>{e(str(r.get('reference')))}</code>" + (f"<br><span class=\"muted\">§{r['paragraph_index']}</span>" if r.get("paragraph_index") else "")
                     + f"</td><td class=\"{cls}\">{e(label)}</td><td>{e(advice)}" + (f"<br><span class=\"muted\">{e(detail)}</span>" if detail else "")
                     + f"</td><td class=\"muted\">{e(statute_excerpt(r))}</td></tr>")
    parts.append("</table>")
    return parts


def statutes_terminal(value: dict, s, width: int, wrap) -> list[str]:
    """The short block `ocl check` prints for statutes; `wrap(text, width, indent)` is the renderer's."""
    rows = value.get("statutes") or []
    if not rows:
        return []
    summary = summarize_statutes(rows)
    lines = ["", s.bold("statutes") + s.dim(f"  {summary['statutes_checked']} found, {summary['statutes_verified']} retrieved, "
                                            f"{summary['statutes_attention']} need attention"
                                            + (f", {summary['statutes_unverifiable']} not checked" if summary["statutes_unverifiable"] else ""))]
    for r in rows:
        state = statute_state(r)
        if state == "verified":
            continue
        label, advice = statute_label(r)
        colour = s.dim if state == "unverifiable" else s.red if label in ("act not found", "article not in the act", "quotation not found") else s.yellow
        lines.append(f"  {colour(label)}{' ' * max(1, 24 - len(label))}{r.get('reference')}")
        detail = statute_detail(r)
        lines += [s.dim(l) for l in wrap(advice + (" " + detail if detail else ""), width, "      ")]
    return lines


# ── offline: a statutes database next to the verification pack ─────────────
_readers: dict = {}
_readers_lock = threading.Lock()


def statutes_path(pack_path) -> Path | None:
    """OCL_STATUTES, else statutes.sqlite (or statutes.db) next to the pack."""
    env = os.environ.get("OCL_STATUTES")
    if env:
        return Path(env).expanduser()
    for name in ("statutes.sqlite", "statutes.db"):
        candidate = Path(pack_path).expanduser().parent / name
        if candidate.is_file():
            return candidate
    return None


class LocalStatutes:
    """Read-only reader of a statutes database (the schema of search_stack/build_statutes_db.py:
    tables laws and articles). One connection per thread; the file is opened immutable."""

    def __init__(self, path):
        self.path = Path(path).expanduser()
        if not self.path.is_file():
            raise ValueError(f"statutes database not found: {self.path}")
        self._local = threading.local()
        self._columns = None

    def _connection(self):
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(f"file:{self.path}?mode=ro&immutable=1", uri=True)
            con.row_factory = sqlite3.Row
            self._local.con = con
        return con

    def columns(self) -> set:
        if self._columns is None:
            self._columns = {r[1] for r in self._connection().execute("PRAGMA table_info(articles)")}
        return self._columns

    def law(self, *, abbreviation: str | None = None, sr_number: str | None = None, article: str | None = None,
            language: str = "de") -> dict:
        con = self._connection()
        language = language if language in ("de", "fr", "it") else "de"
        if not sr_number and abbreviation:
            up = abbreviation.strip().upper()
            row = con.execute("SELECT sr_number FROM laws WHERE UPPER(abbr_de) = ? OR UPPER(abbr_fr) = ? OR UPPER(abbr_it) = ? LIMIT 1",
                              (up, up, up)).fetchone()
            if row is None:
                return {"error": f"No law found with abbreviation '{abbreviation}'.", "offline": True}
            sr_number = row[0]
        if not sr_number:
            return {"error": "Provide sr_number or abbreviation.", "offline": True}
        law = con.execute("SELECT * FROM laws WHERE sr_number = ?", (str(sr_number).strip(),)).fetchone()
        if law is None:
            return {"error": f"No law found with SR number '{sr_number}'.", "offline": True}
        result = {"sr_number": law["sr_number"], "title": law[f"title_{language}"] or law["title_de"],
                  "abbreviation": law[f"abbr_{language}"] or law["abbr_de"], "consolidation_date": law["consolidation_date"],
                  "canton": "CH", "level": "federal", "language": language, "offline": True, "source_label": "statutes database (offline)"}
        cols = self.columns()
        main = " AND section = ''" if "section" in cols else ""
        selected = ", ".join(c for c in ("article_num", "heading", "text", "footnote", "section", "section_heading") if c in cols)
        if not article:
            rows = con.execute(f"SELECT article_num, heading FROM articles WHERE sr_number = ? AND lang = ?{main} "
                               "ORDER BY CAST(article_num AS INTEGER), article_num", (law["sr_number"], language)).fetchall()
            result["article_count"] = len(rows)
            result["articles"] = [{"article_num": r["article_num"], "heading": r["heading"]} for r in rows]
            return result
        wanted = normalise_article(article)
        rows = []
        for lang in dict.fromkeys((language, "de", "fr", "it")):
            rows = con.execute(f"SELECT {selected} FROM articles WHERE sr_number = ? AND lang = ? AND article_num = ?{main}",
                               (law["sr_number"], lang, article)).fetchall()
            if not rows:
                rows = con.execute(f"SELECT {selected} FROM articles WHERE sr_number = ? AND lang = ? "
                                   f"AND REPLACE(LOWER(article_num), ' ', '') = ?{main}", (law["sr_number"], lang, wanted)).fetchall()
            if rows:
                if lang != language:
                    result["article_language_fallback"] = {"requested": language, "served": lang,
                                                           "note": f"Article {article} is not available in '{language}' in this statutes database; showing '{lang}'."}
                break
        if not rows and main:
            rows = con.execute(f"SELECT {selected} FROM articles WHERE sr_number = ? AND lang = ? AND section != '' "
                               "AND REPLACE(LOWER(article_num), ' ', '') = ? ORDER BY section", (law["sr_number"], language, wanted)).fetchall()
            if rows:
                result["article_section_note"] = f"Art. {article} exists only in the transitional / final provisions of this act, not in its main body."
        articles = []
        for r in rows:
            art = {k: r[k] for k in r.keys()}
            note = art.get("footnote")
            body = art.get("text") or ""
            if note and (body == note or not re.search(r"\w", body)):
                art["text"] = ""
                art["empty_body"] = True
            articles.append(art)
        result["articles"] = articles
        return result


def _reader(path: Path) -> LocalStatutes:
    key = str(path.resolve())
    with _readers_lock:
        reader = _readers.get(key)
        if reader is None:
            reader = _readers[key] = LocalStatutes(path)
        return reader


def local_law(pack_path, abbreviation: str, params: dict | None = None) -> dict:
    """Answer GET /api/laws/{abbreviation} offline. Federal acts come from the statutes
    database next to the pack (or OCL_STATUTES); when it is absent, or the act is
    cantonal, the answer says so with `unavailable: true` so the check reports the
    row as not checked rather than as wrong."""
    params = params or {}
    canton = str(params.get("canton") or "CH").upper()
    m = re.match(r"^\s*([A-Za-z]{2})\s*/\s*(\S.*)$", abbreviation or "")
    if m and m.group(1).upper() in CANTONS:
        canton, abbreviation = m.group(1).upper(), m.group(2).strip()
    if params.get("as_of"):
        return {"error": "historical editions (as_of) are not available offline; run without --local", "unavailable": True, "offline": True}
    if canton != "CH":
        return {"error": f"cantonal statutes are not available offline ({canton} {abbreviation}); run without --local",
                "unavailable": True, "offline": True}
    path = statutes_path(pack_path)
    if path is None or not path.is_file():
        return {"error": "statutes not available offline: no statutes.sqlite next to the verification pack and OCL_STATUTES is not set",
                "unavailable": True, "offline": True}
    reader = _reader(path)
    return reader.law(abbreviation=None if abbreviation in ("_", "") else abbreviation, sr_number=params.get("sr_number"),
                      article=params.get("article"), language=str(params.get("language") or "de"))
