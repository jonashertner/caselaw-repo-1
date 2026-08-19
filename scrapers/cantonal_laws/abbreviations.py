"""Cantonal law abbreviations — the lookup key nobody stored.

Why this exists
---------------

`/api/laws/{abbreviation}` is the primary way anyone reaches a statute,
and for cantonal law it answered 3 of 15 everyday requests (measured
2026-08-19). The cause is not coverage: Zurich's tax act is in the
corpus as SR 631.1. It is that cantonal_laws.db has no abbreviation
column at all, so all 15,608 cantonal laws are unreachable by the name
practitioners actually use. Federal law works precisely because Fedlex
hands us abbr_de/abbr_fr/abbr_it.

Where the data actually is
--------------------------

Measured, not assumed:

  * **LexFind** — no. Neither its search results nor its per-law endpoint
    carry a law-level abbreviation; the only `abbreviation` field there
    belongs to the *entity* (the canton: "BE", "ZH"). This matters
    because LexFind is where 26 of 26 cantons currently come from, which
    is exactly why the corpus has none.
  * **LexWork** — yes, and it covers 19 cantons through one identical
    API. `texts_of_law/{sr}/show_as_json` carries it (ZG 231.1 →
    "EG SchKG", LU 1 → "KV"). The lightweight index does not, and there
    is no bulk endpoint, so it costs one request per law.
  * **Titles** — partially, and only where the canton writes it there
    (Geneva does: "Loi sur la procédure administrative (LPA)").

What is deliberately NOT a source
---------------------------------

The law's own body text. An act declares its short form as "(StG)", but
Swiss drafting writes that same bracketed form on first *citation*, so
extraction from the text resolved StG to whichever act happened to cite
the tax law — 6 of 12 lookups "resolved" against production data and
nearly all were the wrong act. A confident wrong statute is worse than
no answer.

Honest ceiling
--------------

Roughly 40% of a canton's laws declare an abbreviation at source; the
rest genuinely have none. Success here is capturing every abbreviation
that exists, not inventing one for every law.

Output: ``output/cantonal_abbreviations.jsonl``, one record per
(canton, sr_number, language), each carrying its `source` so a wrong
entry can be traced to where it came from.
"""
from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path

import requests

from scrapers.cantonal_laws import CANTON_LANG, LEXWORK_HOSTS

logger = logging.getLogger(__name__)

USER_AGENT = (
    "SwissCaselawBot/1.0 (https://github.com/jonashertner/caselaw-repo; "
    "legal research; respects rate limits)"
)
OUTPUT = Path(__file__).parent.parent.parent / "output" / "cantonal_abbreviations.jsonl"
REQUEST_DELAY = 0.5

# The portals put a short title and an abbreviation in one field:
# "Organisationsgesetz, OG" / "PH-Gesetz, PHG" / "Studienreglement, StuR".
# Both are worth indexing — a reader may type either — so the pair is
# split rather than one of them discarded.
_SPLIT = re.compile(r"\s*,\s*")
# An abbreviation is short, starts with a letter, and is not a sentence.
_ABBR_SHAPE = re.compile(r"^[A-Za-zÄÖÜÀÉÈäöüàéè][A-Za-zÄÖÜäöüàéèç0-9./\-]{0,11}"
                         r"(?:\s[A-Za-zÄÖÜ0-9./\-]{1,10}){0,2}$")
# Parenthesised abbreviation at the end of a title: Geneva's house style.
_TITLE_PAREN = re.compile(r"\(([A-ZÄÖÜ][A-Za-zÄÖÜäöüàéè0-9./\-]{1,11}"
                          r"(?:[ /\-][A-Za-zÄÖÜ0-9]{1,8})?)\)")


def looks_like_abbreviation(value: str) -> bool:
    """Reject the long names portals also put in this field."""
    v = (value or "").strip()
    if not v or len(v) > 24:
        return False
    if not _ABBR_SHAPE.match(v):
        return False
    # "Entschädigungsverordnung" is a name, not an abbreviation. An
    # abbreviation is either short or carries internal capitals/spaces.
    if len(v) > 12 and " " not in v and not re.search(r"[A-Z].*[A-Z]", v):
        return False
    return True


def is_plausible_acronym(abbr: str, title: str) -> bool:
    """Could `abbr` be this title's acronym?

    Guards title-derived entries only — a portal's own field needs no
    second-guessing. Two cheap conditions kill the failure mode that
    made naive extraction unusable ("Findelkind" offered as the
    abbreviation of a citizenship act): the letters must appear in the
    title in order, and the first letters must agree.
    """
    a = re.sub(r"[^A-Za-zÄÖÜäöü]", "", abbr or "").lower()
    t = (title or "").lower()
    if not a or not t:
        return False
    words = [w for w in re.split(r"[^a-zà-üä-ü]+", t) if w]
    if not words or a[0] != words[0][0]:
        return False
    i = 0
    for ch in t:
        if i < len(a) and ch == a[i]:
            i += 1
    return i == len(a)


def split_field(value: str) -> tuple[str | None, str | None]:
    """-> (abbreviation, short_title). Either may be None."""
    parts = [p.strip() for p in _SPLIT.split((value or "").strip()) if p.strip()]
    if not parts:
        return None, None
    if len(parts) == 1:
        v = parts[0]
        return (v, None) if looks_like_abbreviation(v) else (None, v)
    # "Long short-title, ABBR" — the abbreviation is conventionally last.
    tail = parts[-1]
    if looks_like_abbreviation(tail):
        return tail, ", ".join(parts[:-1])
    return (None, ", ".join(parts))


def qualify(canton: str, abbreviation: str) -> str | None:
    """The canonical name of a cantonal law: 'ZH/StG'.

    A cantonal abbreviation is only unique inside its canton — StG is the
    tax act in ZH, BE and AG — so the canton travels with it and is part
    of the name rather than a separate parameter that can be dropped.

    Federal law takes NO prefix: bare 'StG' is the federal stamp-duty
    act, 'ZH/StG' is Zurich's tax act, and the two can no longer be
    confused for one another. That asymmetry is deliberate — the
    Confederation's collection is the unprefixed default, exactly as
    practitioners write it.
    """
    a = (abbreviation or "").strip()
    c = (canton or "").strip().upper()
    if not a or not c or c == "CH":
        return a or None
    return f"{c}/{a}"


def parse_qualified(name: str) -> tuple[str | None, str]:
    """'ZH/StG' -> ('ZH', 'StG'); 'StG' -> (None, 'StG').

    None means federal, which is what an unprefixed name denotes.
    """
    m = re.match(r"^\s*([A-Za-z]{2})\s*/\s*(\S.*)$", name or "")
    if not m:
        return None, (name or "").strip()
    return m.group(1).upper(), m.group(2).strip()


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return s


def harvest_lexwork(canton: str, limit: int | None = None,
                    delay: float = REQUEST_DELAY):
    """Yield abbreviation records for one LexWork canton."""
    host = LEXWORK_HOSTS[canton]
    lang = CANTON_LANG[canton]
    s = _session()
    idx = s.get(f"https://{host}/api/{lang}/texts_of_law/lightweight_index",
                timeout=60)
    idx.raise_for_status()
    data = idx.json()
    laws = [x for v in data.values() for x in v] if isinstance(data, dict) else data
    laws = [x for x in laws if x.get("systematic_number")]
    if limit:
        laws = laws[:limit]
    logger.info("[%s] %d laws in index", canton, len(laws))
    for n, law in enumerate(laws, 1):
        sr = law["systematic_number"]
        try:
            r = s.get(f"https://{host}/api/{lang}/texts_of_law/{sr}/show_as_json",
                      timeout=45)
            if r.status_code != 200:
                continue
            tol = (r.json() or {}).get("text_of_law") or {}
        except Exception as e:                          # pragma: no cover
            logger.debug("[%s] %s failed: %s", canton, sr, e)
            continue
        finally:
            time.sleep(delay)
        abbr, short = split_field(tol.get("abbreviation") or "")
        if not abbr and not short:
            continue
        yield {
            "canton": canton,
            "language": lang,
            "sr_number": sr,
            "title": law.get("title") or "",
            "abbreviation": abbr,
            "qualified": qualify(canton, abbr) if abbr else None,
            "short_title": short,
            # Provenance: the canton published this itself, so it needs no
            # acronym check and outranks anything we derive.
            "source": "lexwork_api",
        }
        if n % 200 == 0:
            logger.info("[%s] %d/%d", canton, n, len(laws))


def harvest_titles(rows) -> list:
    """Abbreviations a canton writes into the title itself.

    Free — no requests — but derived, so every candidate must survive the
    acronym check before it is kept.
    """
    out = []
    for r in rows:
        title = r.get("title") or ""
        m = _TITLE_PAREN.search(title)
        if not m:
            continue
        abbr = m.group(1)
        stripped = _TITLE_PAREN.sub("", title)
        if not looks_like_abbreviation(abbr):
            continue
        if not is_plausible_acronym(abbr, stripped):
            continue
        out.append({
            "canton": r.get("canton"),
            "language": r.get("language"),
            "sr_number": r.get("sr_number"),
            "title": title,
            "abbreviation": abbr,
            "qualified": qualify(r.get("canton"), abbr),
            "short_title": None,
            "source": "title",
        })
    return out


def write(records, path: Path = OUTPUT) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(path, "a", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
    return n


def already_harvested(path: Path = OUTPUT) -> set:
    """(canton, sr_number) pairs already on disk, so a re-run resumes
    instead of re-fetching 10,882 laws."""
    seen = set()
    if not path.exists():
        return seen
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            try:
                r = json.loads(line)
            except Exception:
                continue
            seen.add((r.get("canton"), r.get("sr_number")))
    return seen


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Harvest cantonal law abbreviations")
    ap.add_argument("--cantons", help="comma-separated; default all LexWork cantons")
    ap.add_argument("--limit", type=int, help="laws per canton (smoke test)")
    ap.add_argument("--delay", type=float, default=REQUEST_DELAY)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(message)s")
    cantons = ([c.strip().upper() for c in args.cantons.split(",")]
               if args.cantons else sorted(LEXWORK_HOSTS))
    total = 0
    for ct in cantons:
        if ct not in LEXWORK_HOSTS:
            logger.warning("%s is not a LexWork canton — skipping", ct)
            continue
        try:
            n = write(harvest_lexwork(ct, limit=args.limit, delay=args.delay))
        except Exception as e:
            logger.error("[%s] harvest failed: %s", ct, e)
            continue
        total += n
        logger.info("[%s] wrote %d records (running total %d)", ct, n, total)
    logger.info("done: %d records -> %s", total, OUTPUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
