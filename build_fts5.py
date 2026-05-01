#!/usr/bin/env python3
"""
Build/update SQLite FTS5 search database from scraped decisions.

Reads from:
  - output/decisions/*.jsonl  (from run_scraper.py — full Decision objects)
  - output/data/daily/*.parquet (from pipeline.py — Parquet shards)

Produces:
  - output/decisions.db (SQLite with FTS5 full-text search)

The DB schema matches what mcp_server.py expects.

Usage:
    python3 build_fts5.py                          # default: ./output
    python3 build_fts5.py --output /opt/caselaw/repo/output
    python3 build_fts5.py --output ./output --db ~/.swiss-caselaw/decisions.db
    python3 build_fts5.py --watch 60               # rebuild every 60 seconds
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from db_schema import COVERAGE_SCHEMA_SQL, INSERT_COLUMNS, INSERT_OR_IGNORE_SQL, SCHEMA_SQL
from models import make_canonical_key

logger = logging.getLogger("build_fts5")

# ── Text cleaning ────────────────────────────────────────────

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACE_RE = re.compile(r"[ \t]+")
_HTML_ENTITIES = {
    "&nbsp;": " ",
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&#39;": "'",
    "&apos;": "'",
}


def _fix_mojibake(text: str) -> str:
    """Fix double-encoded UTF-8 (UTF-8 bytes decoded as Latin-1).

    Common pattern: 'ä' (U+00E4) stored as 'Ã¤' (C3 A4 decoded as Latin-1).
    """
    try:
        # If the text contains typical mojibake sequences, try to fix
        fixed = text.encode("latin-1").decode("utf-8")
        # Sanity check: the fixed version should be shorter or equal
        if len(fixed) <= len(text):
            return fixed
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    return text


def _clean_text(text: str | None) -> str | None:
    """Strip HTML tags, fix HTML entities, fix mojibake, normalize whitespace."""
    if not text:
        return text

    # Strip HTML tags
    text = _HTML_TAG_RE.sub(" ", text)

    # Replace HTML entities
    for entity, replacement in _HTML_ENTITIES.items():
        if entity in text:
            text = text.replace(entity, replacement)

    # Fix mojibake (only if likely — check for common mojibake markers)
    if "\xc3" in text:
        text = _fix_mojibake(text)

    # Normalize whitespace (collapse runs of spaces/tabs, preserve newlines)
    text = _MULTI_SPACE_RE.sub(" ", text)

    return text.strip()


# ── BGer regeste extraction ──────────────────────────────────

_REGESTE_START_RE = re.compile(
    r"(?:^|\n)\s*Regeste\b[:\s]*\n",
    re.IGNORECASE,
)
_REGESTE_END_MARKERS = [
    re.compile(r"^\s*Sachverhalt\b", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*Faits\b", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*Fatti\b", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*Urteilskopf\b", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*A\.\s", re.MULTILINE),
]


def _extract_regeste_from_text(full_text: str) -> str | None:
    """Try to extract regeste from BGE/BGer full_text.

    BGE decisions typically contain:
      Regeste
      <regeste text in DE>
      Regeste      (or Regesto)
      <regeste in FR/IT>
      Sachverhalt / Faits / Fatti / A.
    """
    m = _REGESTE_START_RE.search(full_text)
    if not m:
        return None

    start = m.end()
    # Find the end: next major section header
    end = len(full_text)
    for pat in _REGESTE_END_MARKERS:
        em = pat.search(full_text, start)
        if em and em.start() < end:
            end = em.start()

    regeste = full_text[start:end].strip()
    # Skip if too short or too long (probably a false match)
    if len(regeste) < 20 or len(regeste) > 5000:
        return None
    return regeste


# ── Dedup + post-processing ──────────────────────────────────

def _dedup_decisions(conn: sqlite3.Connection) -> int:
    """Remove duplicate decisions sharing the same canonical_key.

    The canonical_key aggressively normalizes court + docket + date so that
    formatting variants (dots vs underscores, case, etc.) collapse together.
    Falls back to exact (court, docket_number, decision_date) if canonical_key
    is not yet populated.

    Strategy: keep the row with the most total content (full_text + regeste).
    Before deleting losers, merge their regeste into the survivor if the
    survivor's regeste is empty.

    This avoids the prior bug (2026-04-25 audit) where the rule "non-empty
    regeste wins" caused metadata-only federation stubs (~280 chars text +
    short regeste) to win over rich direct PDF scrapes (~9,000 chars text,
    no regeste). For Tribuna-based scrapers (gr/be/fr/zg/sz) the direct
    scrape never extracts regeste from PDF, so the regeste-precedence rule
    threw away ~9,300 GR full-PDF rows in favor of metadata stubs.
    Returns number of rows deleted.
    """
    # Check if canonical_key column exists and is populated
    has_canonical = False
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE canonical_key IS NOT NULL AND canonical_key != ''"
        ).fetchone()
        has_canonical = row[0] > 0
    except sqlite3.OperationalError:
        pass

    if has_canonical:
        # Exclude keys with empty docket part (format: court|DOCKET|date)
        dup_sql = """
            SELECT canonical_key, COUNT(*) as cnt
            FROM decisions
            WHERE canonical_key IS NOT NULL AND canonical_key != ''
              AND canonical_key NOT LIKE '%||%'
            GROUP BY canonical_key
            HAVING cnt > 1
        """
        groups = conn.execute(dup_sql).fetchall()
        if not groups:
            return 0

        deleted = 0
        for (canonical_key, cnt) in groups:
            rows = conn.execute(
                """
                SELECT decision_id, COALESCE(regeste, ''), LENGTH(COALESCE(full_text, ''))
                FROM decisions
                WHERE canonical_key = ?
                ORDER BY
                    LENGTH(COALESCE(full_text, '')) + LENGTH(COALESCE(regeste, '')) DESC
                """,
                (canonical_key,),
            ).fetchall()
            survivor_id, survivor_regeste, _ = rows[0]
            # Backfill regeste from a loser if survivor has none.
            if not survivor_regeste:
                for _, loser_regeste, _ in rows[1:]:
                    if loser_regeste:
                        conn.execute(
                            "UPDATE decisions SET regeste = ? WHERE decision_id = ?",
                            (loser_regeste, survivor_id),
                        )
                        break
            for row in rows[1:]:
                conn.execute("DELETE FROM decisions WHERE decision_id = ?", (row[0],))
                deleted += 1
    else:
        # Fallback: exact match on (court, docket_number, decision_date)
        dup_sql = """
            SELECT court, docket_number, decision_date, COUNT(*) as cnt
            FROM decisions
            WHERE docket_number IS NOT NULL AND LENGTH(TRIM(docket_number)) > 0
            GROUP BY court, docket_number, decision_date
            HAVING cnt > 1
        """
        groups = conn.execute(dup_sql).fetchall()
        if not groups:
            return 0

        deleted = 0
        for court, docket, date, cnt in groups:
            rows = conn.execute(
                """
                SELECT decision_id, COALESCE(regeste, ''), LENGTH(COALESCE(full_text, ''))
                FROM decisions
                WHERE court = ? AND docket_number = ? AND decision_date IS ?
                ORDER BY
                    LENGTH(COALESCE(full_text, '')) + LENGTH(COALESCE(regeste, '')) DESC
                """,
                (court, docket, date),
            ).fetchall()
            survivor_id, survivor_regeste, _ = rows[0]
            if not survivor_regeste:
                for _, loser_regeste, _ in rows[1:]:
                    if loser_regeste:
                        conn.execute(
                            "UPDATE decisions SET regeste = ? WHERE decision_id = ?",
                            (loser_regeste, survivor_id),
                        )
                        break
            for row in rows[1:]:
                conn.execute("DELETE FROM decisions WHERE decision_id = ?", (row[0],))
                deleted += 1

    # ── Pass 2: date-agnostic dedup ──
    # Same court+docket but different dates (common with entscheidsuche vs
    # direct scrape where publication vs decision date differs).
    # Group by the court|docket portion of canonical_key, ignoring the date.
    all_rows = conn.execute(
        "SELECT decision_id, canonical_key, LENGTH(COALESCE(full_text, '')), "
        "LENGTH(COALESCE(regeste, '')) FROM decisions "
        "WHERE canonical_key IS NOT NULL AND canonical_key <> ''"
    ).fetchall()
    groups2 = defaultdict(list)
    for did, ckey, tlen, rlen in all_rows:
        parts = ckey.split("|")
        if len(parts) == 3 and parts[1]:
            groups2[f"{parts[0]}|{parts[1]}"].append((did, tlen, rlen))

    deleted2 = 0
    for entries in groups2.values():
        if len(entries) < 2:
            continue
        # Keep version with the most total content (full_text + regeste)
        entries.sort(key=lambda x: -(x[1] + x[2]))
        for did, _, _ in entries[1:]:
            conn.execute("DELETE FROM decisions WHERE decision_id = ?", (did,))
            deleted2 += 1
    if deleted2:
        logger.info(f"  Pass 2 (date-agnostic): removed {deleted2} duplicates")
    deleted += deleted2

    conn.commit()
    return deleted


# Courts whose decisions may overlap across different court codes.
# Entscheidsuche often maps to a generic code (e.g. zh_gerichte) while
# direct scrapers use specific codes (zh_obergericht).  Grouping allows
# cross-court dedup within each set.
_COURT_OVERLAP_GROUPS: list[set[str]] = [
    # ZH: entscheidsuche → zh_gerichte, direct scraper → sub-courts
    {"zh_gerichte", "zh_obergericht", "zh_kassationsgericht", "zh_handelsgericht",
     "zh_bezirksgericht_zuerich", "zh_bezirksgericht_winterthur",
     "zh_bezirksgericht_horgen", "zh_bezirksgericht_dietikon",
     "zh_bezirksgericht_buelach", "zh_bezirksgericht_dielsdorf",
     "zh_bezirksgericht_uster", "zh_bezirksgericht_pfaeffikon",
     "zh_bezirksgericht_hinwil", "zh_bezirksgericht_meilen",
     "zh_bezirksgericht_affoltern", "zh_mietgericht", "zh_arbeitsgericht"},
    # SG: entscheidsuche → sg_gerichte, direct scraper (sg_publikationen) → sub-courts
    {"sg_gerichte", "sg_publikationen", "sg_versicherungsgericht",
     "sg_verwaltungsgericht", "sg_verwaltungsrekurskommission",
     "sg_kantonsgericht", "sg_handelsgericht"},
    # AG: entscheidsuche → ag_gerichte, direct scraper → sub-courts
    {"ag_gerichte", "ag_obergericht", "ag_versicherungsgericht",
     "ag_handelsgericht", "ag_spezialverwaltungsgericht",
     "ag_strafgericht", "ag_zivilgericht", "ag_verwaltungsgericht",
     "ag_anwaltskommission", "ag_aufsichtskommission", "ag_regierungsrat",
     "ag_departement_bks", "ag_departement_bvu", "ag_departement_gs",
     "ag_departement_vi", "ag_justizgericht", "ag_justizleitung", "ag_weitere"},
    # SH: entscheidsuche generic vs specific
    {"sh_gerichte", "sh_obergericht"},
    # TG: entscheidsuche → tg_obergericht, direct scraper → tg_gerichte
    {"tg_gerichte", "tg_obergericht"},
    # VD: historical findinfo/omni vs current scraper
    {"vd_findinfo", "vd_gerichte", "vd_omni"},
    # BS: entscheidsuche → bs_gerichte, direct scraper → sub-courts
    {"bs_gerichte", "bs_appellationsgericht", "bs_sozialversicherungsgericht"},
    # BE: steuerrekurs overlaps with verwaltungsgericht
    {"be_steuerrekurs", "be_verwaltungsgericht"},
]

# Build a lookup: court_code → frozenset of group members
_COURT_TO_GROUP: dict[str, frozenset[str]] = {}
for _group in _COURT_OVERLAP_GROUPS:
    _frozen = frozenset(_group)
    for _code in _group:
        _COURT_TO_GROUP[_code] = _frozen


def _cross_court_dedup(conn: sqlite3.Connection) -> int:
    """Remove duplicates where the same docket exists under overlapping court codes.

    Only matches within explicit court overlap groups (not all courts in a canton).
    Keeps the version with the longest full_text.
    """
    # Load decisions from courts that belong to overlap groups
    overlap_courts = set()
    for group in _COURT_OVERLAP_GROUPS:
        overlap_courts.update(group)

    if not overlap_courts:
        return 0

    placeholders = ",".join("?" * len(overlap_courts))
    rows = conn.execute(
        f"SELECT decision_id, court, docket_number, decision_date, "
        f"LENGTH(COALESCE(full_text, '')), LENGTH(COALESCE(regeste, '')) "
        f"FROM decisions "
        f"WHERE court IN ({placeholders}) "
        f"AND docket_number IS NOT NULL AND LENGTH(TRIM(docket_number)) > 0",
        list(overlap_courts),
    ).fetchall()

    # Group by (overlap_group_id, normalized_docket)
    groups: dict[str, list[tuple[str, int, int]]] = defaultdict(list)
    for did, court, docket, date, tlen, rlen in rows:
        group = _COURT_TO_GROUP.get(court)
        if not group:
            continue
        docket_norm = re.sub(r"[^A-Z0-9]", "", (docket or "").upper())
        if "tg_gerichte" in group:
            docket_norm = re.sub(r"NR(?=\d)", "", docket_norm)  # TG "Nr." noise
        # Include date to avoid false matches across years
        date_compact = (date or "").replace("-", "")[:8]
        key = f"{id(group)}|{docket_norm}|{date_compact}"
        groups[key].append((did, tlen, rlen))

    deleted = 0
    for entries in groups.values():
        if len(entries) < 2:
            continue
        # Keep version with the most total content (full_text + regeste)
        entries.sort(key=lambda x: -(x[1] + x[2]))
        for did, _, _ in entries[1:]:
            conn.execute("DELETE FROM decisions WHERE decision_id = ?", (did,))
            deleted += 1

    if deleted:
        conn.commit()
    return deleted


def _normalize_dockets(conn: sqlite3.Connection) -> int:
    """König audit 2026-04-30 + QC follow-up 2026-05-01: normalise whitespace
    in docket_number.

    Two passes (idempotent, run on every nightly):

    1. Trim leading/trailing whitespace. Found ~21,000 rows in 2026-04-30,
       concentrated in zh_verwaltungsgericht (11,359), ch_vb (6,721),
       vd_findinfo (2,705), edoeb (45), sh_obergericht (10), and several
       AG chambers. Whitespace breaks exact-match queries
       (' AEG.2018.00004' won't match 'AEG.2018.00004').

    2. Replace internal newlines (LF / CR) with a single space. The QC
       gate surfaced 5,441 rows in 2026-05-01 audit where the scraper
       grabbed two adjacent table cells and joined them with a newline,
       e.g. 'A 2024 015\\nUrteil vom...'. Internal newlines also break
       URL routing (/entscheid/{id}) and exact-match queries.
    """
    cur = conn.execute(
        "UPDATE decisions SET docket_number = trim(docket_number) "
        "WHERE docket_number != trim(docket_number)"
    )
    fixed = cur.rowcount
    # Pass 2: collapse internal newlines (and tabs) to single space, then
    # collapse multiple spaces to one.
    cur2 = conn.execute(
        "UPDATE decisions SET docket_number = "
        "trim(replace(replace(replace(docket_number, char(10), ' '), "
        "                                              char(13), ' '), "
        "                                              char(9),  ' ')) "
        "WHERE docket_number LIKE '%' || char(10) || '%' "
        "   OR docket_number LIKE '%' || char(13) || '%' "
        "   OR docket_number LIKE '%' || char(9)  || '%'"
    )
    fixed += cur2.rowcount
    # Pass 3: collapse runs of spaces (often left by passes 1+2).
    cur3 = conn.execute(
        "UPDATE decisions SET docket_number = "
        "  trim(replace(replace(replace(docket_number, "
        "    '   ', ' '), '  ', ' '), '  ', ' ')) "
        "WHERE docket_number LIKE '%  %'"
    )
    fixed += cur3.rowcount
    if fixed:
        conn.commit()
    return fixed


def _normalize_source_urls(conn: sqlite3.Connection) -> int:
    """QC follow-up 2026-05-01: prefix relative source_urls with their host.

    König P2 (Apr 29) fixed 694 GL/BS rows at scraper-side, but the same
    pattern keeps reappearing because the Tribuna platform underlying
    bs_gerichte and gl_gerichte serves URLs as bare `/cgi-bin/nph-omniscgi.exe?...`
    paths. Auto-correcting at build time means any future scraper miss
    or re-ingest from the entscheidsuche archive self-heals.

    Court → host mapping:
        bs_gerichte → https://www.gerichte.bs.ch
        gl_gerichte → https://findinfo.gl.ch
    """
    HOST_BY_COURT = {
        "bs_gerichte": "https://www.gerichte.bs.ch",
        "gl_gerichte": "https://findinfo.gl.ch",
    }
    fixed_total = 0
    for court, host in HOST_BY_COURT.items():
        cur = conn.execute(
            "UPDATE decisions SET source_url = ? || source_url "
            "WHERE court = ? AND source_url IS NOT NULL AND source_url != '' "
            "AND source_url NOT LIKE 'http%'",
            (host, court),
        )
        fixed_total += cur.rowcount
    if fixed_total:
        conn.commit()
    return fixed_total


# Boundary markers that separate the head-note (Regeste) from the body
# in HUDOC-sourced ECHR judgments. Used by _truncate_oversized_regestes.
_REGESTE_BODY_BOUNDARIES = (
    "\nSachverhalt\n", "\nFaits\n", "\nFatti\n", "\nFakten\n",
    "\nProcédure\n", "\nProcedura\n",
)


def _truncate_oversized_regestes(conn: sqlite3.Connection) -> int:
    """QC follow-up 2026-05-01: HUDOC scraper duplicates entire judgment
    into BOTH `regeste` and `full_text` fields. Result: 462 rows in the
    2026-05-01 audit have regestes >8000 chars (max 875,989 chars), a
    near-mirror of full_text.

    For every row where the regeste is >8000 chars AND substantially
    duplicates the full_text (length within 90%), keep only the head-note
    portion: text up to the first body-boundary marker (Sachverhalt /
    Faits / Fatti / Fakten / Procédure / Procedura). If no boundary is
    found, truncate to 5000 chars (the longest legitimate Bundesgericht
    regeste is ~4500). full_text is left untouched — no info loss.

    Idempotent: WHERE clause filters to rows still oversized.
    """
    rows = conn.execute(
        "SELECT decision_id, regeste, full_text FROM decisions "
        "WHERE regeste IS NOT NULL AND length(regeste) > 8000"
    ).fetchall()
    updates = []
    for did, regeste, full_text in rows:
        if not regeste:
            continue
        full_len = len(full_text or "")
        # Only collapse when regeste is essentially a duplicate of full_text;
        # otherwise the regeste might be legitimately huge for some reason
        # we don't want to silently destroy.
        if full_len < 1000 or len(regeste) < 0.9 * full_len:
            continue
        cut = None
        for marker in _REGESTE_BODY_BOUNDARIES:
            idx = regeste.find(marker)
            if idx > 0 and (cut is None or idx < cut):
                cut = idx
        new_regeste = regeste[:cut] if cut else regeste[:5000]
        new_regeste = new_regeste.rstrip()
        if new_regeste != regeste:
            updates.append((new_regeste, did))
    if updates:
        conn.executemany(
            "UPDATE decisions SET regeste = ? WHERE decision_id = ?",
            updates,
        )
        conn.commit()
    return len(updates)


def _migrate_short_text_to_regeste(conn: sqlite3.Connection) -> int:
    """König P7: short full_text values are often a regeste in the wrong field.

    Pattern: rows where full_text is 10-100 chars (typically Art./§ references
    + a one-line topic, e.g. 'Art. 116a ZPO. Verhältnis Kostenerlass...') AND
    regeste is empty. Move to regeste; null out full_text to correctly mark
    "no decision body extracted".

    Found 248 affected rows in 2026-04-30 audit (sh_gerichte 204 dominated).
    Auto-applies on every nightly so future scraper regressions self-correct.
    """
    cur = conn.execute(
        "UPDATE decisions SET regeste = full_text, full_text = NULL "
        "WHERE LENGTH(COALESCE(full_text, '')) BETWEEN 10 AND 99 "
        "AND (regeste IS NULL OR regeste = '')"
    )
    migrated = cur.rowcount
    if migrated:
        conn.commit()
    return migrated


def _recover_decision_dates(conn: sqlite3.Connection) -> int:
    """König audit P4: recover decision_date from full_text for NULL rows.

    Only applies to courts with VERIFIED clean anchor-phrase patterns:
      - zh_verwaltungsgericht: "Endentscheid vom DD.MM.YYYY" (100% precision)
      - gr_gerichte:           "Urteil/Entscheid vom DD. Monat YYYY"
      - bl_gerichte:           "Entscheid vom DD. Monat YYYY"
      - fr_gerichte:           "Arrêt du DD mois YYYY" (FR anchors)
      - be_verwaltungsgericht: "Urteil des Einzelrichters vom DD. Monat YYYY"

    Skipped (ambiguous date contexts — wrong dates worse than NULL):
      - ti_gerichte:  custody dates / cited cases dominate first match
                       (handled separately by ti_date_recovery_2026_04_30.py
                       which fetches source URL for full document)
      - mkg:          1914-archive cited Bundesratsbeschluss dates verified
                       to false-positive ~60% of the time on spot-check
      - hudoc_ch:     handled separately by HUDOC API recovery script

    2026-04-30 audit: recovered 5,258 of 5,339 NULL rows (98.5%) across the
    safe courts. Auto-applies on every nightly so future scraper regressions
    are self-healed where the full_text contains a valid anchor + date.
    """
    import re
    from datetime import date as _date

    DE_MONTHS = {
        "januar": 1, "februar": 2, "märz": 3, "maerz": 3, "april": 4,
        "mai": 5, "juni": 6, "juli": 7, "august": 8, "september": 9,
        "oktober": 10, "november": 11, "dezember": 12,
    }
    FR_MONTHS = {
        "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
        "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
        "septembre": 9, "octobre": 10, "novembre": 11,
        "décembre": 12, "decembre": 12,
    }
    IT_MONTHS = {
        "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4,
        "maggio": 5, "giugno": 6, "luglio": 7, "agosto": 8,
        "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
    }
    ALL_MONTHS = {**DE_MONTHS, **FR_MONTHS, **IT_MONTHS}

    DE_RE = re.compile(
        r"(\d{1,2})\.\s*(Januar|Februar|M[äa]rz|April|Mai|Juni|Juli|August|"
        r"September|Oktober|November|Dezember)\s+(\d{4})",
        re.IGNORECASE,
    )
    FR_RE = re.compile(
        r"(\d{1,2})\s+(janvier|f[ée]vrier|mars|avril|mai|juin|juillet|"
        r"ao[ûu]t|septembre|octobre|novembre|d[ée]cembre)\s+(\d{4})",
        re.IGNORECASE,
    )
    IT_RE = re.compile(
        r"(\d{1,2})\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|"
        r"luglio|agosto|settembre|ottobre|novembre|dicembre)\s+(\d{4})",
        re.IGNORECASE,
    )
    DDMMYYYY = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b")
    ANCHORS = (
        # German
        "Urteil vom", "Urteil des", "Urteil der",
        "Entscheid vom", "Entscheid des", "Endentscheid vom",
        "Verfügung vom", "Verfügung des", "Beschluss vom", "Beschluss des",
        # French
        "Arrêt du", "Décision du", "Jugement du", "Ordonnance du",
        # Italian
        "Sentenza del", "Decisione del", "Decreto del",
    )
    THIS_YEAR = _date.today().year

    def _try_parse(d, m, y):
        if isinstance(m, str):
            m = ALL_MONTHS.get(m.lower())
            if not m:
                return None
        if not (1 <= m <= 12 and 1 <= d <= 31):
            return None
        if not (1700 <= y <= THIS_YEAR + 1):
            return None
        try:
            return _date(y, m, d)
        except ValueError:
            return None

    def _extract_first(text):
        cands = []
        for m in DE_RE.finditer(text):
            d = _try_parse(int(m.group(1)), m.group(2), int(m.group(3)))
            if d:
                cands.append((m.start(), d))
        for m in FR_RE.finditer(text):
            d = _try_parse(int(m.group(1)), m.group(2), int(m.group(3)))
            if d:
                cands.append((m.start(), d))
        for m in IT_RE.finditer(text):
            d = _try_parse(int(m.group(1)), m.group(2), int(m.group(3)))
            if d:
                cands.append((m.start(), d))
        for m in DDMMYYYY.finditer(text):
            d = _try_parse(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            if d:
                cands.append((m.start(), d))
        if not cands:
            return None
        cands.sort(key=lambda x: x[0])
        return cands[0][1]

    def _recover(full_text):
        if not full_text:
            return None
        head = full_text[:8000]
        for anchor in ANCHORS:
            idx = 0
            while True:
                i = head.lower().find(anchor.lower(), idx)
                if i < 0:
                    break
                window = head[i:i + 200]
                d = _extract_first(window)
                if d:
                    return d
                idx = i + len(anchor)
        return _extract_first(full_text[:5000])

    SAFE_COURTS = (
        "zh_verwaltungsgericht", "gr_gerichte", "bl_gerichte",
        "fr_gerichte", "be_verwaltungsgericht",
    )
    placeholders = ",".join("?" * len(SAFE_COURTS))
    rows = conn.execute(
        f"SELECT decision_id, full_text FROM decisions "
        f"WHERE court IN ({placeholders}) "
        f"AND (decision_date IS NULL OR decision_date='') "
        f"AND full_text IS NOT NULL AND LENGTH(full_text) > 50",
        SAFE_COURTS,
    ).fetchall()

    updates = []
    for did, ft in rows:
        d = _recover(ft)
        if d:
            updates.append((d.isoformat(), did))

    if updates:
        conn.executemany(
            "UPDATE decisions SET decision_date=? WHERE decision_id=?",
            updates,
        )
        conn.commit()
    return len(updates)


def _normalize_dates(conn: sqlite3.Connection) -> tuple[int, int]:
    """König audit 2026-04-30: sanitise invalid decision_date values.

    - year-0000 markers ("0000-..." pattern, mostly from gr_gerichte's scraper
      default when the date can't be extracted) → NULL. 796 rows in 2026-04-30.
    - obvious future typos (> today + 365d, e.g. zg_obergericht's "2026-11-01"
      Wahlausschreibung mis-dated) → NULL.

    Soft tolerance for near-future dates (< 365d) preserves legitimate pending
    publications and hearing schedules.
    """
    import datetime as _dt
    cur1 = conn.execute(
        "UPDATE decisions SET decision_date = NULL WHERE decision_date LIKE '0000%'"
    )
    n_zero = cur1.rowcount

    cutoff = (_dt.date.today() + _dt.timedelta(days=365)).isoformat()
    cur2 = conn.execute(
        "UPDATE decisions SET decision_date = NULL WHERE decision_date > ?",
        (cutoff,),
    )
    n_future = cur2.rowcount

    if n_zero or n_future:
        conn.commit()
    return n_zero, n_future


def _dedup_egmr_in_bge(conn: sqlite3.Connection) -> int:
    """König audit #1: drop ECHR/CEDH cases that the BGE scraper picked up.

    Some BGE decisions cross-reference Strasbourg judgments and the BGE scraper
    follows the link, ingesting the EGMR case under court='bge' with a
    cedh.coe.int URL. The dedicated bge_egmr scraper also ingests the same case
    under court='bge_egmr'. This produces 474 duplicate pairs (pre-fix).

    Yesterday's audit applied a one-off DELETE that didn't survive the next
    rebuild because the source JSONL still contains the misclassified rows.
    This pass codifies the cleanup so the duplicates don't recur every nightly.

    Conservative: only deletes a bge row when the matching bge_egmr row exists
    (matched on identical source_url). Archive-unique entries are preserved.
    """
    cur = conn.execute(
        """DELETE FROM decisions
           WHERE court = 'bge'
             AND source_url LIKE '%cedh%'
             AND EXISTS (
               SELECT 1 FROM decisions d2
               WHERE d2.court = 'bge_egmr'
                 AND d2.source_url = decisions.source_url
             )""",
    )
    deleted = cur.rowcount
    if deleted:
        conn.commit()
    return deleted


def _remove_stubs(conn: sqlite3.Connection) -> int:
    """Remove decisions that are completely empty (no text AND no regeste).

    Only removes entries where both full_text and regeste are empty or
    near-empty.  Even short entries carry docket numbers, dates, court
    assignments and topic keywords that enable search and coverage.
    Decisions with failed PDF extraction but a regeste, or entscheidsuche
    metadata entries with topic keywords, are all kept.
    """
    result = conn.execute(
        """DELETE FROM decisions
           WHERE LENGTH(COALESCE(full_text, '')) < 10
             AND LENGTH(COALESCE(regeste, '')) < 10""",
    )
    deleted = result.rowcount
    if deleted:
        conn.commit()
    return deleted


def _fill_missing_regeste(conn: sqlite3.Connection) -> int:
    """Extract regeste from full_text for BGer/BGE decisions with empty regeste."""
    cursor = conn.execute(
        """
        SELECT decision_id, full_text FROM decisions
        WHERE court IN ('bger', 'bge')
          AND (regeste IS NULL OR LENGTH(TRIM(regeste)) = 0)
          AND LENGTH(COALESCE(full_text, '')) > 200
        """
    )
    updated = 0
    batch: list[tuple[str, str]] = []
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        for decision_id, full_text in rows:
            regeste = _extract_regeste_from_text(full_text or "")
            if regeste:
                batch.append((regeste, decision_id))
        if batch:
            conn.executemany(
                "UPDATE decisions SET regeste = ? WHERE decision_id = ?",
                batch,
            )
            updated += len(batch)
            batch.clear()

    if updated:
        conn.commit()
    return updated


def _log_quality_summary(conn: sqlite3.Connection) -> None:
    """Log a summary of remaining data quality issues.

    Note: the previous version included a `LENGTH(full_text) < 500` count which
    forced a full table scan reading every full_text blob (~31 min on 970k
    rows / 60 GB DB measured 2026-04-30). That metric was a one-line log only
    — not load-bearing — so we drop it. The remaining two checks scan small
    columns (regeste, decision_date) without overflow pages and are cheap.
    """
    total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    no_regeste = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE regeste IS NULL OR regeste = ''"
    ).fetchone()[0]
    no_date = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date IS NULL OR decision_date = ''"
    ).fetchone()[0]
    logger.info(f"  Quality: {no_regeste} no regeste, {no_date} no date (of {total})")


# Court code remapping: merge historical variants into canonical codes
COURT_REMAP = {
    "bge_historical": "bge",
}
# Decision ID prefix remapping (must match COURT_REMAP)
ID_PREFIX_REMAP = {
    "bge_historical_": "bge_",
}


def insert_decision(conn: sqlite3.Connection, row: dict) -> bool:
    """Insert a single decision. Returns True if inserted, False if skipped (duplicate)."""
    try:
        # Remap court codes and decision IDs (e.g. bge_historical → bge)
        court = row.get("court", "")
        if court in COURT_REMAP:
            row["court"] = COURT_REMAP[court]
        did = row.get("decision_id", "")
        for old_prefix, new_prefix in ID_PREFIX_REMAP.items():
            if did.startswith(old_prefix):
                row["decision_id"] = new_prefix + did[len(old_prefix):]
                break

        # Clean text fields
        for field in ("full_text", "regeste", "title"):
            if field in row and row[field]:
                row[field] = _clean_text(row[field])

        # Handle cited_decisions — could be list or JSON string
        cited = row.get("cited_decisions", [])
        if isinstance(cited, list):
            cited = json.dumps(cited)
        row["cited_decisions"] = cited

        # json_data: full row as JSON blob (after cleaning)
        row["json_data"] = json.dumps(row, default=str)

        # Canonical key for dedup (aggressive normalization of court+docket+date)
        row["canonical_key"] = make_canonical_key(
            row.get("court", ""), row.get("docket_number", ""), row.get("decision_date"),
        )

        # Build values tuple matching INSERT_COLUMNS order.
        # Convert None-like values properly (avoid storing literal "None" strings).
        def _val(col: str):
            v = row.get(col)
            if v is None or v == "None":
                return None
            if col in ("decision_date", "publication_date", "scraped_at") and v:
                return str(v) if v else None
            return v

        values = tuple(_val(col) for col in INSERT_COLUMNS)

        cursor = conn.execute(INSERT_OR_IGNORE_SQL, values)
        return cursor.rowcount > 0
    except Exception as e:
        logger.warning(f"Failed to import {row.get('decision_id', '?')}: {e}")
        return False


def import_jsonl(
    conn: sqlite3.Connection, jsonl_dir: Path, checkpoint: dict | None = None,
) -> tuple[int, int, dict]:
    """Import decisions from JSONL files.

    Args:
        conn: SQLite connection.
        jsonl_dir: Directory containing .jsonl files.
        checkpoint: If provided, a dict mapping filename -> {"size": int, "imported": int}.
            Files whose size matches the checkpoint are skipped entirely; files that grew
            are read starting from the checkpoint byte offset (JSONL files are append-only).

    Returns:
        (imported, skipped, new_checkpoint) where new_checkpoint has the same structure.
    """
    imported = 0
    skipped = 0
    new_checkpoint: dict = {}

    # Process direct shards (filename does not start with "es_") BEFORE
    # entscheidsuche shards.  This prevents the SG-bug-class
    # alphabetical-collision pattern (commit f249f1f, 2026-04-30):
    # es_<court>.jsonl uses generic court_code in SPIDER_MAP, while
    # direct <court>.jsonl writes chamber-specific court codes.  When
    # both shards happen to use the SAME decision_id prefix and es
    # processes first (alphabetical order, 'e' < most letters),
    # INSERT OR IGNORE silently drops direct's chamber-specific rows.
    # Direct-first ordering means direct's chamber labels win all
    # dedups; es supplements with rows that have no direct counterpart.
    # Trade-off: for shared decisions, direct's metadata wins over es.
    # Set BUILD_FTS5_DIRECT_FIRST=0 to revert to plain-alphabetical
    # order (use only as an emergency revert).
    _direct_first = os.environ.get("BUILD_FTS5_DIRECT_FIRST", "1") not in ("0", "false", "no")
    _all_files = sorted(jsonl_dir.glob("*.jsonl"))
    if _direct_first:
        _direct_shards = [f for f in _all_files if not f.name.startswith("es_")]
        _es_shards = [f for f in _all_files if f.name.startswith("es_")]
        _files_to_process = _direct_shards + _es_shards
    else:
        _files_to_process = _all_files

    for jsonl_file in _files_to_process:
        fname = jsonl_file.name
        current_size = jsonl_file.stat().st_size

        if checkpoint is not None:
            prev = checkpoint.get(fname, {})
            prev_size = prev.get("size", 0)

            if current_size == prev_size:
                # No new data — carry forward checkpoint entry unchanged
                new_checkpoint[fname] = prev
                continue

            if current_size < prev_size:
                # File shrank (unexpected) — read from start to be safe
                logger.warning(
                    f"  {fname}: size shrank ({prev_size} → {current_size}), reading from start"
                )
                prev_size = 0
        else:
            prev_size = 0

        file_imported = 0
        with open(jsonl_file, "rb") as fb:
            if prev_size > 0:
                fb.seek(prev_size)
                # Discard partial line at seek position (seek may land mid-
                # UTF-8 character or mid-JSON line if file was rewritten)
                fb.readline()
                logger.debug(f"  {fname}: seeking to byte {prev_size} (skipped partial line)")
            # Wrap in text mode for remaining lines
            f = io.TextIOWrapper(fb, encoding="utf-8", errors="replace")
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    if insert_decision(conn, row):
                        imported += 1
                        file_imported += 1
                    else:
                        skipped += 1
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON in {jsonl_file}: {e}")
                except Exception as e:
                    logger.warning(f"Error importing from {jsonl_file}: {e}")

        if file_imported:
            conn.commit()
            logger.info(f"  {jsonl_file.name}: +{file_imported} decisions")
        elif checkpoint is not None and prev_size > 0:
            logger.debug(f"  {fname}: no new decisions (new bytes were dupes)")

        prev_imported = (checkpoint or {}).get(fname, {}).get("imported", 0)
        new_checkpoint[fname] = {
            "size": current_size,
            "imported": prev_imported + file_imported,
        }

    return imported, skipped, new_checkpoint


def import_parquet(conn: sqlite3.Connection, parquet_dir: Path) -> tuple[int, int]:
    """Import decisions from Parquet shards. Returns (imported, skipped)."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        logger.info("pyarrow not installed, skipping Parquet import")
        return 0, 0

    if not parquet_dir.exists():
        return 0, 0

    imported = 0
    skipped = 0

    for pf in sorted(parquet_dir.glob("*.parquet")):
        try:
            table = pq.read_table(pf)
            file_imported = 0
            for batch in table.to_batches():
                for row in batch.to_pylist():
                    if insert_decision(conn, row):
                        imported += 1
                        file_imported += 1
                    else:
                        skipped += 1
            conn.commit()
            if file_imported:
                logger.info(f"  {pf.name}: +{file_imported} decisions")
        except Exception as e:
            logger.warning(f"Failed to read {pf}: {e}")

    return imported, skipped


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Add missing columns to an existing decisions table.

    Safe to call on every startup — ALTER TABLE ADD COLUMN is a no-op
    if the column already exists (caught by the try/except).
    """
    migrations = [
        ("canonical_key", "TEXT"),
    ]
    for col_name, col_type in migrations:
        try:
            conn.execute(f"ALTER TABLE decisions ADD COLUMN {col_name} {col_type}")
            logger.info(f"Schema migration: added column '{col_name}'")
        except sqlite3.OperationalError:
            pass  # column already exists


def build_database(
    output_dir: Path,
    db_path: Path | None = None,
    incremental: bool = False,
    no_optimize: bool = False,
    full_rebuild: bool = False,
) -> Path:
    """
    Build/update the FTS5 database from all available sources.

    Args:
        output_dir: Directory containing decisions/ and data/ subdirs.
        db_path: Path for the SQLite DB (default: output_dir/decisions.db).
        incremental: Only read new bytes from JSONL files using checkpoint.
        no_optimize: Skip the FTS5 optimize step.
        full_rebuild: Delete existing DB and checkpoint, rebuild from scratch.

    Returns the path to the database.
    """
    db_path = db_path or output_dir / "decisions.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path = output_dir / ".fts5_checkpoint.json"

    # Full rebuild: build to a temp file, swap at the end (zero downtime)
    # Resolve symlinks so temp file is on the same filesystem (atomic rename)
    final_db_path = None
    if full_rebuild:
        final_db_path = db_path.resolve()
        db_path = final_db_path.with_suffix(".db.tmp")
        if db_path.exists():
            logger.info(f"Full rebuild: removing stale temp DB {db_path}")
            db_path.unlink()
        # Also remove any leftover WAL/SHM for the temp DB
        for suffix in (".db.tmp-wal", ".db.tmp-shm"):
            p = db_path.parent / (db_path.stem.replace(".db", "") + suffix)
            if p.exists():
                p.unlink()
        if checkpoint_path.exists():
            logger.info(f"Full rebuild: deleting {checkpoint_path}")
            checkpoint_path.unlink()

    # Load checkpoint for incremental mode
    checkpoint = None
    if incremental and checkpoint_path.exists():
        try:
            checkpoint = json.loads(checkpoint_path.read_text()).get("files", {})
            logger.info(f"Loaded checkpoint: {len(checkpoint)} files tracked")
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load checkpoint, reading all files: {e}")
            checkpoint = None

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA_SQL)
    conn.executescript(COVERAGE_SCHEMA_SQL)

    # Migrate: add columns that may be missing in older databases
    _migrate_schema(conn)

    # Count existing
    existing = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    jsonl_dir = output_dir / "decisions"
    if incremental and checkpoint is None:
        logger.info(
            "No checkpoint file found — first incremental run will read all JSONL files. "
            "Subsequent runs will be fast. To skip this, run --full-rebuild first."
        )

    # Import from JSONL (run_scraper.py output)
    jsonl_imported, jsonl_skipped, new_checkpoint = 0, 0, {}
    if jsonl_dir.exists():
        logger.info(f"Importing from JSONL: {jsonl_dir}")
        jsonl_imported, jsonl_skipped, new_checkpoint = import_jsonl(
            conn, jsonl_dir, checkpoint if incremental else None,
        )

    # Import from Parquet (pipeline.py output)
    parquet_dir = output_dir / "data" / "daily"
    pq_imported, pq_skipped = 0, 0
    if parquet_dir.exists():
        logger.info(f"Importing from Parquet: {parquet_dir}")
        pq_imported, pq_skipped = import_parquet(conn, parquet_dir)

    total_imported = jsonl_imported + pq_imported
    total_skipped = jsonl_skipped + pq_skipped

    # ── Post-import data quality passes ──
    if total_imported > 0:
        logger.info("Deduplicating decisions...")
        deduped = _dedup_decisions(conn)
        if deduped:
            logger.info(f"  Removed {deduped} duplicate decisions")

        logger.info("Cross-court deduplication (overlapping court codes)...")
        cross_deduped = _cross_court_dedup(conn)
        if cross_deduped:
            logger.info(f"  Removed {cross_deduped} cross-court duplicates")

        logger.info("EGMR dedup (König #1: bge with cedh URL covered by bge_egmr)...")
        egmr_deduped = _dedup_egmr_in_bge(conn)
        if egmr_deduped:
            logger.info(f"  Removed {egmr_deduped} bge+cedh duplicates (canonical entries remain in bge_egmr)")

        logger.info("Normalising docket whitespace + invalid dates (König audit 2026-04-30)...")
        ws_fixed = _normalize_dockets(conn)
        if ws_fixed:
            logger.info(f"  Trimmed whitespace from {ws_fixed} docket_numbers")
        n_zero, n_future = _normalize_dates(conn)
        if n_zero or n_future:
            logger.info(f"  Cleared {n_zero} year-0000 dates + {n_future} far-future (>today+365d) dates → NULL")
        recovered = _recover_decision_dates(conn)
        if recovered:
            logger.info(f"  Recovered {recovered} decision_date values from full_text (zh_verwaltungsgericht/gr_gerichte/bl_gerichte)")
        text_migrated = _migrate_short_text_to_regeste(conn)
        if text_migrated:
            logger.info(f"  Migrated {text_migrated} short full_text values → regeste (correct field for Art./§ references)")

        logger.info("Normalising relative source_urls (König P2 follow-up)...")
        urls_fixed = _normalize_source_urls(conn)
        if urls_fixed:
            logger.info(f"  Prefixed host on {urls_fixed} relative source_urls (bs_gerichte / gl_gerichte Tribuna paths)")

        logger.info("Truncating oversized regestes (HUDOC scraper full_text leakage)...")
        regestes_truncated = _truncate_oversized_regestes(conn)
        if regestes_truncated:
            logger.info(f"  Truncated {regestes_truncated} oversized regestes to head-note portion (full_text untouched)")

        logger.info("Removing stub decisions (text <10 AND regeste <10 chars)...")
        stubs_removed = _remove_stubs(conn)
        if stubs_removed:
            logger.info(f"  Removed {stubs_removed} stub decisions")

        logger.info("Filling missing regeste for BGer/BGE decisions...")
        filled = _fill_missing_regeste(conn)
        if filled:
            logger.info(f"  Extracted regeste for {filled} decisions")

        _log_quality_summary(conn)

    if not no_optimize and total_imported > 0:
        logger.info("Running FTS5 optimize...")
        conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('optimize')")
        conn.commit()
    elif no_optimize and total_imported > 0:
        logger.info("Skipping FTS5 optimize (--no-optimize)")
        conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

    # Court breakdown
    courts = conn.execute(
        "SELECT court, COUNT(*) as n FROM decisions GROUP BY court ORDER BY n DESC"
    ).fetchall()

    # Switch from WAL to DELETE mode before closing (immutable=1 compat)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.close()

    # Full rebuild: atomically swap temp DB into place
    if final_db_path is not None:
        import os
        logger.info(f"Swapping {db_path} → {final_db_path}")
        os.replace(str(db_path), str(final_db_path))
        # Clean up any leftover WAL/SHM from the temp DB
        for ext in ("-wal", "-shm"):
            tmp_wal = Path(str(db_path) + ext)
            if tmp_wal.exists():
                tmp_wal.unlink()
        db_path = final_db_path

    # Save checkpoint
    if incremental or full_rebuild:
        now = datetime.now(timezone.utc).isoformat()
        # Load existing checkpoint metadata to preserve last_full_build
        prev_meta = {}
        if checkpoint_path.exists() and not full_rebuild:
            try:
                prev_meta = json.loads(checkpoint_path.read_text())
            except (json.JSONDecodeError, OSError):
                pass

        meta = {
            "files": new_checkpoint,
            "last_full_build": now if full_rebuild else prev_meta.get("last_full_build"),
            "last_incremental": now if incremental else prev_meta.get("last_incremental"),
        }
        checkpoint_path.write_text(json.dumps(meta, indent=2))
        logger.info(f"Saved checkpoint: {len(new_checkpoint)} files tracked")

    logger.info(f"Database: {db_path} ({db_path.stat().st_size / 1024 / 1024:.1f} MB)")
    logger.info(f"  Existing: {existing}, New: {total_imported}, Skipped: {total_skipped}")
    logger.info(f"  Total decisions: {total}")
    for court, n in courts:
        logger.info(f"    {court}: {n}")

    return db_path


def main():
    parser = argparse.ArgumentParser(description="Build FTS5 search database")
    parser.add_argument(
        "--output", type=str, default="output",
        help="Output directory containing decisions/ and data/ subdirs"
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Database path (default: {output}/decisions.db)"
    )
    parser.add_argument(
        "--watch", type=int, default=None,
        help="Rebuild every N seconds (for use alongside running scrapers)"
    )
    parser.add_argument(
        "--incremental", action="store_true",
        help="Only read new bytes from JSONL files (skip already-processed content)"
    )
    parser.add_argument(
        "--no-optimize", action="store_true",
        help="Skip FTS5 optimize step (useful with --incremental)"
    )
    parser.add_argument(
        "--full-rebuild", action="store_true",
        help="Delete existing DB and checkpoint, rebuild from scratch"
    )
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    output_dir = Path(args.output)
    db_path = Path(args.db) if args.db else None

    if args.watch:
        logger.info(f"Watch mode: rebuilding every {args.watch}s")
        while True:
            try:
                build_database(
                    output_dir, db_path,
                    incremental=args.incremental,
                    no_optimize=args.no_optimize,
                    full_rebuild=args.full_rebuild,
                )
            except Exception as e:
                logger.error(f"Build failed: {e}", exc_info=True)
            time.sleep(args.watch)
    else:
        build_database(
            output_dir, db_path,
            incremental=args.incremental,
            no_optimize=args.no_optimize,
            full_rebuild=args.full_rebuild,
        )


if __name__ == "__main__":
    main()
