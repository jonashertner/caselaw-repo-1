#!/usr/bin/env python3
"""Extract Sachverhalt / Erwägungen / Dispositiv from Swiss court decisions.

Federal-first v1: tuned for BGer/BVGer/BStGer/BGE-style structure, with
MKG (Militärkassationsgericht) support added 2026-04-20. Cantonal courts
vary widely — adding court-specific patterns is a follow-up project.

The extracted structure is persisted as a sidecar SQLite (decision_structure.db)
keyed by decision_id, queryable in O(1) and joined to the main FTS5 DB at
query time. Schema is intentionally additive — no changes to the main
Decision model or to bl_gerichte.jsonl-style ingest shards.

This addresses the "Reasoning Error" failure mode (Westlaw 61% per Magesh
et al. 2025) by giving downstream LLMs the operative ruling (Dispositiv)
as a separately queryable field, eliminating holding/dicta confusion.

Usage:
    # Extract for one decision (CLI test)
    python3 extract_decision_structure.py --decision-id bger_5A_42_2026

    # Build full sidecar DB from all federal court JSONL shards
    python3 extract_decision_structure.py --build \\
        --shards bger,bvger,bstger,bge,bpatger,bge_egmr,bge_historical \\
        --output output/decision_structure.db
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

logger = logging.getLogger("extract_decision_structure")


# ---------------------------------------------------------------------------
# Marker patterns — federal-court tuned (BGer/BVGer/BStGer/BGE)
# ---------------------------------------------------------------------------
DISPOSITIV_PATTERNS = {
    "de": [
        (r"Demnach\s+erkennt\s+das\s+Bundesgericht\s*:?", "ranked_de_BGer"),
        (r"Demnach\s+erkennt\s+das\s+Bundesverwaltungsgericht\s*:?", "ranked_de_BVGer"),
        (r"Demnach\s+erkennt\s+das\s+Bundesstrafgericht\s*:?", "ranked_de_BStGer"),
        (r"Demnach\s+erkennt\s+(?:das|die)\s+(?:Bundesgericht|Bundesverwaltungsgericht|Bundesstrafgericht|Bundespatentgericht|Beschwerdekammer|Strafkammer|Anklagekammer|Berufungskammer|I+\.\s+Kammer|Abteilung)[^:\n]*:?", "ranked_de_court"),
        # MKG (Militärkassationsgericht) — separate pattern because its
        # opener is "hat erkannt" (not "erkennt"), with a different verb
        # construction than BGer.
        (r"Das\s+Militärkassationsgericht\s+hat\s+erkannt\s*:?", "ranked_de_MKG"),
        (r"Demnach\s+erkennt\s+(?:der|die)\s+(?:Präsident|Präsidentin|Instruktionsrichter|Einzelrichter|Vizepräsident)[^:\n]*:?", "ranked_de_judge"),
        (r"Demnach\s+(?:verfügt|beschliesst|verfügen|beschliessen)\s+(?:das|der|die)\s+[^:\n]*:?", "ranked_de_verfuegt"),
        (r"Demnach\s+wird\s+(?:erkannt|verfügt|beschlossen)\s*:?", "ranked_de_passive"),
        (r"Aus\s+diesen\s+(?:Gründen|Erwägungen)\s+(?:erkennt|beschliesst|verfügt|ergibt|ist|kann)\b", "fallback_de_aus_gruenden"),
        (r"Demgemäss\s+(?:erkennt|beschliesst|verfügt)\b", "fallback_de_demgemaess"),
    ],
    "fr": [
        (r"Par\s+ces\s+motifs,?\s+le\s+Tribunal\s+(?:fédéral|militaire\s+de\s+cassation|administratif\s+fédéral|pénal\s+fédéral)\s+(?:prononce|ordonne|arrête)\s*:?", "ranked_fr_TF"),
        # MKG FR bare opener — often the only Dispositiv marker in MKG FR.
        (r"Le\s+Tribunal\s+militaire\s+de\s+cassation\s+prononce\s*:?", "ranked_fr_MKG"),
        (r"Par\s+ces\s+motifs,?\s+(?:la\s+Cour|le\s+Président|la\s+Présidente|le\s+Juge\s+instructeur|le\s+Vice-président)[^:\n]*(?:prononce|ordonne|arrête)\s*:?", "ranked_fr_judge"),
        (r"Par\s+ces\s+motifs,?\s+(?:la\s+Cour|le\s+Tribunal|le\s+Président|la\s+Présidente)\b", "fallback_fr_par_ces_motifs"),
        (r"Par\s+ces\s+motifs\s*:?\s*$", "fallback_fr_bare"),
        (r"par\s+ces\s+motifs,?\s+prononce\s*:?", "fallback_fr_lc_prononce"),
    ],
    "it": [
        (r"Per\s+questi\s+motivi,?\s+il\s+Tribunale\s+(?:federale|militare\s+di\s+cassazione|amministrativo\s+federale|penale\s+federale)\s+pronuncia\s*:?", "ranked_it_TF"),
        # MKG IT bare opener.
        (r"Il\s+Tribunale\s+militare\s+di\s+cassazione\s+pronuncia\s*:?", "ranked_it_MKG"),
        (r"Per\s+questi\s+motivi,?\s+(?:il\s+)?(?:Presidente|Giudice\s+istruttore|la\s+Corte(?:\s+dei\s+reclami\s+penali)?)[^:\n]*pronuncia\s*:?", "ranked_it_court"),
        (r"Per\s+questi\s+motivi,?\s+il\s+Tribunale\s+federale\b", "fallback_it_TF_loose"),
        (r"Per\s+questi\s+motivi\s*:?\s*$", "fallback_it_bare"),
    ],
}

ERWAEGUNGEN_PATTERNS = {
    "de": [
        (r"Das\s+Bundesgericht\s+zieht\s+in\s+Erwägung\s*:?", "ranked_de_zieht_BGer"),
        (r"Das\s+Bundesverwaltungsgericht\s+zieht\s+in\s+Erwägung\s*:?", "ranked_de_zieht_BVGer"),
        (r"Das\s+Bundesstrafgericht\s+zieht\s+in\s+Erwägung\s*:?", "ranked_de_zieht_BStGer"),
        (r"(?:Das|Die)\s+(?:Beschwerdekammer|Strafkammer|Berufungskammer|Anklagekammer|Abteilung)\s+zieht\s+in\s+Erwägung\s*:?", "ranked_de_zieht_chamber"),
        # MKG uses "hat erwogen" (seen in older Bände) as a less-common opener.
        (r"Das\s+Militärkassationsgericht\s+hat\s+erwogen\s*:?", "ranked_de_MKG_erwogen"),
        (r"in\s+Erwägung,?\s+dass\b", "ranked_de_in_erwaegung"),
        (r"^\s*Erwägungen\s*:?\s*$", "ranked_de_header"),
        (r"^\s*Erwägung\s*:?\s*$", "ranked_de_singular"),
    ],
    "fr": [
        # BGE-FR canonical opener — covers 99% of BGE-FR decisions
        (r"Extrait\s+des\s+considérants\s*:?", "ranked_fr_BGE_extrait"),
        # Court-specific "considère"
        (r"Le\s+Tribunal\s+(?:fédéral|administratif\s+fédéral|pénal\s+fédéral)\s+considère\s+en\s+(?:droit|fait)\s*:?", "ranked_fr_considere_TF"),
        (r"(?:La\s+Cour|le\s+Tribunal|la\s+Chambre)[^.\n]*considère\s+en\s+(?:droit|fait)\s*:?", "ranked_fr_considere_court"),
        # Looser "considère en (fait et en) droit/fait" — BVGer/BStGer common
        (r"considère\s+en\s+(?:fait\s+et\s+en\s+)?(?:droit|fait)\s*:?", "ranked_fr_considere_loose"),
        # "Considérant en (fait et en) droit/fait" — frequent BGer variant
        (r"Considérant\s+en\s+(?:fait\s+et\s+en\s+)?(?:droit|fait)\s*:?", "ranked_fr_considerant_endroit"),
        # Generic considérant que (older formats)
        (r"\bConsidérant\s+(?:en\s+(?:droit|fait)|que)\b", "ranked_fr_considerant"),
        # Bare headers (covers BGer "Considérant:" alone)
        (r"^\s*Considérant\s*:\s*$", "ranked_fr_bare_header"),
        (r"^\s*Considérants?\s*:?\s*$", "ranked_fr_header"),
        (r"^\s*EN\s+DROIT\s*:?\s*$", "ranked_fr_uppercase"),
    ],
    "it": [
        # BGE-IT
        (r"Estratto\s+dei\s+considerandi\s*:?", "ranked_it_BGE_estratto"),
        # Court-specific "considera"
        (r"Il\s+Tribunale\s+(?:federale|amministrativo\s+federale|penale\s+federale)\s+considera\s+in\s+(?:diritto|fatto)\s*:?", "ranked_it_considera_TF"),
        (r"(?:La\s+Corte(?:\s+dei\s+reclami\s+penali)?|Il\s+Tribunale|Il\s+Giudice)[^.\n]*considera\s+in\s+(?:diritto|fatto)\s*:?", "ranked_it_considera_court"),
        # Looser "considera in (fatto e[d] in) diritto/fatto"
        (r"considera\s+in\s+(?:fatto\s+ed?\s+in\s+)?(?:diritto|fatto)\s*:?", "ranked_it_considera_loose"),
        # Generic "Considerando in diritto/fatto/che"
        (r"\bConsiderando\s+(?:in\s+(?:diritto|fatto)|che)\b", "ranked_it_considerando"),
        # Headers
        (r"^\s*Considerando\s+in\s+diritto\s*:?\s*$", "ranked_it_header"),
        # Standalone "Diritto:" header (BStGer 247x, BVGer 26x)
        (r"^\s*Diritto\s*:\s*$", "ranked_it_diritto_header"),
        (r"^\s*IN\s+DIRITTO\s*:?\s*$", "ranked_it_uppercase"),
    ],
}

SACHVERHALT_PATTERNS = {
    "de": [
        (r"^\s*Sachverhalt\s*:?\s*$", "ranked_de_header"),
        (r"\bSachverhalt\s*:?\s*\n", "ranked_de_inline"),
        # MKG explicit opener (before A. B. C. narrative).
        (r"Das\s+Militärkassationsgericht\s+hat\s+festgestellt\s*:?", "ranked_de_MKG"),
        (r"^A\.\s*-\s*", "fallback_de_alphabetic"),
    ],
    "fr": [
        (r"^\s*Faits\s*:?\s*$", "ranked_fr_header"),
        (r"\bFaits\s*:?\s*\n", "ranked_fr_inline"),
        (r"Le\s+Tribunal\s+militaire\s+de\s+cassation\s+a\s+constat[éè]\s*:?", "ranked_fr_MKG"),
        (r"^A\.\s*-\s*", "fallback_fr_alphabetic"),
    ],
    "it": [
        (r"^\s*Fatti\s*:?\s*$", "ranked_it_header"),
        (r"\bFatti\s*:?\s*\n", "ranked_it_inline"),
        (r"Il\s+Tribunale\s+militare\s+di\s+cassazione\s+ha\s+constatato\s*:?", "ranked_it_MKG"),
        (r"^A\.\s*-\s*", "fallback_it_alphabetic"),
    ],
}

# Fallback dispositiv: enumerated orders followed by court communication
DISPOSITIV_FALLBACK_RE = re.compile(
    r"(?:^|\n)\s*1\.\s+.{20,800}?(?:^|\n)\s*\d+\.\s+.{10,800}?(?:Lausanne|Bellinzona|Berne|Bern|"
    r"St\.\s*Gallen|Dieses\s+Urteil\s+wird|Le\s+présent\s+(?:arrêt|jugement)|"
    r"La\s+presente\s+(?:sentenza|decisione)|Mitteilung)",
    re.DOTALL | re.MULTILINE,
)

# MKG trailer fallback: MKG decisions (esp. FR/IT) often end with a parenthesised
# trailer "(NNN, <date>, <parties>)" instead of a formal Dispositiv. When no
# other Dispositiv marker fires, use this trailer as the Dispositiv end.
MKG_TRAILER_RE = re.compile(
    r"\(\s*(?:MKG|TMC|TMCa|ATMC|STMC|N(?:r\.?|°)|no\.?)?\s*"
    r"\d{2,4}(?:\.\d+)?(?:\s*(?:/|et|und)\s*\d{2,4}(?:\.\d+)?)*\s*"
    r"(?:,\s*(?:arr[eê]t\s+(?:du|rendu\s+le)\s+|urteil\s+vom\s+|sentenza\s+del\s+|del\s+)?"
    r"|\s+(?:du|del|vom)\s+)"
    r"\d{1,2}\.?\s*[A-Za-zÄÖÜäöüéèàùç]+\.?\s*\d{4}\s*,\s*[^()]{2,200}\)\s*$",
    re.I | re.MULTILINE,
)


@dataclass
class DecisionStructure:
    decision_id: str = ""
    language: str = ""
    sachverhalt: str | None = None
    sachverhalt_method: str | None = None
    erwaegungen: str | None = None
    erwaegungen_method: str | None = None
    erwaegungen_paragraphs: list[dict] = field(default_factory=list)
    dispositiv: str | None = None
    dispositiv_method: str | None = None
    dispositiv_orders: list[str] = field(default_factory=list)


# Erwägungs-paragraph-parser. The Schweizer numbered hierarchy
#   1.       — top-level Erwägung (always with trailing dot)
#   1.1      — sub-Erwägung (BGE-style: NO trailing dot)
#   1.1.     — sub-Erwägung (some courts: WITH trailing dot)
#   1.2.3    — sub-sub
# is the actual citable unit ("BGE 140 III 86 E. 2.3"). Splitting it out
# is the Schweizer equivalent of "extract the holding".
#
# Trailing dot is OPTIONAL: BGEs write "6.1" without it; BGer writes "6.1."
# with it. Both must match. The regex captures everything up to the
# numeric part; the trailing "." (if present) is consumed by the optional
# `\.?` after the capture group.
ERW_PARA_RE = re.compile(
    r"(?m)^[ \t]*(\d+(?:\.\d+){0,3})\.?[ \t]*[\-\u2013\u2014]?[ \t]*(?:$|[\n\r])"
)
ERW_PARA_RE_INLINE = re.compile(
    r"(?m)^[ \t]*(\d+(?:\.\d+){0,3})\.?[ \t]*[\-\u2013\u2014]?[ \t]+(?=\S)"
)


def _erw_candidates(text: str) -> list[tuple[int, int, str]]:
    seen = {}
    for pat in (ERW_PARA_RE, ERW_PARA_RE_INLINE):
        for m in pat.finditer(text):
            seen.setdefault(m.start(), (m.end(), m.group(1)))
    return sorted([(s, e, n) for s, (e, n) in seen.items()])


def _validate_erw_sequence(markers: list[tuple[int, int, str]]) -> list[tuple[int, int, str]]:
    """Drop spurious matches (e.g. '140' from a 'BGE 140 III 86' citation)."""
    if not markers:
        return []
    out = []
    seen_paths = set()
    last_top = 0
    for start, end, e_num in markers:
        depth = e_num.count(".") + 1
        first_n = int(e_num.split(".")[0])
        if depth == 1:
            if first_n > 50: continue
            if first_n < last_top: continue
            if first_n > last_top + 5 and last_top > 0: continue
            out.append((start, end, e_num))
            seen_paths.add(e_num)
            last_top = first_n
        else:
            parent = ".".join(e_num.split(".")[:-1])
            if parent in seen_paths:
                last_subnum = int(e_num.split(".")[-1])
                if last_subnum > 30: continue
                out.append((start, end, e_num))
                seen_paths.add(e_num)
    return out


def parse_erwaegungen_paragraphs(erw_text: str) -> list[dict]:
    """Parse Erwägungen into list of {e_number, depth, parent, text}."""
    if not erw_text:
        return []
    valid = _validate_erw_sequence(_erw_candidates(erw_text))
    if not valid:
        # Fallback: whole text as single anonymous paragraph
        return [{"e_number": "0", "depth": 0, "parent": None, "text": erw_text.strip()}]
    paragraphs = []
    for i, (m_start, m_end, e_num) in enumerate(valid):
        next_start = valid[i + 1][0] if i + 1 < len(valid) else len(erw_text)
        body = erw_text[m_end:next_start].strip()
        if not body:
            continue
        paragraphs.append({
            "e_number": e_num,
            "depth": e_num.count(".") + 1,
            "parent": ".".join(e_num.split(".")[:-1]) if e_num.count(".") else None,
            "text": body,
        })
    return paragraphs


def _find(text: str, patterns: dict, lang: str) -> tuple[int | None, int | None, str | None]:
    for pat, label in patterns.get(lang, []):
        m = re.search(pat, text, flags=re.MULTILINE | re.IGNORECASE)
        if m:
            return m.start(), m.end(), label
    return None, None, None


def _split_dispositiv_orders(disp_text: str) -> list[str]:
    items = []
    matches = list(re.finditer(r"(?:^|\n)\s*(\d+)\.\s+", disp_text))
    if not matches:
        return [disp_text.strip()] if disp_text.strip() else []
    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(disp_text)
        body = disp_text[start:end].strip()
        if i == len(matches) - 1:
            cut_re = re.search(
                r"\n\s*(Lausanne|Bellinzona|Berne|Bern|St\.\s*Gallen|"
                r"Im\s+Namen|Au\s+nom|In\s+nome|Dieses\s+Urteil\s+wird|"
                r"La\s+greffière|Le\s+greffier|Le\s+Président|Der\s+Präsident|Il\s+Presidente)",
                body,
            )
            if cut_re:
                body = body[: cut_re.start()].strip()
        if body:
            items.append(body)
    return items


def extract(full_text: str, language: str = "de", decision_id: str = "") -> DecisionStructure:
    out = DecisionStructure(decision_id=decision_id, language=language)
    lang = (language or "de").lower()
    if lang not in DISPOSITIV_PATTERNS:
        lang = "de"
    text = full_text or ""
    if not text:
        return out

    disp_start, disp_end, disp_method = _find(text, DISPOSITIV_PATTERNS, lang)
    if disp_start is not None:
        body = text[disp_end:].strip()
        # Trim trailing MKG-style "(NNN, <date>, <parties>)" trailer if present.
        body = MKG_TRAILER_RE.sub("", body).strip()
        out.dispositiv = body
        out.dispositiv_method = disp_method
        out.dispositiv_orders = _split_dispositiv_orders(body)
    else:
        m = DISPOSITIV_FALLBACK_RE.search(text)
        if m:
            disp_start = m.start()
            disp_end = m.start()
            body = text[disp_start:].strip()
            body = MKG_TRAILER_RE.sub("", body).strip()
            out.dispositiv = body
            out.dispositiv_method = "fallback_enum_near_end"
            out.dispositiv_orders = _split_dispositiv_orders(out.dispositiv)

    erw_start, erw_end, erw_method = _find(text, ERWAEGUNGEN_PATTERNS, lang)
    if erw_start is not None:
        end_idx = disp_start if disp_start is not None and disp_start > erw_end else len(text)
        # Strip a trailing MKG-style "(NNN, <date>, <parties>)" trailer if it
        # sits at the tail (MKG decisions without formal Dispositiv).
        raw = text[erw_end:end_idx]
        raw = MKG_TRAILER_RE.sub("", raw).strip()
        out.erwaegungen = raw
        out.erwaegungen_method = erw_method

    sav_start, sav_end, sav_method = _find(text, SACHVERHALT_PATTERNS, lang)
    if sav_start is not None:
        end_idx = erw_start if erw_start is not None and erw_start > sav_end else (
            disp_start if disp_start is not None and disp_start > sav_end else len(text)
        )
        out.sachverhalt = text[sav_end:end_idx].strip()
        out.sachverhalt_method = sav_method

    # Inferred Erwägungen: if Sachverhalt found but no explicit Erwägungen
    # marker, the reasoning body is everything between Sachverhalt-end and
    # Dispositiv-start (or text end, minus trailer). This is the MKG norm —
    # numbered reasoning paragraphs without a "hat erwogen" opener.
    if out.erwaegungen is None and sav_start is not None:
        inferred_end = disp_start if disp_start is not None and disp_start > sav_end else len(text)
        inferred_body = text[sav_end:inferred_end]
        inferred_body = MKG_TRAILER_RE.sub("", inferred_body).strip()
        if len(inferred_body) > 100:
            out.erwaegungen = inferred_body
            out.erwaegungen_method = "inferred_from_sachverhalt_dispositiv_bounds"

    # Numerical-headers fallback: many cantonal decisions (TI Camera di
    # esecuzione, ZH Obergericht, BE Verwaltungsgericht, etc.) have no
    # Sachverhalt / Erwägungen / Dispositiv markers — they begin the
    # reasoning straight away under bare numbered headers (1, 2, 2.1,
    # 2.2.1, ...).  When none of the marker-driven branches above produced
    # an erwaegungen body, fall back to the numerical headers themselves:
    # if the validator finds a clean, monotone sequence of at least 3
    # top-level numbers, take the body from the first such header onward
    # as the reasoning.  This is the only way to make E. 2.2.1 reachable
    # via get_erwaegung() for cantonal decisions, since their text never
    # carries the federal opener phrases.
    if out.erwaegungen is None:
        candidates = _validate_erw_sequence(_erw_candidates(text))
        top_level = [c for c in candidates if "." not in c[2]]
        if len(top_level) >= 3:
            first_start = top_level[0][0]
            tail_end = disp_start if disp_start is not None and disp_start > first_start else len(text)
            body = text[first_start:tail_end]
            body = MKG_TRAILER_RE.sub("", body).strip()
            if len(body) > 100:
                out.erwaegungen = body
                out.erwaegungen_method = "fallback_numerical_headers"

    # Sub-parse Erwägungen into numbered paragraphs (the actual citable units)
    if out.erwaegungen:
        out.erwaegungen_paragraphs = parse_erwaegungen_paragraphs(out.erwaegungen)

    return out


# ---------------------------------------------------------------------------
# Sidecar DB builder
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS structure (
    decision_id          TEXT PRIMARY KEY,
    court                TEXT,
    canton               TEXT,
    language             TEXT,
    decision_date        TEXT,
    regeste              TEXT,                      -- official BGer-formulated rule (BGE only)
    sachverhalt          TEXT,
    sachverhalt_method   TEXT,
    erwaegungen          TEXT,
    erwaegungen_method   TEXT,
    erwaegungen_paragraph_count INTEGER,
    dispositiv           TEXT,
    dispositiv_method    TEXT,
    dispositiv_orders    TEXT,  -- JSON array
    extracted_at         TEXT
);
CREATE INDEX IF NOT EXISTS idx_court ON structure(court);
CREATE INDEX IF NOT EXISTS idx_method ON structure(dispositiv_method);

-- Each numbered Erwägung as its own row, queryable in O(1)
CREATE TABLE IF NOT EXISTS erwaegungen_paragraph (
    decision_id   TEXT,
    e_number      TEXT,    -- "1", "1.1", "2.3.1"
    depth         INTEGER,
    parent        TEXT,    -- "1" for "1.1", "2" for "2.3" (null for top-level)
    text          TEXT,
    PRIMARY KEY (decision_id, e_number)
);
CREATE INDEX IF NOT EXISTS idx_erw_decision ON erwaegungen_paragraph(decision_id);
CREATE INDEX IF NOT EXISTS idx_erw_depth ON erwaegungen_paragraph(depth);

-- Per-paragraph FTS5 index for claim → Erwägung matching.
-- External-content references erwaegungen_paragraph by rowid; diacritic-
-- stripped tokenizer so "widerrechtlich" matches "widerréchtlich" forms
-- and German Umlaute survive normalization. Rebuilt at end of build_db.
-- The find_relevant_erwaegung MCP tool depends on this index existing —
-- DBs that predate this schema will return a clean error and prompt for
-- a rebuild rather than silently returning a "3.1" guess.
CREATE VIRTUAL TABLE IF NOT EXISTS erwaegungen_paragraph_fts USING fts5(
    text,
    content='erwaegungen_paragraph',
    content_rowid='rowid',
    tokenize='unicode61 remove_diacritics 1'
);
"""


def iter_jsonl(path: Path) -> Iterator[dict]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def build_db(shard_paths: list[Path], out_db: Path) -> dict:
    """Run extractor over each JSONL shard, persist to SQLite. Atomic swap.

    Symlink-aware: when out_db is a symlink (post-2026-05-02 the
    decision_structure.db at /opt/caselaw/repo/output is a symlink to
    /mnt/HC_Volume_104655575/output/decision_structure.db so the 44 GB
    file lives on the data volume, not the 150 GB root disk), resolve
    to the real path before deciding where to put the .tmp. Otherwise
    the .tmp is created next to the symlink (i.e. on /opt) and the
    final os.replace clobbers the symlink — landing the new 44 GB DB
    back on the small disk and re-creating the disk-fill incident class.
    """
    real_out = out_db.resolve() if out_db.is_symlink() else out_db
    tmp_db = real_out.with_suffix(".db.tmp")
    if tmp_db.exists():
        tmp_db.unlink()

    conn = sqlite3.connect(str(tmp_db))
    conn.executescript(SCHEMA)
    cur = conn.cursor()

    stats = {"shards": {}, "total": 0, "with_disp": 0, "with_erw": 0, "with_sav": 0,
             "with_regeste": 0, "with_subnumbered_erw": 0, "all_three": 0,
             "total_paragraphs": 0}
    started = time.time()

    for shard in shard_paths:
        if not shard.exists():
            logger.warning(f"shard not found: {shard}")
            continue
        court_label = shard.stem
        n = 0
        sd = se = ss = sa = sreg = ssub = 0
        n_paragraphs = 0
        rows = []
        para_rows = []
        for entry in iter_jsonl(shard):
            ft = entry.get("full_text") or ""
            if len(ft) < 500:
                continue
            n += 1
            s = extract(ft, entry.get("language", "de"), entry.get("decision_id", ""))
            if s.dispositiv: sd += 1
            if s.erwaegungen: se += 1
            if s.sachverhalt: ss += 1
            if s.dispositiv and s.erwaegungen and s.sachverhalt: sa += 1
            regeste = entry.get("regeste") or None
            if regeste and len(str(regeste)) > 20: sreg += 1
            if any(p["depth"] >= 2 for p in s.erwaegungen_paragraphs): ssub += 1
            n_paragraphs += len(s.erwaegungen_paragraphs)
            rows.append((
                s.decision_id,
                entry.get("court"),
                entry.get("canton"),
                s.language,
                entry.get("decision_date"),
                regeste,
                s.sachverhalt,
                s.sachverhalt_method,
                s.erwaegungen,
                s.erwaegungen_method,
                len(s.erwaegungen_paragraphs),
                s.dispositiv,
                s.dispositiv_method,
                json.dumps(s.dispositiv_orders, ensure_ascii=False) if s.dispositiv_orders else None,
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            ))
            for p in s.erwaegungen_paragraphs:
                if p["depth"] == 0:
                    continue  # skip the synthetic "no markers found" fallback
                para_rows.append((
                    s.decision_id, p["e_number"], p["depth"], p["parent"], p["text"]
                ))
            if len(rows) >= 5000:
                cur.executemany(
                    "INSERT OR REPLACE INTO structure VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    rows,
                )
                cur.executemany(
                    "INSERT OR REPLACE INTO erwaegungen_paragraph VALUES (?,?,?,?,?)",
                    para_rows,
                )
                conn.commit()
                rows = []; para_rows = []
        if rows:
            cur.executemany(
                "INSERT OR REPLACE INTO structure VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
        if para_rows:
            cur.executemany(
                "INSERT OR REPLACE INTO erwaegungen_paragraph VALUES (?,?,?,?,?)",
                para_rows,
            )
        conn.commit()

        stats["shards"][court_label] = {
            "n": n,
            "disp_pct": round(sd / n * 100, 1) if n else 0,
            "erw_pct": round(se / n * 100, 1) if n else 0,
            "sav_pct": round(ss / n * 100, 1) if n else 0,
            "regeste_pct": round(sreg / n * 100, 1) if n else 0,
            "subnumbered_erw_pct": round(ssub / n * 100, 1) if n else 0,
            "avg_paragraphs_per_decision": round(n_paragraphs / n, 1) if n else 0,
            "all_three_pct": round(sa / n * 100, 1) if n else 0,
        }
        stats["total"] += n
        stats["with_disp"] += sd
        stats["with_erw"] += se
        stats["with_sav"] += ss
        stats["with_regeste"] += sreg
        stats["with_subnumbered_erw"] += ssub
        stats["all_three"] += sa
        stats["total_paragraphs"] += n_paragraphs
        # Guard against empty shards (n=0): a recently-retired or
        # not-yet-populated source can land here with zero rows. The
        # stats dict above already uses the same `if n else 0` guard;
        # the logger formatting was the only path that still divided
        # raw and crashed the whole 6h pipeline.
        def _pct(num):
            return f"{num/n*100:.0f}" if n else "0"
        logger.info(
            f"{court_label}: n={n}, disp={sd}({_pct(sd)}%), erw={se}({_pct(se)}%), "
            f"sav={ss}({_pct(ss)}%), regeste={sreg}({_pct(sreg)}%), "
            f"subnum={ssub}({_pct(ssub)}%), paragraphs={n_paragraphs} — {time.time()-started:.0f}s"
        )

    # Build the FTS5 index over erwaegungen_paragraph.text. Done as a
    # single 'rebuild' after all paragraph rows are inserted — far faster
    # than per-row triggers, and the build is one-shot so we don't need
    # incremental maintenance. The find_relevant_erwaegung tool refuses
    # to run when this index is missing rather than fall back to a guess.
    fts_started = time.time()
    cur.execute("INSERT INTO erwaegungen_paragraph_fts(erwaegungen_paragraph_fts) VALUES('rebuild')")
    conn.commit()
    cur.execute("INSERT INTO erwaegungen_paragraph_fts(erwaegungen_paragraph_fts) VALUES('optimize')")
    conn.commit()
    fts_count = cur.execute("SELECT count(*) FROM erwaegungen_paragraph_fts").fetchone()[0]
    logger.info(f"FTS5 index built: {fts_count} paragraphs indexed in {time.time()-fts_started:.1f}s")
    stats["fts_paragraphs"] = fts_count

    conn.close()
    # Atomic swap: rename .tmp into the real (resolved) target, not the
    # symlink. POSIX rename on the same filesystem (both paths now under
    # /mnt) is atomic. The symlink at /opt is untouched and continues to
    # point at the new file.
    tmp_db.replace(real_out)
    stats["duration_s"] = round(time.time() - started, 1)
    return stats


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--build", action="store_true", help="Build the full sidecar DB")
    p.add_argument("--shards", default="bger,bvger,bstger,bge,bpatger,bge_egmr,bge_historical",
                   help="Comma-separated shard names (without .jsonl)")
    p.add_argument("--decisions-dir", default="output/decisions")
    p.add_argument("--output", default="output/decision_structure.db")
    p.add_argument("--decision-id", help="Test extraction on one decision_id (any shard)")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.decision_id:
        for shard_name in args.shards.split(","):
            path = Path(args.decisions_dir) / f"{shard_name}.jsonl"
            if not path.exists():
                continue
            for e in iter_jsonl(path):
                if e.get("decision_id") == args.decision_id:
                    s = extract(e.get("full_text", ""), e.get("language", "de"), args.decision_id)
                    print(json.dumps({
                        "decision_id": s.decision_id,
                        "language": s.language,
                        "sachverhalt_len": len(s.sachverhalt or ""),
                        "sachverhalt_method": s.sachverhalt_method,
                        "erwaegungen_len": len(s.erwaegungen or ""),
                        "erwaegungen_method": s.erwaegungen_method,
                        "dispositiv_len": len(s.dispositiv or ""),
                        "dispositiv_method": s.dispositiv_method,
                        "dispositiv_orders": s.dispositiv_orders,
                    }, indent=2, ensure_ascii=False))
                    return
        print(f"Not found: {args.decision_id}")
        sys.exit(1)

    if args.build:
        shards = [Path(args.decisions_dir) / f"{s}.jsonl" for s in args.shards.split(",")]
        stats = build_db(shards, Path(args.output))
        print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
