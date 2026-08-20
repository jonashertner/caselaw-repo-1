#!/usr/bin/env python3
"""Training sets derived from the corpus itself, not from traffic.

The telemetry has a ceiling this traffic shape cannot lift: 94% of calls
are one crawler's bulk export, MCP sessions are single-call by
construction, and the impression->fetch join yields a handful of rows on
an active evening. No amount of capture engineering changes that.

The corpus does not have that ceiling. It is CC0, it scales with the
collection rather than with traffic, and it carries no privacy dimension
at all — no notice interaction, no session questions, nothing to delete.

Two datasets, both from `decisions`:

  parallel   The Federal Supreme Court publishes each BGE Regeste in
             German, French and Italian — the same holding, written by
             the same court, in three languages, by people whose job is
             to get it right. Verified legal parallel text is rare and
             expensive; here it is a by-product of publication. Trains
             and evaluates translation and cross-lingual retrieval, and
             is the natural ground truth for the cross-lingual MRR the
             project already reports.

  summary    Regeste against the decision body: an expert abstractive
             summary of a long, highly structured legal document, one
             per decision that has both.

Neither needs a single user interaction.

Read-only. Runs a full pass over `decisions`, so it must NOT run during
the nightly build window (invariant #9) — after the pipeline exits, or
against a copy.

    python3 scripts/build_corpus_datasets.py --db output/decisions.db \
        --out output/datasets --dataset parallel
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import re
import sqlite3
import sys
from pathlib import Path

log = logging.getLogger("corpus_datasets")

# The Regeste block for each language opens with its own heading. Italian
# is the reliable anchor — "Regesto" appears in no other language — and
# German and French both say "Regeste", so they are told apart by the
# citation abbreviations the court uses rather than by the heading.
_HEAD = re.compile(r"(?:^|\n)\s*(Regeste|Regesto)\s*([a-z])?\s*(?=\n|$)",
                   re.MULTILINE)

# Abbreviations that only appear in one language's drafting convention.
_MARKERS = {
    "de": (r"\bAbs\.", r"\blit\.", r"\bE\.\s*\d", r"\bZiff\.", r"\bvom\b",
           r"\bund\b"),
    "fr": (r"\bal\.", r"\blet\.", r"\bconsid\.", r"\bch\.", r"\bdu\b",
           r"\bet\b"),
    "it": (r"\bcpv\.", r"\blett\.", r"\bconsid\.", r"\bn\.", r"\bdel\b",
           r"\be\b"),
}


def _score_language(text: str) -> dict:
    return {lang: sum(len(re.findall(p, text)) for p in pats)
            for lang, pats in _MARKERS.items()}


def detect_language(text: str) -> str | None:
    """Which language a Regeste block is written in, or None if unclear.

    Italian and French share `consid.`, so a bare count would call every
    Italian block French. The distinctly-Italian forms (cpv., lett.)
    settle it when present; when nothing distinguishes them, the block is
    dropped rather than guessed — a mislabelled pair is worse than a
    missing one in a translation set.
    """
    if not text or len(text.strip()) < 40:
        return None
    s = _score_language(text)
    it_only = len(re.findall(r"\bcpv\.|\blett\.", text))
    de_only = len(re.findall(r"\bAbs\.|\blit\.|\bE\.\s*\d", text))
    fr_only = len(re.findall(r"\bal\.|\blet\.", text))
    if it_only and it_only >= max(de_only, fr_only):
        return "it"
    if de_only and de_only > fr_only:
        return "de"
    if fr_only and fr_only > de_only:
        return "fr"
    best = max(s, key=s.get)
    return best if s[best] else None


def split_regeste(regeste: str) -> dict:
    """-> {language: text} for the blocks found in one Regeste field."""
    if not regeste:
        return {}
    text = re.sub(r"<br\s*/?>", "\n", regeste)
    heads = list(_HEAD.finditer(text))
    if not heads:
        return {}
    blocks: list[tuple[str, str]] = []
    for i, m in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(text)
        body = text[m.end():end].strip()
        if body:
            blocks.append((m.group(1), body))
    out: dict[str, list] = {}
    for head, body in blocks:
        # "Regesto" is unambiguous; everything else has to be inferred.
        lang = "it" if head == "Regesto" else detect_language(body)
        if lang:
            out.setdefault(lang, []).append(body)
    return {k: "\n\n".join(v).strip() for k, v in out.items()}


def build_parallel(conn: sqlite3.Connection, min_chars: int = 80) -> list:
    """Aligned DE/FR/IT Regeste triples (or pairs)."""
    rows, seen = [], 0
    for r in conn.execute(
            "SELECT decision_id, court, decision_date, regeste FROM decisions "
            "WHERE regeste IS NOT NULL AND length(regeste) > ?", (min_chars,)):
        seen += 1
        parts = split_regeste(r[3])
        parts = {k: v for k, v in parts.items() if len(v) >= min_chars}
        if len(parts) < 2:
            continue          # a single language is not parallel text
        rows.append({
            "decision_id": r[0], "court": r[1], "decision_date": r[2],
            "languages": sorted(parts), "n_languages": len(parts),
            "text": parts,
        })
    log.info("parallel: %d of %d decisions with a Regeste yielded 2+ languages",
             len(rows), seen)
    return rows


def build_summary(conn: sqlite3.Connection, min_body: int = 1500,
                  min_regeste: int = 120) -> list:
    """Regeste against the decision body — expert abstractive summaries."""
    rows, seen = [], 0
    for r in conn.execute(
            "SELECT decision_id, court, language, decision_date, regeste, "
            "full_text FROM decisions WHERE regeste IS NOT NULL "
            "AND full_text IS NOT NULL AND length(full_text) > ?", (min_body,)):
        seen += 1
        parts = split_regeste(r[4])
        # Prefer the block in the decision's own language; a trilingual
        # Regeste against a German body would otherwise teach the model to
        # summarise across languages by accident.
        summary = parts.get(r[2]) or (r[4] if not parts else "")
        summary = (summary or "").strip()
        if len(summary) < min_regeste:
            continue
        body = r[5]
        rows.append({
            "decision_id": r[0], "court": r[1], "language": r[2],
            "decision_date": r[3],
            "summary": summary, "body_chars": len(body),
            "compression": round(len(body) / max(1, len(summary)), 1),
        })
    log.info("summary: %d of %d decisions produced a usable pair", len(rows), seen)
    return rows


_CARD = """# {name}

Generated {date} by `scripts/build_corpus_datasets.py` from the
OpenCaseLaw decision corpus (CC0).

Rows: {n}

{blurb}

Licence: CC0, same as the corpus. Derived from published court decisions;
contains no user data of any kind.
"""

_BLURBS = {
    "parallel": (
        "Aligned Regeste text in two or three of German, French and Italian.\n"
        "The Federal Supreme Court publishes the same holding in each\n"
        "official language, so the alignment is human-made and\n"
        "authoritative rather than machine-aligned. A block whose language\n"
        "cannot be determined is dropped rather than guessed."),
    "summary": (
        "Expert abstractive summary (the Regeste, written by the court)\n"
        "paired with the decision it summarises. `compression` is the\n"
        "body-to-summary character ratio."),
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--db", type=Path, default=Path("output/decisions.db"))
    ap.add_argument("--out", type=Path, default=Path("output/datasets"))
    ap.add_argument("--dataset", choices=("parallel", "summary", "all"),
                    default="all")
    ap.add_argument("--limit", type=int, default=0, help="cap rows (dev)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if not args.db.exists():
        log.error("no such database: %s", args.db)
        return 1
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    wanted = ("parallel", "summary") if args.dataset == "all" else (args.dataset,)
    day = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")

    for name in wanted:
        rows = (build_parallel(conn) if name == "parallel"
                else build_summary(conn))
        if args.limit:
            rows = rows[:args.limit]
        d = args.out / name
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{day}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        (d / "CARD.md").write_text(
            _CARD.format(name=name, date=day, n=len(rows),
                         blurb=_BLURBS[name]), encoding="utf-8")
        log.info("wrote %d rows -> %s", len(rows), path)
        if name == "parallel" and rows:
            from collections import Counter
            c = Counter(tuple(r["languages"]) for r in rows)
            for combo, n in c.most_common(6):
                log.info("   %s: %d", "+".join(combo), n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
