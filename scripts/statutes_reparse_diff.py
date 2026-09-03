#!/usr/bin/env python3
"""
Re-parse every article of a statutes.db with the current parser and report
what a rebuild would change. Read-only: the DB is opened with mode=ro and
nothing is written anywhere.

    python scripts/statutes_reparse_diff.py output/statutes.db
    python scripts/statutes_reparse_diff.py /scratch/statutes.copy.db --sample 20
    python scripts/statutes_reparse_diff.py output/statutes.db \
        --baseline-module /path/to/old/build_statutes_db.py

By default the "old" side is what the DB stores (article_num, heading, text).
With --baseline-module the old side is an older copy of
search_stack/build_statutes_db.py re-run over the same `xml` column, which
is how to measure a parser change against a DB built by an even older
parser.

section_heading cannot be checked here: it comes from the enclosing block
of the whole document, and the per-article `xml` column carries only the
<article> subtree.

Run against a copy, or on the VPS only after the nightly publish has exited.
"""

import argparse
import difflib
import importlib.util
import re
import sqlite3
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import search_stack.build_statutes_db as new_mod  # noqa: E402

ORDINALS = new_mod._ORDINAL_SUFFIXES


def _load_module(path: Path):
    spec = importlib.util.spec_from_file_location("bsd_baseline", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def classify_num_change(old: str, new: str) -> str:
    """Name the kind of article_num repair for the summary table."""
    if old == new:
        return "same"
    if old.startswith("."):
        return "fr_dot_space"                 # ". 264 a" -> "264a"
    if old.startswith(new) and re.fullmatch(r"\d+[a-z]?", old[len(new):]):
        return "range_concat"                 # "135149" -> "135", "24" -> "2", "274274g" -> "274"
    if new.startswith(old) and new[len(old):] in ORDINALS:
        if re.fullmatch(r"\d+[a-z]", old):
            return "letter_ordinal"           # "268a" -> "268abis"
        return "ordinal_attached"             # "260" -> "260sexies"
    if re.fullmatch(r"\d+[a-z]", old) and new == old[:-1]:
        return "range_conjunction"            # it "135a" -> "135" (Art. 135 a 149)
    if old.isdigit() and new.startswith(old) and re.fullmatch(r"\d+[a-z]?[a-z]*", new):
        return "split_num_repair"             # "45" -> "450a": #32 eId repair the stored DB predates
    if (re.fullmatch(r"\d+[a-z]", old) and new.startswith(old)
            and new[len(old) - 1:] in ORDINALS):
        return "ordinal_truncation_repair"    # "179d" -> "179decies": #87 repair the stored DB predates
    return "other"


def _raw_num(elem) -> str:
    num = new_mod._child(elem, "num")
    return new_mod.extract_text(num) if num is not None else ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("db", type=Path)
    ap.add_argument("--baseline-module", type=Path, default=None,
                    help="older build_statutes_db.py to re-run as the 'old' side "
                         "(default: the values stored in the DB)")
    ap.add_argument("--sample", type=int, default=0,
                    help="print N old/new diffs of changed texts")
    ap.add_argument("--sr", nargs="*", default=None, help="restrict to these SR numbers")
    args = ap.parse_args()

    old_mod = _load_module(args.baseline_module) if args.baseline_module else None

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)")}
    if "xml" not in cols:
        print("articles.xml column missing; nothing to re-parse", file=sys.stderr)
        return 1
    sql = "SELECT id, sr_number, lang, article_num, heading, text, footnote, xml FROM articles"
    params: tuple = ()
    if args.sr:
        sql += f" WHERE sr_number IN ({','.join('?' * len(args.sr))})"
        params = tuple(args.sr)

    rows = 0
    text_changed = 0
    chars_old = 0
    chars_new = 0
    heading_changed = 0
    num_kinds: Counter = Counter()
    num_examples: dict[str, list] = defaultdict(list)
    whitespace_nums: list = []
    empty_rows: list = []
    footnote_as_body = 0
    weld_nums: list = []
    weld_text_rows = 0
    dropped_no_num: list = []
    keys: Counter = Counter()
    disp_rows = 0
    samples: list = []

    for rid, sr, lang, s_num, s_head, s_text, s_foot, xml in conn.execute(sql, params):
        rows += 1
        elem = ET.fromstring(xml)
        n_num, n_head, n_text, n_foot = new_mod.parse_article(elem)
        section = new_mod.article_section(elem)
        if section:
            disp_rows += 1

        if old_mod is not None:
            o_num, o_head, o_text, _o_foot = old_mod.parse_article(elem)
        else:
            o_num, o_head, o_text = s_num, s_head, s_text

        if not n_num:
            dropped_no_num.append((sr, lang, elem.get("eId"), _raw_num(elem)))
            continue
        if not n_text:
            empty_rows.append((sr, lang, section, n_num))
        elif n_foot and n_text == n_foot:
            footnote_as_body += 1

        keys[(sr, lang, section, n_num)] += 1
        chars_old += len(o_text or "")
        chars_new += len(n_text or "")
        if (o_text or "") != (n_text or ""):
            text_changed += 1
            if len(samples) < args.sample:
                samples.append((sr, lang, section, o_num, n_num, o_text or "", n_text or ""))
        if (o_head or None) != (n_head or None):
            heading_changed += 1
        kind = classify_num_change(o_num or "", n_num)
        if kind != "same":
            num_kinds[kind] += 1
            if len(num_examples[kind]) < 6:
                num_examples[kind].append((sr, lang, section, o_num, n_num))
        if re.search(r"\s", n_num):
            whitespace_nums.append((sr, lang, section, n_num))
        if re.search(r"\d{5,}", n_num):
            weld_nums.append((sr, lang, section, o_num, n_num))
        new_runs = set(re.findall(r"\d{5,}", n_text or ""))
        if new_runs - set(re.findall(r"\d{5,}", o_text or "")):
            weld_text_rows += 1

    collisions = {k: v for k, v in keys.items() if v > 1}
    old_label = f"re-run of {args.baseline_module}" if old_mod else "values stored in the DB"

    print(f"statutes.db: {args.db}")
    print(f"rows re-parsed: {rows}   (old side = {old_label})")
    print()
    print(f"{'text changed':40s} {text_changed:8d}  ({100.0 * text_changed / max(rows, 1):.1f} %)")
    print(f"{'text chars old -> new':40s} {chars_old:8d} -> {chars_new} ({chars_new - chars_old:+d})")
    print(f"{'headings changed':40s} {heading_changed:8d}")
    print(f"{'article_num changed':40s} {sum(num_kinds.values()):8d}")
    for kind, n in num_kinds.most_common():
        ex = "; ".join(f"{sr}/{lang}{'/' + sec if sec else ''} {o!r}->{nw!r}"
                       for sr, lang, sec, o, nw in num_examples[kind][:3])
        print(f"    {kind:36s} {n:8d}  e.g. {ex}")
    print(f"{'article_num containing whitespace':40s} {len(whitespace_nums):8d}")
    for w in whitespace_nums[:10]:
        print(f"    {w}")
    print(f"{'rows that would be empty':40s} {len(empty_rows):8d}")
    for e in empty_rows[:10]:
        print(f"    {e}")
    print(f"{'rows served from footnote (repealed)':40s} {footnote_as_body:8d}")
    print(f"{'colliding (sr,lang,section,article_num)':40s} {len(collisions):8d}  "
          f"(surplus rows {sum(v - 1 for v in collisions.values())})")
    for k, v in sorted(collisions.items())[:10]:
        print(f"    {k} x{v}")
    print(f"{'article_num with >= 5 digits (weld)':40s} {len(weld_nums):8d}")
    for w in weld_nums[:10]:
        print(f"    {w}")
    print(f"{'rows with new >= 5-digit runs in text':40s} {weld_text_rows:8d}")
    print(f"{'dropped (no article number)':40s} {len(dropped_no_num):8d}")
    for d in dropped_no_num:
        print(f"    {d}")
    print(f"{'transitional/final block rows (disp_*)':40s} {disp_rows:8d}")
    print()
    print("section_heading: not checkable from the article xml alone (it lives on the "
          "enclosing block of the full document); verify it on the built DB.")

    if samples:
        print()
        print(f"=== {len(samples)} sample text diffs (old -> new) ===")
        for sr, lang, section, o_num, n_num, o_text, n_text in samples:
            label = f"{sr} {lang} {section + '/' if section else ''}{o_num}"
            if o_num != n_num:
                label += f" -> {n_num}"
            print(f"--- {label}")
            diff = difflib.unified_diff(
                o_text.split("\n"), n_text.split("\n"), "old", "new", lineterm="", n=0,
            )
            for line in list(diff)[2:]:
                print("   ", line[:400])
    return 0


if __name__ == "__main__":
    sys.exit(main())
