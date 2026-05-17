"""Terminal adjudication tool for the 400-row citation-precision sample.

Designed to make an 8-hour slog tolerable. Single-keystroke decisions,
auto-save after every row, crash-resumable from where you left off.

Usage:
    python3 -m benchmarks.citation_precision_audit_tui \\
        --sample benchmarks/citation_precision_sample_400.jsonl

Controls per row:
    c   mark correct
    w   mark wrong
    u   mark uncertain
    n   open a note (single-line input)
    b   go back one row (re-adjudicate previous)
    s   skip without marking
    q   save and quit (resume later with same command)
    ?   show legend

Behaviour:
- Reads the sample JSONL on start; partial adjudications persist back
  to the SAME file after every key press. No external state.
- On restart, jumps to the first un-adjudicated row.
- Re-marking a row overwrites the prior decision. Notes are preserved
  unless explicitly cleared.
- Quitting (q) writes the file then exits cleanly. SIGINT also saves.

Output stays the source-of-truth JSONL — no separate adjudication file
to merge later. Re-running ``make verify`` does not touch the audit
state; the audit is parallel.
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import termios
import tty
from pathlib import Path
from typing import Optional

# ANSI for a readable display without pulling rich/curses
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
RED = "\033[31m"
MAGENTA = "\033[35m"
CLEAR = "\033[2J\033[H"


def _getch() -> str:
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    return ch


def _load(path: Path) -> list[dict]:
    """Load JSONL; first row is the _meta header."""
    rows = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    if not rows or rows[0].get("q_id") != "_meta":
        raise SystemExit(
            f"{path} missing _meta header — wrong sample file?"
        )
    return rows


def _save(path: Path, rows: list[dict]) -> None:
    """Atomic save: write to .tmp then rename. JSONL with meta header."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(path)


def _render(row: dict, idx: int, total: int, n_done: int) -> None:
    sys.stdout.write(CLEAR)
    pct = 100.0 * n_done / total if total else 0
    sys.stdout.write(
        f"{BOLD}citation-precision audit{RESET}  "
        f"{DIM}row {idx + 1}/{total} • {n_done} adjudicated ({pct:.0f}%){RESET}\n"
    )
    sys.stdout.write("─" * 78 + "\n")

    # match_type + confidence as the headline
    mt = row.get("match_type", "?")
    conf = row.get("confidence_score", 0.0)
    conf_color = GREEN if conf >= 0.9 else YELLOW if conf >= 0.7 else RED
    sys.stdout.write(
        f"{BOLD}{mt}{RESET}  "
        f"confidence={conf_color}{conf:.2f}{RESET}\n\n"
    )

    # Source citation in context
    src = row.get("source_decision_id", "?")
    before = row.get("source_context_before", "")
    after = row.get("source_context_after", "")
    target_ref = row.get("target_ref", "")
    sys.stdout.write(f"{DIM}source decision:{RESET} {CYAN}{src}{RESET}\n")
    sys.stdout.write(f"{DIM}context (raw cite highlighted):{RESET}\n")
    sys.stdout.write(
        f"  …{before} {MAGENTA}{BOLD}[{target_ref}]{RESET} {after}…\n\n"
    )

    # What the resolver chose
    tid = row.get("target_decision_id") or "(unresolved)"
    sys.stdout.write(f"{DIM}resolver matched →{RESET} {CYAN}{tid}{RESET}\n")
    head = row.get("target_regeste_head") or "(no regeste available)"
    sys.stdout.write(f"{DIM}target regeste head:{RESET}\n  {head}\n\n")

    # Current state (if any)
    adj = row.get("adjudication", "") or ""
    notes = row.get("notes", "") or ""
    if adj:
        color = GREEN if adj == "correct" else RED if adj == "wrong" else YELLOW
        sys.stdout.write(f"{DIM}current adjudication:{RESET} {color}{adj}{RESET}\n")
    if notes:
        sys.stdout.write(f"{DIM}notes:{RESET} {notes}\n")
    sys.stdout.write("\n")

    sys.stdout.write(
        f"{BOLD}[c]{RESET}orrect  {BOLD}[w]{RESET}rong  "
        f"{BOLD}[u]{RESET}ncertain  {BOLD}[n]{RESET}ote  "
        f"{BOLD}[b]{RESET}ack  {BOLD}[s]{RESET}kip  "
        f"{BOLD}[q]{RESET}uit-save  {BOLD}[?]{RESET}help\n"
    )
    sys.stdout.flush()


def _ask_note() -> str:
    # Restore line-buffered stdin so the user can type a multi-char note
    sys.stdout.write(f"\n{DIM}note (Enter to save, empty to clear): {RESET}")
    sys.stdout.flush()
    return sys.stdin.readline().rstrip("\n").rstrip("\r")


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--sample", required=True, type=Path,
        help="The sampled JSONL produced by citation_precision_audit.py",
    )
    args = p.parse_args(argv)

    if not args.sample.exists():
        print(f"ERROR: {args.sample} does not exist", file=sys.stderr)
        return 2
    if not sys.stdin.isatty():
        print(
            "ERROR: stdin is not a TTY — this tool needs an interactive shell",
            file=sys.stderr,
        )
        return 2

    rows = _load(args.sample)
    meta = rows[0]
    samples = rows[1:]
    total = len(samples)

    def n_done() -> int:
        return sum(1 for r in samples if r.get("adjudication"))

    # Resume: jump to first un-adjudicated row
    idx = next(
        (i for i, r in enumerate(samples) if not r.get("adjudication")),
        0,
    )

    # Save on SIGINT (Ctrl-C)
    def _sigint_handler(signum, frame):
        _save(args.sample, [meta] + samples)
        sys.stdout.write(f"\n{YELLOW}interrupt → saved {n_done()}/{total}{RESET}\n")
        sys.exit(130)

    signal.signal(signal.SIGINT, _sigint_handler)

    while 0 <= idx < total:
        _render(samples[idx], idx, total, n_done())
        ch = _getch().lower()
        if ch == "c":
            samples[idx]["adjudication"] = "correct"
            _save(args.sample, [meta] + samples)
            idx += 1
        elif ch == "w":
            samples[idx]["adjudication"] = "wrong"
            _save(args.sample, [meta] + samples)
            idx += 1
        elif ch == "u":
            samples[idx]["adjudication"] = "uncertain"
            _save(args.sample, [meta] + samples)
            idx += 1
        elif ch == "n":
            samples[idx]["notes"] = _ask_note()
            _save(args.sample, [meta] + samples)
            # stay on same row so the adjudicator can also mark c/w/u
        elif ch == "b":
            idx = max(0, idx - 1)
        elif ch == "s":
            idx += 1
        elif ch == "q":
            _save(args.sample, [meta] + samples)
            sys.stdout.write(
                f"\n{GREEN}saved {n_done()}/{total} adjudicated. "
                f"Re-run the same command to resume.{RESET}\n"
            )
            return 0
        elif ch == "?":
            sys.stdout.write(
                "\n  c=correct, w=wrong, u=uncertain, n=note,\n"
                "  b=back one row, s=skip without marking,\n"
                "  q=save+quit, ?=this help\n"
                "  (press any key)\n"
            )
            sys.stdout.flush()
            _getch()

    # All done
    sys.stdout.write(
        f"\n{GREEN}{BOLD}all {total} adjudicated.{RESET}\n"
        f"  correct:   {sum(1 for r in samples if r.get('adjudication') == 'correct')}\n"
        f"  wrong:     {sum(1 for r in samples if r.get('adjudication') == 'wrong')}\n"
        f"  uncertain: {sum(1 for r in samples if r.get('adjudication') == 'uncertain')}\n"
    )
    _save(args.sample, [meta] + samples)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
