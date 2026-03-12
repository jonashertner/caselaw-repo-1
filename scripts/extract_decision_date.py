#!/usr/bin/env python3
"""
Extract the actual decision date from Swiss court decision full text.

Parses multilingual (DE/FR/IT) date patterns from decision headers and
sign-off blocks. Used to audit and fix metadata dates that may contain
publication dates instead of actual decision dates.

Usage:
    # As module
    from scripts.extract_decision_date import extract_decision_date
    result = extract_decision_date(full_text, language="fr")

    # CLI audit mode
    python3 scripts/extract_decision_date.py --court fr_gerichte --sample 100
    python3 scripts/extract_decision_date.py --all-courts --sample 50
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("extract_date")

# ============================================================
# Month names (multilingual)
# ============================================================

MONTH_NAMES: dict[str, int] = {
    # German
    "januar": 1, "jänner": 1, "februar": 2, "märz": 3, "april": 4,
    "mai": 5, "juni": 6, "juli": 7, "august": 8, "september": 9,
    "oktober": 10, "november": 11, "dezember": 12,
    # French
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
    # Italian
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5,
    "giugno": 6, "luglio": 7, "agosto": 8, "settembre": 9,
    "ottobre": 10, "dicembre": 12,
    # English (for ECHR decisions)
    "january": 1, "february": 2, "march": 3, "may": 5,
    "june": 6, "july": 7, "august": 8, "october": 10, "december": 12,
}

# Month name regex component (all languages)
_MONTH_RE = "|".join(sorted(MONTH_NAMES.keys(), key=len, reverse=True))

# ============================================================
# Date patterns
# ============================================================

# --- Header patterns (high confidence) ---

# DE: "Urteil vom 23. Februar 2026", "Entscheid vom ...", "Beschluss vom ..."
RE_DE_HEADER = re.compile(
    r"(?:Urteil|Entscheid|Beschluss|Verfügung|Zwischenentscheid|Teilurteil)"
    r"\s+vom\s+(\d{1,2})\.\s*(" + _MONTH_RE + r")\s+(\d{4})",
    re.IGNORECASE,
)

# DE: "mitgeteilt am 4. Februar 2026" (communication/publication date, NOT decision date)
RE_DE_COMMUNICATED = re.compile(
    r"mitgeteilt\s+am\s+(\d{1,2})\.\s*(" + _MONTH_RE + r")\s+(\d{4})",
    re.IGNORECASE,
)

# FR: "Arrêt du 23 février 2026", "Jugement du ...", "Décision du ...", "Ordonnance du ..."
RE_FR_HEADER = re.compile(
    r"(?:Arr[êe]t|Jugement|D[ée]cision|Ordonnance|Sentence)\s+du\s+"
    r"(\d{1,2})(?:er)?\s+(" + _MONTH_RE + r")\s+(\d{4})",
    re.IGNORECASE,
)

# IT: "Sentenza del 4 febbraio 2026", "Decisione del ...", "Giudizio del ..."
RE_IT_HEADER = re.compile(
    r"(?:Sentenza|Decisione|Giudizio|Decreto)\s+del\s+"
    r"(\d{1,2})\s+(" + _MONTH_RE + r")\s+(\d{4})",
    re.IGNORECASE,
)

# IT: "comunicata il 6 febbraio 2026" (communication date, NOT decision date)
RE_IT_COMMUNICATED = re.compile(
    r"comunicat[ao]\s+il\s+(\d{1,2})\s+(" + _MONTH_RE + r")\s+(\d{4})",
    re.IGNORECASE,
)

# --- Sign-off patterns (high confidence) ---

# FR: "Fribourg, le 23 février 2026"
RE_FR_SIGNOFF = re.compile(
    r"[A-ZÀ-Ü][a-zà-ü]+(?:-[A-ZÀ-Ü][a-zà-ü]+)?,\s*le\s+"
    r"(\d{1,2})(?:er)?\s+(" + _MONTH_RE + r")\s+(\d{4})",
    re.IGNORECASE,
)

# DE: "Lausanne, 4. Januar 2000" or "Zürich, den 4. Januar 2000"
RE_DE_SIGNOFF = re.compile(
    r"[A-ZÀ-Ü][a-zà-ü]+(?:-[A-ZÀ-Ü][a-zà-ü]+)?,\s*(?:den\s+)?"
    r"(\d{1,2})\.\s*(" + _MONTH_RE + r")\s+(\d{4})",
    re.IGNORECASE,
)

# IT: "Bellinzona, 4 febbraio 2026" or "Lugano, il 4 febbraio 2026"
RE_IT_SIGNOFF = re.compile(
    r"[A-ZÀ-Ü][a-zà-ü]+(?:-[A-ZÀ-Ü][a-zà-ü]+)?,\s*(?:il\s+)?"
    r"(\d{1,2})\s+(" + _MONTH_RE + r")\s+(\d{4})",
    re.IGNORECASE,
)

# EN: "notified in writing on 19 February 2026" (ECHR)
RE_EN_NOTIFIED = re.compile(
    r"(?:on|dated?)\s+(\d{1,2})\s+(" + _MONTH_RE + r")\s+(\d{4})",
    re.IGNORECASE,
)

# --- Bare date in header (medium confidence) ---

# "23. Februar 2026" or "23 février 2026" or "23 febbraio 2026" (standalone)
RE_BARE_DATE = re.compile(
    r"(?<!\d)(\d{1,2})\.?\s+(" + _MONTH_RE + r")\s+(\d{4})(?!\d)",
    re.IGNORECASE,
)

# Numeric: "23.02.2026"
RE_NUMERIC_DATE = re.compile(
    r"(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{4})(?!\d)",
)

# ECHR: "Arrêt no. 28865/17, 19 février 2026"
RE_ECHR_HEADER = re.compile(
    r"Arr[êe]t\s+no\.\s*\d+/\d+,\s*(\d{1,2})(?:er)?\s+(" + _MONTH_RE + r")\s+(\d{4})",
    re.IGNORECASE,
)


# ============================================================
# Result dataclass
# ============================================================

@dataclass
class DateExtraction:
    """Result of date extraction from decision text."""
    extracted_date: date | None = None
    method: str = ""           # "header", "signoff", "both", "bare", "numeric", "echr"
    confidence: str = "none"   # "high", "medium", "low", "none"
    raw_match: str = ""        # the matched text
    header_date: date | None = None
    signoff_date: date | None = None
    all_dates: list[tuple[date, str]] = field(default_factory=list)

    # Comparison with metadata
    category: str = ""         # MATCH, MISMATCH_FIXABLE, etc.
    metadata_date: date | None = None
    delta_days: int | None = None

    def to_dict(self) -> dict:
        d = {}
        for k in ("method", "confidence", "raw_match", "category"):
            d[k] = getattr(self, k)
        if self.extracted_date:
            d["extracted_date"] = self.extracted_date.isoformat()
        if self.metadata_date:
            d["metadata_date"] = self.metadata_date.isoformat()
        if self.delta_days is not None:
            d["delta_days"] = self.delta_days
        return d


# ============================================================
# Core extraction
# ============================================================

def _parse_match(m: re.Match, numeric: bool = False) -> date | None:
    """Parse a regex match into a date object."""
    try:
        if numeric:
            day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        else:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            month = MONTH_NAMES.get(month_name)
            if not month:
                return None
            year = int(m.group(3))
        if 1800 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
            return date(year, month, day)
    except (ValueError, IndexError):
        pass
    return None


def extract_decision_date(
    full_text: str,
    language: str | None = None,
    metadata_date: date | None = None,
) -> DateExtraction:
    """
    Extract the actual decision date from Swiss court decision full text.

    Args:
        full_text: The full text of the decision.
        language: Language hint ("de", "fr", "it"). If None, tries all.
        metadata_date: The existing metadata date for comparison.

    Returns:
        DateExtraction with extracted date, confidence, and category.
    """
    result = DateExtraction(metadata_date=metadata_date)

    if not full_text or len(full_text) < 50:
        result.category = _categorize(result, metadata_date)
        return result

    # Search zones
    header = full_text[:1500]  # first ~1500 chars for header patterns
    tail = full_text[-1500:]   # last ~1500 chars for sign-off patterns

    header_date = None
    header_match = ""
    header_method = ""
    signoff_date = None
    signoff_match = ""
    all_dates: list[tuple[date, str]] = []

    # --- Header extraction ---

    # Try typed header patterns first (highest confidence)
    header_patterns = [
        (RE_ECHR_HEADER, "echr"),
        (RE_FR_HEADER, "header_fr"),
        (RE_DE_HEADER, "header_de"),
        (RE_IT_HEADER, "header_it"),
    ]

    for pattern, method in header_patterns:
        m = pattern.search(header)
        if m:
            d = _parse_match(m)
            if d:
                header_date = d
                header_match = m.group(0)
                header_method = method
                all_dates.append((d, method))
                break

    # If no typed header, try bare date in first 500 chars only (medium confidence)
    # Restricted to 500 chars to avoid picking up cited decision dates from body
    if not header_date:
        header_narrow = full_text[:500]
        for m in RE_BARE_DATE.finditer(header_narrow):
            d = _parse_match(m)
            if d and 1900 <= d.year:
                # Skip dates that look like statute dates ("vom 17. Juni 2005 (BGG)")
                context = header_narrow[max(0, m.start()-30):m.end()+30]
                if _is_statute_date(context, m.group(0)):
                    continue
                header_date = d
                header_match = m.group(0)
                header_method = "bare_header"
                all_dates.append((d, "bare_header"))
                break

    # --- Sign-off extraction ---

    signoff_patterns = [
        (RE_FR_SIGNOFF, "signoff_fr"),
        (RE_DE_SIGNOFF, "signoff_de"),
        (RE_IT_SIGNOFF, "signoff_it"),
    ]

    # Search from the END of the tail (sign-off is usually the very last thing)
    for pattern, method in signoff_patterns:
        matches = list(pattern.finditer(tail))
        if matches:
            m = matches[-1]  # Take the LAST match (closest to end)
            d = _parse_match(m)
            if d:
                # Skip if this looks like a statute/law date
                context = tail[max(0, m.start()-30):m.end()+30]
                if _is_statute_date(context, m.group(0)):
                    continue
                signoff_date = d
                signoff_match = m.group(0)
                all_dates.append((d, method))
                break

    # --- Determine result ---

    result.header_date = header_date
    result.signoff_date = signoff_date
    result.all_dates = all_dates

    if header_date and signoff_date:
        if header_date == signoff_date:
            # Header and sign-off agree → highest confidence
            result.extracted_date = header_date
            result.method = "both"
            result.confidence = "high"
            result.raw_match = header_match
        else:
            # Header and sign-off disagree — determine which to trust
            delta = abs((header_date - signoff_date).days)
            header_is_typed = header_method.startswith("header") or header_method == "echr"

            if delta <= 7 and header_is_typed:
                # Small disagreement with typed header → trust header
                result.extracted_date = header_date
                result.method = header_method
                result.confidence = "medium"
                result.raw_match = f"{header_match} vs {signoff_match}"
            elif delta > 90 and not header_is_typed:
                # Large disagreement with bare header → trust sign-off
                # (bare_header likely picked up a cited decision date)
                result.extracted_date = signoff_date
                result.method = "signoff"
                result.confidence = "medium"
                result.raw_match = f"{signoff_match} (header had: {header_match})"
            elif delta > 365 and header_is_typed:
                # Very large disagreement even with typed header →
                # the header "Urteil vom" might reference an earlier decision
                # being reviewed; sign-off is the current decision's date
                result.extracted_date = signoff_date
                result.method = "signoff"
                result.confidence = "medium"
                result.raw_match = f"{signoff_match} (header had: {header_match})"
            elif header_is_typed:
                # Moderate disagreement with typed header → trust header
                result.extracted_date = header_date
                result.method = header_method
                result.confidence = "medium"
                result.raw_match = f"{header_match} vs {signoff_match}"
            else:
                # Moderate disagreement with bare header → trust sign-off
                result.extracted_date = signoff_date
                result.method = "signoff"
                result.confidence = "medium"
                result.raw_match = f"{signoff_match} (header had: {header_match})"
    elif header_date:
        result.extracted_date = header_date
        result.method = header_method
        result.confidence = "high" if header_method.startswith("header") or header_method == "echr" else "medium"
        result.raw_match = header_match
    elif signoff_date:
        result.extracted_date = signoff_date
        result.method = "signoff"
        result.confidence = "medium"
        result.raw_match = signoff_match
    else:
        # Last resort: try numeric date in header
        for m in RE_NUMERIC_DATE.finditer(header):
            d = _parse_match(m, numeric=True)
            if d and 1900 <= d.year:
                context = header[max(0, m.start()-30):m.end()+30]
                if _is_statute_date(context, m.group(0)):
                    continue
                result.extracted_date = d
                result.method = "numeric"
                result.confidence = "low"
                result.raw_match = m.group(0)
                all_dates.append((d, "numeric"))
                break

    # --- Compare with metadata ---
    result.category = _categorize(result, metadata_date)
    if result.extracted_date and metadata_date:
        result.delta_days = (metadata_date - result.extracted_date).days

    return result


def _is_statute_date(context: str, match_text: str) -> bool:
    """Check if a date match is likely a statute/law reference, not the decision date."""
    context_lower = context.lower()
    # Patterns that indicate a statute date
    statute_indicators = [
        "gesetz", "bgb", "bgg", "bv ", "or ", "zgb", "stgb", "stpo", "zpo",
        "loi ", "loi du", "loi sur", "loi fédérale",
        "legge", "legge federale",
        "bundesgesetz", "verordnung", "reglement",
        "sr ", "rs ", "(sr", "(rs",
        "du 17 juin 2005",  # BGG
        "du 20 mars 1981",  # LP
        "vom 17. juni 2005",  # BGG
    ]
    for indicator in statute_indicators:
        if indicator in context_lower:
            return True
    return False


def _categorize(result: DateExtraction, metadata_date: date | None) -> str:
    """Categorize the extraction result."""
    if not metadata_date or metadata_date == date(1, 1, 1) or str(metadata_date) == "0000-00-00":
        if result.extracted_date:
            return "NULL_METADATA_FIXABLE"
        return "NULL_METADATA_DATE"

    if not result.extracted_date:
        return "NO_DATE_EXTRACTED"

    if result.extracted_date == metadata_date:
        return "MATCH"

    if result.confidence in ("high", "medium"):
        return "MISMATCH_FIXABLE"

    return "MISMATCH_AMBIGUOUS"


# ============================================================
# CLI: Audit mode
# ============================================================

def _parse_iso_date(s: str | None) -> date | None:
    """Parse ISO date string."""
    if not s:
        return None
    try:
        parts = s.split("-")
        if len(parts) == 3:
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            if y == 0:
                return None
            return date(y, m, d)
    except (ValueError, IndexError):
        pass
    return None


def audit_court(
    jsonl_path: Path,
    sample_size: int = 0,
    verbose: bool = False,
) -> dict:
    """Audit a single court's JSONL file for date mismatches."""
    court_name = jsonl_path.stem
    stats = Counter()
    deltas: list[int] = []
    examples: list[dict] = []

    count = 0
    for line in open(jsonl_path, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        full_text = obj.get("full_text", "") or ""
        lang = obj.get("language", "") or ""
        meta_date_str = obj.get("decision_date", "")
        meta_date = _parse_iso_date(meta_date_str)

        result = extract_decision_date(full_text, language=lang, metadata_date=meta_date)
        stats[result.category] += 1

        if result.delta_days is not None and result.delta_days != 0:
            deltas.append(result.delta_days)

        # Collect examples of mismatches
        if result.category in ("MISMATCH_FIXABLE", "MISMATCH_AMBIGUOUS", "NULL_METADATA_FIXABLE"):
            if len(examples) < 10:
                examples.append({
                    "decision_id": obj.get("decision_id", ""),
                    "metadata_date": meta_date_str,
                    "extracted_date": result.extracted_date.isoformat() if result.extracted_date else None,
                    "method": result.method,
                    "confidence": result.confidence,
                    "raw_match": result.raw_match[:100],
                    "delta_days": result.delta_days,
                })

        count += 1
        if sample_size and count >= sample_size:
            break

    total = sum(stats.values())
    return {
        "court": court_name,
        "total": total,
        "stats": dict(stats),
        "deltas": deltas,
        "examples": examples,
        "avg_delta": sum(abs(d) for d in deltas) / len(deltas) if deltas else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Audit decision dates")
    parser.add_argument("--court", type=str, help="Single court to audit (e.g., fr_gerichte)")
    parser.add_argument("--all-courts", action="store_true", help="Audit all courts")
    parser.add_argument("--sample", type=int, default=0, help="Sample size per court (0=all)")
    parser.add_argument("--decisions-dir", type=Path,
                        default=Path(os.environ.get("DECISIONS_DIR", "output/decisions")))
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--output", type=Path, help="Output JSON report path")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    results = []

    if args.court:
        jsonl = args.decisions_dir / f"{args.court}.jsonl"
        if not jsonl.exists():
            log.error("File not found: %s", jsonl)
            sys.exit(1)
        log.info("Auditing %s (sample=%s)...", args.court, args.sample or "all")
        r = audit_court(jsonl, sample_size=args.sample, verbose=args.verbose)
        results.append(r)
    elif args.all_courts:
        jsonl_files = sorted(args.decisions_dir.glob("*.jsonl"))
        log.info("Auditing %d courts (sample=%s)...", len(jsonl_files), args.sample or "all")
        for jsonl in jsonl_files:
            log.info("  %s...", jsonl.stem)
            r = audit_court(jsonl, sample_size=args.sample, verbose=args.verbose)
            results.append(r)
    else:
        parser.print_help()
        sys.exit(1)

    # Print summary
    print("\n" + "=" * 80)
    print("DATE AUDIT SUMMARY")
    print("=" * 80)

    total_all = 0
    total_match = 0
    total_mismatch = 0
    total_null = 0
    total_no_extract = 0

    for r in results:
        s = r["stats"]
        total = r["total"]
        match = s.get("MATCH", 0)
        mismatch = s.get("MISMATCH_FIXABLE", 0) + s.get("MISMATCH_AMBIGUOUS", 0)
        null_fix = s.get("NULL_METADATA_FIXABLE", 0) + s.get("NULL_METADATA_DATE", 0)
        no_ext = s.get("NO_DATE_EXTRACTED", 0)

        total_all += total
        total_match += match
        total_mismatch += mismatch
        total_null += null_fix
        total_no_extract += no_ext

        if mismatch > 0 or null_fix > 0:
            flag = " *** NEEDS FIX ***" if (mismatch / total > 0.05 if total else False) else ""
            print(f"\n{r['court']} ({total} decisions):{flag}")
            for cat, cnt in sorted(s.items()):
                pct = cnt / total * 100 if total else 0
                print(f"  {cat:25s}: {cnt:6d} ({pct:5.1f}%)")
            if r["deltas"]:
                avg = r["avg_delta"]
                print(f"  Avg |delta|: {avg:.1f} days")
            if r["examples"]:
                print("  Examples:")
                for ex in r["examples"][:5]:
                    print(f"    {ex['decision_id']}: meta={ex['metadata_date']} → "
                          f"extracted={ex['extracted_date']} (Δ{ex['delta_days']}d) "
                          f"[{ex['method']}/{ex['confidence']}] {ex['raw_match'][:60]}")
        else:
            match_pct = match / total * 100 if total else 0
            no_ext_pct = no_ext / total * 100 if total else 0
            print(f"{r['court']:30s}: {total:6d} decisions, "
                  f"{match:5d} match ({match_pct:.0f}%), "
                  f"{no_ext:5d} no extraction ({no_ext_pct:.0f}%)")

    print(f"\n{'TOTAL':30s}: {total_all:6d} decisions")
    print(f"  {'MATCH':25s}: {total_match:6d} ({total_match/total_all*100:.1f}%)" if total_all else "")
    print(f"  {'MISMATCH (fixable+ambig)':25s}: {total_mismatch:6d} ({total_mismatch/total_all*100:.1f}%)" if total_all else "")
    print(f"  {'NULL metadata':25s}: {total_null:6d} ({total_null/total_all*100:.1f}%)" if total_all else "")
    print(f"  {'No extraction':25s}: {total_no_extract:6d} ({total_no_extract/total_all*100:.1f}%)" if total_all else "")

    # Save JSON report
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        log.info("Report saved to %s", args.output)


if __name__ == "__main__":
    main()
