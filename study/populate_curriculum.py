"""One-time script: query find_leading_cases to populate curriculum files.

Usage: python3 -m study.populate_curriculum

Queries the local mcp_server internals to find most-cited BGE per statute range,
then prints candidates for manual curation into curriculum JSON files.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Add repo root to path so we can import mcp_server internals
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp_server import _find_leading_cases


QUERIES = [
    # Vertragsrecht
    ("vertragsrecht", "vertragsschluss", "OR", "1", "Vertragsschluss"),
    ("vertragsrecht", "vertragsschluss", "OR", "18", "Vertragsauslegung"),
    ("vertragsrecht", "willensmangel", "OR", "23", "Willensmängel Irrtum"),
    ("vertragsrecht", "erfullung", "OR", "97", "Leistungsstörungen"),
    ("vertragsrecht", "erfullung", "OR", "107", "Verzug Rücktritt"),
    # Haftpflicht
    ("haftpflicht", "verschuldenshaftung", "OR", "41", "Verschuldenshaftung"),
    ("haftpflicht", "kausalitat", "OR", "42", "Schadensbeweis"),
    ("haftpflicht", "gefahrdungshaftung", "OR", "55", "Geschäftsherrenhaftung"),
    # Sachenrecht
    ("sachenrecht", "eigentum", "ZGB", "641", "Eigentum"),
    ("sachenrecht", "besitz", "ZGB", "919", "Besitz"),
    ("sachenrecht", "grundbuch", "ZGB", "942", "Grundbuch"),
    # Grundrechte
    ("grundrechte", "rechtsgleichheit", "BV", "8", "Rechtsgleichheit"),
    ("grundrechte", "personliche_freiheit", "BV", "10", "Persönliche Freiheit"),
    ("grundrechte", "wirtschaftsfreiheit", "BV", "27", "Wirtschaftsfreiheit"),
    ("grundrechte", "verfahrensgarantien", "BV", "29", "Verfahrensgarantien"),
    # Strafrecht AT
    ("strafrecht_at", "vorsatz", "StGB", "12", "Vorsatz Fahrlässigkeit"),
    ("strafrecht_at", "versuch", "StGB", "22", "Versuch"),
    ("strafrecht_at", "teilnahme", "StGB", "24", "Anstiftung Gehilfenschaft"),
    ("strafrecht_at", "sanktionen", "StGB", "47", "Strafzumessung"),
]


def main():
    for area, module, law_code, article, label in QUERIES:
        print(f"\n{'='*60}")
        print(f"{area}/{module}: {law_code} Art. {article} — {label}")
        print(f"{'='*60}")
        result = _find_leading_cases(
            law_code=law_code,
            article=article,
            court="bge",
            limit=15,
        )
        if "error" in result:
            print(f"  ERROR: {result['error']}")
            continue
        for case in result.get("cases", []):
            did = case.get("decision_id", "?")
            docket = case.get("docket_number", "?")
            date = case.get("decision_date", "?")
            count = case.get("citation_count", 0)
            print(f"  {did} | {docket} | {date} | cited {count}x")


if __name__ == "__main__":
    main()
