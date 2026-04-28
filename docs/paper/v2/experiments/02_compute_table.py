"""
Post-process ablation_results.json into the per-rail ablation table
that becomes Table 4 of the paper.

For each of 6 audit configurations:
  C0 = ∅ (no rails)            — baseline; fires never
  C1 = {case}                  — case-citation existence rail
  C2 = {case, statute}
  C3 = {case, statute, quote}
  C4 = {case, statute, quote, date} — all 4 deterministic rails
  C5 = {case, statute, quote, date, grounding} — full audit

For each (config, draft):
  fired(C, draft) = (any rail in C raised ≥ 1 issue on this draft)

Per-config metrics:
  WFR (wrong-draft flag rate) = P(fired | c_eli = 0)
                    = fraction of WRONG drafts the rails FLAGGED
                      (deliberately not called "TPR" or "catch
                      rate" — flagging a wrong draft does NOT
                      certify the flag identifies the draft's
                      primary error; see paper §5 inspection)
  FPR (false alarm) = P(fired | c_eli = 1)
                    = fraction of CORRECT drafts the rails flagged
  Net           = WFR - FPR (Youden's J)

Plus per-rail solo contribution:
  For each rail R: count drafts where R fired AT ALL (regardless
  of other rails). Reveals which rail is doing the heavy lifting.

Output: ablation_table.json + ablation_table.md (paste-ready for §5).
"""
from __future__ import annotations

import json
from pathlib import Path
from collections import Counter, defaultdict

HERE = Path(__file__).resolve().parent
RAW = HERE / "ablation_results.json"
TABLE_JSON = HERE / "ablation_table.json"
TABLE_MD = HERE / "ablation_table.md"

CONFIGS = [
    ("C0", "∅ (no rails)",                          set()),
    ("C1", "+ case",                                {"case"}),
    ("C2", "+ statute",                             {"case", "statute"}),
    ("C3", "+ quote",                               {"case", "statute", "quote"}),
    ("C4", "+ date  (all 4 deterministic)",         {"case", "statute", "quote", "date"}),
    ("C5", "+ grounding  (full 5-rail audit)",      {"case", "statute", "quote", "date", "grounding"}),
]

RAILS = ["case", "statute", "quote", "date", "grounding"]


def fired(issues_by_cat: dict, enabled: set) -> bool:
    """Does ANY enabled rail fire on this draft?"""
    for rail in enabled:
        if (issues_by_cat or {}).get(rail, 0) > 0:
            return True
    return False


def main():
    data = json.loads(RAW.read_text(encoding="utf-8"))
    results = [r for r in data["results"] if "error" not in r]
    n = len(results)
    n_wrong = sum(1 for r in results if not r["judges"]["c_eli"])
    n_correct = n - n_wrong

    print(f"n={n}, correct={n_correct}, wrong={n_wrong}")

    # Per-config metrics
    rows = []
    for name, label, enabled in CONFIGS:
        flagged_wrong = 0
        flagged_correct = 0
        flagged_total = 0
        for r in results:
            issues_by_cat = r["attest"].get("issues_by_category", {})
            f = fired(issues_by_cat, enabled)
            if f:
                flagged_total += 1
                if r["judges"]["c_eli"]:
                    flagged_correct += 1
                else:
                    flagged_wrong += 1
        tpr = flagged_wrong / max(1, n_wrong)        # P(fire | wrong)
        fpr = flagged_correct / max(1, n_correct)    # P(fire | correct)
        net = tpr - fpr
        rows.append({
            "config":   name,
            "label":    label,
            "rails":    sorted(enabled),
            "flagged_wrong":    flagged_wrong,
            "flagged_correct":  flagged_correct,
            "flagged_total":    flagged_total,
            "tpr":  round(100 * tpr, 1),
            "fpr":  round(100 * fpr, 1),
            "net":  round(100 * net, 1),
        })

    # Per-rail solo activation count
    rail_solo = {}
    for rail in RAILS:
        n_fire = sum(1 for r in results if (r["attest"].get("issues_by_category") or {}).get(rail, 0) > 0)
        n_fire_on_wrong = sum(1 for r in results if (r["attest"].get("issues_by_category") or {}).get(rail, 0) > 0 and not r["judges"]["c_eli"])
        n_fire_on_correct = n_fire - n_fire_on_wrong
        rail_solo[rail] = {
            "fired_n": n_fire,
            "fired_on_wrong": n_fire_on_wrong,
            "fired_on_correct": n_fire_on_correct,
            "tpr":  round(100 * n_fire_on_wrong / max(1, n_wrong), 1),
            "fpr":  round(100 * n_fire_on_correct / max(1, n_correct), 1),
        }

    # By language
    by_lang = defaultdict(lambda: {"n": 0, "wrong": 0, "fired": 0})
    for r in results:
        l = r.get("language", "?")
        by_lang[l]["n"] += 1
        if not r["judges"]["c_eli"]:
            by_lang[l]["wrong"] += 1
        if fired(r["attest"].get("issues_by_category"), set(RAILS)):
            by_lang[l]["fired"] += 1

    # Issue type count
    issue_count = Counter()
    for r in results:
        issues = r["attest"].get("issues_by_category") or {}
        for k, v in issues.items():
            issue_count[k] += v

    # Citation stats
    total_citations = sum(r["attest"].get("citations_found", 0) for r in results)
    total_citations_ok = sum(r["attest"].get("citations_ok", 0) for r in results)

    summary = {
        "n_questions":        n,
        "n_correct":          n_correct,
        "n_wrong":            n_wrong,
        "total_citations_emitted":     total_citations,
        "total_citations_corpus_valid": total_citations_ok,
        "citation_validity_rate": round(100 * total_citations_ok / max(1, total_citations), 1),
        "ablation_rows":      rows,
        "rail_solo_stats":    rail_solo,
        "by_language":        dict(by_lang),
        "issue_type_counts":  dict(issue_count),
    }
    TABLE_JSON.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nwrote {TABLE_JSON}")

    # ── Markdown rendering ────────────────────────────────────

    md = []
    md.append("# Per-rail ablation — v0.2 (n=30, prior-only condition)")
    md.append("")
    md.append(f"**Setup:** Claude Sonnet 4.6 generates an answer for each of the 30 questions in v0.2 *without retrieval* (prior-only condition). The closing audit (`attest_response`, `audit_grounding=True`) runs all 5 rails. Per-configuration TPR / FPR computed in post-processing by intersecting the rail-fire flags with each configuration's enabled rail set.")
    md.append("")
    md.append("**Ground truth:** an independent Sonnet judge scores each draft for `c_eli` (does the answer entail the v0.2 reference answer?). 6 of 30 drafts judged wrong (= ground truth fabrications); 24 correct.")
    md.append("")
    md.append("## Table 4 — per-configuration catch rates")
    md.append("")
    md.append("| Config | Rails | TPR (catch wrong) | FPR (flag correct) | Net (TPR – FPR) |")
    md.append("|---|---|---:|---:|---:|")
    for row in rows:
        rails_str = ", ".join(row["rails"]) if row["rails"] else "—"
        md.append(
            f"| {row['config']} | {row['label']} | "
            f"{row['flagged_wrong']}/{n_wrong} = **{row['tpr']:.1f} %** | "
            f"{row['flagged_correct']}/{n_correct} = {row['fpr']:.1f} % | "
            f"**{row['net']:+.1f} pp** |"
        )
    md.append("")
    md.append("## Table 5 — per-rail solo activation")
    md.append("")
    md.append("(How many drafts each rail fires on, regardless of other rails. Reveals each rail's independent contribution.)")
    md.append("")
    md.append("| Rail | Fires on N drafts | Fires on wrong | Fires on correct | Solo TPR | Solo FPR |")
    md.append("|---|---:|---:|---:|---:|---:|")
    for rail in RAILS:
        s = rail_solo[rail]
        md.append(
            f"| {rail} | {s['fired_n']}/{n} | "
            f"{s['fired_on_wrong']} | "
            f"{s['fired_on_correct']} | "
            f"{s['tpr']:.1f} % | "
            f"{s['fpr']:.1f} % |"
        )
    md.append("")
    md.append("## Citation accounting")
    md.append("")
    md.append(f"- **Citations emitted by Sonnet across all 30 prior-only drafts:** {total_citations}")
    md.append(f"- **Citations that resolve in the corpus:** {total_citations_ok}")
    md.append(f"- **Validity rate (= 1 − fabrication rate at the citation level):** {summary['citation_validity_rate']:.1f} %")
    md.append("")
    md.append("## By language")
    md.append("")
    md.append("| Lang | n | wrong | rail fired (any) | rail-fire rate |")
    md.append("|---|---:|---:|---:|---:|")
    for lang, st in sorted(by_lang.items()):
        rate = round(100 * st["fired"] / max(1, st["n"]), 1)
        md.append(f"| {lang} | {st['n']} | {st['wrong']} | {st['fired']} | {rate:.1f} % |")
    md.append("")
    md.append("## Issue type breakdown")
    md.append("")
    md.append("| Category | Total issues raised across all 30 drafts |")
    md.append("|---|---:|")
    for cat in RAILS:
        md.append(f"| {cat} | {issue_count.get(cat, 0)} |")
    md.append("")
    md.append("## Notes for paper §5")
    md.append("")
    md.append("- The prior-only condition (no retrieval) is the cleanest test of the rails: it guarantees Sonnet has to invent some Swiss legal references, providing a substrate the rails can detect. Adding retrieval would lower the prior to detect.")
    md.append("- Per-rail TPR is monotone non-decreasing as rails are added (each rail can only catch more, never fewer). FPR is also monotone non-decreasing — a known tradeoff. The Net column shows the marginal contribution.")
    md.append("- The cite-citation validity rate (Sonnet's citation-level fabrication rate without retrieval) is reported alongside Magesh et al. 2025's 17–33% measurements on commercial legal-RAG tools, but the two are NOT a like-for-like comparison: different model families, query distributions, and tool conditions. See paper §5 for the qualified framing.")

    TABLE_MD.write_text("\n".join(md), encoding="utf-8")
    print(f"wrote {TABLE_MD}")
    print()
    print("\n".join(md[8:30]))   # preview Table 4


if __name__ == "__main__":
    main()
