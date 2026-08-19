"""Regenerate every number, table and figure input for the P2 paper from
data/p2_backscan.json (+ probe results when present). No number in
paper.tex is transcribed by hand: the paper \\inputs macros.tex and the
table fragments this script writes.

Statistics:
  - Wilson 95% intervals for all proportions (z = 1.959964).
  - Cluster bootstrap by DECISION for the headline rate: findings cluster
    inside decisions (boilerplate repeats), so token-level Wilson is
    anti-conservative. The scan emits a Counter over per-decision
    (language, year, court_class, tokens, findings) types; we resample
    decisions via one multinomial draw over types per replicate
    (10,000 replicates, fixed seed) — equivalent to resampling the 86,946
    decisions with replacement, at 1/60,000th of the memory.

Run:  .venv/bin/python3 docs/paper/p2-citations/tables/build_tables.py
      (or: make paper2-tables)
Requires numpy (bootstrap only).
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
TAB = ROOT / "tables"

S = json.loads((DATA / "p2_backscan.json").read_text())
PROBE_PATH = DATA / "p2_probe.json"
PROBE = json.loads(PROBE_PATH.read_text()) if PROBE_PATH.exists() else None

Z = 1.959964
SEED = 20260813
B = 10_000

LANG_NAMES = {"de": "German", "fr": "French", "it": "Italian"}
MECH_NAMES = {
    "division_substitution": "Division substitution",
    "volume_substitution": "Volume substitution",
    "page_extra_digit": "Extra digit in page",
    "page_doubled_digit": "Doubled digit in page",
    "page_transposition": "Transposed page digits",
    "year_for_page": "Year in page position",
    "dropped_leading_volume_digit": "Dropped leading volume digit",
    "unlabelled": "Unlabelled residual",
}


def wilson(k: int, n: int):
    """Wilson 95% interval for k/n, returned in ppm."""
    if n == 0:
        return 0.0, 0.0
    p = k / n
    d = 1 + Z * Z / n
    c = p + Z * Z / (2 * n)
    h = Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n))
    return 1e6 * (c - h) / d, 1e6 * (c + h) / d


def ppm(k: int, n: int) -> float:
    return 1e6 * k / max(1, n)


def fmt_int(n) -> str:
    return f"{n:,}".replace(",", "{,}")


def fmt_ppm(x: float) -> str:
    return f"{x:,.0f}".replace(",", "{,}")


def write(name: str, content: str):
    p = TAB / name
    p.write_text(content)
    print(f"  wrote {p.relative_to(ROOT)}")


# ── pull the scan's own numbers ──────────────────────────────────────────
N_TOK = S["denominator_prefixed_total"]
N_FIND = S["findings_primary"]
DEN_L = S["denominator_prefixed_by_language"]
FIND_L = S["findings_by_language"]
N_DEC_TOK = S["decisions_with_prefixed_token"]
N_DEC_FIND = S["decisions_with_finding"]
N_DISTINCT_REF = S["denominator_distinct_refs_total"]
N_DISTINCT_TOK = S["distinct_tokens"]
QUOTE_N = S["findings_with_quote_marker"]
POOL = S["pre1955_pool"]
MECH = S["findings_by_mechanism"]
BY_YEAR = S["findings_by_year"]
BY_CLASS = S["findings_by_court_class"]

# Full per-token multiplicity and mechanism×language cross from the
# findings file itself. The summary's tokens_repeated is capped at
# most_common(15) — deriving dedup numbers from it silently deduplicated
# only the top fifteen tokens (caught in review 2026-08-13).
from collections import Counter as _Counter

tok_full = _Counter()
mech_lang = {}
mech_multi = mech_single = mech_none = 0
finding_tokens = []
with open(DATA / "p2_findings.jsonl") as fh:
    for line in fh:
        f = json.loads(line)
        if not f.get("qualified") or f.get("form") != "prefixed":
            continue
        tok_full[f["token"]] += 1
        finding_tokens.append(f["token"])
        nlab = len(f.get("mechanism_labels") or [])
        if nlab == 0:
            mech_none += 1
        elif nlab == 1:
            mech_single += 1
        else:
            mech_multi += 1
        mech_lang.setdefault(f["mechanism"], {}).setdefault(f["language"], 0)
        mech_lang[f["mechanism"]][f["language"]] += 1
assert sum(tok_full.values()) == N_FIND
assert len(tok_full) == N_DISTINCT_TOK

# provability tiers: findings whose distinct token the resolver probe
# confirmed (404) vs tokens below its coverage floor (vol < 80)
tier_conf = tier_ooc = 0
if PROBE:
    status = {r["token"]: r["status"] for r in PROBE["results"]}
    for t in finding_tokens:
        s = status.get(t)
        if s == "confirmed_nonexistent":
            tier_conf += 1
        elif s == "out_of_coverage":
            tier_ooc += 1

rate = ppm(N_FIND, N_TOK)
w_lo, w_hi = wilson(N_FIND, N_TOK)

# ── cluster bootstrap over pair types ────────────────────────────────────
types = []
for key, count in S["cluster_pairs"].items():
    lang, year, cls, t, f = key.rsplit("|", 4)
    types.append((int(t), int(f), int(count), lang, year, cls))
tok_arr = np.array([t for t, f, c, *_ in types], dtype=np.float64)
fnd_arr = np.array([f for t, f, c, *_ in types], dtype=np.float64)
cnt_arr = np.array([c for t, f, c, *_ in types], dtype=np.float64)
n_clusters = int(cnt_arr.sum())
assert n_clusters == N_DEC_TOK, (n_clusters, N_DEC_TOK)
# sanity: pair-type totals must reproduce the scan's own totals
assert int((tok_arr * cnt_arr).sum()) == N_TOK
assert int((fnd_arr * cnt_arr).sum()) == N_FIND

rng = np.random.default_rng(SEED)
draws = rng.multinomial(n_clusters, cnt_arr / n_clusters, size=B)
boot_rates = 1e6 * (draws @ fnd_arr) / (draws @ tok_arr)
b_lo, b_hi = np.percentile(boot_rates, [2.5, 97.5])

# per-decision and per-distinct views
d_rate = ppm(N_DEC_FIND, N_DEC_TOK)
d_lo, d_hi = wilson(N_DEC_FIND, N_DEC_TOK)
t_rate = ppm(N_DISTINCT_TOK, N_DISTINCT_REF)
t_lo, t_hi = wilson(N_DISTINCT_TOK, N_DISTINCT_REF)

# sensitivity views — from the FULL token multiplicity, not the capped
# summary field. Counting each distinct token once is exactly the
# distinct-token count.
rep_tokens = sum(1 for c in tok_full.values() if c > 1)
singletons = sum(1 for c in tok_full.values() if c == 1)
rep_total = N_FIND - singletons             # findings from tokens seen >1×
sens_dedup_k = len(tok_full)                # each distinct token once
assert sens_dedup_k == N_DISTINCT_TOK
sens_quote_k = N_FIND - QUOTE_N

# ── macros.tex ───────────────────────────────────────────────────────────
scan_date = S["generated_at"][:10]
graph_date = S["graph_mtime"][:10]
one_in = int(round(N_TOK / N_FIND))

m = []
m.append("% Auto-generated by tables/build_tables.py — do not edit.")
m.append(f"% Source: data/p2_backscan.json (generated {S['generated_at']},"
         f" graph {S['graph_mtime']})")
m.append(f"\\newcommand{{\\ScanSince}}{{{S['since'][:4]}-01-01}}")
m.append(f"\\newcommand{{\\ScanDate}}{{{scan_date}}}")
m.append(f"\\newcommand{{\\ScanGraphDate}}{{{graph_date}}}")
m.append(f"\\newcommand{{\\NWindowDecisions}}{{{fmt_int(S['decisions_in_window'])}}}")
m.append(f"\\newcommand{{\\NDenomTokens}}{{{fmt_int(N_TOK)}}}")
m.append(f"\\newcommand{{\\NFindings}}{{{N_FIND}}}")
m.append(f"\\newcommand{{\\RatePpm}}{{{fmt_ppm(rate)} per million"
         f" (one in {fmt_int(one_in)})}}")
m.append(f"\\newcommand{{\\RatePpmBare}}{{{fmt_ppm(rate)}}}")
m.append(f"\\newcommand{{\\RateWilson}}{{{fmt_ppm(w_lo)}--{fmt_ppm(w_hi)}}}")
m.append(f"\\newcommand{{\\RateCluster}}{{{fmt_ppm(b_lo)}--{fmt_ppm(b_hi)}}}")
m.append(f"\\newcommand{{\\NDecTok}}{{{fmt_int(N_DEC_TOK)}}}")
m.append(f"\\newcommand{{\\NDecFind}}{{{N_DEC_FIND}}}")
m.append(f"\\newcommand{{\\DecShare}}{{{100 * N_DEC_FIND / N_DEC_TOK:.2f}\\,\\%}}")
m.append(f"\\newcommand{{\\NDistinctRefs}}{{{fmt_int(N_DISTINCT_REF)}}}")
m.append(f"\\newcommand{{\\NDistinctTokens}}{{{N_DISTINCT_TOK}}}")
m.append(f"\\newcommand{{\\DistinctShare}}{{{100 * N_DISTINCT_TOK / N_DISTINCT_REF:.2f}\\,\\%}}")
for lang in ("de", "fr", "it"):
    L = lang.capitalize()
    m.append(f"\\newcommand{{\\NFindings{L}}}{{{FIND_L.get(lang, 0)}}}")
    m.append(f"\\newcommand{{\\Rate{L}}}{{{fmt_ppm(ppm(FIND_L.get(lang, 0), DEN_L.get(lang, 1)))}}}")
m.append(f"\\newcommand{{\\NQuoteMarker}}{{{QUOTE_N}}}")
m.append(f"\\newcommand{{\\QuoteShare}}{{{100 * QUOTE_N / N_FIND:.1f}\\,\\%}}")
m.append(f"\\newcommand{{\\NPoolPre}}{{{fmt_int(POOL['n'])}}}")
m.append(f"\\newcommand{{\\PoolPpm}}{{{fmt_ppm(POOL['rate_ppm'])}}}")
m.append(f"\\newcommand{{\\NPoolPlausible}}{{{POOL['plus100_plausible']}}}")
m.append(f"\\newcommand{{\\NPoolStart}}{{{POOL['plus100_start']}}}")
m.append(f"\\newcommand{{\\NBareFindings}}{{{S['bare_channel']['findings_qualified']}}}")
m.append(f"\\newcommand{{\\NRawFindings}}{{{fmt_int(S['findings_raw'])}}}")
m.append(f"\\newcommand{{\\NDroppedGuard}}{{{S['dropped_by_guard']}}}")
m.append(f"\\newcommand{{\\NMechDivision}}{{{MECH.get('division_substitution', 0)}}}")
m.append(f"\\newcommand{{\\NMechVolume}}{{{MECH.get('volume_substitution', 0)}}}")
m.append(f"\\newcommand{{\\NMechYear}}{{{MECH.get('year_for_page', 0)}}}")
m.append(f"\\newcommand{{\\NMechDropped}}{{{MECH.get('dropped_leading_volume_digit', 0)}}}")
m.append(f"\\newcommand{{\\NMechPageDigit}}{{{MECH.get('page_extra_digit', 0) + MECH.get('page_doubled_digit', 0) + MECH.get('page_transposition', 0)}}}")
m.append(f"\\newcommand{{\\NMechUnlabelled}}{{{MECH.get('unlabelled', 0)}}}")
# propagation
top_tok, top_n = tok_full.most_common(1)[0]
m.append(f"\\newcommand{{\\NRepeatTokens}}{{{rep_tokens}}}")
m.append(f"\\newcommand{{\\NRepeatFindings}}{{{rep_total}}}")
m.append(f"\\newcommand{{\\TopToken}}{{{top_tok}}}")
m.append(f"\\newcommand{{\\TopTokenN}}{{{top_n}}}")
# sensitivity
m.append(f"\\newcommand{{\\SensDedupRate}}{{{fmt_ppm(ppm(sens_dedup_k, N_TOK))}}}")
m.append(f"\\newcommand{{\\SensQuoteRate}}{{{fmt_ppm(ppm(sens_quote_k, N_TOK))}}}")
# probe (filled once data/p2_probe.json exists)
if PROBE:
    m.append(f"\\newcommand{{\\NProbeTokens}}{{{PROBE['n_tokens']}}}")
    m.append(f"\\newcommand{{\\NProbeConfirmed}}{{{PROBE['n_confirmed_nonexistent']}}}")
    m.append(f"\\newcommand{{\\NProbeExists}}{{{PROBE['n_exists']}}}")
    m.append(f"\\newcommand{{\\NProbeOOC}}{{{PROBE['n_out_of_coverage']}}}")
    m.append(f"\\newcommand{{\\NProbeCand}}{{{PROBE['n_candidates_probed']}}}")
    m.append(f"\\newcommand{{\\NProbeCandExist}}{{{PROBE['n_candidates_exist']}}}")
    m.append(f"\\newcommand{{\\NFindConfirmed}}{{{tier_conf}}}")
    m.append(f"\\newcommand{{\\NFindOOC}}{{{tier_ooc}}}")
# mechanism signature multiplicity (review #13: labels are signatures)
m.append(f"\\newcommand{{\\NMechMulti}}{{{mech_multi}}}")
m.append(f"\\newcommand{{\\NMechSingle}}{{{mech_single}}}")
m.append(f"\\newcommand{{\\NMechNone}}{{{mech_none}}}")
write("macros.tex", "\n".join(m) + "\n")

# ── table: three rate views ──────────────────────────────────────────────
write("rates.tex", f"""% Auto-generated by tables/build_tables.py
\\begin{{table}}[t]
\\centering\\small
\\caption{{Provably nonexistent reporter citations in Swiss decisions
issued since \\ScanSince{{}} (scan of {scan_date}, graph of {graph_date}).
Wilson 95\\,\\% intervals; the headline per-token rate additionally
carries a cluster-bootstrap interval by decision
(10{{,}}000 replicates).}}
\\label{{tab:rates}}
\\begin{{tabular}}{{l r r r l}}
\\toprule
Unit & Errors & Universe & Rate (ppm) & 95\\,\\% CI (ppm) \\\\
\\midrule
Citation tokens & {N_FIND} & {fmt_int(N_TOK)} & {fmt_ppm(rate)} &
  {fmt_ppm(w_lo)}--{fmt_ppm(w_hi)} (cluster {fmt_ppm(b_lo)}--{fmt_ppm(b_hi)}) \\\\
Decisions ($\\geq$1 finding) & {N_DEC_FIND} & {fmt_int(N_DEC_TOK)} &
  {fmt_ppm(d_rate)} & {fmt_ppm(d_lo)}--{fmt_ppm(d_hi)} \\\\
Distinct cited loci & {N_DISTINCT_TOK} & {fmt_int(N_DISTINCT_REF)} &
  {fmt_ppm(t_rate)} & {fmt_ppm(t_lo)}--{fmt_ppm(t_hi)} \\\\
\\bottomrule
\\end{{tabular}}
\\end{{table}}
""")

# ── table: per-language ──────────────────────────────────────────────────
rows = []
for lang in ("de", "fr", "it"):
    k, n = FIND_L.get(lang, 0), DEN_L.get(lang, 0)
    lo, hi = wilson(k, n)
    rows.append(f"{LANG_NAMES[lang]} & {k} & {fmt_int(n)} & "
                f"{fmt_ppm(ppm(k, n))} & {fmt_ppm(lo)}--{fmt_ppm(hi)} \\\\")
write("languages.tex", f"""% Auto-generated by tables/build_tables.py
\\begin{{table}}[t]
\\centering\\small
\\caption{{Per-language rates over distinct prefixed citation edges,
with Wilson 95\\,\\% intervals.}}
\\label{{tab:languages}}
\\begin{{tabular}}{{l r r r l}}
\\toprule
Language & Errors & Tokens & Rate (ppm) & Wilson 95\\,\\% \\\\
\\midrule
{chr(10).join(rows)}
\\bottomrule
\\end{{tabular}}
\\end{{table}}
""")

# ── table: mechanisms × language (cross computed above) ──────────────────
order = sorted(MECH, key=lambda k: -MECH[k])
rows = []
for mk in order:
    ml = mech_lang.get(mk, {})
    rows.append(f"{MECH_NAMES.get(mk, mk)} & {ml.get('de', 0)} & "
                f"{ml.get('fr', 0)} & {ml.get('it', 0)} & {MECH[mk]} \\\\")
write("mechanisms.tex", f"""% Auto-generated by tables/build_tables.py
\\begin{{table}}[t]
\\centering\\small
\\caption{{Deterministic mechanism labels over all \\NFindings{{}}
findings: each label is a pure function of the token and the series
index (Section~\\ref{{sec:taxonomy}}). A finding may satisfy several
rules; the first in priority order is counted.}}
\\label{{tab:mechanisms}}
\\begin{{tabular}}{{l r r r r}}
\\toprule
Mechanism & DE & FR & IT & Total \\\\
\\midrule
{chr(10).join(rows)}
\\bottomrule
\\end{{tabular}}
\\end{{table}}
""")

# ── table: sensitivity ───────────────────────────────────────────────────
# the cluster pairs partition the DENOMINATOR by class and year too:
# tokens per stratum = sum t*count over matching pair types.
den_class = {}
den_year = {}
for t, f, c, lang, year, cls in types:
    den_class[cls] = den_class.get(cls, 0) + t * c
    den_year[year] = den_year.get(year, 0) + t * c
class_rows = []
for cls in ("federal", "cantonal", "other"):
    k, n = BY_CLASS.get(cls, 0), den_class.get(cls, 0)
    class_rows.append(f"{cls.capitalize()} courts & {k} & "
                      f"{fmt_int(n)} & {fmt_ppm(ppm(k, n))} \\\\")
# two-proportion z-test, federal vs cantonal (normal approximation) —
# review #12: state the difference, do not assert equality
k1, n1 = BY_CLASS.get("federal", 0), den_class.get("federal", 1)
k2, n2 = BY_CLASS.get("cantonal", 0), den_class.get("cantonal", 1)
p_pool = (k1 + k2) / (n1 + n2)
se = math.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2))
z = (k1 / n1 - k2 / n2) / se if se else 0.0
p_fc = 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))
# language homogeneity chi-square (2 df) via Pearson
chi = 0.0
for lang in ("de", "fr", "it"):
    k, n = FIND_L.get(lang, 0), DEN_L.get(lang, 1)
    e = n * (N_FIND / N_TOK)
    chi += (k - e) ** 2 / e + ((n - k) - (n - e)) ** 2 / (n - e)
year_rows = []
for y in sorted(BY_YEAR):
    k, n = BY_YEAR[y], den_year.get(y, 0)
    year_rows.append(f"Issued {y} & {k} & {fmt_int(n)} & "
                     f"{fmt_ppm(ppm(k, n))} \\\\")
write("sensitivity.tex", f"""% Auto-generated by tables/build_tables.py
\\begin{{table}}[t]
\\centering\\small
\\caption{{Sensitivity and strata. Every row divides errors by the
matching edge universe from the same single-pass cluster structure as
the headline; the 2026 row covers the partial year through the scan
date.}}
\\label{{tab:sensitivity}}
\\begin{{tabular}}{{l r r r}}
\\toprule
View & Errors & Universe & Rate (ppm) \\\\
\\midrule
Headline & {N_FIND} & {fmt_int(N_TOK)} & {fmt_ppm(rate)} \\\\
Excluding quotation-marked contexts & {sens_quote_k} & {fmt_int(N_TOK)} &
  {fmt_ppm(ppm(sens_quote_k, N_TOK))} \\\\
{chr(10).join(class_rows)}
{chr(10).join(year_rows)}
\\bottomrule
\\end{{tabular}}
\\end{{table}}
""")
# expose class rates + tests as macros for prose
m2 = [f"\\newcommand{{\\RateFederal}}{{{fmt_ppm(ppm(BY_CLASS.get('federal', 0), den_class.get('federal', 1)))}}}",
      f"\\newcommand{{\\RateCantonal}}{{{fmt_ppm(ppm(BY_CLASS.get('cantonal', 0), den_class.get('cantonal', 1)))}}}",
      f"\\newcommand{{\\NFederalFindings}}{{{BY_CLASS.get('federal', 0)}}}",
      f"\\newcommand{{\\PFedCant}}{{{p_fc:.2f}}}",
      f"\\newcommand{{\\ChiLang}}{{{chi:.1f}}}",
      f"\\newcommand{{\\SensDedupRateDistinct}}{{{fmt_ppm(t_rate)}}}"]
with open(TAB / "macros.tex", "a") as fh:
    fh.write("\n".join(m2) + "\n")

# ── comparability table (static definitions, cited sources) ──────────────
write("comparability.tex", """% Auto-generated by tables/build_tables.py
% Definitions extracted from the primary sources (novelty-audit.md).
\\begin{table*}[t]
\\centering\\small
\\caption{What each study measures. The rates are not directly
comparable --- the units differ --- which is precisely why the human
baseline must be stated in its own unit rather than inferred from the
machine studies.}
\\label{tab:comparability}
\\begin{tabular}{L{2.6cm} L{3.6cm} L{3.6cm} L{4.6cm}}
\\toprule
 & Dahl et al.~2024 & Magesh et al.~2025 & This study \\\\
\\midrule
Unit & response to a case-law query & response to a legal research
query & distinct citation edge in a published decision \\\\
Population & 4 general LLMs; stratified federal case sample & 3
commercial legal-RAG tools + GPT-4 & every Swiss decision issued since
2024 in the corpus \\\\
Universe & 14 QA tasks: 9 reference-based, 5 reference-free;
$n{=}5{,}000$ per court level, $n{=}100$ high-complexity & 202
preregistered queries & \\NDenomTokens{} prefixed reporter-citation
edges \\\\
Detection & reference-based: contradiction with known metadata;
reference-free: self-contradiction & human coding of responses and
cited sources & decidable nonexistence against the complete reporter
series, cross-checked on the court's resolver \\\\
Error definition & response inconsistent with legal facts (existence,
court, citation, author, disposition, \\ldots) & false statement, or
source that does not support the claim & cited locus provably absent
from the series \\\\
Includes wrong-but-existent citations & yes & yes & no (floor) \\\\
Headline & 58--88\\,\\% of responses, reference-based tasks pooled by
model & 17--33\\,\\% of responses (GPT-4 43\\,\\%) & \\RatePpmBare{}
ppm of citations \\\\
\\bottomrule
\\end{tabular}
\\end{table*}
""")

# ── decidability figure (TikZ, from the series index) ────────────────────
IDX = json.loads((DATA / "bge_series_index.json").read_text())["families"]


def fam(vol, div):
    return IDX.get(f"{vol}|{div}", {"starts": [], "window": 30})

# Panel data: BGE 139 II (real example DTF 139 II 4040 → 139 II 404).
f139 = fam(139, "II")
last139 = f139["starts"][-1]
w139 = f139["window"]
# Division panel: BGE 148 I vs IV (real example 148 I 356 → 148 IV 356).
f148i = fam(148, "I")
f148iv = fam(148, "IV")
write("figure_decidability.tex", f"""% Auto-generated by tables/build_tables.py
% Data: bge_series_index.json — BGE 139 II starts (last {last139},
% window {w139}); BGE 148 I last start {f148i['starts'][-1]};
% 356 {'IS' if 356 in f148iv['starts'] else 'IS NOT'} a start in 148 IV.
\\begin{{figure}}[t]
\\centering
\\begin{{tikzpicture}}[x=0.0145cm, y=1cm, font=\\scriptsize]
% ── panel 1: page axis of BGE 139 II ──
\\node[anchor=west] at (-70, 1.05) {{\\textbf{{BGE 139 II}} ---
  decisions begin at {len(f139['starts'])} known pages}};
\\draw[->] (0,0) -- (640,0) node[right] {{page}};
\\foreach \\p in {{{', '.join(str(p) for p in f139['starts'])}}}
  \\draw[gray] (\\p, 0) -- (\\p, 0.16);
\\draw[very thick] ({last139},0) -- ({last139},0.3);
\\node[anchor=east] at ({last139 - 6}, 0.34) {{last start {last139}}};
\\draw[thick, dashed] ({last139 + w139},-0.12) -- ({last139 + w139},0.46);
\\node[anchor=west] at ({last139 + w139 + 6}, -0.24)
  {{limit {last139}$+${w139}}};
\\node[red!70!black, anchor=west] at ({last139 + w139 + 6}, 0.52)
  {{\\textbf{{DTF 139 II 4040}}: provably outside}};
\\node[anchor=west, gray] at (0, -0.34) {{any page $\\leq$
  {last139 + w139} may be a deep pin-cite --- never flagged}};
% ── panel 2: division absent / substitution ──
\\node[anchor=west] at (-70, -1.15) {{\\textbf{{BGE 148 I 356}} ---
  division I ends at {f148i['starts'][-1]}$+${f148i['window']};
  page 356 is a decision start under division IV}};
\\draw[->] (0,-1.75) -- (400,-1.75) node[right] {{148 I}};
\\foreach \\p in {{{', '.join(str(p) for p in f148i['starts'])}}}
  \\draw[gray] (\\p,-1.75) -- (\\p,-1.61);
\\node[red!70!black] at (356,-1.5) {{$\\times$ 356}};
\\draw[->] (0,-2.45) -- (560,-2.45) node[right] {{148 IV}};
\\foreach \\p in {{{', '.join(str(p) for p in f148iv['starts'][:60])}}}
  \\draw[gray] (\\p,-2.45) -- (\\p,-2.31);
\\draw[very thick, teal!70!black] (356,-2.45) -- (356,-2.24)
  node[above] {{356 exists}};
\\draw[->, red!70!black] (356,-1.66) to[bend left=18] (362,-2.2);
\\end{{tikzpicture}}
\\caption{{Decidability on the closed series, drawn from the released
index. Top: continuous pagination fixes, for every (volume, division),
the last decision start and an adaptive window; only pages beyond that
limit are flagged. Bottom: \\emph{{BGE 148 I 356}} is provably
nonexistent --- and the same page is a decision start under division IV,
the deterministic \\emph{{division substitution}} signature.}}
\\label{{fig:decidability}}
\\end{{figure}}
""")

# ── release manifest: SHA-256 over every data artifact ───────────────────
import hashlib

manifest = {}
for p in sorted(DATA.iterdir()):
    if p.name == "MANIFEST.json" or p.name.startswith("."):
        continue
    h = hashlib.sha256(p.read_bytes()).hexdigest()
    manifest[p.name] = {"sha256": h, "bytes": p.stat().st_size}
DB_HASHES = {}
if (DATA / "db_hashes.json").exists():
    DB_HASHES = {k: v for k, v in
                 json.loads((DATA / "db_hashes.json").read_text()).items()
                 if not k.startswith("_")}
(DATA / "MANIFEST.json").write_text(json.dumps({
    "_": "P2 release artifacts; regenerate all paper numbers via "
         "tables/build_tables.py",
    "scan_generated_at": S["generated_at"],
    "graph_mtime": S["graph_mtime"],
    "scanned_databases": DB_HASHES,
    "files": manifest,
}, indent=1))
print(f"  wrote data/MANIFEST.json ({len(manifest)} artifacts)")

print(f"""
summary:
  headline   {N_FIND}/{fmt_int(N_TOK)} = {rate:.1f} ppm
             Wilson {w_lo:.0f}-{w_hi:.0f}; cluster bootstrap {b_lo:.0f}-{b_hi:.0f}
  decisions  {N_DEC_FIND}/{N_DEC_TOK} = {d_rate:.0f} ppm ({100 * N_DEC_FIND / N_DEC_TOK:.2f}%)
  distinct   {N_DISTINCT_TOK}/{N_DISTINCT_REF} = {t_rate:.0f} ppm
  sensitivity: dedup {ppm(sens_dedup_k, N_TOK):.0f} ppm, no-quote {ppm(sens_quote_k, N_TOK):.0f} ppm
  probe: {'present' if PROBE else 'absent (data/p2_probe.json not yet built)'}""")
