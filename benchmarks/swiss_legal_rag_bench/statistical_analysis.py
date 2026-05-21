"""Statistical analysis layer over the v1.1 RAG-bench + cross-lingual results.

Adds the analyses the paper-v1.2 review (2026-05-21) flagged as missing:

  - Wilson 95% CIs on every reported proportion
  - Paired McNemar exact test on prior-only vs RAG-aug correctness
  - Bootstrap 10k-resample 95% CIs on per-cell MRR@10 + Hit@10
  - Cohen's h effect size on prior-only vs RAG-aug

The output is a single JSON manifest that the paper tables can read from.
This is a *post-hoc* analysis over already-collected per-question outcomes;
it does not re-run any generator or judge call.

Usage:
    python -m benchmarks.swiss_legal_rag_bench.statistical_analysis

Writes:
    benchmarks/swiss_legal_rag_bench/results/statistical_analysis.json
"""
from __future__ import annotations

import json
import math
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "benchmarks" / "swiss_legal_rag_bench" / "results"


def wilson_ci(k: int, n: int, alpha: float = 0.05) -> tuple[float, float]:
    """Wilson score 95% CI for a binomial proportion. Robust at small n
    where the normal-approximation CI breaks down."""
    if n == 0:
        return (0.0, 0.0)
    z = 1.959963984540054  # qnorm(0.975)
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))


def cohen_h(p1: float, p2: float) -> float:
    """Cohen's h effect size for two proportions. Conventional cutoffs:
    h=0.2 small, h=0.5 medium, h=0.8 large."""
    phi1 = 2 * math.asin(math.sqrt(p1))
    phi2 = 2 * math.asin(math.sqrt(p2))
    return abs(phi1 - phi2)


def mcnemar_exact(b: int, c: int) -> dict:
    """Paired McNemar exact (binomial) test on the discordant pairs.

    b = pairs where condition-1 correct, condition-2 wrong
    c = pairs where condition-1 wrong, condition-2 correct
    """
    n = b + c
    if n == 0:
        return {"n_discordant": 0, "p_two_sided": 1.0, "interpretation": "no discordant pairs"}
    # Exact two-sided binomial test under H0: p = 0.5
    k = min(b, c)
    # sum P(X <= k) under Binomial(n, 0.5), times 2 for two-sided (clip to 1)
    cdf = sum(math.comb(n, i) * 0.5**n for i in range(k + 1))
    p = min(1.0, 2 * cdf)
    return {
        "n_discordant": n,
        "b_corrected_to_wrong": b,
        "c_wrong_to_corrected": c,
        "p_two_sided": p,
        "test": "McNemar exact (binomial on discordant pairs)",
    }


def bootstrap_ci(values: list[float], stat=lambda v: sum(v) / len(v) if v else 0.0,
                 n_resamples: int = 10000, alpha: float = 0.05,
                 seed: int = 0) -> tuple[float, float, float]:
    """Percentile bootstrap CI for an arbitrary statistic."""
    rng = random.Random(seed)
    n = len(values)
    if n == 0:
        return (0.0, 0.0, 0.0)
    point = stat(values)
    replicates = [stat([values[rng.randrange(n)] for _ in range(n)])
                  for _ in range(n_resamples)]
    replicates.sort()
    lo = replicates[int(alpha / 2 * n_resamples)]
    hi = replicates[int((1 - alpha / 2) * n_resamples)]
    return (point, lo, hi)


def analyse_rag_bench(prior_path: Path, rag_path: Path) -> dict:
    prior = json.loads(prior_path.read_text())
    rag = json.loads(rag_path.read_text())

    out: dict = {
        "version": "v1.2_stats",
        "based_on": {
            "prior_only_result": str(prior_path.name),
            "rag_aug_result": str(rag_path.name),
            "corpus_snapshot_date": prior.get("corpus_snapshot_date"),
        },
    }

    pq_prior = {q["question_id"]: q for q in prior["per_question"]}
    pq_rag = {q["question_id"]: q for q in rag["per_question"]}
    common = sorted(set(pq_prior) & set(pq_rag))
    n = len(common)

    # Load question-language map from questions.jsonl
    q_path = ROOT / "benchmarks" / "swiss_legal_rag_bench" / "questions.jsonl"
    q_lang = {}
    for line in q_path.read_text().splitlines():
        if line.strip():
            q = json.loads(line)
            q_lang[q["id"]] = q.get("language", "?")

    # Correctness: paired arrays (field is 'correctness', 1/0)
    prior_correct = [int(pq_prior[qid].get("correctness", 0)) for qid in common]
    rag_correct = [int(pq_rag[qid].get("correctness", 0)) for qid in common]

    k_prior = sum(prior_correct)
    k_rag = sum(rag_correct)

    out["correctness"] = {
        "n_questions": n,
        "prior_only": {
            "k": k_prior, "p": k_prior / n,
            "wilson_95_ci": list(wilson_ci(k_prior, n)),
        },
        "rag_aug": {
            "k": k_rag, "p": k_rag / n,
            "wilson_95_ci": list(wilson_ci(k_rag, n)),
        },
        "delta_prior_minus_rag": (k_prior - k_rag) / n,
        "cohen_h": cohen_h(k_prior / n, k_rag / n),
    }

    # Paired McNemar on correctness
    b = sum(1 for p, r in zip(prior_correct, rag_correct) if p == 1 and r == 0)
    c = sum(1 for p, r in zip(prior_correct, rag_correct) if p == 0 and r == 1)
    out["mcnemar_correctness"] = mcnemar_exact(b, c)

    # Hallucination rate (prior-only) — use error_class field, look up language
    halluc_by_lang = {"de": 0, "fr": 0, "it": 0}
    n_by_lang = {"de": 0, "fr": 0, "it": 0}
    for qid in common:
        lang = q_lang.get(qid, "?")
        if lang in n_by_lang:
            n_by_lang[lang] += 1
            if pq_prior[qid].get("error_class") == "hallucination":
                halluc_by_lang[lang] += 1
    out["hallucination_prior_only_by_lang"] = {
        lang: {
            "k": halluc_by_lang[lang], "n": n_by_lang[lang],
            "p": halluc_by_lang[lang] / n_by_lang[lang] if n_by_lang[lang] else 0,
            "wilson_95_ci": list(wilson_ci(halluc_by_lang[lang], n_by_lang[lang])) if n_by_lang[lang] else [0, 0],
        }
        for lang in ("de", "fr", "it")
    }

    # Error-class breakdown for RAG-aug, with Wilson CIs
    error_class_counts = {"correct": 0, "hallucination": 0, "retrieval": 0, "reasoning": 0}
    for qid in common:
        ec = pq_rag[qid].get("error_class", "?")
        if ec in error_class_counts:
            error_class_counts[ec] += 1
    out["rag_aug_error_classes"] = {
        cls: {
            "k": cnt, "n": n,
            "p": cnt / n,
            "wilson_95_ci": list(wilson_ci(cnt, n)),
        }
        for cls, cnt in error_class_counts.items()
    }

    return out


def analyse_crosslingual(path: Path) -> dict:
    data = json.loads(path.read_text())
    summary = data["summary"]
    by_cell = summary["by_cell"]
    per_query = data["per_query"]

    out: dict = {
        "version": "v1.2_stats",
        "based_on": str(path.name),
        "queries_total": summary["queries_total"],
    }

    # Bootstrap CI per cell on MRR@10 and Hit@10
    cell_stats = {}
    for cell_name, cell in by_cell.items():
        # cell_name like 'de_to_de' → q_lang='de', target_lang='de'
        q_lang, _, t_lang = cell_name.partition("_to_")
        cell_qs = [q for q in per_query
                   if q.get("q_lang") == q_lang and q.get("target_lang") == t_lang]
        # Use the per-query 'rr' field (reciprocal rank, 0 if not in top-K)
        mrr_vals = [float(q.get("rr", 0.0)) for q in cell_qs]
        hit10_vals = [float(q.get("hit10", 0)) for q in cell_qs]
        mrr_point, mrr_lo, mrr_hi = bootstrap_ci(mrr_vals)
        hit_point, hit_lo, hit_hi = bootstrap_ci(hit10_vals)
        # Wilson CI on Hit@10 (binomial)
        k_hit = int(sum(hit10_vals))
        n_cell = len(hit10_vals)
        wilson = wilson_ci(k_hit, n_cell) if n_cell else (0.0, 0.0)
        cell_stats[cell_name] = {
            "n": n_cell,
            "mrr_at_10": {"point": mrr_point, "boot_95_ci": [mrr_lo, mrr_hi]},
            "hit_at_10": {
                "point": hit_point,
                "boot_95_ci": [hit_lo, hit_hi],
                "wilson_95_ci": list(wilson),
            },
        }
    out["per_cell"] = cell_stats

    # Overlap test: is IT→DE significantly better than DE→FR (the headline asymmetry)?
    # Permutation test on MRR.
    it_de_q = [q for q in per_query if q.get("q_lang") == "it" and q.get("target_lang") == "de"]
    de_fr_q = [q for q in per_query if q.get("q_lang") == "de" and q.get("target_lang") == "fr"]
    it_de_mrr = [float(q.get("rr", 0.0)) for q in it_de_q]
    de_fr_mrr = [float(q.get("rr", 0.0)) for q in de_fr_q]
    observed_diff = (sum(it_de_mrr) / len(it_de_mrr) if it_de_mrr else 0) - \
                    (sum(de_fr_mrr) / len(de_fr_mrr) if de_fr_mrr else 0)
    pooled = it_de_mrr + de_fr_mrr
    rng = random.Random(0)
    perm_diffs = []
    n1 = len(it_de_mrr)
    for _ in range(10000):
        rng.shuffle(pooled)
        a = pooled[:n1]
        b = pooled[n1:]
        diff = (sum(a) / len(a) if a else 0) - (sum(b) / len(b) if b else 0)
        perm_diffs.append(diff)
    p_two = sum(1 for d in perm_diffs if abs(d) >= abs(observed_diff)) / 10000
    out["asymmetry_test"] = {
        "claim": "IT→DE retrieves better than DE→FR",
        "it_to_de_mrr": sum(it_de_mrr) / len(it_de_mrr) if it_de_mrr else 0,
        "de_to_fr_mrr": sum(de_fr_mrr) / len(de_fr_mrr) if de_fr_mrr else 0,
        "observed_diff": observed_diff,
        "p_two_sided_permutation": p_two,
        "n_resamples": 10000,
    }

    return out


def main() -> None:
    out = {
        "rag_bench": analyse_rag_bench(
            RESULTS / "v1_1_prior_only.json",
            RESULTS / "v1_1_rag_aug.json",
        ),
        "cross_lingual": analyse_crosslingual(RESULTS / "cross_lingual_v1.json"),
    }
    target = RESULTS / "statistical_analysis.json"
    target.write_text(json.dumps(out, indent=2))
    # Print a human-readable summary
    print(f"Wrote {target}")
    print()
    print("=== Correctness with CIs (n=30) ===")
    cor = out["rag_bench"]["correctness"]
    print(f'  Prior-only: {cor["prior_only"]["k"]}/{cor["correctness"]["n_questions"] if "n_questions" in cor else cor["prior_only"]["k"]+0}: p={cor["prior_only"]["p"]:.3f}'
          if False else
          f'  Prior-only: {cor["prior_only"]["k"]}/{out["rag_bench"]["correctness"]["n_questions"]} = {cor["prior_only"]["p"]:.3f}, Wilson 95% CI [{cor["prior_only"]["wilson_95_ci"][0]:.3f}, {cor["prior_only"]["wilson_95_ci"][1]:.3f}]')
    print(f'  RAG-aug:    {cor["rag_aug"]["k"]}/{out["rag_bench"]["correctness"]["n_questions"]} = {cor["rag_aug"]["p"]:.3f}, Wilson 95% CI [{cor["rag_aug"]["wilson_95_ci"][0]:.3f}, {cor["rag_aug"]["wilson_95_ci"][1]:.3f}]')
    print(f'  Cohen\'s h: {cor["cohen_h"]:.3f}  (0.2 small / 0.5 medium / 0.8 large)')
    mc = out["rag_bench"]["mcnemar_correctness"]
    print(f'  McNemar paired exact: p = {mc["p_two_sided"]:.4f}  (n_discordant={mc["n_discordant"]}, b={mc.get("b_corrected_to_wrong","?")}, c={mc.get("c_wrong_to_corrected","?")})')
    print()
    print("=== Cross-lingual asymmetry test ===")
    at = out["cross_lingual"]["asymmetry_test"]
    print(f'  IT→DE MRR = {at["it_to_de_mrr"]:.3f}')
    print(f'  DE→FR MRR = {at["de_to_fr_mrr"]:.3f}')
    print(f'  observed diff = {at["observed_diff"]:.3f}')
    print(f'  permutation p (two-sided, 10k resamples) = {at["p_two_sided_permutation"]:.4f}')


if __name__ == "__main__":
    main()
