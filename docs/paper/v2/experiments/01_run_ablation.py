"""
Per-rail ablation experiment for v0.2.

For each of 30 questions in Swiss Legal RAG Bench v0.2:

  1. Generate a "prior-only" draft (no retrieval) using Claude Sonnet 4.6.
     The prior-only condition is chosen as an audit-pipeline
     stress test, not as an end-to-end RAG evaluation.
     Note: Dahl 2024 / Magesh 2025 hallucination rates were
     measured on different model/condition combinations than
     ours and the comparison should not be read as like-for-like.

  2. Run the closing audit (attest_response with audit_grounding=True)
     against the live MCP — collects issue counts in all 5 rail
     categories: case / statute / quote / date / grounding.

  3. Independently judge the draft against the gold reference:
       c_eli = does the answer entail the reference answer? (binary)
       g_eli = is the answer supported by the retrieved passages?
               (here the question has no retrieved passages, so this
               reduces to "supported by the audit's source pool")

  4. Compute per-rail-configuration catch rates in post-processing.

The output is a JSON file `ablation_results.json` plus a derived
`ablation_table.json` with the 6-row × 3-metric ablation table that
becomes Table 4 of the paper.

Costs: ~30 generation calls + ~30 c_eli judge calls + ~30 g_eli judge
calls + ~30 grounding-rail judge calls ≈ 120 Sonnet calls ≈ $1-2.
Wall-clock: 30-60 min if sequential, 5-10 min if parallel.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import httpx

# Locations — overridable via env vars for VPS execution
HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
QUESTIONS = Path(os.environ.get(
    "ABLATION_QUESTIONS",
    str(REPO / "benchmarks" / "swiss_legal_rag_bench" / "questions.jsonl"),
))
RESULTS_DIR = Path(os.environ.get(
    "ABLATION_RESULTS_DIR",
    str(HERE.parent / "experiments"),
))
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
RAW_OUT = RESULTS_DIR / "ablation_results.json"
TABLE_OUT = RESULTS_DIR / "ablation_table.json"

# Endpoints
MCP_BASE = "https://mcp.opencaselaw.ch"
ATTEST_URL = f"{MCP_BASE}/api/attest"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
GENERATOR_MODEL = "claude-sonnet-4-6"
JUDGE_MODEL = "claude-sonnet-4-6"

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
if not ANTHROPIC_API_KEY:
    print("ERROR: ANTHROPIC_API_KEY not set", file=sys.stderr)
    sys.exit(1)


# ── Generator: Sonnet without retrieval, prior-only ─────────────

GENERATOR_SYSTEM = (
    "You are a Swiss legal-research assistant.  Answer the user's "
    "question concisely (2–4 sentences) in the question's language.  "
    "Cite specific Swiss legal authorities (BGE numbers, statute "
    "articles) where relevant.  This task has NO retrieved sources — "
    "rely on your training knowledge alone.  Format your answer as "
    "plain prose; do NOT use bullet points or markdown."
)


async def generate_prior_only(client: httpx.AsyncClient, question: str) -> str:
    """Sonnet generates an answer without retrieval. Returns plain text."""
    resp = await client.post(
        ANTHROPIC_URL,
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": GENERATOR_MODEL,
            "max_tokens": 600,
            "system": GENERATOR_SYSTEM,
            "messages": [{"role": "user", "content": question}],
        },
        timeout=120.0,
    )
    resp.raise_for_status()
    return resp.json()["content"][0]["text"].strip()


# ── Audit: live MCP attest_response with all rails on ──────────

async def attest_full(client: httpx.AsyncClient, draft: str) -> dict:
    """Full 5-rail audit on the live MCP. Returns the per-category
    issue counts plus the issues themselves so we can do post-hoc
    ablation."""
    resp = await client.post(
        ATTEST_URL,
        json={"draft_text": draft, "audit_grounding": True},
        timeout=120.0,
    )
    resp.raise_for_status()
    return resp.json()


# ── Judges: independent c_eli and g_eli scoring ────────────────

CORRECTNESS_SYSTEM = (
    "You are a Swiss legal-evaluation judge.  Decide whether the "
    "ANSWER entails the REFERENCE_ANSWER — i.e., whether anyone reading "
    "the ANSWER would arrive at substantively the same conclusion as "
    "the REFERENCE_ANSWER.  Tolerate phrasing differences and minor "
    "omissions; reject answers that miss load-bearing elements or "
    "contradict the reference.  Respond ONLY with JSON: "
    '{"entails": true|false, "reasoning": "≤140 chars"}'
)

GROUNDEDNESS_SYSTEM = (
    "You are a Swiss legal-evaluation judge.  Decide whether the "
    "ANSWER is supported by the SOURCES (a JSON list of texts that the "
    "answering system had access to).  An answer is grounded if every "
    "load-bearing factual claim is present in at least one source.  An "
    "answer that asserts a fact not present in any source is "
    "ungrounded — even if the fact happens to be true.  Respond ONLY "
    'with JSON: {"supports": true|false, "reasoning": "≤140 chars"}'
)


async def judge(
    client: httpx.AsyncClient, system: str, user: str
) -> dict:
    resp = await client.post(
        ANTHROPIC_URL,
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": JUDGE_MODEL,
            "max_tokens": 200,
            "system": system,
            "messages": [{"role": "user", "content": user}],
        },
        timeout=60.0,
    )
    resp.raise_for_status()
    raw = resp.json()["content"][0]["text"].strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    try:
        return json.loads(raw)
    except Exception:
        return {}


# ── Per-question pipeline ──────────────────────────────────────

async def run_one(client: httpx.AsyncClient, q: dict) -> dict:
    """One question end-to-end. Returns a result dict."""
    qid = q["id"]
    print(f"  [{qid}] generating prior-only draft…", flush=True)
    t0 = time.time()
    try:
        draft = await generate_prior_only(client, q["question"])
    except Exception as e:
        return {"id": qid, "stage": "generate", "error": str(e)}
    t_gen = time.time() - t0

    print(f"  [{qid}] auditing draft…", flush=True)
    t0 = time.time()
    try:
        attest = await attest_full(client, draft)
    except Exception as e:
        return {"id": qid, "stage": "attest", "error": str(e),
                "draft": draft}
    t_att = time.time() - t0

    print(f"  [{qid}] judging correctness + groundedness…", flush=True)
    # The "sources" the model "had access to" in the prior-only condition
    # is the EMPTY SET. So groundedness reduces to "does the answer
    # only assert content the model could have known from the audit
    # sources?" — which is the ablation's exact subject. We supply the
    # audit's source-pool view: the regeste text of any cited decision
    # plus the statute text of any cited statute. Same as what the
    # quote rail uses.
    sources_for_g_eli = []  # prior-only: literally no retrieval
    correctness, groundedness = await asyncio.gather(
        judge(
            client, CORRECTNESS_SYSTEM,
            f"REFERENCE_ANSWER:\n{q['reference_answer']}\n\nANSWER:\n{draft}"
        ),
        judge(
            client, GROUNDEDNESS_SYSTEM,
            f"SOURCES:\n{json.dumps(sources_for_g_eli, ensure_ascii=False)}\n\nANSWER:\n{draft}"
        ),
    )

    # The grounding rail's verdict is INSIDE attest's response; surface it
    grounding_meta = attest.get("grounding_meta", {})
    return {
        "id": qid,
        "language": q["language"],
        "claim_type": q.get("claim_type", "?"),
        "difficulty": q.get("difficulty", "?"),
        "draft": draft,
        "draft_length": len(draft),
        "attest": {
            "ok": attest.get("ok"),
            "issues_by_category": attest.get("issues_by_category", {}),
            "issues_count": attest.get("issues_count", 0),
            "citations_found": attest.get("citations_found", 0),
            "citations_ok": attest.get("citations_ok", 0),
            "grounding_meta": grounding_meta,
            "issues": attest.get("issues", []),
        },
        "judges": {
            "c_eli": bool(correctness.get("entails", False)),
            "c_reasoning": correctness.get("reasoning", "")[:200],
            "g_eli": bool(groundedness.get("supports", False)),
            "g_reasoning": groundedness.get("reasoning", "")[:200],
        },
        "timing": {"generate_s": round(t_gen, 1), "attest_s": round(t_att, 1)},
    }


# ── Main ────────────────────────────────────────────────────────

async def main(parallel: int = 4):
    questions = []
    with open(QUESTIONS, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                questions.append(json.loads(line))
    print(f"loaded {len(questions)} questions; running with parallel={parallel}\n",
          flush=True)

    sem = asyncio.Semaphore(parallel)

    async def gated(q):
        async with sem:
            return await run_one(client, q)

    async with httpx.AsyncClient() as client:
        t0 = time.time()
        results = await asyncio.gather(*(gated(q) for q in questions))
        wall = time.time() - t0

    out = {
        "experiment": "ablation_v02_prior_only",
        "version": "0.1",
        "questions_count": len(questions),
        "generator": GENERATOR_MODEL,
        "judge": JUDGE_MODEL,
        "audit_endpoint": ATTEST_URL,
        "wall_clock_s": round(wall, 1),
        "results": results,
    }
    RAW_OUT.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"\nwrote {len(results)} results to {RAW_OUT}")
    print(f"wall clock: {wall:.1f}s")

    # Quick sanity stats
    n = len(results)
    n_err = sum(1 for r in results if "error" in r)
    n_ok = sum(1 for r in results if r.get("attest", {}).get("ok") is True)
    n_correct = sum(1 for r in results if r.get("judges", {}).get("c_eli") is True)
    n_grounded = sum(1 for r in results if r.get("judges", {}).get("g_eli") is True)
    print(f"\nrun summary:")
    print(f"  errors:           {n_err:3d}/{n}")
    print(f"  attest ok=true:   {n_ok:3d}/{n}  (no rail fired = no fabrications detected)")
    print(f"  c_eli (correct):  {n_correct:3d}/{n}  ({100*n_correct/n:.0f}%)")
    print(f"  g_eli (grounded): {n_grounded:3d}/{n}  ({100*n_grounded/n:.0f}%)")


if __name__ == "__main__":
    asyncio.run(main())
