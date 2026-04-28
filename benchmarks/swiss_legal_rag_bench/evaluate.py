"""
Swiss Legal RAG Bench — evaluation harness.

Runs a configurable RAG pipeline over the seed-question set in
questions.jsonl and computes the three Butler & Butler 2026 dimensions
(correctness c, groundedness g, retrieval accuracy r) plus the
hierarchical error decomposition (hallucination / retrieval / reasoning).

Usage
-----
    python3 -m benchmarks.swiss_legal_rag_bench.evaluate \
        --questions benchmarks/swiss_legal_rag_bench/questions.jsonl \
        --top-k 5 \
        --output benchmarks/swiss_legal_rag_bench/results/run.json

Plug-in points (override via subclass + --import-module):

    retrieve(query, language, top_k) -> list[dict(decision_id, text, ...)]
    generate(query, retrieved_passages) -> str
    judge_correctness(answer, reference) -> bool
    judge_groundedness(answer, retrieved_passages_text) -> bool

Defaults use the live OpenCaseLaw MCP server for retrieval, Claude
Sonnet 4.6 for generation, and Claude Sonnet 4.6 for both judges.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable

logger = logging.getLogger("swiss_legal_rag_bench")


# ── Data models ────────────────────────────────────────────────────

@dataclass
class Question:
    id: str
    language: str
    legal_area: str
    difficulty: str
    question: str
    reference_answer: str
    evidence: dict
    claim_type: str


@dataclass
class QuestionResult:
    question_id: str
    retrieved_decision_ids: list[str]
    retrieved_statute_ids: list[str]
    answer: str
    correctness: bool
    groundedness: bool
    retrieval_accuracy: bool
    error_class: str            # "correct" | "hallucination" | "retrieval" | "reasoning"
    latency_ms: dict = field(default_factory=dict)
    judge_notes: dict = field(default_factory=dict)


# ── Retrieval (default: live MCP) ──────────────────────────────────

def _mcp_retrieve(query: str, language: str, top_k: int = 5,
                  mcp_url: str = "https://mcp.opencaselaw.ch") -> list[dict]:
    """Call MCP search_decisions via the /api/decisions REST endpoint."""
    import httpx
    params = {"query": query, "limit": top_k, "language": language}
    with httpx.Client(timeout=30.0) as client:
        r = client.get(f"{mcp_url}/api/decisions", params=params)
        r.raise_for_status()
        body = r.json()
    out = []
    for hit in body.get("results", [])[:top_k]:
        out.append({
            "decision_id": hit.get("decision_id"),
            "court": hit.get("court_name") or hit.get("court"),
            "regeste": hit.get("regeste") or "",
            "snippet": hit.get("snippet") or "",
            "url": hit.get("canonical_url") or "",
        })
    return out


def _mcp_get_decision_text(decision_id: str,
                           mcp_url: str = "https://mcp.opencaselaw.ch",
                           max_chars: int = 4000) -> str:
    """Fetch a decision's regeste + first chunk of full_text."""
    import httpx
    with httpx.Client(timeout=30.0) as client:
        r = client.get(f"{mcp_url}/api/decisions/{decision_id}")
        if r.status_code != 200:
            return ""
        body = r.json()
    parts = []
    if body.get("regeste"):
        parts.append("REGESTE: " + body["regeste"])
    if body.get("full_text"):
        parts.append("FULL_TEXT: " + body["full_text"][:max_chars])
    return "\n\n".join(parts)


def _mcp_get_statute_text(law_code: str, article: str, language: str = "de",
                          mcp_url: str = "https://mcp.opencaselaw.ch") -> str:
    """Fetch a statute article's text via /api/laws/{abbr}?article=N."""
    import httpx
    params = {"article": article, "language": language}
    with httpx.Client(timeout=30.0) as client:
        r = client.get(f"{mcp_url}/api/laws/{law_code}", params=params)
        if r.status_code != 200:
            return ""
        body = r.json()
    if not isinstance(body, dict):
        return ""
    arts = body.get("articles") or []
    for a in arts:
        if (a.get("article_num") or "").strip() == str(article).strip():
            return a.get("text") or ""
    if arts:
        return arts[0].get("text") or ""
    return body.get("text") or body.get("text_de") or ""


# ── Generation (default: Claude) ───────────────────────────────────

def _claude_generate(query: str, retrieved_text: str,
                     model: str = "claude-sonnet-4-6",
                     api_key: str | None = None) -> str:
    """Default generator. Strict instruction: answer using only the
    retrieved passages. Returns the raw answer string."""
    import httpx
    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return "[generator unavailable: ANTHROPIC_API_KEY not set]"
    system = (
        "You are a Swiss legal-research assistant. Answer the QUESTION "
        "concisely using ONLY the RETRIEVED PASSAGES. If the passages "
        "do not contain enough information, say so explicitly. Do not "
        "rely on external knowledge. Cite the source(s) you used by "
        "referencing the passage label (e.g. [P1])."
    )
    user = f"QUESTION:\n{query}\n\nRETRIEVED PASSAGES:\n{retrieved_text}"
    with httpx.Client(timeout=60.0) as client:
        r = client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 800,
                "system": system,
                "messages": [{"role": "user", "content": user}],
            },
        )
        r.raise_for_status()
        return r.json()["content"][0]["text"].strip()


# ── Judges (default: Claude) ───────────────────────────────────────

def _claude_judge(system_prompt: str, user_prompt: str,
                  model: str = "claude-sonnet-4-6",
                  api_key: str | None = None) -> dict:
    """Generic JSON-returning judge. Returns parsed dict, or {} on failure."""
    import httpx
    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {}
    with httpx.Client(timeout=30.0) as client:
        r = client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 400,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            },
        )
        if r.status_code != 200:
            return {}
        raw = r.json()["content"][0]["text"].strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        try:
            return json.loads(raw)
        except Exception:
            return {}


def judge_correctness(answer: str, reference: str) -> tuple[bool, str]:
    """Does `answer` entail `reference`?"""
    system = (
        "You are a Swiss legal-evaluation judge. Decide whether the "
        "ANSWER entails the REFERENCE_ANSWER — i.e., whether anyone "
        "reading the ANSWER would arrive at substantively the same "
        "conclusion as the REFERENCE_ANSWER. Tolerate phrasing "
        "differences and minor omissions; reject answers that miss "
        "load-bearing elements or contradict the reference.\n\n"
        "Respond ONLY with JSON: "
        '{"entails": true|false, "reasoning": "≤140 chars"}'
    )
    user = f"REFERENCE_ANSWER:\n{reference}\n\nANSWER:\n{answer}"
    v = _claude_judge(system, user)
    return bool(v.get("entails")), v.get("reasoning", "")


def judge_groundedness(answer: str, retrieved_text: str) -> tuple[bool, str]:
    """Is every load-bearing claim in `answer` supported by `retrieved_text`?"""
    system = (
        "You are a Swiss legal-evaluation judge. Decide whether every "
        "load-bearing factual claim in the ANSWER is supported by the "
        "RETRIEVED PASSAGES. Ignore generic legal background; focus on "
        "specific propositions (numbers, conditions, statute references, "
        "case holdings). If the ANSWER asserts a fact not in the "
        "passages, judge supports = false.\n\n"
        "Respond ONLY with JSON: "
        '{"supports": true|false, "unsupported_claim": "≤120 chars" | null, '
        '"reasoning": "≤140 chars"}'
    )
    user = f"RETRIEVED PASSAGES:\n{retrieved_text}\n\nANSWER:\n{answer}"
    v = _claude_judge(system, user)
    return bool(v.get("supports")), v.get("reasoning", "")


# ── Pipeline ───────────────────────────────────────────────────────

def evaluate_question(q: Question, *,
                      mcp_url: str,
                      top_k: int,
                      generator: Callable | None = None,
                      retriever: Callable | None = None) -> QuestionResult:
    """Run the full pipeline on one question and decompose the error."""
    retriever = retriever or _mcp_retrieve
    generator = generator or _claude_generate

    # 1. Retrieval
    t0 = time.perf_counter()
    retrieved = retriever(q.question, q.language, top_k)
    t_retrieve = (time.perf_counter() - t0) * 1000

    retrieved_ids = [r["decision_id"] for r in retrieved if r.get("decision_id")]

    # 2. For statute-bound questions we ALSO surface the named statute
    #    text into the generator's context (the statute exists in the
    #    Fedlex mirror; including it isolates "did the embedder retrieve
    #    the right CASE" from "did the system have the right STATUTE").
    statute_passages: list[tuple[str, str]] = []
    for s in q.evidence.get("statutes", []) or []:
        text = _mcp_get_statute_text(
            s["law_code"], s["article"], s.get("language", q.language),
            mcp_url=mcp_url,
        )
        if text:
            statute_passages.append((
                f"Art. {s['article']} {s['law_code']}", text[:1500]
            ))

    # 3. Build the LLM context
    context_parts = []
    for label, text in statute_passages:
        context_parts.append(f"[STATUTE: {label}]\n{text}")
    for i, hit in enumerate(retrieved):
        body = hit.get("regeste") or hit.get("snippet") or ""
        context_parts.append(
            f"[P{i+1} {hit.get('decision_id','?')} — {hit.get('court','?')}]\n{body[:1000]}"
        )
    retrieved_text = "\n\n".join(context_parts) or "(no passages retrieved)"

    # 4. Generation
    t0 = time.perf_counter()
    answer = generator(q.question, retrieved_text)
    t_generate = (time.perf_counter() - t0) * 1000

    # 5. Judge dimensions
    correctness, c_reason = judge_correctness(answer, q.reference_answer)
    groundedness, g_reason = judge_groundedness(answer, retrieved_text)

    # 6. Retrieval accuracy: did at least one annotated decision land in top-K?
    annotated_decision_ids = set(q.evidence.get("decisions", []) or [])
    retrieval_accuracy = bool(annotated_decision_ids & set(retrieved_ids))
    # If the question is purely statute-bound (no decisions annotated) we
    # treat retrieval_accuracy as N/A → True only if statute text was found.
    if not annotated_decision_ids:
        retrieval_accuracy = bool(statute_passages)

    # 7. Error class (Butler & Butler Fig 1)
    if correctness and groundedness:
        error_class = "correct"
    elif not groundedness:
        error_class = "hallucination"
    elif not retrieval_accuracy:
        error_class = "retrieval"
    else:
        error_class = "reasoning"

    return QuestionResult(
        question_id=q.id,
        retrieved_decision_ids=retrieved_ids,
        retrieved_statute_ids=[f"{s['law_code']}_{s['article']}"
                                for s in (q.evidence.get("statutes") or [])],
        answer=answer,
        correctness=correctness,
        groundedness=groundedness,
        retrieval_accuracy=retrieval_accuracy,
        error_class=error_class,
        latency_ms={"retrieve": round(t_retrieve, 1),
                    "generate": round(t_generate, 1)},
        judge_notes={"correctness": c_reason, "groundedness": g_reason},
    )


def aggregate(results: list[QuestionResult]) -> dict:
    n = len(results) or 1
    by_lang: dict[str, list[QuestionResult]] = {"de": [], "fr": [], "it": []}
    for r in results:
        lang = (r.judge_notes.get("language") or "").lower()
        if lang in by_lang:
            by_lang[lang].append(r)
    return {
        "n_questions": len(results),
        "correctness_pct":         round(100 * sum(r.correctness for r in results) / n, 1),
        "groundedness_pct":        round(100 * sum(r.groundedness for r in results) / n, 1),
        "retrieval_accuracy_pct":  round(100 * sum(r.retrieval_accuracy for r in results) / n, 1),
        "error_decomposition": {
            "correct":       sum(1 for r in results if r.error_class == "correct"),
            "hallucination": sum(1 for r in results if r.error_class == "hallucination"),
            "retrieval":     sum(1 for r in results if r.error_class == "retrieval"),
            "reasoning":     sum(1 for r in results if r.error_class == "reasoning"),
        },
        "by_language": {
            lang: {
                "n": len(rs),
                "correct_pct":     round(100 * sum(r.correctness for r in rs) / max(1, len(rs)), 1),
                "grounded_pct":    round(100 * sum(r.groundedness for r in rs) / max(1, len(rs)), 1),
                "retrieval_pct":   round(100 * sum(r.retrieval_accuracy for r in rs) / max(1, len(rs)), 1),
            }
            for lang, rs in by_lang.items()
        },
    }


# ── CLI ────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Swiss Legal RAG Bench — evaluator")
    ap.add_argument("--questions", default=str(
        Path(__file__).parent / "questions.jsonl"))
    ap.add_argument("--mcp-url", default="https://mcp.opencaselaw.ch")
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--output", default=None,
                    help="If given, write JSON results to this path.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Run only the first N questions (smoke test).")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    questions: list[Question] = []
    with open(args.questions, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            q = json.loads(line)
            questions.append(Question(**q))
    if args.limit:
        questions = questions[: args.limit]

    logger.info("Running %d questions...", len(questions))
    results: list[QuestionResult] = []
    by_q_lang: dict[str, str] = {}
    for q in questions:
        logger.info("[%s] %s", q.id, q.question[:90])
        try:
            r = evaluate_question(q, mcp_url=args.mcp_url, top_k=args.top_k)
        except Exception as e:
            logger.exception("[%s] crashed: %s", q.id, e)
            continue
        by_q_lang[q.id] = q.language
        results.append(r)
        flag = {"correct": "✓", "hallucination": "✗ HALLUC",
                "retrieval": "✗ RETR", "reasoning": "✗ REAS"}[r.error_class]
        logger.info("  %s  c=%s g=%s r=%s  (retrieve=%sms generate=%sms)",
                    flag, r.correctness, r.groundedness, r.retrieval_accuracy,
                    r.latency_ms["retrieve"], r.latency_ms["generate"])

    # Annotate language onto results post-hoc for aggregate()
    for r in results:
        r.judge_notes["language"] = by_q_lang.get(r.question_id, "?")

    summary = {
        "version": "v0.1",
        "corpus_snapshot_date": "2026-04-28",
        "evaluator": {
            "mcp_url": args.mcp_url,
            "top_k": args.top_k,
            "generator_model": "claude-sonnet-4-6",
            "judge_model": "claude-sonnet-4-6",
        },
        "aggregate": aggregate(results),
        "per_question": [asdict(r) for r in results],
    }

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
        logger.info("Wrote results to %s", out)
    print(json.dumps(summary["aggregate"], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
