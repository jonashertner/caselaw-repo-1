#!/usr/bin/env python3
"""
Ingest Swiss caselaw JSONL into OpenSearch decisions/chunks/reference indices.

This script is safe to run in dry-run mode without OpenSearch dependencies.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from search_stack.opensearch_schema import build_assets  # noqa: E402
from search_stack.reference_extraction import extract_case_citations, extract_statute_references  # noqa: E402


@dataclass
class IngestCounters:
    decisions: int = 0
    chunks: int = 0
    references: int = 0
    sample_decision_action: dict[str, Any] | None = None


class OptionalEmbedder:
    """Lazy sentence-transformer loader for optional index-time embeddings."""

    def __init__(self, model_name: str | None, *, batch_size: int = 64):
        self.model_name = (model_name or "").strip()
        self.batch_size = max(1, int(batch_size))
        self._model = None
        self.vector_dim: int | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.model_name)

    def load(self) -> None:
        if not self.enabled:
            return
        if self._model is not None:
            return
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as e:
            raise SystemExit(
                "Embedding model requested but sentence-transformers is not installed. "
                "Install with: pip install sentence-transformers"
            ) from e
        self._model = SentenceTransformer(self.model_name)
        probe = self._model.encode("passage: probe", normalize_embeddings=True)
        self.vector_dim = len(probe)

    def encode_passage(self, text: str) -> list[float] | None:
        if not self.enabled:
            return None
        self.load()
        assert self._model is not None
        payload = (text or "").strip()
        if not payload:
            return None
        if not payload.lower().startswith("passage:"):
            payload = f"passage: {payload}"
        vector = self._model.encode(payload, normalize_embeddings=True)
        return [float(v) for v in vector]

    def encode_passages(self, texts: list[str]) -> list[list[float] | None]:
        if not self.enabled:
            return [None for _ in texts]
        self.load()
        assert self._model is not None
        valid_idx: list[int] = []
        payloads: list[str] = []
        for idx, raw in enumerate(texts):
            t = (raw or "").strip()
            if not t:
                continue
            if not t.lower().startswith("passage:"):
                t = f"passage: {t}"
            valid_idx.append(idx)
            payloads.append(t)
        out: list[list[float] | None] = [None for _ in texts]
        if not payloads:
            return out
        vectors = self._model.encode(
            payloads,
            normalize_embeddings=True,
            batch_size=self.batch_size,
            show_progress_bar=False,
        )
        for idx, vec in zip(valid_idx, vectors):
            out[idx] = [float(v) for v in vec]
        return out


def iter_decisions(input_dir: Path, limit: int | None = None) -> Iterator[dict[str, Any]]:
    count = 0
    for jsonl in sorted(input_dir.glob("*.jsonl")):
        with open(jsonl, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                if not row.get("decision_id"):
                    continue
                yield row
                count += 1
                if limit and count >= limit:
                    return


def chunk_text(text: str, chunk_size: int = 1400, overlap: int = 200) -> list[str]:
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]

    chunks: list[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + chunk_size, n)
        piece = text[start:end].strip()
        if piece:
            chunks.append(piece)
        if end >= n:
            break
        start = max(start + 1, end - overlap)
    return chunks


def _docket_norm(value: str | None) -> str:
    if not value:
        return ""
    out = value.upper().replace("-", "_").replace(".", "_").replace("/", "_")
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_")


def _decision_embedding_text(row: dict[str, Any]) -> str:
    title = (row.get("title") or "").strip()
    regeste = (row.get("regeste") or "").strip()
    full_text = (row.get("full_text") or "").strip()
    if len(full_text) > 4500:
        full_text = full_text[:4500]
    return "\n".join(part for part in (title, regeste, full_text) if part)


def decision_doc(
    row: dict[str, Any],
    *,
    embedder: OptionalEmbedder | None = None,
) -> dict[str, Any]:
    text_blob = " ".join([row.get("title") or "", row.get("regeste") or "", row.get("full_text") or ""])
    statutes = extract_statute_references(text_blob)
    citations = extract_case_citations(text_blob)
    doc = {
        "decision_id": row.get("decision_id"),
        "docket_number": row.get("docket_number"),
        "docket_number_2": row.get("docket_number_2"),
        "court": row.get("court"),
        "canton": row.get("canton"),
        "chamber": row.get("chamber"),
        "language": row.get("language"),
        "decision_date": row.get("decision_date"),
        "publication_date": row.get("publication_date"),
        "title": row.get("title"),
        "legal_area": row.get("legal_area"),
        "regeste": row.get("regeste"),
        "full_text": row.get("full_text"),
        "decision_type": row.get("decision_type"),
        "outcome": row.get("outcome"),
        "source_url": row.get("source_url"),
        "pdf_url": row.get("pdf_url"),
        "scraped_at": row.get("scraped_at"),
        "decision_refs": [c.normalized for c in citations],
        "statute_refs": [
            {
                "law_code": s.law_code,
                "article": s.article,
                "paragraph": s.paragraph,
                "normalized": s.normalized,
            }
            for s in statutes
        ],
    }
    if embedder and embedder.enabled:
        vector = embedder.encode_passage(_decision_embedding_text(row))
        if vector is not None:
            doc["full_text_embedding"] = vector
    return doc


def reference_docs(row: dict[str, Any]) -> list[dict[str, Any]]:
    decision_id = row.get("decision_id")
    docket = row.get("docket_number")
    text_blob = " ".join([row.get("title") or "", row.get("regeste") or "", row.get("full_text") or ""])
    statutes = extract_statute_references(text_blob)
    citations = extract_case_citations(text_blob)

    docs: list[dict[str, Any]] = []
    for idx, st in enumerate(statutes):
        docs.append(
            {
                "edge_id": f"{decision_id}::statute::{idx}",
                "source_decision_id": decision_id,
                "source_docket_number": docket,
                "source_court": row.get("court"),
                "source_date": row.get("decision_date"),
                "ref_type": "statute_mention",
                "law_code": st.law_code,
                "article": st.article,
                "paragraph": st.paragraph,
                "normalized_ref": st.normalized,
            }
        )
    for idx, ct in enumerate(citations):
        docs.append(
            {
                "edge_id": f"{decision_id}::citation::{idx}",
                "source_decision_id": decision_id,
                "source_docket_number": docket,
                "source_court": row.get("court"),
                "source_date": row.get("decision_date"),
                "ref_type": "decision_citation",
                "target_docket_number": ct.normalized if ct.citation_type == "docket" else None,
                "normalized_ref": ct.normalized,
            }
        )
    return docs


def chunk_docs(
    row: dict[str, Any],
    chunk_size: int,
    overlap: int,
    *,
    embedder: OptionalEmbedder | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    texts = chunk_text(row.get("full_text") or "", chunk_size=chunk_size, overlap=overlap)
    vectors = (
        embedder.encode_passages(texts)
        if embedder and embedder.enabled
        else [None for _ in texts]
    )
    for idx, text in enumerate(texts):
        out.append(
            {
                "chunk_id": f"{row.get('decision_id')}::{idx}",
                "decision_id": row.get("decision_id"),
                "court": row.get("court"),
                "canton": row.get("canton"),
                "language": row.get("language"),
                "decision_date": row.get("decision_date"),
                "chunk_index": idx,
                "chunk_text": text,
                **({"chunk_embedding": vectors[idx]} if vectors[idx] is not None else {}),
            }
        )
    return out


def apply_schema_assets(
    client,
    *,
    index_prefix: str,
    vector_dim: int,
    shards: int,
    replicas: int,
) -> None:
    assets = build_assets(
        index_prefix=index_prefix,
        vector_dim=vector_dim,
        shards=shards,
        replicas=replicas,
    )
    for index_name, body in assets["indices"].items():
        if not client.indices.exists(index=index_name):
            client.indices.create(index=index_name, body=body)
    for pipeline_name, body in assets["search_pipeline"].items():
        client.transport.perform_request("PUT", f"/_search/pipeline/{pipeline_name}", body=body)


def iter_bulk_actions(
    *,
    input_dir: Path,
    limit: int | None,
    decisions_index: str,
    chunks_index: str,
    refs_index: str,
    chunk_size: int,
    chunk_overlap: int,
    counters: IngestCounters,
    embedder: OptionalEmbedder | None,
) -> Iterator[dict[str, Any]]:
    for row in iter_decisions(input_dir, limit=limit):
        decision_id = row["decision_id"]
        decision_action = {
            "_op_type": "index",
            "_index": decisions_index,
            "_id": decision_id,
            "_source": decision_doc(row, embedder=embedder),
        }
        counters.decisions += 1
        if counters.sample_decision_action is None:
            counters.sample_decision_action = decision_action
        yield decision_action

        for chunk in chunk_docs(
            row,
            chunk_size=chunk_size,
            overlap=chunk_overlap,
            embedder=embedder,
        ):
            counters.chunks += 1
            yield {
                "_op_type": "index",
                "_index": chunks_index,
                "_id": chunk["chunk_id"],
                "_source": chunk,
            }

        for ref in reference_docs(row):
            counters.references += 1
            yield {
                "_op_type": "index",
                "_index": refs_index,
                "_id": ref["edge_id"],
                "_source": ref,
            }


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest decisions JSONL into OpenSearch")
    parser.add_argument("--input", type=Path, default=Path("output/decisions"))
    parser.add_argument("--index-prefix", default="swiss-caselaw")
    parser.add_argument("--host", default="http://localhost:9200")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--vector-dim", type=int, default=384)
    parser.add_argument(
        "--embed-model",
        default="",
        help=(
            "Optional sentence-transformers model used to create decision/chunk embeddings "
            "(e.g. intfloat/multilingual-e5-small)."
        ),
    )
    parser.add_argument("--embed-batch-size", type=int, default=64)
    parser.add_argument("--chunk-size", type=int, default=1400)
    parser.add_argument("--chunk-overlap", type=int, default=200)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-schema", action="store_true")
    args = parser.parse_args()

    decisions_index = f"{args.index_prefix}-decisions-v1"
    chunks_index = f"{args.index_prefix}-chunks-v1"
    refs_index = f"{args.index_prefix}-references-v1"

    embedder = OptionalEmbedder(args.embed_model, batch_size=args.embed_batch_size)
    if embedder.enabled:
        embedder.load()
        assert embedder.vector_dim is not None
        if embedder.vector_dim != args.vector_dim:
            raise SystemExit(
                f"Vector dimension mismatch: model '{embedder.model_name}' produces "
                f"{embedder.vector_dim} dims, but --vector-dim is {args.vector_dim}."
            )

    counters = IngestCounters()
    actions_iter = iter_bulk_actions(
        input_dir=args.input,
        limit=args.limit,
        decisions_index=decisions_index,
        chunks_index=chunks_index,
        refs_index=refs_index,
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap,
        counters=counters,
        embedder=embedder if embedder.enabled else None,
    )

    if args.dry_run:
        for _ in actions_iter:
            pass
        print(
            json.dumps(
                {
                    "input_files": len(list(args.input.glob("*.jsonl"))),
                    "decisions": counters.decisions,
                    "chunks": counters.chunks,
                    "references": counters.references,
                    "sample_decision_action": counters.sample_decision_action,
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        return

    try:
        from opensearchpy import OpenSearch, helpers
    except ImportError as e:
        raise SystemExit(
            "opensearch-py is required for live ingestion. Install with: pip install opensearch-py"
        ) from e

    client = OpenSearch(
        hosts=[args.host],
        http_auth=(args.username, args.password) if args.username and args.password else None,
        use_ssl=args.host.startswith("https://"),
        verify_certs=args.host.startswith("https://"),
    )

    if not args.no_schema:
        apply_schema_assets(
            client,
            index_prefix=args.index_prefix,
            vector_dim=args.vector_dim,
            shards=1,
            replicas=0,
        )

    success_count, failed_count = helpers.bulk(
        client,
        actions_iter,
        chunk_size=args.batch_size,
        request_timeout=120,
        stats_only=True,
        raise_on_error=False,
    )

    print(
        json.dumps(
            {
                "indexed_decisions": counters.decisions,
                "indexed_chunks": counters.chunks,
                "indexed_references": counters.references,
                "bulk_actions_successful": success_count,
                "bulk_actions_failed": failed_count,
                "decisions_index": decisions_index,
                "chunks_index": chunks_index,
                "references_index": refs_index,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
