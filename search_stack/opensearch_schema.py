#!/usr/bin/env python3
"""
OpenSearch schema assets for Swiss caselaw hybrid search.

Includes:
- decisions index (document-level)
- chunks index (chunk-level for semantic retrieval)
- references index (citations/statutes)
- search pipeline (RRF score ranker)
"""
from __future__ import annotations

import argparse
import json
from typing import Any


def _analysis_settings() -> dict[str, Any]:
    return {
        "analysis": {
            "filter": {
                "de_stemmer": {"type": "stemmer", "language": "german"},
                "fr_stemmer": {"type": "stemmer", "language": "french"},
                "it_stemmer": {"type": "stemmer", "language": "italian"},
            },
            "analyzer": {
                "legal_default": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"],
                },
                "legal_de": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding", "de_stemmer"],
                },
                "legal_fr": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding", "fr_stemmer"],
                },
                "legal_it": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding", "it_stemmer"],
                },
            },
            "normalizer": {
                "lowercase_keyword": {
                    "type": "custom",
                    "filter": ["lowercase", "asciifolding"],
                }
            },
        }
    }


def decisions_index_body(
    *,
    shards: int = 1,
    replicas: int = 0,
    vector_dim: int = 1024,
) -> dict[str, Any]:
    return {
        "settings": {
            "index": {
                "number_of_shards": shards,
                "number_of_replicas": replicas,
                "knn": True,
            },
            **_analysis_settings(),
        },
        "mappings": {
            "dynamic": False,
            "properties": {
                "decision_id": {"type": "keyword"},
                "docket_number": {
                    "type": "text",
                    "analyzer": "legal_default",
                    "fields": {
                        "raw": {"type": "keyword", "normalizer": "lowercase_keyword"},
                    },
                },
                "docket_number_2": {"type": "keyword", "normalizer": "lowercase_keyword"},
                "court": {"type": "keyword"},
                "canton": {"type": "keyword"},
                "chamber": {"type": "keyword"},
                "language": {"type": "keyword"},
                "decision_date": {"type": "date", "format": "yyyy-MM-dd||strict_date_optional_time"},
                "publication_date": {"type": "date", "format": "yyyy-MM-dd||strict_date_optional_time"},
                "title": {"type": "text", "analyzer": "legal_default", "copy_to": "all_text"},
                "legal_area": {"type": "keyword"},
                "decision_type": {"type": "keyword"},
                "outcome": {"type": "keyword"},
                "regeste": {"type": "text", "analyzer": "legal_default", "copy_to": "all_text"},
                "full_text": {"type": "text", "analyzer": "legal_default", "copy_to": "all_text"},
                "all_text": {"type": "text", "analyzer": "legal_default"},
                "source_url": {"type": "keyword", "index": False},
                "pdf_url": {"type": "keyword", "index": False},
                "scraped_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "decision_refs": {"type": "keyword"},
                "statute_refs": {
                    "type": "nested",
                    "properties": {
                        "law_code": {"type": "keyword"},
                        "article": {"type": "keyword"},
                        "paragraph": {"type": "keyword"},
                        "normalized": {"type": "keyword"},
                    },
                },
                "full_text_embedding": {
                    "type": "knn_vector",
                    "dimension": vector_dim,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesimil",
                        "engine": "lucene",
                        "parameters": {"ef_construction": 128, "m": 16},
                    },
                },
            },
        },
    }


def chunks_index_body(
    *,
    shards: int = 1,
    replicas: int = 0,
    vector_dim: int = 1024,
) -> dict[str, Any]:
    return {
        "settings": {
            "index": {
                "number_of_shards": shards,
                "number_of_replicas": replicas,
                "knn": True,
            },
            **_analysis_settings(),
        },
        "mappings": {
            "dynamic": False,
            "properties": {
                "chunk_id": {"type": "keyword"},
                "decision_id": {"type": "keyword"},
                "court": {"type": "keyword"},
                "canton": {"type": "keyword"},
                "language": {"type": "keyword"},
                "decision_date": {"type": "date", "format": "yyyy-MM-dd||strict_date_optional_time"},
                "chunk_index": {"type": "integer"},
                "chunk_text": {"type": "text", "analyzer": "legal_default"},
                "chunk_embedding": {
                    "type": "knn_vector",
                    "dimension": vector_dim,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesimil",
                        "engine": "lucene",
                        "parameters": {"ef_construction": 128, "m": 16},
                    },
                },
            },
        },
    }


def references_index_body(*, shards: int = 1, replicas: int = 0) -> dict[str, Any]:
    return {
        "settings": {
            "index": {"number_of_shards": shards, "number_of_replicas": replicas},
            **_analysis_settings(),
        },
        "mappings": {
            "dynamic": False,
            "properties": {
                "edge_id": {"type": "keyword"},
                "source_decision_id": {"type": "keyword"},
                "source_docket_number": {"type": "keyword"},
                "source_court": {"type": "keyword"},
                "source_date": {"type": "date", "format": "yyyy-MM-dd||strict_date_optional_time"},
                "ref_type": {"type": "keyword"},  # decision_citation | statute_mention
                "target_decision_id": {"type": "keyword"},
                "target_docket_number": {"type": "keyword"},
                "law_code": {"type": "keyword"},
                "article": {"type": "keyword"},
                "paragraph": {"type": "keyword"},
                "normalized_ref": {"type": "keyword"},
                "context": {"type": "text", "analyzer": "legal_default"},
            },
        },
    }


def search_pipeline_body(*, rank_constant: int = 60, window_size: int = 300) -> dict[str, Any]:
    return {
        "description": "Hybrid lexical/vector reciprocal rank fusion for Swiss caselaw",
        "phase_results_processors": [
            {
                "score-ranker-processor": {
                    "combination": {
                        "technique": "rrf",
                        "rank_constant": rank_constant,
                        "window_size": window_size,
                    }
                }
            }
        ],
    }


def build_assets(
    *,
    index_prefix: str = "swiss-caselaw",
    vector_dim: int = 1024,
    shards: int = 1,
    replicas: int = 0,
) -> dict[str, Any]:
    return {
        "indices": {
            f"{index_prefix}-decisions-v1": decisions_index_body(
                shards=shards, replicas=replicas, vector_dim=vector_dim
            ),
            f"{index_prefix}-chunks-v1": chunks_index_body(
                shards=shards, replicas=replicas, vector_dim=vector_dim
            ),
            f"{index_prefix}-references-v1": references_index_body(
                shards=shards, replicas=replicas
            ),
        },
        "search_pipeline": {
            f"{index_prefix}-hybrid-rrf-v1": search_pipeline_body(),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate OpenSearch schema assets")
    parser.add_argument("--index-prefix", default="swiss-caselaw")
    parser.add_argument("--vector-dim", type=int, default=1024)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--replicas", type=int, default=0)
    args = parser.parse_args()

    payload = build_assets(
        index_prefix=args.index_prefix,
        vector_dim=args.vector_dim,
        shards=args.shards,
        replicas=args.replicas,
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

