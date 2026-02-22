"""Split Swiss court decisions into meaningful chunks for embedding.

Strategies (in order of preference):
1. Section-based: Split at Sachverhalt/Erwägungen/Dispositiv boundaries
2. Paragraph-based: Split at double-newline paragraph breaks
3. Positional: Take start, middle, and end segments

Each chunk is truncated to max_chunk_chars for embedding efficiency.
"""

from __future__ import annotations

import re

# Swiss court decision section headers across DE/FR/IT
_SECTION_PATTERNS = [
    re.compile(
        r"^\s*(?:[A-Z][\.\)]\s*|[IVXLC]+[\.\)]\s*)?"
        r"(?:Sachverhalt|Faits|Fatti|Tatbestand)",
        re.IGNORECASE | re.MULTILINE,
    ),
    re.compile(
        r"^\s*(?:[A-Z][\.\)]\s*|[IVXLC]+[\.\)]\s*)?"
        r"(?:Erwägungen?|Considérants?|Considerandi|Begründung|Motivazione|Motivation|In Erwägung)",
        re.IGNORECASE | re.MULTILINE,
    ),
    re.compile(
        r"^\s*(?:[A-Z][\.\)]\s*|[IVXLC]+[\.\)]\s*)?"
        r"(?:Dispositiv|Dispositif|Dispositivo|Urteilsformel|Demnach erkennt)",
        re.IGNORECASE | re.MULTILINE,
    ),
]

# Minimum section length to be considered a real section
_MIN_SECTION_CHARS = 50


def chunk_decision(
    text: str | None,
    max_chunks: int = 3,
    max_chunk_chars: int = 500,
) -> list[str]:
    """Split a decision into up to max_chunks meaningful sections.

    Each chunk is truncated to max_chunk_chars for embedding.

    Args:
        text: Full text of the decision (or None).
        max_chunks: Maximum number of chunks to return (default 3).
        max_chunk_chars: Maximum characters per chunk (default 500).

    Returns:
        List of text chunks (0 to max_chunks items). Empty for None/empty input.
    """
    if not text or len(text) < 100:
        return [text[:max_chunk_chars]] if text else []

    # Strategy 1: Section-based splitting
    chunks = _split_by_sections(text, max_chunks)
    if len(chunks) >= 2:
        return [c[:max_chunk_chars] for c in chunks[:max_chunks]]

    # Strategy 2: Paragraph-based splitting
    chunks = _split_by_paragraphs(text, max_chunks)
    if len(chunks) >= 2:
        return [c[:max_chunk_chars] for c in chunks[:max_chunks]]

    # Strategy 3: Positional splitting (start, middle, end)
    return _split_positional(text, max_chunks, max_chunk_chars)


def _split_by_sections(text: str, max_chunks: int) -> list[str]:
    """Try to split text at major section boundaries."""
    # Find all section header positions
    split_points: list[int] = []
    for pattern in _SECTION_PATTERNS:
        for match in pattern.finditer(text):
            split_points.append(match.start())

    if not split_points:
        return []

    split_points = sorted(set(split_points))

    # Build sections from split points
    sections: list[str] = []

    # Text before first section header (preamble)
    if split_points[0] > _MIN_SECTION_CHARS:
        sections.append(text[: split_points[0]].strip())

    for i, start in enumerate(split_points):
        end = split_points[i + 1] if i + 1 < len(split_points) else len(text)
        section = text[start:end].strip()
        if len(section) >= _MIN_SECTION_CHARS:
            sections.append(section)

    return sections[:max_chunks] if len(sections) >= 2 else []


def _split_by_paragraphs(text: str, max_chunks: int) -> list[str]:
    """Split text at double-newline paragraph boundaries."""
    paragraphs = re.split(r"\n\s*\n", text)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]

    if len(paragraphs) < 2:
        return []

    if len(paragraphs) <= max_chunks:
        return paragraphs

    # Merge short paragraphs into max_chunks groups
    total_len = sum(len(p) for p in paragraphs)
    target_len = total_len // max_chunks
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for p in paragraphs:
        current.append(p)
        current_len += len(p)
        if current_len >= target_len and len(chunks) < max_chunks - 1:
            chunks.append("\n\n".join(current))
            current = []
            current_len = 0

    if current:
        chunks.append("\n\n".join(current))

    return chunks[:max_chunks]


def _split_positional(
    text: str,
    max_chunks: int,
    max_chunk_chars: int,
) -> list[str]:
    """Fall back to taking start, middle, and end segments."""
    n = len(text)
    if max_chunks == 1:
        return [text[:max_chunk_chars]]

    if max_chunks == 2:
        return [
            text[:max_chunk_chars],
            text[n // 2 : n // 2 + max_chunk_chars],
        ]

    return [
        text[:max_chunk_chars],
        text[n // 3 : n // 3 + max_chunk_chars],
        text[2 * n // 3 : 2 * n // 3 + max_chunk_chars],
    ][:max_chunks]
