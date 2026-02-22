"""Tests for search_stack.chunker — decision text splitting."""

from search_stack.chunker import chunk_decision


class TestChunkDecisionSections:
    """Section-based splitting (Sachverhalt/Erwägungen/Dispositiv)."""

    def test_german_sections(self):
        text = (
            "A. Sachverhalt\n\n"
            "Der Beschwerdeführer reichte eine Beschwerde ein. " * 5 + "\n\n"
            "B. Erwägungen\n\n"
            "Das Gericht hat die Beschwerde geprüft. " * 5 + "\n\n"
            "C. Dispositiv\n\n"
            "Die Beschwerde wird abgewiesen."
        )
        chunks = chunk_decision(text, max_chunks=3, max_chunk_chars=200)
        assert len(chunks) >= 2
        assert len(chunks) <= 3
        for c in chunks:
            assert len(c) <= 200

    def test_french_sections(self):
        text = (
            "A. Faits\n\n"
            "Le recourant a déposé un recours. " * 5 + "\n\n"
            "B. Considérants\n\n"
            "Le tribunal a examiné le recours. " * 5 + "\n\n"
            "C. Dispositif\n\n"
            "Le recours est rejeté."
        )
        chunks = chunk_decision(text, max_chunks=3, max_chunk_chars=200)
        assert len(chunks) >= 2

    def test_italian_sections(self):
        text = (
            "A. Fatti\n\n"
            "Il ricorrente ha presentato un ricorso. " * 5 + "\n\n"
            "B. Considerandi\n\n"
            "Il tribunale ha esaminato il ricorso. " * 5 + "\n\n"
            "C. Dispositivo\n\n"
            "Il ricorso è respinto."
        )
        chunks = chunk_decision(text, max_chunks=3, max_chunk_chars=200)
        assert len(chunks) >= 2


class TestChunkDecisionParagraphs:
    """Paragraph-based splitting when no section headers found."""

    def test_paragraph_split(self):
        text = "\n\n".join([
            "Paragraph one about contract law. " * 5,
            "Paragraph two about tort liability. " * 5,
            "Paragraph three about damages. " * 5,
        ])
        chunks = chunk_decision(text, max_chunks=3, max_chunk_chars=200)
        assert len(chunks) >= 2
        assert len(chunks) <= 3

    def test_many_paragraphs_merged(self):
        """Many short paragraphs should be merged into max_chunks groups."""
        text = "\n\n".join([f"Short paragraph {i}." for i in range(10)])
        chunks = chunk_decision(text, max_chunks=3, max_chunk_chars=500)
        assert len(chunks) <= 3


class TestChunkDecisionPositional:
    """Positional fallback when no sections or paragraphs."""

    def test_single_block(self):
        text = "A" * 2000  # no paragraph breaks, no section headers
        chunks = chunk_decision(text, max_chunks=3, max_chunk_chars=500)
        assert len(chunks) == 3
        for c in chunks:
            assert len(c) == 500

    def test_two_chunks(self):
        text = "B" * 1500
        chunks = chunk_decision(text, max_chunks=2, max_chunk_chars=400)
        assert len(chunks) == 2
        for c in chunks:
            assert len(c) <= 400


class TestChunkDecisionEdgeCases:
    """Edge cases."""

    def test_empty_text(self):
        assert chunk_decision("") == []
        assert chunk_decision(None) == []

    def test_short_text(self):
        chunks = chunk_decision("Short", max_chunks=3, max_chunk_chars=500)
        assert len(chunks) == 1
        assert chunks[0] == "Short"

    def test_max_chunk_chars_respected(self):
        text = (
            "Sachverhalt\n\n" + "X" * 1000 + "\n\n"
            "Erwägungen\n\n" + "Y" * 1000 + "\n\n"
            "Dispositiv\n\n" + "Z" * 1000
        )
        chunks = chunk_decision(text, max_chunks=3, max_chunk_chars=100)
        for c in chunks:
            assert len(c) <= 100

    def test_max_chunks_one(self):
        text = "A" * 2000
        chunks = chunk_decision(text, max_chunks=1, max_chunk_chars=500)
        assert len(chunks) == 1
        assert len(chunks[0]) == 500
