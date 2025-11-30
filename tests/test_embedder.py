"""Tests for local embedder - written FIRST per TDD."""
import pytest
import numpy as np


class TestLocalEmbedder:
    """Test cases for LocalEmbedder class."""

    def test_embed_single_text(self):
        """Should embed a single text string."""
        from budget_automation.intelligence.embedder import LocalEmbedder

        embedder = LocalEmbedder()
        embedding = embedder.embed("NETFLIX monthly subscription $15.99")

        assert embedding is not None
        assert isinstance(embedding, np.ndarray)
        assert embedding.shape == (384,)
        assert embedding.dtype == np.float32

    def test_embed_batch(self):
        """Should embed multiple texts in batch."""
        from budget_automation.intelligence.embedder import LocalEmbedder

        embedder = LocalEmbedder()
        texts = [
            "NETFLIX subscription",
            "WHOLE FOODS groceries",
            "SHELL gas station"
        ]

        embeddings = embedder.embed_batch(texts)

        assert len(embeddings) == 3
        assert all(e.shape == (384,) for e in embeddings)

    def test_similar_texts_have_similar_embeddings(self):
        """Similar transaction descriptions should have high cosine similarity."""
        from budget_automation.intelligence.embedder import LocalEmbedder

        embedder = LocalEmbedder()

        # Similar texts
        e1 = embedder.embed("NETFLIX monthly subscription streaming")
        e2 = embedder.embed("HULU streaming service subscription")

        # Different text
        e3 = embedder.embed("SHELL gas station fuel purchase")

        # Cosine similarity
        def cosine_sim(a, b):
            return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

        sim_similar = cosine_sim(e1, e2)
        sim_different = cosine_sim(e1, e3)

        assert sim_similar > sim_different

    def test_singleton_pattern(self):
        """Embedder should use singleton pattern for efficiency."""
        from budget_automation.intelligence.embedder import LocalEmbedder

        e1 = LocalEmbedder()
        e2 = LocalEmbedder()

        # Should share the same model
        assert e1._model is e2._model

    def test_lazy_loading(self):
        """Model should only load on first embed call."""
        from budget_automation.intelligence.embedder import LocalEmbedder

        # Force a fresh instance for this test
        LocalEmbedder._instance = None
        LocalEmbedder._model = None

        embedder = LocalEmbedder()

        # Model should be None before first embed
        assert LocalEmbedder._model is None

        # First embed should load the model
        embedder.embed("test")

        # Now model should be loaded
        assert LocalEmbedder._model is not None

    def test_embed_transaction_format(self):
        """Should embed transaction in standard format."""
        from budget_automation.intelligence.embedder import LocalEmbedder

        embedder = LocalEmbedder()
        embedding = embedder.embed_transaction(
            merchant="WHOLE FOODS",
            description="Weekly groceries",
            amount=-125.50
        )

        assert embedding.shape == (384,)

    def test_batch_size_handling(self):
        """Should handle batch sizes correctly for large inputs."""
        from budget_automation.intelligence.embedder import LocalEmbedder

        embedder = LocalEmbedder()

        # Create 100 texts
        texts = [f"Transaction {i}" for i in range(100)]

        embeddings = embedder.embed_batch(texts, batch_size=32)

        assert len(embeddings) == 100

    def test_empty_text_handling(self):
        """Should handle empty text gracefully."""
        from budget_automation.intelligence.embedder import LocalEmbedder

        embedder = LocalEmbedder()

        # Should not crash on empty string
        embedding = embedder.embed("")

        assert embedding.shape == (384,)

    def test_normalize_embeddings(self):
        """Embeddings should be normalized."""
        from budget_automation.intelligence.embedder import LocalEmbedder

        embedder = LocalEmbedder()
        embedding = embedder.embed("test transaction")

        # Check that embedding is normalized (L2 norm ≈ 1)
        norm = np.linalg.norm(embedding)
        assert 0.99 < norm < 1.01
