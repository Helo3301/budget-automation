"""Tests for LanceDB vector store - written FIRST per TDD."""
import pytest
import numpy as np
from pathlib import Path
from typing import List


class TestVectorStore:
    """Test cases for VectorStore class."""

    def test_init_creates_table(self, temp_vector_path: Path):
        """Store should create table on initialization."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)

        assert store.table is not None
        store.close()

    def test_add_embedding(self, temp_vector_path: Path):
        """Should add an embedding with metadata."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)
        embedding = np.random.rand(384).astype(np.float32)

        store.add_embedding(
            transaction_id=1,
            embedding=embedding,
            merchant="NETFLIX",
            category="Entertainment",
            amount=-15.99
        )

        # Verify it was added
        results = store.search(embedding, k=1)
        assert len(results) == 1
        assert results[0]["transaction_id"] == 1
        store.close()

    def test_add_embeddings_batch(self, temp_vector_path: Path):
        """Should add multiple embeddings in batch."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)
        embeddings = [
            {
                "transaction_id": i,
                "embedding": np.random.rand(384).astype(np.float32),
                "merchant": f"MERCHANT_{i}",
                "category": "Shopping",
                "amount": -10.0 * i
            }
            for i in range(1, 6)
        ]

        store.add_embeddings(embeddings)

        # Verify all were added
        results = store.search(embeddings[0]["embedding"], k=10)
        assert len(results) == 5
        store.close()

    def test_search_returns_similar(self, temp_vector_path: Path):
        """Should return most similar embeddings."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)

        # Add embeddings with known similarity
        base_embedding = np.ones(384, dtype=np.float32) * 0.5
        similar_embedding = base_embedding + np.random.rand(384).astype(np.float32) * 0.1
        different_embedding = np.random.rand(384).astype(np.float32)

        store.add_embedding(1, similar_embedding, "SIMILAR", "Cat1", -10.0)
        store.add_embedding(2, different_embedding, "DIFFERENT", "Cat2", -20.0)

        # Search with base embedding
        results = store.search(base_embedding, k=2)

        # Similar should be first (higher score)
        assert results[0]["transaction_id"] == 1
        store.close()

    def test_search_with_k_limit(self, temp_vector_path: Path):
        """Should respect k limit in search."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)

        # Add 10 embeddings
        for i in range(10):
            store.add_embedding(
                i,
                np.random.rand(384).astype(np.float32),
                f"MERCHANT_{i}",
                "Category",
                -10.0
            )

        query = np.random.rand(384).astype(np.float32)
        results = store.search(query, k=3)

        assert len(results) == 3
        store.close()

    def test_search_returns_scores(self, temp_vector_path: Path):
        """Search results should include similarity scores."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)
        embedding = np.random.rand(384).astype(np.float32)
        store.add_embedding(1, embedding, "MERCHANT", "Cat", -10.0)

        results = store.search(embedding, k=1)

        assert "_distance" in results[0] or "score" in results[0]
        store.close()

    def test_search_empty_store(self, temp_vector_path: Path):
        """Should handle search on empty store gracefully."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)
        query = np.random.rand(384).astype(np.float32)

        results = store.search(query, k=5)

        assert results == []
        store.close()

    def test_delete_embedding(self, temp_vector_path: Path):
        """Should delete embedding by transaction ID."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)
        embedding = np.random.rand(384).astype(np.float32)
        store.add_embedding(1, embedding, "MERCHANT", "Cat", -10.0)

        store.delete_embedding(1)

        results = store.search(embedding, k=1)
        assert len(results) == 0
        store.close()

    def test_get_embedding(self, temp_vector_path: Path):
        """Should retrieve embedding by transaction ID."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)
        embedding = np.random.rand(384).astype(np.float32)
        store.add_embedding(1, embedding, "NETFLIX", "Entertainment", -15.99)

        result = store.get_embedding(1)

        assert result is not None
        assert result["merchant"] == "NETFLIX"
        assert np.allclose(result["vector"], embedding, atol=1e-5)
        store.close()

    def test_update_category(self, temp_vector_path: Path):
        """Should update category for an embedding."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)
        embedding = np.random.rand(384).astype(np.float32)
        store.add_embedding(1, embedding, "MERCHANT", "OldCategory", -10.0)

        store.update_category(1, "NewCategory")

        result = store.get_embedding(1)
        assert result["category"] == "NewCategory"
        store.close()

    def test_count(self, temp_vector_path: Path):
        """Should return correct count of embeddings."""
        from budget_automation.db.vector_store import VectorStore

        store = VectorStore(temp_vector_path)

        assert store.count() == 0

        for i in range(5):
            store.add_embedding(
                i,
                np.random.rand(384).astype(np.float32),
                f"M{i}",
                "C",
                -10.0
            )

        assert store.count() == 5
        store.close()
