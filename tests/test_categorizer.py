"""Tests for RAG categorizer - written FIRST per TDD."""
import pytest
from pathlib import Path
from unittest.mock import Mock, patch
import numpy as np


class TestRAGCategorizer:
    """Test cases for RAGCategorizer class."""

    def test_auto_categorize_with_high_confidence(self, temp_db_path: Path, temp_vector_path: Path):
        """Should auto-categorize when similar transactions agree."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.db.vector_store import VectorStore
        from budget_automation.intelligence.embedder import LocalEmbedder
        from budget_automation.intelligence.categorizer import RAGCategorizer

        store = SQLiteStore(temp_db_path)
        vector_store = VectorStore(temp_vector_path)
        embedder = LocalEmbedder()

        categories = store.get_all_categories()
        entertainment = next(c for c in categories if c["name"] == "Entertainment")

        # Add several similar Netflix transactions
        for i in range(5):
            txn_id = store.add_transaction(
                date=f"2024-01-{i+1:02d}",
                amount=-15.99,
                merchant="NETFLIX",
                description="Streaming subscription"
            )
            if txn_id:
                # Manually categorize them
                store.update_transaction_category(txn_id, entertainment["id"])
                # Add to vector store
                embedding = embedder.embed_transaction("NETFLIX", "Streaming subscription", -15.99)
                vector_store.add_embedding(
                    txn_id, embedding, "NETFLIX", "Entertainment", -15.99
                )

        # Now categorize a new similar transaction
        categorizer = RAGCategorizer(store, vector_store, embedder)

        new_txn = {
            "id": 100,
            "merchant": "NETFLIX",
            "description": "Monthly subscription",
            "amount": -15.99
        }

        result = categorizer.categorize(new_txn)

        # Should auto-categorize as Entertainment without API call
        assert result["category_id"] == entertainment["id"]
        assert result["confidence"] >= 0.85
        assert result["method"] == "auto"  # No API call needed
        store.close()
        vector_store.close()

    def test_calls_claude_when_low_confidence(self, temp_db_path: Path, temp_vector_path: Path):
        """Should call Claude API when similar transactions disagree."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.db.vector_store import VectorStore
        from budget_automation.intelligence.embedder import LocalEmbedder
        from budget_automation.intelligence.categorizer import RAGCategorizer

        store = SQLiteStore(temp_db_path)
        vector_store = VectorStore(temp_vector_path)
        embedder = LocalEmbedder()

        categories = store.get_all_categories()

        # Add mixed category transactions
        for i, cat_name in enumerate(["Entertainment", "Shopping", "Subscriptions"]):
            cat = next(c for c in categories if c["name"] == cat_name)
            txn_id = store.add_transaction(
                date=f"2024-01-{i+1:02d}",
                amount=-9.99,
                merchant="GENERIC STORE",
                description="Purchase"
            )
            if txn_id:
                store.update_transaction_category(txn_id, cat["id"])
                embedding = embedder.embed_transaction("GENERIC STORE", "Purchase", -9.99)
                vector_store.add_embedding(
                    txn_id, embedding, "GENERIC STORE", cat_name, -9.99
                )

        categorizer = RAGCategorizer(store, vector_store, embedder)

        new_txn = {
            "id": 100,
            "merchant": "GENERIC STORE",
            "description": "New purchase",
            "amount": -9.99
        }

        result = categorizer.categorize(new_txn)

        # Should return needs_review when confidence is low (no Claude API call)
        assert result["method"] == "needs_review"
        assert result["category_id"] is None
        assert "suggestions" in result

        store.close()
        vector_store.close()

    def test_builds_rag_context(self, temp_db_path: Path, temp_vector_path: Path):
        """Should build proper context with similar transactions."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.db.vector_store import VectorStore
        from budget_automation.intelligence.embedder import LocalEmbedder
        from budget_automation.intelligence.categorizer import RAGCategorizer

        store = SQLiteStore(temp_db_path)
        vector_store = VectorStore(temp_vector_path)
        embedder = LocalEmbedder()

        categories = store.get_all_categories()
        food = next(c for c in categories if c["name"] == "Food & Dining")

        # Add some food transactions
        for i, merchant in enumerate(["STARBUCKS", "CHIPOTLE", "SUBWAY"]):
            txn_id = store.add_transaction(
                date=f"2024-01-{i+1:02d}",
                amount=-12.00,
                merchant=merchant,
                description="Food purchase"
            )
            if txn_id:
                store.update_transaction_category(txn_id, food["id"])
                embedding = embedder.embed_transaction(merchant, "Food purchase", -12.00)
                vector_store.add_embedding(
                    txn_id, embedding, merchant, "Food & Dining", -12.00
                )

        categorizer = RAGCategorizer(store, vector_store, embedder)

        new_txn = {
            "id": 100,
            "merchant": "PANERA BREAD",
            "description": "Lunch",
            "amount": -15.00
        }

        context = categorizer._build_rag_context(new_txn)

        # Context should include similar transactions
        assert "similar_transactions" in context
        assert len(context["similar_transactions"]) > 0
        store.close()
        vector_store.close()

    def test_returns_explanation(self, temp_db_path: Path, temp_vector_path: Path):
        """Should return explanation for categorization."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.db.vector_store import VectorStore
        from budget_automation.intelligence.embedder import LocalEmbedder
        from budget_automation.intelligence.categorizer import RAGCategorizer

        store = SQLiteStore(temp_db_path)
        vector_store = VectorStore(temp_vector_path)
        embedder = LocalEmbedder()

        categories = store.get_all_categories()
        entertainment = next(c for c in categories if c["name"] == "Entertainment")

        # Add similar transactions
        for i in range(4):
            txn_id = store.add_transaction(
                date=f"2024-01-{i+1:02d}",
                amount=-15.99,
                merchant="NETFLIX",
                description="Streaming"
            )
            if txn_id:
                store.update_transaction_category(txn_id, entertainment["id"])
                embedding = embedder.embed_transaction("NETFLIX", "Streaming", -15.99)
                vector_store.add_embedding(
                    txn_id, embedding, "NETFLIX", "Entertainment", -15.99
                )

        categorizer = RAGCategorizer(store, vector_store, embedder)

        new_txn = {
            "id": 100,
            "merchant": "NETFLIX",
            "description": "Monthly",
            "amount": -15.99
        }

        result = categorizer.categorize(new_txn)

        assert "explanation" in result
        assert len(result["explanation"]) > 0
        store.close()
        vector_store.close()

    def test_handles_empty_vector_store(self, temp_db_path: Path, temp_vector_path: Path):
        """Should handle case with no similar transactions."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.db.vector_store import VectorStore
        from budget_automation.intelligence.embedder import LocalEmbedder
        from budget_automation.intelligence.categorizer import RAGCategorizer

        store = SQLiteStore(temp_db_path)
        vector_store = VectorStore(temp_vector_path)
        embedder = LocalEmbedder()

        categorizer = RAGCategorizer(store, vector_store, embedder)

        new_txn = {
            "id": 1,
            "merchant": "NEW MERCHANT",
            "description": "Unknown purchase",
            "amount": -50.00
        }

        result = categorizer.categorize(new_txn)

        # Should return needs_review when no similar transactions (no Claude API call)
        assert "category_id" in result
        assert result["method"] == "needs_review"
        assert result["category_id"] is None

        store.close()
        vector_store.close()

    def test_logs_categorization(self, temp_db_path: Path, temp_vector_path: Path):
        """Should log categorization decision to database."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.db.vector_store import VectorStore
        from budget_automation.intelligence.embedder import LocalEmbedder
        from budget_automation.intelligence.categorizer import RAGCategorizer

        store = SQLiteStore(temp_db_path)
        vector_store = VectorStore(temp_vector_path)
        embedder = LocalEmbedder()

        categories = store.get_all_categories()
        entertainment = next(c for c in categories if c["name"] == "Entertainment")

        # Add similar transactions
        for i in range(4):
            txn_id = store.add_transaction(
                date=f"2024-01-{i+1:02d}",
                amount=-15.99,
                merchant="NETFLIX",
                description="Streaming"
            )
            if txn_id:
                store.update_transaction_category(txn_id, entertainment["id"])
                embedding = embedder.embed_transaction("NETFLIX", "Streaming", -15.99)
                vector_store.add_embedding(
                    txn_id, embedding, "NETFLIX", "Entertainment", -15.99
                )

        categorizer = RAGCategorizer(store, vector_store, embedder)

        # Add a new transaction to categorize
        new_txn_id = store.add_transaction(
            date="2024-01-15",
            amount=-15.99,
            merchant="NETFLIX",
            description="Monthly subscription"
        )

        new_txn = {
            "id": new_txn_id,
            "merchant": "NETFLIX",
            "description": "Monthly subscription",
            "amount": -15.99
        }

        categorizer.categorize_and_update(new_txn)

        # Check that log was created
        log = store.get_categorization_log(new_txn_id)
        assert log is not None
        assert "explanation" in log
        store.close()
        vector_store.close()

    def test_get_available_categories(self, temp_db_path: Path, temp_vector_path: Path):
        """Should return list of available categories for prompt."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.db.vector_store import VectorStore
        from budget_automation.intelligence.embedder import LocalEmbedder
        from budget_automation.intelligence.categorizer import RAGCategorizer

        store = SQLiteStore(temp_db_path)
        vector_store = VectorStore(temp_vector_path)
        embedder = LocalEmbedder()

        categorizer = RAGCategorizer(store, vector_store, embedder)
        categories = categorizer.get_available_categories()

        assert len(categories) > 0
        assert any(c["name"] == "Food & Dining" for c in categories)
        store.close()
        vector_store.close()
