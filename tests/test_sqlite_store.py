"""Tests for SQLite store - written FIRST per TDD."""
import pytest
from pathlib import Path
from datetime import datetime

# Tests written before implementation
class TestSQLiteStore:
    """Test cases for SQLiteStore class."""

    def test_init_creates_tables(self, temp_db_path: Path):
        """Store should create all required tables on initialization."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)

        # Check tables exist
        tables = store.get_tables()
        assert "transactions" in tables
        assert "categories" in tables
        assert "categorization_log" in tables
        store.close()

    def test_init_seeds_default_categories(self, temp_db_path: Path):
        """Store should seed default categories on first init."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        categories = store.get_all_categories()

        assert len(categories) >= 10
        assert any(c["name"] == "Housing" for c in categories)
        assert any(c["name"] == "Food & Dining" for c in categories)
        store.close()

    def test_add_transaction(self, temp_db_path: Path, sample_transactions: list):
        """Should add a transaction and return its ID."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        txn = sample_transactions[0]

        txn_id = store.add_transaction(
            date=txn["date"],
            amount=txn["amount"],
            merchant=txn["merchant"],
            description=txn["description"]
        )

        assert txn_id is not None
        assert isinstance(txn_id, int)
        assert txn_id > 0
        store.close()

    def test_get_transaction(self, temp_db_path: Path, sample_transactions: list):
        """Should retrieve a transaction by ID."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        txn = sample_transactions[0]

        txn_id = store.add_transaction(
            date=txn["date"],
            amount=txn["amount"],
            merchant=txn["merchant"],
            description=txn["description"]
        )

        retrieved = store.get_transaction(txn_id)

        assert retrieved is not None
        assert retrieved["date"] == txn["date"]
        assert retrieved["amount"] == txn["amount"]
        assert retrieved["merchant"] == txn["merchant"]
        store.close()

    def test_add_multiple_transactions(self, temp_db_path: Path, sample_transactions: list):
        """Should add multiple transactions in batch."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)

        ids = store.add_transactions(sample_transactions)

        assert len(ids) == len(sample_transactions)
        assert all(isinstance(id, int) for id in ids)
        store.close()

    def test_get_uncategorized_transactions(self, temp_db_path: Path, sample_transactions: list):
        """Should return only transactions without a category."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        store.add_transactions(sample_transactions)

        uncategorized = store.get_uncategorized_transactions()

        assert len(uncategorized) == len(sample_transactions)
        store.close()

    def test_update_transaction_category(self, temp_db_path: Path, sample_transactions: list):
        """Should update a transaction's category."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        txn_id = store.add_transaction(**sample_transactions[0])
        categories = store.get_all_categories()
        cat_id = categories[0]["id"]

        store.update_transaction_category(txn_id, cat_id)

        txn = store.get_transaction(txn_id)
        assert txn["category_id"] == cat_id
        store.close()

    def test_add_categorization_log(self, temp_db_path: Path, sample_transactions: list):
        """Should log categorization with explanation."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        txn_id = store.add_transaction(**sample_transactions[0])
        categories = store.get_all_categories()
        cat_id = categories[0]["id"]

        log_id = store.add_categorization_log(
            transaction_id=txn_id,
            category_id=cat_id,
            confidence=0.95,
            explanation="This is a Netflix subscription for entertainment.",
            similar_transaction_ids=[1, 2, 3]
        )

        assert log_id is not None
        store.close()

    def test_get_transactions_by_merchant(self, temp_db_path: Path, sample_transactions: list):
        """Should find transactions by merchant name."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        store.add_transactions(sample_transactions)

        netflix_txns = store.get_transactions_by_merchant("NETFLIX")

        assert len(netflix_txns) == 1
        assert netflix_txns[0]["merchant"] == "NETFLIX"
        store.close()

    def test_get_transactions_by_category(self, temp_db_path: Path, sample_transactions: list):
        """Should find transactions by category ID."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        txn_id = store.add_transaction(**sample_transactions[0])
        categories = store.get_all_categories()
        entertainment = next(c for c in categories if c["name"] == "Entertainment")

        store.update_transaction_category(txn_id, entertainment["id"])

        txns = store.get_transactions_by_category(entertainment["id"])
        assert len(txns) == 1
        store.close()

    def test_get_category_stats(self, temp_db_path: Path, anomaly_transactions: list):
        """Should compute per-category statistics."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        for txn in anomaly_transactions[:4]:  # First 4 normal ones
            store.add_transaction(**txn)

        stats = store.get_category_stats(3)  # Food & Dining

        assert "mean" in stats
        assert "std" in stats
        assert "q1" in stats
        assert "q3" in stats
        store.close()

    def test_mark_transaction_recurring(self, temp_db_path: Path, sample_transactions: list):
        """Should mark transaction as recurring."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        txn_id = store.add_transaction(**sample_transactions[0])

        store.mark_transaction_recurring(txn_id, group_id=1)

        txn = store.get_transaction(txn_id)
        assert txn["is_recurring"] == 1
        assert txn["recurring_group_id"] == 1
        store.close()

    def test_mark_transaction_anomaly(self, temp_db_path: Path, sample_transactions: list):
        """Should mark transaction as anomaly."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        txn_id = store.add_transaction(**sample_transactions[0])

        store.mark_transaction_anomaly(txn_id)

        txn = store.get_transaction(txn_id)
        assert txn["is_anomaly"] == 1
        store.close()

    def test_get_all_transactions(self, temp_db_path: Path, sample_transactions: list):
        """Should retrieve all transactions with optional filters."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)
        store.add_transactions(sample_transactions)

        all_txns = store.get_all_transactions()
        assert len(all_txns) == len(sample_transactions)

        # Test date filter
        jan_17_txns = store.get_all_transactions(start_date="2024-01-17", end_date="2024-01-17")
        assert len(jan_17_txns) == 1
        store.close()

    def test_dedupe_on_insert(self, temp_db_path: Path, sample_transactions: list):
        """Should not insert duplicate transactions."""
        from budget_automation.db.sqlite_store import SQLiteStore

        store = SQLiteStore(temp_db_path)

        # Insert twice
        store.add_transactions(sample_transactions)
        store.add_transactions(sample_transactions)

        all_txns = store.get_all_transactions()
        assert len(all_txns) == len(sample_transactions)
        store.close()

    def test_context_manager(self, temp_db_path: Path):
        """Should support context manager protocol."""
        from budget_automation.db.sqlite_store import SQLiteStore

        with SQLiteStore(temp_db_path) as store:
            tables = store.get_tables()
            assert "transactions" in tables
        # Connection should be closed after context
