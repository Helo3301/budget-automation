"""Tests for anomaly detector - written FIRST per TDD."""
import pytest
from pathlib import Path


class TestAnomalyDetector:
    """Test cases for AnomalyDetector class."""

    def test_detect_amount_anomaly(self, temp_db_path: Path):
        """Should detect unusually large amounts in a category."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.anomaly_detector import AnomalyDetector

        store = SQLiteStore(temp_db_path)
        categories = store.get_all_categories()
        food_cat = next(c for c in categories if c["name"] == "Food & Dining")

        # Normal grocery amounts
        normal_txns = [
            {"date": "2024-01-01", "amount": -50.00, "merchant": "GROCERY", "description": "Food", "category_id": food_cat["id"]},
            {"date": "2024-01-08", "amount": -45.00, "merchant": "GROCERY", "description": "Food", "category_id": food_cat["id"]},
            {"date": "2024-01-15", "amount": -55.00, "merchant": "GROCERY", "description": "Food", "category_id": food_cat["id"]},
            {"date": "2024-01-22", "amount": -48.00, "merchant": "GROCERY", "description": "Food", "category_id": food_cat["id"]},
        ]
        store.add_transactions(normal_txns)

        # Anomaly - way higher than normal
        anomaly_id = store.add_transaction(
            date="2024-01-29",
            amount=-500.00,
            merchant="GROCERY",
            description="Food",
            category_id=food_cat["id"]
        )

        detector = AnomalyDetector(store)
        anomalies = detector.detect()

        assert any(a["transaction_id"] == anomaly_id for a in anomalies)
        store.close()

    def test_detect_new_merchant_high_amount(self, temp_db_path: Path):
        """Should flag new merchants with high amounts."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.anomaly_detector import AnomalyDetector

        store = SQLiteStore(temp_db_path)

        # Establish some transaction history
        for i in range(5):
            store.add_transaction(
                date=f"2024-01-{i+1:02d}",
                amount=-30.00,
                merchant="KNOWN_STORE",
                description="Regular purchase"
            )

        # New merchant with high amount
        anomaly_id = store.add_transaction(
            date="2024-01-20",
            amount=-500.00,
            merchant="NEVER_SEEN_BEFORE",
            description="Unknown purchase"
        )

        detector = AnomalyDetector(store)
        anomalies = detector.detect()

        # Should flag the new merchant high amount
        assert any(a["transaction_id"] == anomaly_id for a in anomalies)
        store.close()

    def test_ignore_normal_amounts(self, temp_db_path: Path):
        """Should not flag transactions within normal range."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.anomaly_detector import AnomalyDetector

        store = SQLiteStore(temp_db_path)
        categories = store.get_all_categories()
        food_cat = next(c for c in categories if c["name"] == "Food & Dining")

        # All normal amounts
        transactions = [
            {"date": "2024-01-01", "amount": -50.00, "merchant": "GROCERY", "description": "Food", "category_id": food_cat["id"]},
            {"date": "2024-01-08", "amount": -45.00, "merchant": "GROCERY", "description": "Food", "category_id": food_cat["id"]},
            {"date": "2024-01-15", "amount": -55.00, "merchant": "GROCERY", "description": "Food", "category_id": food_cat["id"]},
            {"date": "2024-01-22", "amount": -48.00, "merchant": "GROCERY", "description": "Food", "category_id": food_cat["id"]},
            {"date": "2024-01-29", "amount": -52.00, "merchant": "GROCERY", "description": "Food", "category_id": food_cat["id"]},
        ]
        ids = store.add_transactions(transactions)

        detector = AnomalyDetector(store)
        anomalies = detector.detect()

        # None of these should be anomalies
        anomaly_ids = [a["transaction_id"] for a in anomalies]
        assert not any(id in anomaly_ids for id in ids)
        store.close()

    def test_marks_anomalies_in_database(self, temp_db_path: Path):
        """Should mark detected anomalies in database."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.anomaly_detector import AnomalyDetector

        store = SQLiteStore(temp_db_path)
        categories = store.get_all_categories()
        food_cat = next(c for c in categories if c["name"] == "Food & Dining")

        # Normal then anomaly
        for i in range(4):
            store.add_transaction(
                date=f"2024-01-{i+1:02d}",
                amount=-50.00,
                merchant="GROCERY",
                description="Food",
                category_id=food_cat["id"]
            )

        anomaly_id = store.add_transaction(
            date="2024-01-29",
            amount=-500.00,
            merchant="GROCERY",
            description="Food",
            category_id=food_cat["id"]
        )

        detector = AnomalyDetector(store)
        detector.detect_and_mark()

        txn = store.get_transaction(anomaly_id)
        assert txn["is_anomaly"] == 1
        store.close()

    def test_returns_anomaly_details(self, temp_db_path: Path):
        """Should return detailed info about anomalies."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.anomaly_detector import AnomalyDetector

        store = SQLiteStore(temp_db_path)
        categories = store.get_all_categories()
        food_cat = next(c for c in categories if c["name"] == "Food & Dining")

        for i in range(4):
            store.add_transaction(
                date=f"2024-01-{i+1:02d}",
                amount=-50.00,
                merchant="GROCERY",
                description="Food",
                category_id=food_cat["id"]
            )

        store.add_transaction(
            date="2024-01-29",
            amount=-500.00,
            merchant="GROCERY",
            description="Food",
            category_id=food_cat["id"]
        )

        detector = AnomalyDetector(store)
        anomalies = detector.detect()

        if anomalies:
            anomaly = anomalies[0]
            assert "transaction_id" in anomaly
            assert "reason" in anomaly
            assert "amount" in anomaly
        store.close()

    def test_handles_empty_category(self, temp_db_path: Path):
        """Should handle categories with no prior transactions."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.anomaly_detector import AnomalyDetector

        store = SQLiteStore(temp_db_path)
        categories = store.get_all_categories()
        travel_cat = next(c for c in categories if c["name"] == "Travel")

        # First transaction in category - can't be amount anomaly
        store.add_transaction(
            date="2024-01-15",
            amount=-500.00,
            merchant="AIRLINE",
            description="Flight",
            category_id=travel_cat["id"]
        )

        detector = AnomalyDetector(store)
        # Should not crash
        anomalies = detector.detect()
        assert isinstance(anomalies, list)
        store.close()

    def test_handles_empty_database(self, temp_db_path: Path):
        """Should handle empty database gracefully."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.anomaly_detector import AnomalyDetector

        store = SQLiteStore(temp_db_path)
        detector = AnomalyDetector(store)

        anomalies = detector.detect()
        assert anomalies == []
        store.close()
