"""Tests for recurring transaction detector - written FIRST per TDD."""
import pytest
from pathlib import Path
from datetime import datetime, timedelta


class TestRecurringDetector:
    """Test cases for RecurringDetector class."""

    def test_detect_monthly_subscription(self, temp_db_path: Path):
        """Should detect monthly recurring transactions."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.recurring_detector import RecurringDetector

        store = SQLiteStore(temp_db_path)

        # Add Netflix subscription - monthly on 15th
        transactions = [
            {"date": "2024-01-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
            {"date": "2024-02-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
            {"date": "2024-03-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
        ]
        store.add_transactions(transactions)

        detector = RecurringDetector(store)
        recurring = detector.detect()

        assert len(recurring) >= 1
        netflix_group = next((r for r in recurring if "NETFLIX" in r["merchant"]), None)
        assert netflix_group is not None
        assert netflix_group["interval_days"] in [28, 29, 30, 31]  # Monthly
        store.close()

    def test_detect_weekly_subscription(self, temp_db_path: Path):
        """Should detect weekly recurring transactions."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.recurring_detector import RecurringDetector

        store = SQLiteStore(temp_db_path)

        # Weekly grocery shopping
        transactions = [
            {"date": "2024-01-07", "amount": -100.00, "merchant": "GROCERY", "description": "Weekly shop"},
            {"date": "2024-01-14", "amount": -100.00, "merchant": "GROCERY", "description": "Weekly shop"},
            {"date": "2024-01-21", "amount": -100.00, "merchant": "GROCERY", "description": "Weekly shop"},
            {"date": "2024-01-28", "amount": -100.00, "merchant": "GROCERY", "description": "Weekly shop"},
        ]
        store.add_transactions(transactions)

        detector = RecurringDetector(store)
        recurring = detector.detect()

        assert len(recurring) >= 1
        grocery_group = next((r for r in recurring if "GROCERY" in r["merchant"]), None)
        assert grocery_group is not None
        assert grocery_group["interval_days"] == 7
        store.close()

    def test_detect_annual_subscription(self, temp_db_path: Path):
        """Should detect annual recurring transactions."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.recurring_detector import RecurringDetector

        store = SQLiteStore(temp_db_path)

        # Annual domain renewal
        transactions = [
            {"date": "2022-03-01", "amount": -12.00, "merchant": "GODADDY", "description": "Domain"},
            {"date": "2023-03-01", "amount": -12.00, "merchant": "GODADDY", "description": "Domain"},
            {"date": "2024-03-01", "amount": -12.00, "merchant": "GODADDY", "description": "Domain"},
        ]
        store.add_transactions(transactions)

        detector = RecurringDetector(store)
        recurring = detector.detect()

        assert len(recurring) >= 1
        domain_group = next((r for r in recurring if "GODADDY" in r["merchant"]), None)
        assert domain_group is not None
        assert domain_group["interval_days"] in [365, 366]  # Yearly
        store.close()

    def test_ignore_one_off_transactions(self, temp_db_path: Path):
        """Should not flag one-off transactions as recurring."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.recurring_detector import RecurringDetector

        store = SQLiteStore(temp_db_path)

        # One-off transactions
        transactions = [
            {"date": "2024-01-15", "amount": -50.00, "merchant": "RESTAURANT", "description": "Dinner"},
            {"date": "2024-02-20", "amount": -75.00, "merchant": "STORE", "description": "Shopping"},
        ]
        store.add_transactions(transactions)

        detector = RecurringDetector(store)
        recurring = detector.detect()

        # No recurring patterns
        assert len(recurring) == 0
        store.close()

    def test_tolerate_slight_date_variations(self, temp_db_path: Path):
        """Should detect recurring even with slight date variations."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.recurring_detector import RecurringDetector

        store = SQLiteStore(temp_db_path)

        # Rent - usually on 1st but sometimes 2nd or 3rd
        transactions = [
            {"date": "2024-01-01", "amount": -2000.00, "merchant": "LANDLORD", "description": "Rent"},
            {"date": "2024-02-02", "amount": -2000.00, "merchant": "LANDLORD", "description": "Rent"},  # +1 day
            {"date": "2024-03-01", "amount": -2000.00, "merchant": "LANDLORD", "description": "Rent"},
            {"date": "2024-04-03", "amount": -2000.00, "merchant": "LANDLORD", "description": "Rent"},  # +2 days
        ]
        store.add_transactions(transactions)

        detector = RecurringDetector(store)
        recurring = detector.detect()

        assert len(recurring) >= 1
        rent_group = next((r for r in recurring if "LANDLORD" in r["merchant"]), None)
        assert rent_group is not None
        store.close()

    def test_marks_transactions_as_recurring(self, temp_db_path: Path):
        """Should mark detected transactions as recurring in database."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.recurring_detector import RecurringDetector

        store = SQLiteStore(temp_db_path)

        transactions = [
            {"date": "2024-01-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
            {"date": "2024-02-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
            {"date": "2024-03-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
        ]
        ids = store.add_transactions(transactions)

        detector = RecurringDetector(store)
        detector.detect_and_mark()

        # Check that transactions are marked
        for txn_id in ids:
            txn = store.get_transaction(txn_id)
            assert txn["is_recurring"] == 1
        store.close()

    def test_returns_recurring_groups(self, temp_db_path: Path):
        """Should return groups with all relevant info."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.recurring_detector import RecurringDetector

        store = SQLiteStore(temp_db_path)

        transactions = [
            {"date": "2024-01-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
            {"date": "2024-02-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
        ]
        store.add_transactions(transactions)

        detector = RecurringDetector(store)
        recurring = detector.detect()

        if recurring:
            group = recurring[0]
            assert "merchant" in group
            assert "amount" in group
            assert "interval_days" in group
            assert "transaction_ids" in group
            assert "count" in group
        store.close()

    def test_handles_empty_database(self, temp_db_path: Path):
        """Should handle empty database gracefully."""
        from budget_automation.db.sqlite_store import SQLiteStore
        from budget_automation.intelligence.recurring_detector import RecurringDetector

        store = SQLiteStore(temp_db_path)
        detector = RecurringDetector(store)

        recurring = detector.detect()
        assert recurring == []
        store.close()
