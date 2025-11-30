"""Integration tests for the full budget automation system."""
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch


class TestIntegration:
    """End-to-end integration tests."""

    @pytest.fixture
    def temp_service(self):
        """Create a temporary service for testing."""
        from budget_automation.api.budget_service import BudgetService

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            vector_path = Path(tmpdir) / "vectors"

            with BudgetService(db_path=db_path, vector_path=vector_path) as service:
                yield service

    @pytest.fixture
    def sample_csv_file(self, tmp_path) -> Path:
        """Create a sample CSV file."""
        csv_path = tmp_path / "transactions.csv"
        csv_path.write_text("""Date,Amount,Merchant,Description
2024-01-15,-15.99,NETFLIX,Streaming subscription
2024-01-16,-125.50,WHOLE FOODS,Weekly groceries
2024-01-17,-2500.00,LANDLORD,January rent
2024-01-18,3500.00,EMPLOYER,Payroll
2024-02-15,-15.99,NETFLIX,Streaming subscription
2024-02-16,-130.00,WHOLE FOODS,Weekly groceries
2024-02-17,-2500.00,LANDLORD,February rent
2024-02-18,3500.00,EMPLOYER,Payroll
2024-03-15,-15.99,NETFLIX,Streaming subscription
2024-03-16,-120.00,WHOLE FOODS,Weekly groceries
2024-03-17,-2500.00,LANDLORD,March rent
2024-03-18,3500.00,EMPLOYER,Payroll
""")
        return csv_path

    def test_full_import_and_analyze_flow(self, temp_service, sample_csv_file):
        """Test full flow: import -> analyze -> summary."""
        # Import
        result = temp_service.import_file(sample_csv_file, auto_categorize=False)
        assert result["added"] == 12

        # Analyze
        analysis = temp_service.analyze_transactions()
        assert len(analysis["recurring_patterns"]) >= 2  # Netflix and Landlord should be detected

        # Summary
        summary = temp_service.get_summary()
        assert summary["total_transactions"] == 12
        assert summary["total_income"] == 10500.00  # 3 * 3500
        assert summary["total_expenses"] > 0

    def test_semantic_search(self, temp_service, sample_csv_file):
        """Test semantic search functionality."""
        # Import with categorization
        with patch.object(temp_service.categorizer, '_call_claude') as mock_claude:
            mock_claude.return_value = {
                "category": "Entertainment",
                "confidence": 0.9,
                "explanation": "Streaming service"
            }
            temp_service.import_file(sample_csv_file, auto_categorize=True)

        # Search for streaming
        results = temp_service.search_similar("streaming video service", k=3)
        assert len(results) > 0
        # Netflix transactions should be top results
        assert any("NETFLIX" in r["merchant"] for r in results)

    def test_recurring_detection(self, temp_service, sample_csv_file):
        """Test recurring transaction detection."""
        temp_service.import_file(sample_csv_file, auto_categorize=False)
        analysis = temp_service.analyze_transactions()

        # Should detect Netflix (monthly $15.99)
        netflix = next((r for r in analysis["recurring_patterns"]
                       if "NETFLIX" in r["merchant"]), None)
        assert netflix is not None
        assert netflix["amount"] == -15.99

        # Should detect Landlord (monthly $2500)
        rent = next((r for r in analysis["recurring_patterns"]
                    if "LANDLORD" in r["merchant"]), None)
        assert rent is not None
        assert rent["amount"] == -2500.00

    def test_categories_available(self, temp_service):
        """Test that default categories are available."""
        categories = temp_service.get_categories()
        assert len(categories) >= 10
        assert any(c["name"] == "Housing" for c in categories)
        assert any(c["name"] == "Entertainment" for c in categories)
        assert any(c["name"] == "Food & Dining" for c in categories)

    def test_manual_categorization(self, temp_service, sample_csv_file):
        """Test manual categorization."""
        temp_service.import_file(sample_csv_file, auto_categorize=False)

        # Get uncategorized
        uncategorized = temp_service.get_uncategorized()
        assert len(uncategorized) > 0

        # Manually categorize first one
        txn_id = uncategorized[0]["id"]
        success = temp_service.categorize_transaction(txn_id, "Entertainment")
        assert success

        # Verify it's now categorized
        txn = temp_service.get_transaction(txn_id)
        assert txn["category_id"] is not None

    def test_duplicate_prevention(self, temp_service, sample_csv_file):
        """Test that duplicates are not imported twice."""
        # Import once
        result1 = temp_service.import_file(sample_csv_file, auto_categorize=False)
        assert result1["added"] == 12

        # Import again
        result2 = temp_service.import_file(sample_csv_file, auto_categorize=False)
        assert result2["added"] == 0
        assert result2["duplicates"] == 12

    def test_auto_categorization_with_rag(self, temp_service):
        """Test RAG-based auto-categorization."""
        # First, manually categorize some transactions to build context
        categories = temp_service.get_categories()
        entertainment = next(c for c in categories if c["name"] == "Entertainment")
        food = next(c for c in categories if c["name"] == "Food & Dining")

        # Add and categorize Netflix transactions
        for i in range(5):
            txn_id = temp_service.store.add_transaction(
                date=f"2024-01-{i+1:02d}",
                amount=-15.99,
                merchant="NETFLIX",
                description="Streaming"
            )
            if txn_id:
                temp_service.categorize_transaction(txn_id, "Entertainment")

        # Add and categorize food transactions
        for i in range(5):
            txn_id = temp_service.store.add_transaction(
                date=f"2024-01-{i+10:02d}",
                amount=-50.00,
                merchant="CHIPOTLE",
                description="Lunch"
            )
            if txn_id:
                temp_service.categorize_transaction(txn_id, "Food & Dining")

        # Now add a new similar transaction
        new_txn_id = temp_service.store.add_transaction(
            date="2024-02-01",
            amount=-15.99,
            merchant="NETFLIX",
            description="Monthly subscription"
        )

        # Categorize it - should auto-categorize as Entertainment
        txn = temp_service.get_transaction(new_txn_id)
        result = temp_service.categorizer.categorize({
            "id": new_txn_id,
            "merchant": txn["merchant"],
            "description": txn.get("description"),
            "amount": txn["amount"]
        })

        # Should be auto-categorized (not using Claude)
        assert result["method"] == "auto"
        assert result["category_id"] == entertainment["id"]
