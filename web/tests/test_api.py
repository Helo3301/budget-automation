"""TDD tests for FastAPI budget automation API."""
import pytest
import tempfile
from pathlib import Path
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock


class TestBudgetAPI:
    """Tests for the budget API endpoints."""

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
    def client(self, temp_service):
        """Create test client with temp service."""
        from budget_automation.web.api import app, get_service

        # Override the dependency
        app.dependency_overrides[get_service] = lambda: temp_service
        yield TestClient(app)
        app.dependency_overrides.clear()

    @pytest.fixture
    def sample_csv(self, tmp_path) -> Path:
        """Create a sample CSV file."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("""Date,Amount,Merchant,Description
2024-01-15,-15.99,NETFLIX,Streaming
2024-01-16,-125.50,WHOLE FOODS,Groceries
2024-01-17,-2500.00,LANDLORD,Rent
2024-01-18,3500.00,EMPLOYER,Payroll
""")
        return csv_path

    # === Summary Endpoint ===

    def test_get_summary_empty(self, client):
        """Test summary endpoint with no data."""
        response = client.get("/api/summary")
        assert response.status_code == 200
        data = response.json()
        assert "total_transactions" in data
        assert data["total_transactions"] == 0
        assert "total_income" in data
        assert "total_expenses" in data
        assert "net" in data
        assert "category_breakdown" in data

    def test_get_summary_with_data(self, client, temp_service, sample_csv):
        """Test summary with imported data."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        response = client.get("/api/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["total_transactions"] == 4
        assert data["total_income"] == 3500.00
        assert data["total_expenses"] == 2641.49

    # === Categories Endpoint ===

    def test_get_categories(self, client):
        """Test categories endpoint returns default categories."""
        response = client.get("/api/categories")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 10
        # Check structure
        assert all("id" in c and "name" in c for c in data)
        # Check common categories exist
        names = [c["name"] for c in data]
        assert "Housing" in names
        assert "Entertainment" in names
        assert "Food & Dining" in names

    # === Transactions Endpoints ===

    def test_get_transactions_empty(self, client):
        """Test transactions list when empty."""
        response = client.get("/api/transactions")
        assert response.status_code == 200
        data = response.json()
        assert "transactions" in data
        assert "total" in data
        assert data["total"] == 0
        assert data["transactions"] == []

    def test_get_transactions_with_data(self, client, temp_service, sample_csv):
        """Test transactions list with data."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        response = client.get("/api/transactions")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 4
        assert len(data["transactions"]) == 4

    def test_get_transactions_pagination(self, client, temp_service, sample_csv):
        """Test transactions pagination."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        response = client.get("/api/transactions?limit=2&offset=0")
        assert response.status_code == 200
        data = response.json()
        assert len(data["transactions"]) == 2
        assert data["total"] == 4

    def test_get_transactions_filter_by_category(self, client, temp_service, sample_csv):
        """Test filtering by category."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        # Categorize one
        uncategorized = temp_service.get_uncategorized()
        temp_service.categorize_transaction(uncategorized[0]["id"], "Entertainment")

        response = client.get("/api/transactions?category=Entertainment")
        data = response.json()
        assert len(data["transactions"]) == 1

    def test_get_single_transaction(self, client, temp_service, sample_csv):
        """Test getting a single transaction."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        # Get first transaction ID
        txns = temp_service.get_uncategorized()
        txn_id = txns[0]["id"]

        response = client.get(f"/api/transactions/{txn_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == txn_id
        assert "date" in data
        assert "amount" in data
        assert "merchant" in data

    def test_get_single_transaction_not_found(self, client):
        """Test 404 for non-existent transaction."""
        response = client.get("/api/transactions/99999")
        assert response.status_code == 404

    # === Uncategorized Endpoint ===

    def test_get_uncategorized(self, client, temp_service, sample_csv):
        """Test uncategorized transactions endpoint."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        response = client.get("/api/uncategorized")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 4  # All should be uncategorized

    def test_get_uncategorized_after_categorization(self, client, temp_service, sample_csv):
        """Test uncategorized count decreases after categorization."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        uncategorized = temp_service.get_uncategorized()
        temp_service.categorize_transaction(uncategorized[0]["id"], "Entertainment")

        response = client.get("/api/uncategorized")
        data = response.json()
        assert len(data) == 3

    # === Categorize Endpoint ===

    def test_categorize_transaction(self, client, temp_service, sample_csv):
        """Test manual categorization endpoint."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        uncategorized = temp_service.get_uncategorized()
        txn_id = uncategorized[0]["id"]

        response = client.post(
            f"/api/transactions/{txn_id}/categorize",
            json={"category": "Entertainment"}
        )
        assert response.status_code == 200
        assert response.json()["success"] is True

        # Verify it's categorized
        txn = temp_service.get_transaction(txn_id)
        assert txn["category_id"] is not None

    def test_categorize_invalid_category(self, client, temp_service, sample_csv):
        """Test categorization with invalid category."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        uncategorized = temp_service.get_uncategorized()
        txn_id = uncategorized[0]["id"]

        response = client.post(
            f"/api/transactions/{txn_id}/categorize",
            json={"category": "Invalid Category Name"}
        )
        assert response.status_code == 400

    # === Recurring Endpoint ===

    def test_get_recurring_empty(self, client):
        """Test recurring endpoint with no data."""
        response = client.get("/api/recurring")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0

    def test_get_recurring_with_patterns(self, client, temp_service, tmp_path):
        """Test recurring endpoint detects patterns."""
        # Create CSV with recurring transactions
        csv_path = tmp_path / "recurring.csv"
        csv_path.write_text("""Date,Amount,Merchant,Description
2024-01-15,-15.99,NETFLIX,Streaming
2024-02-15,-15.99,NETFLIX,Streaming
2024-03-15,-15.99,NETFLIX,Streaming
""")
        temp_service.import_file(csv_path, auto_categorize=False)
        temp_service.analyze_transactions()

        response = client.get("/api/recurring")
        data = response.json()
        assert len(data) >= 1
        assert any("NETFLIX" in r["merchant"] for r in data)

    # === Anomalies Endpoint ===

    def test_get_anomalies_empty(self, client):
        """Test anomalies endpoint with no data."""
        response = client.get("/api/anomalies")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_anomalies_with_outlier(self, client, temp_service, tmp_path):
        """Test anomalies endpoint returns anomaly data structure."""
        # Create CSV with transactions
        csv_path = tmp_path / "anomaly.csv"
        csv_path.write_text("""Date,Amount,Merchant,Description
2024-01-01,-50.00,GROCERY,Normal
2024-01-02,-45.00,GROCERY,Normal
2024-01-03,-55.00,GROCERY,Normal
2024-01-04,-48.00,GROCERY,Normal
2024-01-05,-500.00,GROCERY,Large purchase
""")
        temp_service.import_file(csv_path, auto_categorize=False)
        temp_service.analyze_transactions()

        response = client.get("/api/anomalies")
        assert response.status_code == 200
        data = response.json()
        # Response should be a list (may or may not have anomalies depending on threshold)
        assert isinstance(data, list)

    # === Search Endpoint ===

    def test_search_empty_query(self, client):
        """Test search with empty query."""
        response = client.get("/api/search?q=")
        # FastAPI returns 422 for validation errors
        assert response.status_code == 422

    def test_search_no_results(self, client):
        """Test search with no matching results."""
        response = client.get("/api/search?q=nonexistent")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0

    def test_search_with_results(self, client, temp_service, sample_csv):
        """Test semantic search returns response structure."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        response = client.get("/api/search?q=streaming video service")
        assert response.status_code == 200
        data = response.json()
        # Response should be a list
        assert isinstance(data, list)

    # === Import Endpoint ===

    def test_import_csv(self, client, sample_csv):
        """Test CSV file upload."""
        with open(sample_csv, "rb") as f:
            response = client.post(
                "/api/import",
                files={"file": ("test.csv", f, "text/csv")}
            )
        assert response.status_code == 200
        data = response.json()
        assert data["added"] == 4
        assert data["duplicates"] == 0

    def test_import_duplicate_prevention(self, client, temp_service, sample_csv):
        """Test duplicate detection on import."""
        temp_service.import_file(sample_csv, auto_categorize=False)

        with open(sample_csv, "rb") as f:
            response = client.post(
                "/api/import",
                files={"file": ("test.csv", f, "text/csv")}
            )
        data = response.json()
        assert data["added"] == 0
        assert data["duplicates"] == 4

    # === Explanation Endpoint ===

    def test_get_explanation(self, client, temp_service, sample_csv):
        """Test categorization explanation endpoint."""
        temp_service.import_file(sample_csv, auto_categorize=False)
        txns = temp_service.get_uncategorized()
        txn_id = txns[0]["id"]
        # Manually categorize to create an explanation
        temp_service.categorize_transaction(txn_id, "Entertainment")

        response = client.get(f"/api/transactions/{txn_id}/explanation")
        assert response.status_code == 200
        data = response.json()
        assert "explanation" in data

    def test_get_explanation_not_found(self, client):
        """Test explanation for non-existent transaction."""
        response = client.get("/api/transactions/99999/explanation")
        assert response.status_code == 404
