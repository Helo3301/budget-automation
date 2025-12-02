"""
Pytest configuration and fixtures for Playwright tests.
"""
import pytest
import subprocess
import time
import os
import sys
import requests

# Add the project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_URL = "http://localhost:5000"
TEST_DB_PATH = "/tmp/test_budget.db"


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    """Configure browser context for all tests."""
    return {
        **browser_context_args,
        "viewport": {"width": 1280, "height": 720},
        "ignore_https_errors": True,
    }


@pytest.fixture(scope="session", autouse=True)
def ensure_server_running():
    """Ensure the test server is running before tests."""
    max_retries = 10
    for i in range(max_retries):
        try:
            response = requests.get(f"{BASE_URL}/api/categories", timeout=2)
            if response.status_code == 200:
                print(f"Server is running at {BASE_URL}")
                return
        except requests.exceptions.RequestException:
            pass

        if i < max_retries - 1:
            print(f"Waiting for server... ({i+1}/{max_retries})")
            time.sleep(2)

    pytest.fail(f"Server not running at {BASE_URL}. Please start it first.")


@pytest.fixture
def api_client():
    """Return a simple API client for making requests."""
    class APIClient:
        def __init__(self, base_url):
            self.base_url = base_url

        def get(self, path):
            return requests.get(f"{self.base_url}{path}")

        def post(self, path, json=None):
            return requests.post(f"{self.base_url}{path}", json=json)

        def put(self, path, json=None):
            return requests.put(f"{self.base_url}{path}", json=json)

        def delete(self, path):
            return requests.delete(f"{self.base_url}{path}")

        def reset_database(self):
            """Reset the database to a clean state."""
            return requests.post(f"{self.base_url}/api/admin/reset")

    return APIClient(BASE_URL)


@pytest.fixture
def clean_database(api_client):
    """Reset the database before and after each test."""
    api_client.reset_database()
    yield
    api_client.reset_database()


# Fake data for testing
FAKE_ACCOUNTS = [
    {"name": "Test Checking", "institution": "Test Bank", "account_type": "checking", "current_balance": 5000.00},
    {"name": "Test Savings", "institution": "Test Credit Union", "account_type": "savings", "current_balance": 10000.00},
    {"name": "Test Credit Card", "institution": "Test Card Co", "account_type": "credit_card", "current_balance": -500.00},
]

FAKE_TRANSACTIONS = [
    # Food/Restaurant transactions
    {"date": "2024-11-01", "amount": -12.50, "merchant": "Dairy Queen", "description": "Ice cream and burgers"},
    {"date": "2024-11-02", "amount": -25.00, "merchant": "McDonald's", "description": "Fast food lunch"},
    {"date": "2024-11-03", "amount": -45.00, "merchant": "Olive Garden", "description": "Italian dinner"},
    {"date": "2024-11-04", "amount": -8.50, "merchant": "Starbucks", "description": "Coffee and pastry"},
    {"date": "2024-11-05", "amount": -15.00, "merchant": "Subway", "description": "Sandwich for lunch"},

    # Grocery transactions
    {"date": "2024-11-06", "amount": -125.00, "merchant": "Walmart Grocery", "description": "Weekly groceries"},
    {"date": "2024-11-07", "amount": -85.00, "merchant": "Kroger", "description": "Groceries and household items"},
    {"date": "2024-11-08", "amount": -45.00, "merchant": "Trader Joe's", "description": "Specialty groceries"},

    # Gas/Transportation
    {"date": "2024-11-09", "amount": -55.00, "merchant": "Shell Gas Station", "description": "Gas fill-up"},
    {"date": "2024-11-10", "amount": -48.00, "merchant": "BP Gas", "description": "Fuel for car"},
    {"date": "2024-11-11", "amount": -35.00, "merchant": "Uber", "description": "Ride to airport"},

    # Utilities
    {"date": "2024-11-12", "amount": -120.00, "merchant": "Electric Company", "description": "Monthly electric bill"},
    {"date": "2024-11-13", "amount": -80.00, "merchant": "Water Utility", "description": "Water and sewer"},
    {"date": "2024-11-14", "amount": -65.00, "merchant": "Internet Provider", "description": "Monthly internet"},

    # Entertainment
    {"date": "2024-11-15", "amount": -15.99, "merchant": "Netflix", "description": "Monthly subscription"},
    {"date": "2024-11-16", "amount": -12.99, "merchant": "Spotify", "description": "Music subscription"},
    {"date": "2024-11-17", "amount": -45.00, "merchant": "AMC Theaters", "description": "Movie tickets"},

    # Shopping
    {"date": "2024-11-18", "amount": -150.00, "merchant": "Amazon", "description": "Online shopping"},
    {"date": "2024-11-19", "amount": -85.00, "merchant": "Target", "description": "Household items"},
    {"date": "2024-11-20", "amount": -200.00, "merchant": "Best Buy", "description": "Electronics"},

    # Banking/ATM
    {"date": "2024-11-21", "amount": 5.00, "merchant": "ATM Fee Reimbursement", "description": "Monthly ATM fee refund"},
    {"date": "2024-11-22", "amount": -3.00, "merchant": "ATM Withdrawal Fee", "description": "Out of network ATM fee"},

    # Income
    {"date": "2024-11-01", "amount": 3500.00, "merchant": "Employer Inc", "description": "Paycheck deposit"},
    {"date": "2024-11-15", "amount": 3500.00, "merchant": "Employer Inc", "description": "Paycheck deposit"},

    # Healthcare
    {"date": "2024-11-23", "amount": -50.00, "merchant": "CVS Pharmacy", "description": "Prescription medications"},
    {"date": "2024-11-24", "amount": -25.00, "merchant": "Doctor's Office", "description": "Copay for visit"},
]

FAKE_CATEGORIES = [
    {"name": "Food & Dining", "monthly_budget": 500.00},
    {"name": "Groceries", "monthly_budget": 400.00},
    {"name": "Transportation", "monthly_budget": 300.00},
    {"name": "Utilities", "monthly_budget": 300.00},
    {"name": "Entertainment", "monthly_budget": 150.00},
    {"name": "Shopping", "monthly_budget": 200.00},
    {"name": "Healthcare", "monthly_budget": 100.00},
    {"name": "Income", "monthly_budget": 0.00},
]


@pytest.fixture
def setup_fake_data(api_client, clean_database):
    """Set up fake data for comprehensive testing."""
    # Create categories first
    categories = {}
    for cat in FAKE_CATEGORIES:
        resp = api_client.post("/api/categories", json=cat)
        if resp.status_code == 200:
            data = resp.json()
            categories[cat["name"]] = data.get("id")

    # Create accounts
    accounts = {}
    for acc in FAKE_ACCOUNTS:
        resp = api_client.post("/api/accounts", json=acc)
        if resp.status_code == 200:
            data = resp.json()
            accounts[acc["name"]] = data.get("id")

    # Create transactions (assign to first account)
    account_id = list(accounts.values())[0] if accounts else None
    for txn in FAKE_TRANSACTIONS:
        txn_data = {**txn}
        if account_id:
            txn_data["account_id"] = account_id
        api_client.post("/api/transactions", json=txn_data)

    return {"categories": categories, "accounts": accounts}
