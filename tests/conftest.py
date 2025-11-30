"""Pytest fixtures for budget automation tests."""
import pytest
import sqlite3
import tempfile
from pathlib import Path
from datetime import date, datetime
from typing import Generator

@pytest.fixture
def temp_db_path() -> Generator[Path, None, None]:
    """Provide a temporary database path for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = Path(f.name)
    yield path
    if path.exists():
        path.unlink()

@pytest.fixture
def temp_vector_path() -> Generator[Path, None, None]:
    """Provide a temporary vector store path for testing."""
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)

@pytest.fixture
def sample_transactions() -> list:
    """Sample transactions for testing."""
    return [
        {
            "date": "2024-01-15",
            "amount": -45.99,
            "merchant": "NETFLIX",
            "description": "Monthly subscription"
        },
        {
            "date": "2024-01-16",
            "amount": -125.50,
            "merchant": "WHOLE FOODS",
            "description": "Groceries"
        },
        {
            "date": "2024-01-17",
            "amount": -2500.00,
            "merchant": "LANDLORD PROP MGMT",
            "description": "January rent"
        },
        {
            "date": "2024-01-18",
            "amount": 3500.00,
            "merchant": "ACME CORP",
            "description": "Payroll deposit"
        },
        {
            "date": "2024-01-19",
            "amount": -8.99,
            "merchant": "SPOTIFY",
            "description": "Premium subscription"
        },
    ]

@pytest.fixture
def recurring_transactions() -> list:
    """Transactions with recurring patterns for testing."""
    return [
        # Netflix - monthly on 15th
        {"date": "2024-01-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
        {"date": "2024-02-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
        {"date": "2024-03-15", "amount": -15.99, "merchant": "NETFLIX", "description": "Streaming"},
        # Rent - monthly on 1st
        {"date": "2024-01-01", "amount": -2000.00, "merchant": "LANDLORD", "description": "Rent"},
        {"date": "2024-02-01", "amount": -2000.00, "merchant": "LANDLORD", "description": "Rent"},
        {"date": "2024-03-01", "amount": -2000.00, "merchant": "LANDLORD", "description": "Rent"},
        # Random one-off
        {"date": "2024-02-10", "amount": -50.00, "merchant": "RESTAURANT", "description": "Dinner"},
    ]

@pytest.fixture
def anomaly_transactions() -> list:
    """Transactions with anomalies for testing."""
    return [
        # Normal grocery amounts
        {"date": "2024-01-01", "amount": -50.00, "merchant": "GROCERY STORE", "description": "Food", "category_id": 3},
        {"date": "2024-01-08", "amount": -45.00, "merchant": "GROCERY STORE", "description": "Food", "category_id": 3},
        {"date": "2024-01-15", "amount": -55.00, "merchant": "GROCERY STORE", "description": "Food", "category_id": 3},
        {"date": "2024-01-22", "amount": -48.00, "merchant": "GROCERY STORE", "description": "Food", "category_id": 3},
        # Anomaly - way higher than normal
        {"date": "2024-01-29", "amount": -500.00, "merchant": "GROCERY STORE", "description": "Food", "category_id": 3},
        # New merchant with high amount (anomaly)
        {"date": "2024-01-30", "amount": -200.00, "merchant": "UNKNOWN SHOP", "description": "Something"},
    ]
