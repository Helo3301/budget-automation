"""Tests for CSV/Excel parser - written FIRST per TDD."""
import pytest
import tempfile
from pathlib import Path
import pandas as pd


@pytest.fixture
def sample_csv(tmp_path) -> Path:
    """Create a sample CSV file for testing."""
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text("""Date,Amount,Description,Merchant
2024-01-15,-45.99,Monthly subscription,NETFLIX
2024-01-16,-125.50,Weekly groceries,WHOLE FOODS
2024-01-17,-2500.00,January rent,LANDLORD PROP MGMT
2024-01-18,3500.00,Payroll deposit,ACME CORP
2024-01-19,-8.99,Premium subscription,SPOTIFY
""")
    return csv_path


@pytest.fixture
def chase_format_csv(tmp_path) -> Path:
    """Create a Chase bank format CSV."""
    csv_path = tmp_path / "chase.csv"
    csv_path.write_text("""Transaction Date,Post Date,Description,Category,Type,Amount,Memo
01/15/2024,01/15/2024,NETFLIX.COM,Entertainment,Sale,-15.99,
01/16/2024,01/17/2024,WHOLE FOODS #123,Groceries,Sale,-85.50,
01/18/2024,01/18/2024,DIRECT DEP ACME,Income,Payment,3500.00,
""")
    return csv_path


@pytest.fixture
def bofa_format_csv(tmp_path) -> Path:
    """Create a Bank of America format CSV."""
    csv_path = tmp_path / "bofa.csv"
    csv_path.write_text("""Posted Date,Reference Number,Payee,Address,Amount
01/15/2024,123456,AMAZON.COM,SEATTLE WA,-45.99
01/16/2024,123457,STARBUCKS,AUSTIN TX,-5.50
""")
    return csv_path


@pytest.fixture
def sample_excel(tmp_path) -> Path:
    """Create a sample Excel file for testing."""
    excel_path = tmp_path / "transactions.xlsx"
    df = pd.DataFrame({
        "Date": ["2024-01-15", "2024-01-16"],
        "Amount": [-45.99, -125.50],
        "Description": ["Subscription", "Groceries"],
        "Merchant": ["NETFLIX", "WHOLE FOODS"]
    })
    df.to_excel(excel_path, index=False)
    return excel_path


class TestCSVParser:
    """Test cases for CSVParser class."""

    def test_parse_basic_csv(self, sample_csv: Path):
        """Should parse a basic CSV file."""
        from budget_automation.ingestion.csv_parser import CSVParser

        parser = CSVParser()
        transactions = parser.parse(sample_csv)

        assert len(transactions) == 5
        assert transactions[0]["merchant"] == "NETFLIX"
        assert transactions[0]["amount"] == -45.99

    def test_parse_chase_format(self, chase_format_csv: Path):
        """Should auto-detect and parse Chase format."""
        from budget_automation.ingestion.csv_parser import CSVParser

        parser = CSVParser()
        transactions = parser.parse(chase_format_csv)

        assert len(transactions) == 3
        assert "NETFLIX" in transactions[0]["merchant"]

    def test_parse_bofa_format(self, bofa_format_csv: Path):
        """Should auto-detect and parse Bank of America format."""
        from budget_automation.ingestion.csv_parser import CSVParser

        parser = CSVParser()
        transactions = parser.parse(bofa_format_csv)

        assert len(transactions) == 2
        assert "AMAZON" in transactions[0]["merchant"]

    def test_parse_excel(self, sample_excel: Path):
        """Should parse Excel files."""
        from budget_automation.ingestion.csv_parser import CSVParser

        parser = CSVParser()
        transactions = parser.parse(sample_excel)

        assert len(transactions) == 2
        assert transactions[0]["merchant"] == "NETFLIX"

    def test_normalize_dates(self, tmp_path: Path):
        """Should normalize various date formats to ISO."""
        from budget_automation.ingestion.csv_parser import CSVParser

        csv_path = tmp_path / "dates.csv"
        csv_path.write_text("""Date,Amount,Merchant
01/15/2024,-10.00,M1
2024-01-16,-20.00,M2
15-Jan-2024,-30.00,M3
""")

        parser = CSVParser()
        transactions = parser.parse(csv_path)

        # All should be ISO format
        assert transactions[0]["date"] == "2024-01-15"
        assert transactions[1]["date"] == "2024-01-16"
        assert transactions[2]["date"] == "2024-01-15"

    def test_normalize_amounts(self, tmp_path: Path):
        """Should handle various amount formats."""
        from budget_automation.ingestion.csv_parser import CSVParser

        csv_path = tmp_path / "amounts.csv"
        csv_path.write_text("""Date,Amount,Merchant
2024-01-15,$-45.99,M1
2024-01-16,"(125.50)",M2
2024-01-17,100.00,M3
""")

        parser = CSVParser()
        transactions = parser.parse(csv_path)

        assert transactions[0]["amount"] == -45.99
        assert transactions[1]["amount"] == -125.50  # Parentheses = negative
        assert transactions[2]["amount"] == 100.00

    def test_custom_column_mapping(self, tmp_path: Path):
        """Should support custom column mapping."""
        from budget_automation.ingestion.csv_parser import CSVParser

        csv_path = tmp_path / "custom.csv"
        csv_path.write_text("""trans_date,trans_amt,payee_name
2024-01-15,-45.99,STORE
""")

        parser = CSVParser(column_mapping={
            "date": "trans_date",
            "amount": "trans_amt",
            "merchant": "payee_name"
        })
        transactions = parser.parse(csv_path)

        assert transactions[0]["merchant"] == "STORE"

    def test_skip_header_detection(self, tmp_path: Path):
        """Should detect and skip header rows."""
        from budget_automation.ingestion.csv_parser import CSVParser

        csv_path = tmp_path / "header.csv"
        csv_path.write_text("""Some Bank Statement
Account: 1234

Date,Amount,Merchant
2024-01-15,-45.99,STORE
""")

        parser = CSVParser()
        transactions = parser.parse(csv_path)

        assert len(transactions) == 1
        assert transactions[0]["merchant"] == "STORE"

    def test_handles_empty_rows(self, tmp_path: Path):
        """Should skip empty rows."""
        from budget_automation.ingestion.csv_parser import CSVParser

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("""Date,Amount,Merchant
2024-01-15,-45.99,M1

2024-01-16,-20.00,M2

""")

        parser = CSVParser()
        transactions = parser.parse(csv_path)

        assert len(transactions) == 2

    def test_returns_dict_format(self, sample_csv: Path):
        """Transactions should have correct dict structure."""
        from budget_automation.ingestion.csv_parser import CSVParser

        parser = CSVParser()
        transactions = parser.parse(sample_csv)

        txn = transactions[0]
        assert "date" in txn
        assert "amount" in txn
        assert "merchant" in txn
        assert "description" in txn or txn.get("description") is None

    def test_detects_format_from_headers(self, chase_format_csv: Path, bofa_format_csv: Path):
        """Should detect bank format from header patterns."""
        from budget_automation.ingestion.csv_parser import CSVParser

        parser = CSVParser()

        chase_format = parser.detect_format(chase_format_csv)
        bofa_format = parser.detect_format(bofa_format_csv)

        assert chase_format == "chase"
        assert bofa_format == "bofa"
