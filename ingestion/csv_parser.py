"""CSV and Excel parser for transaction import."""
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd


# Known bank format patterns
BANK_FORMATS = {
    "chase": {
        "headers": ["Transaction Date", "Post Date", "Description", "Category", "Type", "Amount"],
        "mapping": {
            "date": "Transaction Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Memo"
        }
    },
    "bofa": {
        "headers": ["Posted Date", "Reference Number", "Payee", "Address", "Amount"],
        "mapping": {
            "date": "Posted Date",
            "merchant": "Payee",
            "amount": "Amount",
            "description": "Address"
        }
    },
    "mint": {
        "headers": ["Date", "Description", "Original Description", "Amount", "Transaction Type"],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Original Description"
        }
    },
    "generic": {
        "headers": ["Date", "Amount", "Merchant", "Description"],
        "mapping": {
            "date": "Date",
            "merchant": "Merchant",
            "amount": "Amount",
            "description": "Description"
        }
    }
}

# Common column name variations
DATE_COLUMNS = ["date", "transaction date", "trans date", "posted date", "trans_date", "posting date"]
AMOUNT_COLUMNS = ["amount", "trans_amt", "transaction amount", "debit/credit", "value"]
MERCHANT_COLUMNS = ["merchant", "payee", "description", "payee_name", "vendor", "name"]
DESC_COLUMNS = ["description", "memo", "notes", "details", "original description", "address"]


class CSVParser:
    """Parser for CSV and Excel transaction files with auto-format detection."""

    def __init__(self, column_mapping: Optional[Dict[str, str]] = None):
        """Initialize parser with optional custom column mapping.

        Args:
            column_mapping: Custom mapping of {output_field: input_column}
        """
        self.column_mapping = column_mapping

    def detect_format(self, file_path: Path) -> str:
        """Detect bank format from file headers.

        Args:
            file_path: Path to CSV/Excel file

        Returns:
            Format name (chase, bofa, mint, generic)
        """
        df = self._read_file(file_path, nrows=5)
        headers = [str(h).lower().strip() for h in df.columns]

        # Check against known formats
        for format_name, config in BANK_FORMATS.items():
            expected = [h.lower() for h in config["headers"]]
            if all(any(exp in h for h in headers) for exp in expected[:3]):
                return format_name

        return "generic"

    def parse(self, file_path: Path) -> List[Dict[str, Any]]:
        """Parse a CSV or Excel file into transaction dicts.

        Args:
            file_path: Path to the file

        Returns:
            List of transaction dicts with date, amount, merchant, description
        """
        df = self._read_file(file_path)

        # Clean up dataframe
        df = self._clean_dataframe(df)

        if df.empty:
            return []

        # Determine column mapping
        mapping = self._get_column_mapping(df)

        # Convert to transactions
        transactions = []
        for _, row in df.iterrows():
            txn = self._row_to_transaction(row, mapping)
            if txn:
                transactions.append(txn)

        return transactions

    def _read_file(self, file_path: Path, nrows: Optional[int] = None) -> pd.DataFrame:
        """Read CSV or Excel file into DataFrame."""
        path = Path(file_path)

        if path.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(path, nrows=nrows)
        else:
            # Try to detect the actual header row
            df = self._read_csv_with_header_detection(path, nrows)

        return df

    def _read_csv_with_header_detection(
        self,
        path: Path,
        nrows: Optional[int] = None
    ) -> pd.DataFrame:
        """Read CSV and detect where the actual header row is."""
        # First, read raw to find header
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()

        # Count non-blank lines to match pandas' row counting
        # pandas skips blank lines by default
        pandas_row = 0
        header_row = 0

        for line in lines[:20]:  # Check first 20 lines
            lower = line.lower().strip()
            # Skip empty lines (pandas does too)
            if not lower:
                continue
            # Look for a line that has comma-separated column names
            # Must have a comma (to be a proper header row) and contain common column names
            if ',' in lower and any(col in lower for col in ["date", "amount", "merchant", "payee", "description"]):
                header_row = pandas_row
                break
            pandas_row += 1

        df = pd.read_csv(path, header=header_row, nrows=nrows)
        return df

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the dataframe - remove empty rows, normalize headers."""
        # Normalize column names
        df.columns = [str(c).strip() for c in df.columns]

        # Drop rows where all values are NaN
        df = df.dropna(how='all')

        # Drop rows where critical columns are NaN (we'll check this after mapping)
        return df

    def _get_column_mapping(self, df: pd.DataFrame) -> Dict[str, str]:
        """Determine column mapping for the dataframe."""
        if self.column_mapping:
            return self.column_mapping

        # Auto-detect columns
        columns_lower = {str(c).lower(): c for c in df.columns}
        mapping = {}

        # Find date column
        for col in DATE_COLUMNS:
            if col in columns_lower:
                mapping["date"] = columns_lower[col]
                break

        # Find amount column
        for col in AMOUNT_COLUMNS:
            if col in columns_lower:
                mapping["amount"] = columns_lower[col]
                break

        # Find merchant column
        for col in MERCHANT_COLUMNS:
            if col in columns_lower:
                mapping["merchant"] = columns_lower[col]
                break

        # Find description column
        for col in DESC_COLUMNS:
            if col in columns_lower and columns_lower[col] != mapping.get("merchant"):
                mapping["description"] = columns_lower[col]
                break

        return mapping

    def _row_to_transaction(
        self,
        row: pd.Series,
        mapping: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        """Convert a DataFrame row to a transaction dict."""
        try:
            # Extract values
            date_val = row.get(mapping.get("date", ""))
            amount_val = row.get(mapping.get("amount", ""))
            merchant_val = row.get(mapping.get("merchant", ""))
            desc_val = row.get(mapping.get("description", ""))

            # Skip if critical values are missing
            if pd.isna(date_val) or pd.isna(amount_val):
                return None

            # Normalize date
            date_str = self._normalize_date(date_val)
            if not date_str:
                return None

            # Normalize amount
            amount = self._normalize_amount(amount_val)
            if amount is None:
                return None

            # Normalize merchant
            merchant = str(merchant_val).strip() if not pd.isna(merchant_val) else "UNKNOWN"

            # Description
            description = str(desc_val).strip() if not pd.isna(desc_val) else None

            return {
                "date": date_str,
                "amount": amount,
                "merchant": merchant,
                "description": description
            }

        except Exception:
            return None

    def _normalize_date(self, date_val: Any) -> Optional[str]:
        """Normalize various date formats to ISO (YYYY-MM-DD)."""
        if pd.isna(date_val):
            return None

        # If already a datetime
        if isinstance(date_val, (datetime, pd.Timestamp)):
            return date_val.strftime("%Y-%m-%d")

        date_str = str(date_val).strip()

        # Try various formats
        formats = [
            "%Y-%m-%d",      # ISO
            "%m/%d/%Y",      # US
            "%m/%d/%y",      # US short year
            "%d-%b-%Y",      # 15-Jan-2024
            "%d/%m/%Y",      # European
            "%Y/%m/%d",      # Alternative ISO
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Try pandas parser as fallback
        try:
            dt = pd.to_datetime(date_str)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None

    def _normalize_amount(self, amount_val: Any) -> Optional[float]:
        """Normalize various amount formats to float."""
        if pd.isna(amount_val):
            return None

        # If already a number
        if isinstance(amount_val, (int, float)):
            return float(amount_val)

        amount_str = str(amount_val).strip()

        # Remove currency symbols and commas
        amount_str = re.sub(r'[$,]', '', amount_str)

        # Handle parentheses as negative (accounting format)
        if amount_str.startswith('(') and amount_str.endswith(')'):
            amount_str = '-' + amount_str[1:-1]

        try:
            return float(amount_str)
        except ValueError:
            return None
