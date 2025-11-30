"""CSV and Excel parser for transaction import with smart bank detection."""
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from collections import Counter
import pandas as pd


# Comprehensive bank format patterns - covers major US banks and financial apps
BANK_FORMATS = {
    # Major US Banks
    "chase_credit": {
        "name": "Chase Credit Card",
        "institution": "Chase",
        "account_type": "credit_card",
        "headers": ["Transaction Date", "Post Date", "Description", "Category", "Type", "Amount"],
        "signature_columns": ["Transaction Date", "Post Date", "Category", "Type"],
        "mapping": {
            "date": "Transaction Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Category"
        }
    },
    "chase_checking": {
        "name": "Chase Checking/Savings",
        "institution": "Chase",
        "account_type": "checking",
        "headers": ["Details", "Posting Date", "Description", "Amount", "Type", "Balance", "Check or Slip #"],
        "signature_columns": ["Details", "Posting Date", "Check or Slip #"],
        "mapping": {
            "date": "Posting Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Details"
        }
    },
    "bofa": {
        "name": "Bank of America",
        "institution": "Bank of America",
        "account_type": "checking",
        "headers": ["Posted Date", "Reference Number", "Payee", "Address", "Amount"],
        "signature_columns": ["Reference Number", "Payee", "Address"],
        "mapping": {
            "date": "Posted Date",
            "merchant": "Payee",
            "amount": "Amount",
            "description": "Address"
        }
    },
    "wells_fargo": {
        "name": "Wells Fargo",
        "institution": "Wells Fargo",
        "account_type": "checking",
        "headers": ["Date", "Amount", "Description"],
        "signature_columns": [],  # Generic format, detect by filename
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": None
        }
    },
    "capital_one": {
        "name": "Capital One",
        "institution": "Capital One",
        "account_type": "credit_card",
        "headers": ["Transaction Date", "Posted Date", "Card No.", "Description", "Category", "Debit", "Credit"],
        "signature_columns": ["Card No.", "Debit", "Credit"],
        "mapping": {
            "date": "Transaction Date",
            "merchant": "Description",
            "amount": "Debit",  # Will need to combine Debit/Credit
            "debit_column": "Debit",
            "credit_column": "Credit",
            "description": "Category"
        }
    },
    "citi": {
        "name": "Citi",
        "institution": "Citi",
        "account_type": "credit_card",
        "headers": ["Status", "Date", "Description", "Debit", "Credit"],
        "signature_columns": ["Status", "Debit", "Credit"],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Debit",
            "debit_column": "Debit",
            "credit_column": "Credit",
            "description": None
        }
    },
    "amex": {
        "name": "American Express",
        "institution": "American Express",
        "account_type": "credit_card",
        "headers": ["Date", "Description", "Card Member", "Account #", "Amount"],
        "signature_columns": ["Card Member", "Account #"],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": None
        }
    },
    "discover": {
        "name": "Discover",
        "institution": "Discover",
        "account_type": "credit_card",
        "headers": ["Trans. Date", "Post Date", "Description", "Amount", "Category"],
        "signature_columns": ["Trans. Date", "Post Date", "Category"],
        "mapping": {
            "date": "Trans. Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Category"
        }
    },
    "usaa": {
        "name": "USAA",
        "institution": "USAA",
        "account_type": "checking",
        "headers": ["Date", "Description", "Original Description", "Category", "Amount"],
        "signature_columns": ["Original Description"],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Original Description"
        }
    },
    "navy_federal": {
        "name": "Navy Federal Credit Union",
        "institution": "Navy Federal",
        "account_type": "checking",
        "headers": ["Date", "Description", "Amount", "Balance"],
        "signature_columns": ["Balance"],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": None
        }
    },
    "pnc": {
        "name": "PNC Bank",
        "institution": "PNC",
        "account_type": "checking",
        "headers": ["Date", "Description", "Withdrawals", "Deposits", "Balance"],
        "signature_columns": ["Withdrawals", "Deposits", "Balance"],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Withdrawals",
            "debit_column": "Withdrawals",
            "credit_column": "Deposits",
            "description": None
        }
    },
    "td_bank": {
        "name": "TD Bank",
        "institution": "TD Bank",
        "account_type": "checking",
        "headers": ["Date", "Activity", "Credited", "Debited"],
        "signature_columns": ["Activity", "Credited", "Debited"],
        "mapping": {
            "date": "Date",
            "merchant": "Activity",
            "amount": "Debited",
            "debit_column": "Debited",
            "credit_column": "Credited",
            "description": None
        }
    },
    "us_bank": {
        "name": "US Bank",
        "institution": "US Bank",
        "account_type": "checking",
        "headers": ["Date", "Transaction", "Name", "Memo", "Amount"],
        "signature_columns": ["Transaction", "Name", "Memo"],
        "mapping": {
            "date": "Date",
            "merchant": "Name",
            "amount": "Amount",
            "description": "Memo"
        }
    },
    # Financial Apps & Services
    "mint": {
        "name": "Mint Export",
        "institution": "Mint",
        "account_type": "aggregator",
        "headers": ["Date", "Description", "Original Description", "Amount", "Transaction Type", "Category", "Account Name"],
        "signature_columns": ["Original Description", "Transaction Type", "Account Name"],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Original Description"
        }
    },
    "venmo": {
        "name": "Venmo",
        "institution": "Venmo",
        "account_type": "payment_app",
        "headers": ["ID", "Datetime", "Type", "Status", "Note", "From", "To", "Amount (total)", "Amount (fee)"],
        "signature_columns": ["ID", "Datetime", "Type", "Status", "Note", "From", "To"],
        "mapping": {
            "date": "Datetime",
            "merchant": "Note",
            "amount": "Amount (total)",
            "description": "Type"
        }
    },
    "paypal": {
        "name": "PayPal",
        "institution": "PayPal",
        "account_type": "payment_app",
        "headers": ["Date", "Time", "TimeZone", "Name", "Type", "Status", "Currency", "Gross", "Fee", "Net"],
        "signature_columns": ["TimeZone", "Currency", "Gross", "Fee", "Net"],
        "mapping": {
            "date": "Date",
            "merchant": "Name",
            "amount": "Gross",
            "description": "Type"
        }
    },
    "apple_card": {
        "name": "Apple Card",
        "institution": "Apple",
        "account_type": "credit_card",
        "headers": ["Transaction Date", "Clearing Date", "Description", "Merchant", "Category", "Type", "Amount"],
        "signature_columns": ["Clearing Date", "Merchant", "Type"],
        "mapping": {
            "date": "Transaction Date",
            "merchant": "Merchant",
            "amount": "Amount",
            "description": "Category"
        }
    },
    "sofi": {
        "name": "SoFi",
        "institution": "SoFi",
        "account_type": "checking",
        "headers": ["Date", "Description", "Type", "Amount", "Balance"],
        "signature_columns": ["Type", "Balance"],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Type"
        }
    },
    "ally": {
        "name": "Ally Bank",
        "institution": "Ally",
        "account_type": "checking",
        "headers": ["Date", "Time", "Amount", "Type", "Description"],
        "signature_columns": ["Time", "Type"],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Type"
        }
    },
    "marcus": {
        "name": "Marcus by Goldman Sachs",
        "institution": "Goldman Sachs",
        "account_type": "savings",
        "headers": ["Date", "Description", "Amount", "Balance"],
        "signature_columns": [],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": None
        }
    },
    # Credit Unions
    "alliant": {
        "name": "Alliant Credit Union",
        "institution": "Alliant",
        "account_type": "checking",
        "headers": ["Post Date", "Effective Date", "Transaction Type", "Description", "Amount", "Balance"],
        "signature_columns": ["Post Date", "Effective Date", "Transaction Type"],
        "mapping": {
            "date": "Post Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Transaction Type"
        }
    },
    # Budgeting Apps
    "ynab": {
        "name": "YNAB (You Need A Budget)",
        "institution": "YNAB",
        "account_type": "aggregator",
        "headers": ["Account", "Flag", "Date", "Payee", "Category Group/Category", "Category Group", "Category", "Memo", "Outflow", "Inflow", "Cleared"],
        "signature_columns": ["Account", "Flag", "Payee", "Category Group/Category", "Outflow", "Inflow", "Cleared"],
        "mapping": {
            "date": "Date",
            "merchant": "Payee",
            "amount": "Outflow",
            "debit_column": "Outflow",
            "credit_column": "Inflow",
            "description": "Memo"
        }
    },
    "personal_capital": {
        "name": "Empower (Personal Capital)",
        "institution": "Empower",
        "account_type": "aggregator",
        "headers": ["Date", "Account", "Description", "Category", "Tags", "Amount"],
        "signature_columns": ["Account", "Category", "Tags"],
        "mapping": {
            "date": "Date",
            "merchant": "Description",
            "amount": "Amount",
            "description": "Category"
        }
    },
    # Generic fallback
    "generic": {
        "name": "Generic Format",
        "institution": "Unknown",
        "account_type": "unknown",
        "headers": ["Date", "Amount", "Merchant", "Description"],
        "signature_columns": [],
        "mapping": {
            "date": "Date",
            "merchant": "Merchant",
            "amount": "Amount",
            "description": "Description"
        }
    }
}

# Common column name variations for smart detection
DATE_COLUMNS = [
    "date", "transaction date", "trans date", "posted date", "trans_date",
    "posting date", "post date", "effective date", "datetime", "trans. date",
    "clearing date", "settlement date", "value date"
]
AMOUNT_COLUMNS = [
    "amount", "trans_amt", "transaction amount", "debit/credit", "value",
    "gross", "net", "total", "amount (total)", "sum"
]
DEBIT_COLUMNS = [
    "debit", "debited", "withdrawals", "withdrawal", "outflow", "expense",
    "payment", "charge", "money out"
]
CREDIT_COLUMNS = [
    "credit", "credited", "deposits", "deposit", "inflow", "income",
    "refund", "money in"
]
MERCHANT_COLUMNS = [
    "merchant", "payee", "description", "payee_name", "vendor", "name",
    "to", "recipient", "store", "company", "transaction"
]
DESC_COLUMNS = [
    "description", "memo", "notes", "details", "original description",
    "address", "category", "note", "reference"
]
BALANCE_COLUMNS = [
    "balance", "running balance", "available balance", "ledger balance"
]

# Patterns for detecting transaction types
INCOME_PATTERNS = [
    r"direct deposit", r"payroll", r"salary", r"wages", r"paycheck",
    r"ach credit", r"interest payment", r"dividend", r"refund",
    r"tax refund", r"venmo from", r"zelle from", r"transfer from"
]
EXPENSE_PATTERNS = [
    r"purchase", r"pos", r"debit card", r"withdrawal", r"payment",
    r"transfer to", r"venmo to", r"zelle to", r"atm"
]
SUBSCRIPTION_PATTERNS = [
    r"netflix", r"spotify", r"hulu", r"disney\+", r"amazon prime",
    r"apple\.com", r"google\s*(play|storage|one)", r"microsoft",
    r"adobe", r"dropbox", r"youtube premium", r"hbo\s*max"
]


class CSVParser:
    """Parser for CSV and Excel transaction files with auto-format detection."""

    def __init__(self, column_mapping: Optional[Dict[str, str]] = None):
        """Initialize parser with optional custom column mapping.

        Args:
            column_mapping: Custom mapping of {output_field: input_column}
        """
        self.column_mapping = column_mapping

    def detect_format(self, file_path: Path, filename: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
        """Detect bank format from file headers and content.

        Args:
            file_path: Path to CSV/Excel file
            filename: Original filename (may contain bank hints)

        Returns:
            Tuple of (format_name, format_info_dict)
        """
        df = self._read_file(file_path, nrows=20)
        headers = [str(h).strip() for h in df.columns]
        headers_lower = [h.lower() for h in headers]

        # Score each format based on signature column matches
        best_match = "generic"
        best_score = 0
        best_info = BANK_FORMATS["generic"].copy()

        for format_name, config in BANK_FORMATS.items():
            if format_name == "generic":
                continue

            score = 0
            signature_cols = config.get("signature_columns", [])

            # Check signature columns (exact or partial match)
            for sig_col in signature_cols:
                sig_lower = sig_col.lower()
                if any(sig_lower == h or sig_lower in h for h in headers_lower):
                    score += 3  # High weight for signature columns

            # Check regular headers
            for exp_header in config["headers"]:
                exp_lower = exp_header.lower()
                if any(exp_lower == h or exp_lower in h for h in headers_lower):
                    score += 1

            # Bonus for filename hints
            if filename:
                fn_lower = filename.lower()
                institution = config.get("institution", "").lower()
                if institution and institution in fn_lower:
                    score += 5

            if score > best_score:
                best_score = score
                best_match = format_name
                best_info = config.copy()

        return best_match, best_info

    def analyze_file(self, file_path: Path, filename: Optional[str] = None) -> Dict[str, Any]:
        """Comprehensive analysis of a CSV/Excel file for smart import.

        Returns detailed info including:
        - Detected bank format and institution
        - Suggested column mappings
        - Account type detection
        - Date range
        - Transaction summary (income vs expenses)
        - Confidence scores
        """
        df = self._read_file(file_path)
        df = self._clean_dataframe(df)

        if df.empty:
            return {"error": "No data found in file", "rows": 0}

        # Detect format
        format_name, format_info = self.detect_format(file_path, filename)

        # Get all columns
        columns = [str(c) for c in df.columns.tolist()]

        # Smart column detection (combines format hints + data analysis)
        detected_mapping = self._smart_detect_columns(df, format_info)

        # Detect if we have separate debit/credit columns
        has_split_amounts = self._detect_split_amounts(df, columns)

        # Analyze transaction data
        analysis = self._analyze_transactions(df, detected_mapping, has_split_amounts)

        # Detect account type from transaction patterns
        account_type = self._detect_account_type(df, detected_mapping, format_info)

        return {
            "format": {
                "id": format_name,
                "name": format_info.get("name", "Unknown Format"),
                "institution": format_info.get("institution", "Unknown"),
                "confidence": self._calculate_confidence(format_name, columns, detected_mapping)
            },
            "columns": columns,
            "detected_mapping": detected_mapping,
            "has_split_amounts": has_split_amounts,
            "account_type": account_type,
            "total_rows": len(df),
            "analysis": analysis,
            "sample_rows": self._get_sample_rows(df, 5)
        }

    def _smart_detect_columns(self, df: pd.DataFrame, format_info: Dict) -> Dict[str, Optional[str]]:
        """Detect columns using both format hints and data pattern analysis."""
        columns = [str(c) for c in df.columns]
        columns_lower = {c.lower(): c for c in columns}

        mapping = {
            "date_column": None,
            "amount_column": None,
            "merchant_column": None,
            "description_column": None,
            "debit_column": None,
            "credit_column": None,
            "balance_column": None
        }

        # First try format-specific mapping
        format_mapping = format_info.get("mapping", {})
        for field, col_name in format_mapping.items():
            if col_name and col_name in columns:
                if field == "date":
                    mapping["date_column"] = col_name
                elif field == "amount":
                    mapping["amount_column"] = col_name
                elif field == "merchant":
                    mapping["merchant_column"] = col_name
                elif field == "description":
                    mapping["description_column"] = col_name
                elif field == "debit_column":
                    mapping["debit_column"] = col_name
                elif field == "credit_column":
                    mapping["credit_column"] = col_name

        # Fall back to generic detection for missing fields
        if not mapping["date_column"]:
            mapping["date_column"] = self._find_date_column(df, columns_lower)

        if not mapping["amount_column"] and not mapping["debit_column"]:
            amount_cols = self._find_amount_columns(df, columns_lower)
            if amount_cols.get("single"):
                mapping["amount_column"] = amount_cols["single"]
            if amount_cols.get("debit"):
                mapping["debit_column"] = amount_cols["debit"]
            if amount_cols.get("credit"):
                mapping["credit_column"] = amount_cols["credit"]

        if not mapping["merchant_column"]:
            mapping["merchant_column"] = self._find_merchant_column(df, columns_lower, mapping)

        if not mapping["description_column"]:
            mapping["description_column"] = self._find_description_column(columns_lower, mapping)

        # Look for balance column
        mapping["balance_column"] = self._find_balance_column(columns_lower)

        return mapping

    def _find_date_column(self, df: pd.DataFrame, columns_lower: Dict[str, str]) -> Optional[str]:
        """Find the date column by name and data validation."""
        # First try name matching
        for col_name in DATE_COLUMNS:
            if col_name in columns_lower:
                return columns_lower[col_name]

        # Try data analysis - find column with date-like values
        for col in df.columns:
            sample = df[col].dropna().head(10)
            if len(sample) == 0:
                continue

            date_count = 0
            for val in sample:
                if self._normalize_date(val):
                    date_count += 1

            if date_count >= len(sample) * 0.7:  # 70% are valid dates
                return str(col)

        return None

    def _find_amount_columns(self, df: pd.DataFrame, columns_lower: Dict[str, str]) -> Dict[str, Optional[str]]:
        """Find amount columns - handles single amount or split debit/credit."""
        result = {"single": None, "debit": None, "credit": None}

        # Check for split debit/credit columns first
        for col_name in DEBIT_COLUMNS:
            if col_name in columns_lower:
                result["debit"] = columns_lower[col_name]
                break

        for col_name in CREDIT_COLUMNS:
            if col_name in columns_lower:
                result["credit"] = columns_lower[col_name]
                break

        # If no split columns, look for single amount
        if not result["debit"] and not result["credit"]:
            for col_name in AMOUNT_COLUMNS:
                if col_name in columns_lower:
                    result["single"] = columns_lower[col_name]
                    break

        # Data analysis fallback - find numeric columns
        if not result["single"] and not result["debit"]:
            for col in df.columns:
                sample = df[col].dropna().head(20)
                if len(sample) == 0:
                    continue

                numeric_count = 0
                for val in sample:
                    if self._normalize_amount(val) is not None:
                        numeric_count += 1

                if numeric_count >= len(sample) * 0.8:  # 80% are valid amounts
                    col_lower = str(col).lower()
                    # Skip if it looks like a balance column
                    if any(bal in col_lower for bal in ["balance", "running"]):
                        continue
                    result["single"] = str(col)
                    break

        return result

    def _find_merchant_column(self, df: pd.DataFrame, columns_lower: Dict[str, str],
                               existing_mapping: Dict) -> Optional[str]:
        """Find merchant/payee column."""
        used_columns = set(v for v in existing_mapping.values() if v)

        for col_name in MERCHANT_COLUMNS:
            if col_name in columns_lower:
                col = columns_lower[col_name]
                if col not in used_columns:
                    return col

        # Data analysis - find text column with varied values
        for col in df.columns:
            if str(col) in used_columns:
                continue

            sample = df[col].dropna().head(20)
            if len(sample) < 5:
                continue

            # Check if it's text with variety
            unique_ratio = len(sample.unique()) / len(sample)
            avg_len = sample.astype(str).str.len().mean()

            if unique_ratio > 0.5 and avg_len > 5:
                # Looks like descriptions/merchant names
                return str(col)

        return None

    def _find_description_column(self, columns_lower: Dict[str, str],
                                   existing_mapping: Dict) -> Optional[str]:
        """Find description/memo column."""
        used_columns = set(v for v in existing_mapping.values() if v)

        for col_name in DESC_COLUMNS:
            if col_name in columns_lower:
                col = columns_lower[col_name]
                if col not in used_columns:
                    return col

        return None

    def _find_balance_column(self, columns_lower: Dict[str, str]) -> Optional[str]:
        """Find balance column if present."""
        for col_name in BALANCE_COLUMNS:
            if col_name in columns_lower:
                return columns_lower[col_name]
        return None

    def _detect_split_amounts(self, df: pd.DataFrame, columns: List[str]) -> Dict[str, Any]:
        """Detect if the file uses separate debit/credit columns."""
        columns_lower = [c.lower() for c in columns]

        has_debit = any(d in columns_lower for d in ["debit", "debited", "withdrawals", "outflow"])
        has_credit = any(c in columns_lower for c in ["credit", "credited", "deposits", "inflow"])

        return {
            "detected": has_debit and has_credit,
            "debit_column": next((columns[i] for i, c in enumerate(columns_lower)
                                  if any(d in c for d in ["debit", "withdraw", "outflow"])), None),
            "credit_column": next((columns[i] for i, c in enumerate(columns_lower)
                                   if any(d in c for d in ["credit", "deposit", "inflow"])), None)
        }

    def _analyze_transactions(self, df: pd.DataFrame, mapping: Dict,
                               has_split: Dict) -> Dict[str, Any]:
        """Analyze transaction patterns in the data."""
        analysis = {
            "date_range": {"start": None, "end": None},
            "total_income": 0.0,
            "total_expenses": 0.0,
            "transaction_count": 0,
            "income_count": 0,
            "expense_count": 0,
            "avg_transaction": 0.0,
            "largest_expense": 0.0,
            "largest_income": 0.0,
            "potential_subscriptions": [],
            "potential_income_sources": []
        }

        # Parse dates
        date_col = mapping.get("date_column")
        if date_col and date_col in df.columns:
            dates = []
            for val in df[date_col].dropna():
                parsed = self._normalize_date(val)
                if parsed:
                    dates.append(parsed)

            if dates:
                dates.sort()
                analysis["date_range"]["start"] = dates[0]
                analysis["date_range"]["end"] = dates[-1]

        # Analyze amounts
        amounts = []
        merchants = []

        amount_col = mapping.get("amount_column")
        debit_col = mapping.get("debit_column")
        credit_col = mapping.get("credit_column")
        merchant_col = mapping.get("merchant_column")

        for _, row in df.iterrows():
            amount = None

            if amount_col and amount_col in df.columns:
                amount = self._normalize_amount(row.get(amount_col))
            elif debit_col or credit_col:
                debit = self._normalize_amount(row.get(debit_col)) if debit_col else 0
                credit = self._normalize_amount(row.get(credit_col)) if credit_col else 0
                debit = debit or 0
                credit = credit or 0
                amount = credit - debit if credit else -abs(debit) if debit else None

            if amount is not None:
                amounts.append(amount)
                analysis["transaction_count"] += 1

                if amount > 0:
                    analysis["income_count"] += 1
                    analysis["total_income"] += amount
                    if amount > analysis["largest_income"]:
                        analysis["largest_income"] = amount
                elif amount < 0:
                    analysis["expense_count"] += 1
                    analysis["total_expenses"] += abs(amount)
                    if abs(amount) > analysis["largest_expense"]:
                        analysis["largest_expense"] = abs(amount)

            # Track merchants
            if merchant_col and merchant_col in df.columns:
                merchant = str(row.get(merchant_col, "")).strip().lower()
                if merchant:
                    merchants.append(merchant)

        if amounts:
            analysis["avg_transaction"] = round(sum(abs(a) for a in amounts) / len(amounts), 2)

        # Detect subscriptions
        merchant_counts = Counter(merchants)
        for merchant, count in merchant_counts.most_common(20):
            if count >= 2:  # Appears multiple times
                for pattern in SUBSCRIPTION_PATTERNS:
                    if re.search(pattern, merchant, re.IGNORECASE):
                        analysis["potential_subscriptions"].append(merchant.title())
                        break

        # Detect income sources
        for merchant, count in merchant_counts.most_common(20):
            for pattern in INCOME_PATTERNS:
                if re.search(pattern, merchant, re.IGNORECASE):
                    analysis["potential_income_sources"].append(merchant.title())
                    break

        # Round totals
        analysis["total_income"] = round(analysis["total_income"], 2)
        analysis["total_expenses"] = round(analysis["total_expenses"], 2)

        return analysis

    def _detect_account_type(self, df: pd.DataFrame, mapping: Dict,
                              format_info: Dict) -> Dict[str, Any]:
        """Detect account type from transaction patterns."""
        # Start with format hint
        account_type = format_info.get("account_type", "unknown")
        confidence = "low"
        hints = []

        # Analyze transaction patterns
        amount_col = mapping.get("amount_column")
        merchant_col = mapping.get("merchant_column")
        balance_col = mapping.get("balance_column")

        if not amount_col and not mapping.get("debit_column"):
            return {"type": account_type, "confidence": confidence, "hints": hints}

        amounts = []
        for _, row in df.iterrows():
            if amount_col:
                amt = self._normalize_amount(row.get(amount_col))
            else:
                debit = self._normalize_amount(row.get(mapping.get("debit_column"))) or 0
                credit = self._normalize_amount(row.get(mapping.get("credit_column"))) or 0
                amt = credit - debit if credit else -debit

            if amt is not None:
                amounts.append(amt)

        if not amounts:
            return {"type": account_type, "confidence": confidence, "hints": hints}

        positive_count = sum(1 for a in amounts if a > 0)
        negative_count = sum(1 for a in amounts if a < 0)
        total = len(amounts)

        # Credit cards: mostly negative (expenses), payments show as positive
        if negative_count > total * 0.7:
            account_type = "credit_card"
            confidence = "medium"
            hints.append("Mostly expenses (typical for credit card)")

            # High confidence if we see payment patterns
            if merchant_col:
                merchants = df[merchant_col].astype(str).str.lower()
                payment_count = merchants.str.contains(r"payment|thank you", regex=True).sum()
                if payment_count >= 1:
                    confidence = "high"
                    hints.append("Found payment transactions")

        # Checking: mix of income and expenses
        elif positive_count > total * 0.1 and negative_count > total * 0.3:
            # If we see payroll/direct deposits, likely checking
            if merchant_col:
                merchants = df[merchant_col].astype(str).str.lower()
                income_matches = merchants.str.contains(r"direct dep|payroll|salary", regex=True).sum()
                if income_matches >= 1:
                    account_type = "checking"
                    confidence = "high"
                    hints.append("Found direct deposit transactions")
                else:
                    account_type = "checking"
                    confidence = "medium"
                    hints.append("Mix of income and expenses")

        # Savings: mostly positive (deposits) or few transactions
        elif positive_count > total * 0.5:
            account_type = "savings"
            confidence = "medium"
            hints.append("Mostly deposits")

        # Balance column is strong hint for checking/savings
        if balance_col:
            hints.append("Has running balance")
            if account_type == "unknown":
                account_type = "checking"
                confidence = "medium"

        # Override with format info if it had high specificity
        if format_info.get("account_type") and format_info.get("account_type") != "unknown":
            if format_info["account_type"] in ["credit_card", "checking", "savings"]:
                account_type = format_info["account_type"]
                if confidence == "low":
                    confidence = "medium"
                hints.append(f"Format signature: {format_info.get('name', 'Unknown')}")

        return {
            "type": account_type,
            "confidence": confidence,
            "hints": hints
        }

    def _calculate_confidence(self, format_name: str, columns: List[str],
                               mapping: Dict) -> str:
        """Calculate overall confidence in the detection."""
        if format_name == "generic":
            return "low"

        # Count how many required fields we found
        found = sum(1 for v in mapping.values() if v)
        if found >= 4:
            return "high"
        elif found >= 2:
            return "medium"
        return "low"

    def _get_sample_rows(self, df: pd.DataFrame, count: int = 5) -> List[Dict]:
        """Get sample rows for preview."""
        sample_rows = []
        for _, row in df.head(count).iterrows():
            sample_rows.append({
                str(col): str(val) if not pd.isna(val) else ""
                for col, val in row.items()
            })
        return sample_rows

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
