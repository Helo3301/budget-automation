"""SQLite store for transactions and categories."""
import sqlite3
import hashlib
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import statistics

from .schema import SCHEMA_SQL, DEFAULT_CATEGORIES_SQL


class SQLiteStore:
    """SQLite storage for transactions, categories, and categorization logs."""

    def __init__(self, db_path: Path):
        """Initialize the store with database path."""
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema and seed defaults.

        Note: We run migrations BEFORE the full schema to handle cases where
        the database was created with an older schema (e.g., without account_id).
        """
        cursor = self.conn.cursor()
        # Run migrations first to add any missing columns/tables
        self._run_migrations()
        # Now create the full schema (will skip existing objects due to IF NOT EXISTS)
        cursor.executescript(SCHEMA_SQL)
        cursor.executescript(DEFAULT_CATEGORIES_SQL)
        self.conn.commit()

    def _run_migrations(self) -> None:
        """Run any pending migrations for schema updates.

        This runs BEFORE the full schema to ensure columns exist before indexes are created.
        """
        # Check if tables exist first (for existing databases)
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='categories'"
        )
        categories_exists = cursor.fetchone() is not None

        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'"
        )
        transactions_exists = cursor.fetchone() is not None

        # Migration: Add budget_amount column to categories if table exists and column missing
        if categories_exists:
            cursor = self.conn.execute("PRAGMA table_info(categories)")
            columns = [row[1] for row in cursor.fetchall()]
            if "budget_amount" not in columns:
                self.conn.execute("ALTER TABLE categories ADD COLUMN budget_amount REAL DEFAULT 0")
                self.conn.commit()

            # Re-check columns for color migration
            cursor = self.conn.execute("PRAGMA table_info(categories)")
            columns = [row[1] for row in cursor.fetchall()]
            if "color" not in columns:
                self.conn.execute("ALTER TABLE categories ADD COLUMN color TEXT DEFAULT '#6B7280'")
                # Set distinct colors for default categories
                category_colors = {
                    'Housing': '#EF4444',      # Red
                    'Transportation': '#F97316', # Orange
                    'Food & Dining': '#F59E0B', # Amber
                    'Utilities': '#84CC16',    # Lime
                    'Healthcare': '#22C55E',   # Green
                    'Entertainment': '#14B8A6', # Teal
                    'Shopping': '#06B6D4',     # Cyan
                    'Personal Care': '#3B82F6', # Blue
                    'Education': '#6366F1',    # Indigo
                    'Travel': '#8B5CF6',       # Violet
                    'Subscriptions': '#A855F7', # Purple
                    'Income': '#10B981',       # Emerald
                    'Transfer': '#6B7280',     # Gray
                    'Other': '#78716C',        # Stone
                }
                for name, color in category_colors.items():
                    self.conn.execute("UPDATE categories SET color = ? WHERE name = ?", (color, name))
                self.conn.commit()

        # Migration: Add account_id column to transactions if table exists and column missing
        if transactions_exists:
            cursor = self.conn.execute("PRAGMA table_info(transactions)")
            txn_columns = [row[1] for row in cursor.fetchall()]
            if "account_id" not in txn_columns:
                self.conn.execute("ALTER TABLE transactions ADD COLUMN account_id INTEGER")
                self.conn.commit()

        # Migration: Create accounts table if not exists (for existing DBs)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                institution TEXT,
                account_type TEXT NOT NULL DEFAULT 'checking',
                last_four TEXT,
                color TEXT DEFAULT '#3B82F6',
                initial_balance REAL DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

        # Create indexes only if the columns/tables exist
        if transactions_exists:
            # Re-check if account_id exists now (either from migration or original schema)
            cursor = self.conn.execute("PRAGMA table_info(transactions)")
            txn_columns = [row[1] for row in cursor.fetchall()]
            if "account_id" in txn_columns:
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id)")
                self.conn.commit()

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(is_active)")
        self.conn.commit()

        # Migration: Create recurring_transactions table for subscription tracking
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS recurring_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                merchant TEXT,
                amount REAL NOT NULL,
                frequency TEXT NOT NULL DEFAULT 'monthly',
                category_id INTEGER,
                account_id INTEGER,
                start_date TEXT NOT NULL,
                next_due_date TEXT,
                last_paid_date TEXT,
                is_active INTEGER DEFAULT 1,
                auto_detect INTEGER DEFAULT 0,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (category_id) REFERENCES categories(id),
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_recurring_active ON recurring_transactions(is_active)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_recurring_next_due ON recurring_transactions(next_due_date)")
        self.conn.commit()

        # Migration: Create transaction_splits table for splitting transactions across categories
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS transaction_splits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                description TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (transaction_id) REFERENCES transactions(id) ON DELETE CASCADE,
                FOREIGN KEY (category_id) REFERENCES categories(id)
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_splits_transaction ON transaction_splits(transaction_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_splits_category ON transaction_splits(category_id)")
        self.conn.commit()

        # Migration: Add is_split column to transactions table
        if transactions_exists:
            cursor = self.conn.execute("PRAGMA table_info(transactions)")
            txn_columns = [row[1] for row in cursor.fetchall()]
            if "is_split" not in txn_columns:
                self.conn.execute("ALTER TABLE transactions ADD COLUMN is_split INTEGER DEFAULT 0")
                self.conn.commit()

        # Migration: Create bill_payments table for payment history tracking
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bill_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recurring_id INTEGER NOT NULL,
                amount_paid REAL NOT NULL,
                due_date TEXT NOT NULL,
                paid_date TEXT NOT NULL,
                payment_method TEXT,
                confirmation_number TEXT,
                notes TEXT,
                transaction_id INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (recurring_id) REFERENCES recurring_transactions(id) ON DELETE CASCADE,
                FOREIGN KEY (transaction_id) REFERENCES transactions(id)
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_bill_payments_recurring ON bill_payments(recurring_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_bill_payments_due_date ON bill_payments(due_date)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_bill_payments_paid_date ON bill_payments(paid_date)")
        self.conn.commit()

        # Migration: Create budget_settings table for income and budget configuration
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS budget_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL UNIQUE,
                value TEXT NOT NULL,
                description TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Insert default monthly income if not exists
        self.conn.execute("""
            INSERT OR IGNORE INTO budget_settings (key, value, description) VALUES
            ('monthly_income', '0', 'Expected monthly take-home income'),
            ('savings_target_percent', '20', 'Target percentage of income to save'),
            ('emergency_fund_months', '6', 'Target months of expenses for emergency fund'),
            ('discretionary_warning_percent', '80', 'Warn when discretionary spending exceeds this % of budget')
        """)
        self.conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def get_tables(self) -> List[str]:
        """Get list of tables in the database."""
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_all_categories(self) -> List[Dict[str, Any]]:
        """Get all categories."""
        cursor = self.conn.execute("SELECT * FROM categories ORDER BY name")
        return [dict(row) for row in cursor.fetchall()]

    def get_category(self, category_id: int) -> Optional[Dict[str, Any]]:
        """Get a single category by ID."""
        cursor = self.conn.execute(
            "SELECT * FROM categories WHERE id = ?", (category_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_category_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a category by name."""
        cursor = self.conn.execute(
            "SELECT * FROM categories WHERE name = ?", (name,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def add_category(
        self,
        name: str,
        keywords: Optional[str] = None,
        budget_amount: float = 0
    ) -> int:
        """Add a new category. Returns the new category ID."""
        cursor = self.conn.execute(
            "INSERT INTO categories (name, keywords, budget_amount) VALUES (?, ?, ?)",
            (name, keywords, budget_amount)
        )
        self.conn.commit()
        return cursor.lastrowid

    def update_category(self, category_id: int, **kwargs) -> bool:
        """Update a category's fields."""
        allowed = {"name", "keywords", "budget_amount"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False

        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        params = list(updates.values()) + [category_id]
        self.conn.execute(f"UPDATE categories SET {set_clause} WHERE id = ?", params)
        self.conn.commit()
        return True

    def get_category_spending(
        self,
        category_id: int,
        year: Optional[int] = None,
        month: Optional[int] = None
    ) -> float:
        """Get total spending for a category in a given month (or all time)."""
        query = "SELECT SUM(amount) FROM transactions WHERE category_id = ? AND amount < 0"
        params = [category_id]

        if year and month:
            # Filter by year-month
            date_prefix = f"{year:04d}-{month:02d}"
            query += " AND date LIKE ?"
            params.append(f"{date_prefix}%")

        cursor = self.conn.execute(query, params)
        result = cursor.fetchone()[0]
        return abs(result) if result else 0

    def delete_category(self, category_id: int) -> bool:
        """Delete a category. Returns False if category has transactions."""
        # Check if any transactions use this category
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE category_id = ?",
            (category_id,)
        )
        count = cursor.fetchone()[0]
        if count > 0:
            return False

        self.conn.execute("DELETE FROM categories WHERE id = ?", (category_id,))
        self.conn.commit()
        return True

    def get_category_transaction_count(self, category_id: int) -> int:
        """Get the count of transactions using this category."""
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE category_id = ?",
            (category_id,)
        )
        return cursor.fetchone()[0]

    # === Account Methods ===

    def get_all_accounts(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get all accounts."""
        query = "SELECT * FROM accounts"
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY name"
        cursor = self.conn.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    def get_account(self, account_id: int) -> Optional[Dict[str, Any]]:
        """Get a single account by ID."""
        cursor = self.conn.execute(
            "SELECT * FROM accounts WHERE id = ?", (account_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_account_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get an account by name."""
        cursor = self.conn.execute(
            "SELECT * FROM accounts WHERE name = ?", (name,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def add_account(
        self,
        name: str,
        institution: Optional[str] = None,
        account_type: str = "checking",
        last_four: Optional[str] = None,
        color: str = "#3B82F6",
        initial_balance: float = 0
    ) -> int:
        """Add a new account. Returns the new account ID."""
        cursor = self.conn.execute(
            """INSERT INTO accounts (name, institution, account_type, last_four, color, initial_balance)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (name, institution, account_type, last_four, color, initial_balance)
        )
        self.conn.commit()
        return cursor.lastrowid

    def update_account(self, account_id: int, **kwargs) -> bool:
        """Update an account's fields."""
        allowed = {"name", "institution", "account_type", "last_four", "color", "initial_balance", "is_active"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False

        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        params = list(updates.values()) + [account_id]
        self.conn.execute(f"UPDATE accounts SET {set_clause} WHERE id = ?", params)
        self.conn.commit()
        return True

    def delete_account(self, account_id: int) -> bool:
        """Soft delete an account (set is_active = 0)."""
        self.conn.execute("UPDATE accounts SET is_active = 0 WHERE id = ?", (account_id,))
        self.conn.commit()
        return True

    def get_account_transaction_count(self, account_id: int) -> int:
        """Get the count of transactions for an account."""
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE account_id = ?",
            (account_id,)
        )
        return cursor.fetchone()[0]

    def get_account_balance(self, account_id: int) -> float:
        """Calculate current balance for an account.

        For credit cards: balance shows amount owed (positive = debt)
        - initial_balance = credit limit (used for available_credit calculation)
        - transactions: negative = purchases (increase debt), positive = payments (reduce debt)
        - Balance owed = -(sum of transactions) = abs(purchases) - payments

        For regular accounts (checking, savings):
        - balance = initial_balance + sum(transactions)
        """
        account = self.get_account(account_id)
        if not account:
            return 0

        initial = account.get("initial_balance", 0) or 0
        account_type = account.get("account_type", "checking")

        cursor = self.conn.execute(
            "SELECT SUM(amount) FROM transactions WHERE account_id = ?",
            (account_id,)
        )
        result = cursor.fetchone()[0]
        txn_sum = result if result else 0

        if account_type == "credit":
            # For credit cards: txn_sum is negative for purchases, positive for payments
            # Balance owed = -txn_sum (purchases become positive debt)
            # Example: -$500 purchase -> owed $500, +$200 payment -> owed $300
            return -txn_sum  # Returns amount owed (positive = debt)
        else:
            # Regular account: initial + sum
            return initial + txn_sum

    def get_credit_card_available(self, account_id: int) -> float:
        """Get available credit on a credit card."""
        account = self.get_account(account_id)
        if not account or account.get("account_type") != "credit":
            return 0

        limit = account.get("initial_balance", 0) or 0  # initial_balance = credit limit
        owed = self.get_account_balance(account_id)
        return max(0, limit - owed)  # Can't be negative

    def get_account_spending(
        self,
        account_id: int,
        year: Optional[int] = None,
        month: Optional[int] = None
    ) -> float:
        """Get total spending (negative transactions) for an account in a given month."""
        query = "SELECT SUM(amount) FROM transactions WHERE account_id = ? AND amount < 0"
        params = [account_id]

        if year and month:
            date_prefix = f"{year:04d}-{month:02d}"
            query += " AND date LIKE ?"
            params.append(f"{date_prefix}%")

        cursor = self.conn.execute(query, params)
        result = cursor.fetchone()[0]
        return abs(result) if result else 0

    def get_account_income(
        self,
        account_id: int,
        year: Optional[int] = None,
        month: Optional[int] = None
    ) -> float:
        """Get total income (positive transactions) for an account in a given month."""
        query = "SELECT SUM(amount) FROM transactions WHERE account_id = ? AND amount > 0"
        params = [account_id]

        if year and month:
            date_prefix = f"{year:04d}-{month:02d}"
            query += " AND date LIKE ?"
            params.append(f"{date_prefix}%")

        cursor = self.conn.execute(query, params)
        result = cursor.fetchone()[0]
        return result if result else 0

    def get_transactions_by_account(
        self,
        account_id: int,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get transactions for an account."""
        cursor = self.conn.execute(
            """SELECT * FROM transactions WHERE account_id = ?
               ORDER BY date DESC LIMIT ?""",
            (account_id, limit)
        )
        return [dict(row) for row in cursor.fetchall()]

    def update_transaction_account(self, txn_id: int, account_id: Optional[int]) -> None:
        """Update a transaction's account."""
        self.conn.execute(
            "UPDATE transactions SET account_id = ? WHERE id = ?",
            (account_id, txn_id)
        )
        self.conn.commit()

    def _compute_hash(self, date: str, amount: float, merchant: str) -> str:
        """Compute hash for deduplication."""
        data = f"{date}|{amount}|{merchant}".encode()
        return hashlib.sha256(data).hexdigest()

    def _transaction_exists(self, date: str, amount: float, merchant: str) -> bool:
        """Check if transaction already exists (for deduplication)."""
        txn_hash = self._compute_hash(date, amount, merchant)
        cursor = self.conn.execute(
            """SELECT id FROM transactions
               WHERE date = ? AND amount = ? AND merchant = ?""",
            (date, amount, merchant)
        )
        return cursor.fetchone() is not None

    def add_transaction(
        self,
        date: str,
        amount: float,
        merchant: str,
        description: Optional[str] = None,
        category_id: Optional[int] = None
    ) -> Optional[int]:
        """Add a transaction, returns ID or None if duplicate."""
        if self._transaction_exists(date, amount, merchant):
            return None

        cursor = self.conn.execute(
            """INSERT INTO transactions (date, amount, merchant, description, category_id)
               VALUES (?, ?, ?, ?, ?)""",
            (date, amount, merchant, description, category_id)
        )
        self.conn.commit()
        return cursor.lastrowid

    def add_transactions(self, transactions: List[Dict[str, Any]]) -> List[int]:
        """Add multiple transactions, returns list of IDs (skips duplicates)."""
        ids = []
        for txn in transactions:
            txn_id = self.add_transaction(
                date=txn["date"],
                amount=txn["amount"],
                merchant=txn["merchant"],
                description=txn.get("description"),
                category_id=txn.get("category_id")
            )
            if txn_id is not None:
                ids.append(txn_id)
        return ids

    def get_transaction(self, txn_id: int) -> Optional[Dict[str, Any]]:
        """Get a transaction by ID."""
        cursor = self.conn.execute(
            "SELECT * FROM transactions WHERE id = ?", (txn_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_uncategorized_transactions(self) -> List[Dict[str, Any]]:
        """Get all transactions without a category."""
        cursor = self.conn.execute(
            "SELECT * FROM transactions WHERE category_id IS NULL ORDER BY date"
        )
        return [dict(row) for row in cursor.fetchall()]

    def update_transaction_category(self, txn_id: int, category_id: int) -> None:
        """Update a transaction's category."""
        self.conn.execute(
            "UPDATE transactions SET category_id = ? WHERE id = ?",
            (category_id, txn_id)
        )
        self.conn.commit()

    def add_categorization_log(
        self,
        transaction_id: int,
        category_id: int,
        confidence: float,
        explanation: str,
        similar_transaction_ids: Optional[List[int]] = None
    ) -> int:
        """Log a categorization decision with explanation."""
        similar_ids_json = json.dumps(similar_transaction_ids or [])
        cursor = self.conn.execute(
            """INSERT INTO categorization_log
               (transaction_id, category_id, confidence, explanation, similar_transaction_ids)
               VALUES (?, ?, ?, ?, ?)""",
            (transaction_id, category_id, confidence, explanation, similar_ids_json)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_transactions_by_merchant(self, merchant: str) -> List[Dict[str, Any]]:
        """Get all transactions for a merchant."""
        cursor = self.conn.execute(
            "SELECT * FROM transactions WHERE merchant = ? ORDER BY date",
            (merchant,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_transactions_by_category(self, category_id: int) -> List[Dict[str, Any]]:
        """Get all transactions for a category."""
        cursor = self.conn.execute(
            "SELECT * FROM transactions WHERE category_id = ? ORDER BY date",
            (category_id,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_category_stats(self, category_id: int) -> Dict[str, float]:
        """Get statistics for a category (for anomaly detection)."""
        cursor = self.conn.execute(
            "SELECT amount FROM transactions WHERE category_id = ?",
            (category_id,)
        )
        amounts = [abs(row[0]) for row in cursor.fetchall()]

        if not amounts:
            return {"mean": 0, "std": 0, "q1": 0, "q3": 0}

        amounts_sorted = sorted(amounts)
        n = len(amounts_sorted)

        mean = statistics.mean(amounts)
        std = statistics.stdev(amounts) if n > 1 else 0

        # Calculate quartiles
        q1_idx = n // 4
        q3_idx = (3 * n) // 4
        q1 = amounts_sorted[q1_idx] if n > 0 else 0
        q3 = amounts_sorted[q3_idx] if n > 0 else 0

        return {"mean": mean, "std": std, "q1": q1, "q3": q3}

    def mark_transaction_recurring(self, txn_id: int, group_id: int) -> None:
        """Mark a transaction as recurring."""
        self.conn.execute(
            "UPDATE transactions SET is_recurring = 1, recurring_group_id = ? WHERE id = ?",
            (group_id, txn_id)
        )
        self.conn.commit()

    def mark_transaction_anomaly(self, txn_id: int) -> None:
        """Mark a transaction as an anomaly."""
        self.conn.execute(
            "UPDATE transactions SET is_anomaly = 1 WHERE id = ?",
            (txn_id,)
        )
        self.conn.commit()

    def get_all_transactions(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all transactions with optional date filters."""
        query = "SELECT * FROM transactions WHERE 1=1"
        params = []

        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)

        query += " ORDER BY date"
        cursor = self.conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def get_merchant_amount_groups(self) -> List[Dict[str, Any]]:
        """Get transactions grouped by merchant and amount for recurring detection."""
        cursor = self.conn.execute(
            """SELECT merchant, amount, GROUP_CONCAT(id) as txn_ids,
                      GROUP_CONCAT(date) as dates, COUNT(*) as count
               FROM transactions
               GROUP BY merchant, amount
               HAVING count >= 2
               ORDER BY count DESC"""
        )
        results = []
        for row in cursor.fetchall():
            results.append({
                "merchant": row[0],
                "amount": row[1],
                "txn_ids": [int(x) for x in row[2].split(",")],
                "dates": row[3].split(","),
                "count": row[4]
            })
        return results

    def get_all_merchants(self) -> List[str]:
        """Get list of all unique merchants."""
        cursor = self.conn.execute(
            "SELECT DISTINCT merchant FROM transactions"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_categorization_log(self, txn_id: int) -> Optional[Dict[str, Any]]:
        """Get categorization log for a transaction."""
        cursor = self.conn.execute(
            "SELECT * FROM categorization_log WHERE transaction_id = ? ORDER BY created_at DESC LIMIT 1",
            (txn_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    # === Categorization Rules ===

    def add_rule(
        self,
        name: str,
        rule_type: str,
        pattern: str,
        category_id: int,
        priority: int = 0,
        notes: Optional[str] = None
    ) -> int:
        """Add a categorization rule."""
        cursor = self.conn.execute(
            """INSERT INTO categorization_rules (name, rule_type, pattern, category_id, priority, notes)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (name, rule_type, pattern, category_id, priority, notes)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_all_rules(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get all categorization rules."""
        query = "SELECT r.*, c.name as category_name FROM categorization_rules r JOIN categories c ON r.category_id = c.id"
        if active_only:
            query += " WHERE r.is_active = 1"
        query += " ORDER BY r.priority DESC, r.created_at"
        cursor = self.conn.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    def get_rule(self, rule_id: int) -> Optional[Dict[str, Any]]:
        """Get a single rule by ID."""
        cursor = self.conn.execute(
            """SELECT r.*, c.name as category_name FROM categorization_rules r
               JOIN categories c ON r.category_id = c.id WHERE r.id = ?""",
            (rule_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def update_rule(self, rule_id: int, **kwargs) -> bool:
        """Update a rule's fields."""
        allowed = {"name", "rule_type", "pattern", "category_id", "priority", "is_active", "notes"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False

        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        params = list(updates.values()) + [rule_id]
        self.conn.execute(f"UPDATE categorization_rules SET {set_clause} WHERE id = ?", params)
        self.conn.commit()
        return True

    def delete_rule(self, rule_id: int) -> bool:
        """Delete a rule (or deactivate it)."""
        self.conn.execute("UPDATE categorization_rules SET is_active = 0 WHERE id = ?", (rule_id,))
        self.conn.commit()
        return True

    def apply_rules_to_transaction(self, txn: Dict[str, Any]) -> Optional[int]:
        """Apply rules to a transaction, returns category_id if matched."""
        rules = self.get_all_rules(active_only=True)
        merchant = txn.get("merchant", "").lower()
        description = (txn.get("description") or "").lower()
        amount = txn.get("amount", 0)

        for rule in rules:
            rule_type = rule["rule_type"]
            pattern = rule["pattern"].lower()

            matched = False
            if rule_type == "merchant_contains":
                matched = pattern in merchant
            elif rule_type == "merchant_exact":
                matched = pattern == merchant
            elif rule_type == "description_contains":
                matched = pattern in description
            elif rule_type == "amount_range":
                # Pattern format: "min,max" (e.g., "10,50")
                try:
                    min_amt, max_amt = map(float, pattern.split(","))
                    matched = min_amt <= abs(amount) <= max_amt
                except ValueError:
                    pass

            if matched:
                return rule["category_id"]

        return None

    def apply_rules_to_uncategorized(self) -> int:
        """Apply rules to all uncategorized transactions, returns count updated."""
        uncategorized = self.get_uncategorized_transactions()
        updated = 0
        for txn in uncategorized:
            category_id = self.apply_rules_to_transaction(txn)
            if category_id:
                self.update_transaction_category(txn["id"], category_id)
                updated += 1
        return updated

    def find_similar_by_merchant(self, merchant: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Find transactions with similar merchant names."""
        # Simple substring match
        cursor = self.conn.execute(
            """SELECT * FROM transactions
               WHERE LOWER(merchant) LIKE ?
               ORDER BY date DESC LIMIT ?""",
            (f"%{merchant.lower()}%", limit)
        )
        return [dict(row) for row in cursor.fetchall()]

    def reset_all_data(self) -> Dict[str, int]:
        """Reset all data: completely clear all tables.

        Returns counts of deleted items.
        """
        counts = {}

        # Count before deleting
        counts["transactions"] = self.conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        counts["accounts"] = self.conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
        counts["rules"] = self.conn.execute("SELECT COUNT(*) FROM categorization_rules").fetchone()[0]
        counts["logs"] = self.conn.execute("SELECT COUNT(*) FROM categorization_log").fetchone()[0]
        counts["categories"] = self.conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]

        # Count additional tables
        try:
            counts["subscriptions"] = self.conn.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0]
        except:
            counts["subscriptions"] = 0
        try:
            counts["subscription_payments"] = self.conn.execute("SELECT COUNT(*) FROM subscription_payments").fetchone()[0]
        except:
            counts["subscription_payments"] = 0
        try:
            counts["savings_goals"] = self.conn.execute("SELECT COUNT(*) FROM savings_goals").fetchone()[0]
        except:
            counts["savings_goals"] = 0
        try:
            counts["goal_contributions"] = self.conn.execute("SELECT COUNT(*) FROM goal_contributions").fetchone()[0]
        except:
            counts["goal_contributions"] = 0
        try:
            counts["transaction_splits"] = self.conn.execute("SELECT COUNT(*) FROM transaction_splits").fetchone()[0]
        except:
            counts["transaction_splits"] = 0
        try:
            counts["budget_settings"] = self.conn.execute("SELECT COUNT(*) FROM budget_settings").fetchone()[0]
        except:
            counts["budget_settings"] = 0

        # Delete all data (order matters due to foreign keys)
        self.conn.execute("DELETE FROM categorization_log")
        self.conn.execute("DELETE FROM categorization_rules")

        # Clear subscription payments before subscriptions
        try:
            self.conn.execute("DELETE FROM subscription_payments")
        except:
            pass

        # Clear transaction splits before transactions
        try:
            self.conn.execute("DELETE FROM transaction_splits")
        except:
            pass

        self.conn.execute("DELETE FROM transactions")
        self.conn.execute("DELETE FROM accounts")
        self.conn.execute("DELETE FROM categories")

        # Clear subscriptions
        try:
            self.conn.execute("DELETE FROM subscriptions")
        except:
            pass

        # Clear savings goals and contributions
        try:
            self.conn.execute("DELETE FROM goal_contributions")
        except:
            pass
        try:
            self.conn.execute("DELETE FROM savings_goals")
        except:
            pass

        # Clear budget settings
        try:
            self.conn.execute("DELETE FROM budget_settings")
        except:
            pass

        self.conn.commit()

        return counts

    # === Analytics Methods ===

    def get_monthly_totals(self, months: int = 12) -> List[Dict[str, Any]]:
        """Get income and expenses by month for the last N months."""
        cursor = self.conn.execute("""
            SELECT
                strftime('%Y-%m', date) as month,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
                SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as expenses
            FROM transactions
            WHERE date >= date('now', '-' || ? || ' months')
            GROUP BY strftime('%Y-%m', date)
            ORDER BY month
        """, (months,))
        return [{"month": row[0], "income": row[1] or 0, "expenses": row[2] or 0} for row in cursor.fetchall()]

    def get_category_totals(self, year: Optional[int] = None, month: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get spending totals by category."""
        query = """
            SELECT c.id, c.name, c.color, SUM(ABS(t.amount)) as total, COUNT(t.id) as count
            FROM categories c
            LEFT JOIN transactions t ON t.category_id = c.id AND t.amount < 0
        """
        params = []
        if year and month:
            query += " AND strftime('%Y-%m', t.date) = ?"
            params.append(f"{year:04d}-{month:02d}")
        query += " GROUP BY c.id ORDER BY total DESC"

        cursor = self.conn.execute(query, params)
        return [{"id": row[0], "name": row[1], "color": row[2], "total": row[3] or 0, "count": row[4] or 0}
                for row in cursor.fetchall()]

    def get_daily_spending(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get daily spending for the last N days."""
        cursor = self.conn.execute("""
            SELECT date, SUM(ABS(amount)) as total
            FROM transactions
            WHERE amount < 0 AND date >= date('now', '-' || ? || ' days')
            GROUP BY date
            ORDER BY date
        """, (days,))
        return [{"date": row[0], "total": row[1] or 0} for row in cursor.fetchall()]

    def get_top_merchants(self, limit: int = 10, days: int = 90) -> List[Dict[str, Any]]:
        """Get top merchants by spending."""
        cursor = self.conn.execute("""
            SELECT merchant, SUM(ABS(amount)) as total, COUNT(*) as count
            FROM transactions
            WHERE amount < 0 AND date >= date('now', '-' || ? || ' days')
            GROUP BY merchant
            ORDER BY total DESC
            LIMIT ?
        """, (days, limit))
        return [{"merchant": row[0], "total": row[1] or 0, "count": row[2]} for row in cursor.fetchall()]

    def get_spending_by_day_of_week(self) -> List[Dict[str, Any]]:
        """Get average spending by day of week."""
        cursor = self.conn.execute("""
            SELECT
                CAST(strftime('%w', date) AS INTEGER) as day_num,
                CASE CAST(strftime('%w', date) AS INTEGER)
                    WHEN 0 THEN 'Sun' WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue'
                    WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' ELSE 'Sat'
                END as day_name,
                AVG(ABS(amount)) as avg_amount,
                SUM(ABS(amount)) as total
            FROM transactions
            WHERE amount < 0
            GROUP BY day_num
            ORDER BY day_num
        """)
        return [{"day": row[1], "avg": row[2] or 0, "total": row[3] or 0} for row in cursor.fetchall()]

    def get_category_trend(self, category_id: int, months: int = 6) -> List[Dict[str, Any]]:
        """Get spending trend for a specific category over months."""
        cursor = self.conn.execute("""
            SELECT strftime('%Y-%m', date) as month, SUM(ABS(amount)) as total
            FROM transactions
            WHERE category_id = ? AND amount < 0 AND date >= date('now', '-' || ? || ' months')
            GROUP BY month
            ORDER BY month
        """, (category_id, months))
        return [{"month": row[0], "total": row[1] or 0} for row in cursor.fetchall()]

    # === Recurring Transactions / Subscription Methods ===

    def get_recurring_transactions(self, include_inactive: bool = False) -> List[Dict[str, Any]]:
        """Get all recurring transactions with category and account info."""
        query = """
            SELECT r.*, c.name as category_name, a.name as account_name
            FROM recurring_transactions r
            LEFT JOIN categories c ON r.category_id = c.id
            LEFT JOIN accounts a ON r.account_id = a.id
        """
        if not include_inactive:
            query += " WHERE r.is_active = 1"
        query += " ORDER BY r.next_due_date ASC NULLS LAST, r.name"
        cursor = self.conn.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    def get_recurring_transaction(self, recurring_id: int) -> Optional[Dict[str, Any]]:
        """Get a single recurring transaction by ID."""
        cursor = self.conn.execute("""
            SELECT r.*, c.name as category_name, a.name as account_name
            FROM recurring_transactions r
            LEFT JOIN categories c ON r.category_id = c.id
            LEFT JOIN accounts a ON r.account_id = a.id
            WHERE r.id = ?
        """, (recurring_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def create_recurring_transaction(self, name: str, amount: float, frequency: str,
                                      start_date: str, merchant: str = None,
                                      category_id: int = None, account_id: int = None,
                                      notes: str = None) -> int:
        """Create a new recurring transaction. Returns the new ID."""
        # Calculate next due date based on frequency and start date
        next_due = self._calculate_next_due_date(start_date, frequency)
        cursor = self.conn.execute("""
            INSERT INTO recurring_transactions
            (name, merchant, amount, frequency, category_id, account_id, start_date, next_due_date, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, merchant, amount, frequency, category_id, account_id, start_date, next_due, notes))
        self.conn.commit()
        return cursor.lastrowid

    def update_recurring_transaction(self, recurring_id: int, **kwargs) -> bool:
        """Update a recurring transaction. Returns True if updated."""
        allowed_fields = {'name', 'merchant', 'amount', 'frequency', 'category_id',
                         'account_id', 'start_date', 'next_due_date', 'is_active', 'notes'}
        updates = {k: v for k, v in kwargs.items() if k in allowed_fields}
        if not updates:
            return False

        # If frequency or start_date changed, recalculate next_due_date
        if 'frequency' in updates or 'start_date' in updates:
            current = self.get_recurring_transaction(recurring_id)
            if current:
                freq = updates.get('frequency', current['frequency'])
                start = updates.get('start_date', current['start_date'])
                updates['next_due_date'] = self._calculate_next_due_date(start, freq)

        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        cursor = self.conn.execute(
            f"UPDATE recurring_transactions SET {set_clause} WHERE id = ?",
            list(updates.values()) + [recurring_id]
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def delete_recurring_transaction(self, recurring_id: int) -> bool:
        """Delete a recurring transaction. Returns True if deleted."""
        cursor = self.conn.execute(
            "DELETE FROM recurring_transactions WHERE id = ?", (recurring_id,)
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def mark_recurring_paid(self, recurring_id: int, paid_date: str = None,
                            amount_paid: float = None, payment_method: str = None,
                            confirmation_number: str = None, notes: str = None,
                            transaction_id: int = None) -> Optional[int]:
        """Mark a recurring transaction as paid, record payment history, and update next due date.
        Returns the bill_payment ID if successful, None if failed."""
        from datetime import date
        if paid_date is None:
            paid_date = date.today().isoformat()

        recurring = self.get_recurring_transaction(recurring_id)
        if not recurring:
            return None

        # Use recurring amount if not specified
        if amount_paid is None:
            amount_paid = abs(recurring['amount'])

        # Get the due date that was just paid
        due_date = recurring.get('next_due_date') or paid_date

        # Calculate the next due date
        next_due = self._calculate_next_due_date(paid_date, recurring['frequency'])

        # Record payment in bill_payments table
        cursor = self.conn.execute("""
            INSERT INTO bill_payments
            (recurring_id, amount_paid, due_date, paid_date, payment_method, confirmation_number, notes, transaction_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (recurring_id, amount_paid, due_date, paid_date, payment_method, confirmation_number, notes, transaction_id))
        payment_id = cursor.lastrowid

        # Update the recurring transaction
        self.conn.execute("""
            UPDATE recurring_transactions
            SET last_paid_date = ?, next_due_date = ?
            WHERE id = ?
        """, (paid_date, next_due, recurring_id))
        self.conn.commit()
        return payment_id

    def get_upcoming_recurring(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get recurring transactions due within the next N days."""
        cursor = self.conn.execute("""
            SELECT r.*, c.name as category_name, a.name as account_name
            FROM recurring_transactions r
            LEFT JOIN categories c ON r.category_id = c.id
            LEFT JOIN accounts a ON r.account_id = a.id
            WHERE r.is_active = 1
            AND r.next_due_date IS NOT NULL
            AND r.next_due_date <= date('now', '+' || ? || ' days')
            ORDER BY r.next_due_date ASC
        """, (days,))
        return [dict(row) for row in cursor.fetchall()]

    def get_overdue_recurring(self) -> List[Dict[str, Any]]:
        """Get recurring transactions that are past due."""
        cursor = self.conn.execute("""
            SELECT r.*, c.name as category_name, a.name as account_name
            FROM recurring_transactions r
            LEFT JOIN categories c ON r.category_id = c.id
            LEFT JOIN accounts a ON r.account_id = a.id
            WHERE r.is_active = 1
            AND r.next_due_date IS NOT NULL
            AND r.next_due_date < date('now')
            ORDER BY r.next_due_date ASC
        """)
        return [dict(row) for row in cursor.fetchall()]

    def get_recurring_summary(self) -> Dict[str, Any]:
        """Get summary statistics for recurring transactions."""
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as total_count,
                SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_count,
                SUM(CASE WHEN is_active = 1 THEN ABS(amount) ELSE 0 END) as monthly_total,
                SUM(CASE WHEN is_active = 1 AND next_due_date < date('now') THEN 1 ELSE 0 END) as overdue_count,
                SUM(CASE WHEN is_active = 1 AND next_due_date <= date('now', '+7 days') AND next_due_date >= date('now') THEN 1 ELSE 0 END) as upcoming_count
            FROM recurring_transactions
        """)
        row = cursor.fetchone()
        return {
            "total_count": row[0] or 0,
            "active_count": row[1] or 0,
            "monthly_total": row[2] or 0,
            "overdue_count": row[3] or 0,
            "upcoming_count": row[4] or 0
        }

    def _calculate_next_due_date(self, from_date: str, frequency: str) -> str:
        """Calculate the next due date based on frequency."""
        from datetime import datetime, timedelta
        from dateutil.relativedelta import relativedelta

        try:
            dt = datetime.strptime(from_date, "%Y-%m-%d")
        except ValueError:
            dt = datetime.now()

        today = datetime.now().date()

        # Map frequency to delta
        freq_map = {
            'daily': relativedelta(days=1),
            'weekly': relativedelta(weeks=1),
            'biweekly': relativedelta(weeks=2),
            'monthly': relativedelta(months=1),
            'quarterly': relativedelta(months=3),
            'yearly': relativedelta(years=1),
            'annually': relativedelta(years=1)
        }

        delta = freq_map.get(frequency.lower(), relativedelta(months=1))

        # Find the next occurrence after today
        next_date = dt.date()
        while next_date <= today:
            next_date = next_date + delta

        return next_date.isoformat()

    # === Bill Payment History Methods ===

    def get_bill_payments(self, recurring_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        """Get payment history for a recurring bill."""
        cursor = self.conn.execute("""
            SELECT bp.*, r.name as bill_name
            FROM bill_payments bp
            LEFT JOIN recurring_transactions r ON r.id = bp.recurring_id
            WHERE bp.recurring_id = ?
            ORDER BY bp.paid_date DESC, bp.created_at DESC
            LIMIT ?
        """, (recurring_id, limit))
        return [dict(row) for row in cursor.fetchall()]

    def get_all_bill_payments(self, limit: int = 50, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """Get all recent bill payments across all bills."""
        query = """
            SELECT bp.*, r.name as bill_name, r.merchant, c.name as category_name
            FROM bill_payments bp
            LEFT JOIN recurring_transactions r ON r.id = bp.recurring_id
            LEFT JOIN categories c ON c.id = r.category_id
            WHERE 1=1
        """
        params = []
        if start_date:
            query += " AND bp.paid_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND bp.paid_date <= ?"
            params.append(end_date)
        query += " ORDER BY bp.paid_date DESC, bp.created_at DESC LIMIT ?"
        params.append(limit)

        cursor = self.conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def get_bill_payment(self, payment_id: int) -> Optional[Dict[str, Any]]:
        """Get a single bill payment by ID."""
        cursor = self.conn.execute("""
            SELECT bp.*, r.name as bill_name
            FROM bill_payments bp
            LEFT JOIN recurring_transactions r ON r.id = bp.recurring_id
            WHERE bp.id = ?
        """, (payment_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def delete_bill_payment(self, payment_id: int) -> bool:
        """Delete a bill payment record. Returns True if deleted."""
        cursor = self.conn.execute(
            "DELETE FROM bill_payments WHERE id = ?", (payment_id,)
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def get_bills_due_soon(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get bills due within the specified number of days with payment status."""
        cursor = self.conn.execute("""
            SELECT r.*, c.name as category_name, a.name as account_name,
                   CASE
                       WHEN r.next_due_date < date('now') THEN 'overdue'
                       WHEN r.next_due_date <= date('now', '+3 days') THEN 'due_soon'
                       ELSE 'upcoming'
                   END as status,
                   julianday(r.next_due_date) - julianday('now') as days_until_due
            FROM recurring_transactions r
            LEFT JOIN categories c ON r.category_id = c.id
            LEFT JOIN accounts a ON r.account_id = a.id
            WHERE r.is_active = 1
            AND r.next_due_date IS NOT NULL
            AND r.next_due_date <= date('now', '+' || ? || ' days')
            ORDER BY r.next_due_date ASC
        """, (days,))
        return [dict(row) for row in cursor.fetchall()]

    def get_monthly_bill_summary(self, year: int = None, month: int = None) -> Dict[str, Any]:
        """Get summary of bills paid in a specific month."""
        from datetime import date
        if year is None:
            year = date.today().year
        if month is None:
            month = date.today().month

        month_str = f"{year}-{month:02d}"
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as payments_count,
                SUM(amount_paid) as total_paid,
                COUNT(DISTINCT recurring_id) as unique_bills
            FROM bill_payments
            WHERE strftime('%Y-%m', paid_date) = ?
        """, (month_str,))
        row = cursor.fetchone()
        return {
            "year": year,
            "month": month,
            "payments_count": row[0] or 0,
            "total_paid": row[1] or 0,
            "unique_bills": row[2] or 0
        }

    # === Transaction Splitting Methods ===

    def get_transaction_splits(self, transaction_id: int) -> List[Dict[str, Any]]:
        """Get all splits for a transaction."""
        cursor = self.conn.execute("""
            SELECT ts.id, ts.transaction_id, ts.category_id, ts.amount, ts.description,
                   ts.created_at, c.name as category_name
            FROM transaction_splits ts
            LEFT JOIN categories c ON c.id = ts.category_id
            WHERE ts.transaction_id = ?
            ORDER BY ts.id
        """, (transaction_id,))
        return [dict(row) for row in cursor.fetchall()]

    def create_transaction_splits(self, transaction_id: int, splits: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create splits for a transaction. Validates that amounts sum correctly."""
        # Get the original transaction
        cursor = self.conn.execute("SELECT amount FROM transactions WHERE id = ?", (transaction_id,))
        row = cursor.fetchone()
        if not row:
            return {"success": False, "error": "Transaction not found"}

        original_amount = abs(row[0])
        split_total = sum(abs(s.get('amount', 0)) for s in splits)

        # Allow small rounding differences (within 1 cent)
        if abs(split_total - original_amount) > 0.01:
            return {"success": False, "error": f"Split amounts ({split_total:.2f}) must equal transaction amount ({original_amount:.2f})"}

        # Delete existing splits
        self.conn.execute("DELETE FROM transaction_splits WHERE transaction_id = ?", (transaction_id,))

        # Create new splits
        for split in splits:
            self.conn.execute("""
                INSERT INTO transaction_splits (transaction_id, category_id, amount, description)
                VALUES (?, ?, ?, ?)
            """, (transaction_id, split.get('category_id'), abs(split.get('amount', 0)), split.get('description')))

        # Mark transaction as split and clear the main category
        self.conn.execute("""
            UPDATE transactions SET is_split = 1, category_id = NULL WHERE id = ?
        """, (transaction_id,))

        self.conn.commit()
        return {"success": True, "split_count": len(splits)}

    def delete_transaction_splits(self, transaction_id: int) -> Dict[str, Any]:
        """Remove all splits from a transaction (unsplit it)."""
        # Delete the splits
        cursor = self.conn.execute("DELETE FROM transaction_splits WHERE transaction_id = ?", (transaction_id,))
        deleted = cursor.rowcount

        # Mark transaction as not split
        self.conn.execute("UPDATE transactions SET is_split = 0 WHERE id = ?", (transaction_id,))
        self.conn.commit()

        return {"success": True, "deleted_count": deleted}

    def get_split_category_totals(self, year: Optional[int] = None, month: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get category totals including split amounts."""
        # This query combines regular transaction category totals with split amounts
        query = """
            WITH combined_amounts AS (
                -- Regular non-split transactions
                SELECT t.category_id, ABS(t.amount) as amount
                FROM transactions t
                WHERE t.amount < 0 AND (t.is_split IS NULL OR t.is_split = 0)
                {date_filter}

                UNION ALL

                -- Split transaction amounts
                SELECT ts.category_id, ts.amount
                FROM transaction_splits ts
                JOIN transactions t ON t.id = ts.transaction_id
                WHERE t.amount < 0
                {split_date_filter}
            )
            SELECT c.id, c.name, c.color,
                   COALESCE(SUM(ca.amount), 0) as total,
                   COUNT(ca.amount) as count
            FROM categories c
            LEFT JOIN combined_amounts ca ON ca.category_id = c.id
            GROUP BY c.id
            ORDER BY total DESC
        """

        params = []
        date_filter = ""
        split_date_filter = ""

        if year and month:
            date_filter = "AND strftime('%Y-%m', t.date) = ?"
            split_date_filter = "AND strftime('%Y-%m', t.date) = ?"
            params = [f"{year:04d}-{month:02d}", f"{year:04d}-{month:02d}"]

        query = query.format(date_filter=date_filter, split_date_filter=split_date_filter)
        cursor = self.conn.execute(query, params)
        return [{"id": row[0], "name": row[1], "color": row[2], "total": row[3] or 0, "count": row[4] or 0}
                for row in cursor.fetchall()]

    # === Savings Goals Methods ===

    def create_savings_goal(
        self,
        name: str,
        target_amount: float,
        description: Optional[str] = None,
        target_date: Optional[str] = None,
        color: str = '#10B981',
        icon: str = 'piggy-bank'
    ) -> int:
        """Create a new savings goal."""
        cursor = self.conn.execute(
            """INSERT INTO savings_goals (name, description, target_amount, target_date, color, icon)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (name, description, target_amount, target_date, color, icon)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_all_savings_goals(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get all savings goals with contribution counts."""
        query = """
            SELECT g.*,
                   (SELECT COUNT(*) FROM goal_contributions WHERE goal_id = g.id) as contribution_count,
                   (SELECT SUM(amount) FROM goal_contributions WHERE goal_id = g.id) as total_contributed
            FROM savings_goals g
        """
        if active_only:
            query += " WHERE g.is_active = 1"
        query += " ORDER BY g.created_at DESC"
        cursor = self.conn.execute(query)
        results = []
        for row in cursor.fetchall():
            goal = dict(row)
            # Calculate progress percentage
            goal["progress"] = (goal["current_amount"] / goal["target_amount"] * 100) if goal["target_amount"] > 0 else 0
            results.append(goal)
        return results

    def get_savings_goal(self, goal_id: int) -> Optional[Dict[str, Any]]:
        """Get a single savings goal with details."""
        cursor = self.conn.execute(
            """SELECT g.*,
                      (SELECT COUNT(*) FROM goal_contributions WHERE goal_id = g.id) as contribution_count
               FROM savings_goals g WHERE g.id = ?""",
            (goal_id,)
        )
        row = cursor.fetchone()
        if row:
            goal = dict(row)
            goal["progress"] = (goal["current_amount"] / goal["target_amount"] * 100) if goal["target_amount"] > 0 else 0
            return goal
        return None

    def update_savings_goal(
        self,
        goal_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        target_amount: Optional[float] = None,
        target_date: Optional[str] = None,
        color: Optional[str] = None,
        icon: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> bool:
        """Update a savings goal."""
        updates = []
        params = []
        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if description is not None:
            updates.append("description = ?")
            params.append(description)
        if target_amount is not None:
            updates.append("target_amount = ?")
            params.append(target_amount)
        if target_date is not None:
            updates.append("target_date = ?")
            params.append(target_date)
        if color is not None:
            updates.append("color = ?")
            params.append(color)
        if icon is not None:
            updates.append("icon = ?")
            params.append(icon)
        if is_active is not None:
            updates.append("is_active = ?")
            params.append(1 if is_active else 0)

        if not updates:
            return False

        params.append(goal_id)
        self.conn.execute(
            f"UPDATE savings_goals SET {', '.join(updates)} WHERE id = ?",
            params
        )
        self.conn.commit()
        return True

    def delete_savings_goal(self, goal_id: int) -> bool:
        """Delete a savings goal and its contributions."""
        self.conn.execute("DELETE FROM goal_contributions WHERE goal_id = ?", (goal_id,))
        cursor = self.conn.execute("DELETE FROM savings_goals WHERE id = ?", (goal_id,))
        self.conn.commit()
        return cursor.rowcount > 0

    def add_goal_contribution(
        self,
        goal_id: int,
        amount: float,
        note: Optional[str] = None,
        transaction_id: Optional[int] = None
    ) -> int:
        """Add a contribution to a savings goal."""
        cursor = self.conn.execute(
            """INSERT INTO goal_contributions (goal_id, amount, note, transaction_id)
               VALUES (?, ?, ?, ?)""",
            (goal_id, amount, note, transaction_id)
        )
        # Update the goal's current_amount
        self.conn.execute(
            "UPDATE savings_goals SET current_amount = current_amount + ? WHERE id = ?",
            (amount, goal_id)
        )
        # Check if goal is now complete
        goal = self.get_savings_goal(goal_id)
        if goal and goal["current_amount"] >= goal["target_amount"] and not goal["completed_at"]:
            self.conn.execute(
                "UPDATE savings_goals SET completed_at = CURRENT_TIMESTAMP WHERE id = ?",
                (goal_id,)
            )
        self.conn.commit()
        return cursor.lastrowid

    def get_goal_contributions(self, goal_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """Get contributions for a savings goal."""
        cursor = self.conn.execute(
            """SELECT gc.*, t.merchant, t.date as txn_date
               FROM goal_contributions gc
               LEFT JOIN transactions t ON gc.transaction_id = t.id
               WHERE gc.goal_id = ?
               ORDER BY gc.created_at DESC
               LIMIT ?""",
            (goal_id, limit)
        )
        return [dict(row) for row in cursor.fetchall()]

    def delete_goal_contribution(self, contribution_id: int) -> bool:
        """Delete a contribution and update goal amount."""
        # Get the contribution first
        cursor = self.conn.execute(
            "SELECT goal_id, amount FROM goal_contributions WHERE id = ?",
            (contribution_id,)
        )
        row = cursor.fetchone()
        if not row:
            return False

        goal_id, amount = row
        # Delete the contribution
        self.conn.execute("DELETE FROM goal_contributions WHERE id = ?", (contribution_id,))
        # Update the goal's current_amount
        self.conn.execute(
            "UPDATE savings_goals SET current_amount = current_amount - ?, completed_at = NULL WHERE id = ?",
            (amount, goal_id)
        )
        self.conn.commit()
        return True

    # === Budget Settings Methods ===

    def get_budget_setting(self, key: str) -> Optional[str]:
        """Get a budget setting value by key."""
        cursor = self.conn.execute(
            "SELECT value FROM budget_settings WHERE key = ?", (key,)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def get_all_budget_settings(self) -> Dict[str, Any]:
        """Get all budget settings as a dictionary."""
        cursor = self.conn.execute("SELECT key, value, description FROM budget_settings")
        settings = {}
        for row in cursor.fetchall():
            key, value, desc = row
            # Try to convert to appropriate type
            try:
                if '.' in value:
                    settings[key] = float(value)
                else:
                    settings[key] = int(value)
            except ValueError:
                settings[key] = value
        return settings

    def update_budget_setting(self, key: str, value: Any) -> bool:
        """Update a budget setting value."""
        cursor = self.conn.execute(
            """UPDATE budget_settings
               SET value = ?, updated_at = CURRENT_TIMESTAMP
               WHERE key = ?""",
            (str(value), key)
        )
        if cursor.rowcount == 0:
            # Insert if doesn't exist
            self.conn.execute(
                "INSERT INTO budget_settings (key, value) VALUES (?, ?)",
                (key, str(value))
            )
        self.conn.commit()
        return True

    def get_fixed_costs_monthly(self) -> Dict[str, Any]:
        """Get all fixed costs (active recurring bills) calculated to monthly amounts."""
        cursor = self.conn.execute("""
            SELECT r.*, c.name as category_name
            FROM recurring_transactions r
            LEFT JOIN categories c ON r.category_id = c.id
            WHERE r.is_active = 1
        """)

        bills = []
        total_monthly = 0.0
        by_category = {}

        for row in cursor.fetchall():
            bill = dict(row)
            amount = abs(bill['amount'])
            freq = bill['frequency']

            # Convert to monthly equivalent
            if freq == 'daily':
                monthly = amount * 30
            elif freq == 'weekly':
                monthly = amount * 4.33
            elif freq == 'biweekly':
                monthly = amount * 2.17
            elif freq == 'monthly':
                monthly = amount
            elif freq == 'quarterly':
                monthly = amount / 3
            elif freq in ('yearly', 'annually'):
                monthly = amount / 12
            else:
                monthly = amount

            bill['monthly_equivalent'] = round(monthly, 2)
            bills.append(bill)
            total_monthly += monthly

            # Group by category
            cat_name = bill['category_name'] or 'Uncategorized'
            if cat_name not in by_category:
                by_category[cat_name] = {'bills': [], 'total': 0}
            by_category[cat_name]['bills'].append(bill)
            by_category[cat_name]['total'] += monthly

        return {
            'bills': bills,
            'total_monthly': round(total_monthly, 2),
            'by_category': {k: {'bills': v['bills'], 'total': round(v['total'], 2)}
                           for k, v in by_category.items()}
        }

    def get_category_budgets(self) -> List[Dict[str, Any]]:
        """Get all categories with their budget amounts."""
        cursor = self.conn.execute("""
            SELECT id, name, budget_amount, color
            FROM categories
            WHERE budget_amount > 0
            ORDER BY budget_amount DESC
        """)
        return [dict(row) for row in cursor.fetchall()]

    def get_budget_overview(self, month: str = None) -> Dict[str, Any]:
        """
        Get comprehensive budget overview showing:
        - Monthly income
        - Fixed costs (recurring bills)
        - Category budgets (variable/discretionary)
        - Discretionary available
        - Actual spending vs budget
        - Warnings/alerts
        """
        from datetime import datetime, date

        # Default to current month
        if not month:
            month = date.today().strftime('%Y-%m')

        year, month_num = map(int, month.split('-'))
        month_start = f"{month}-01"
        # Get last day of month
        if month_num == 12:
            month_end = f"{year + 1}-01-01"
        else:
            month_end = f"{year}-{month_num + 1:02d}-01"

        # Get settings
        settings = self.get_all_budget_settings()
        monthly_income = settings.get('monthly_income', 0)
        savings_target_pct = settings.get('savings_target_percent', 20)
        warning_pct = settings.get('discretionary_warning_percent', 80)

        # Get fixed costs
        fixed_costs = self.get_fixed_costs_monthly()
        total_fixed = fixed_costs['total_monthly']

        # Calculate savings target
        savings_target = monthly_income * (savings_target_pct / 100)

        # Get category budgets (excluding fixed cost categories)
        cursor = self.conn.execute("""
            SELECT c.id, c.name, c.budget_amount, c.color,
                   COALESCE(SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END), 0) as spent
            FROM categories c
            LEFT JOIN transactions t ON t.category_id = c.id
                AND t.date >= ? AND t.date < ?
            GROUP BY c.id
            ORDER BY c.budget_amount DESC
        """, (month_start, month_end))

        category_budgets = []
        total_budgeted = 0
        total_spent_budgeted = 0

        for row in cursor.fetchall():
            cat = dict(row)
            if cat['budget_amount'] and cat['budget_amount'] > 0:
                cat['remaining'] = cat['budget_amount'] - cat['spent']
                cat['percent_used'] = round((cat['spent'] / cat['budget_amount']) * 100, 1) if cat['budget_amount'] > 0 else 0
                cat['over_budget'] = cat['spent'] > cat['budget_amount']
                category_budgets.append(cat)
                total_budgeted += cat['budget_amount']
                total_spent_budgeted += cat['spent']

        # Calculate discretionary
        # Discretionary = Income - Fixed Costs - Savings Target
        discretionary_budget = monthly_income - total_fixed - savings_target

        # Get actual total spending this month (all expenses)
        cursor = self.conn.execute("""
            SELECT COALESCE(SUM(ABS(amount)), 0) as total_spent
            FROM transactions
            WHERE amount < 0 AND date >= ? AND date < ?
        """, (month_start, month_end))
        total_spent = cursor.fetchone()[0]

        # Spending on non-fixed items (discretionary spending)
        # This is total spending minus what was allocated to fixed bills
        # For simplicity, we'll consider all spending as potential discretionary
        discretionary_spent = total_spent
        discretionary_remaining = discretionary_budget - discretionary_spent

        # Generate warnings
        warnings = []
        if monthly_income == 0:
            warnings.append({
                'type': 'info',
                'message': 'Set your monthly income to see accurate budget calculations',
                'action': 'settings'
            })

        if discretionary_remaining < 0:
            warnings.append({
                'type': 'danger',
                'message': f'Overspent by ${abs(discretionary_remaining):.2f} - dipping into savings/assets',
                'severity': 'high'
            })
        elif discretionary_budget > 0:
            pct_used = (discretionary_spent / discretionary_budget) * 100
            if pct_used >= warning_pct:
                warnings.append({
                    'type': 'warning',
                    'message': f'Discretionary spending at {pct_used:.0f}% of budget',
                    'severity': 'medium'
                })

        # Check individual categories over budget
        for cat in category_budgets:
            if cat['over_budget']:
                warnings.append({
                    'type': 'warning',
                    'message': f"{cat['name']} is ${cat['spent'] - cat['budget_amount']:.2f} over budget",
                    'category_id': cat['id']
                })

        return {
            'month': month,
            'income': {
                'monthly': monthly_income,
                'is_set': monthly_income > 0
            },
            'fixed_costs': {
                'total': total_fixed,
                'bills': fixed_costs['bills'],
                'by_category': fixed_costs['by_category']
            },
            'savings': {
                'target_percent': savings_target_pct,
                'target_amount': round(savings_target, 2),
            },
            'discretionary': {
                'budget': round(discretionary_budget, 2),
                'spent': round(discretionary_spent, 2),
                'remaining': round(discretionary_remaining, 2),
                'percent_used': round((discretionary_spent / discretionary_budget) * 100, 1) if discretionary_budget > 0 else 0,
                'is_over': discretionary_remaining < 0
            },
            'category_budgets': {
                'categories': category_budgets,
                'total_budgeted': round(total_budgeted, 2),
                'total_spent': round(total_spent_budgeted, 2)
            },
            'summary': {
                'total_income': monthly_income,
                'total_fixed': round(total_fixed, 2),
                'total_savings_target': round(savings_target, 2),
                'total_discretionary': round(discretionary_budget, 2),
                'total_spent': round(total_spent, 2),
                'net_remaining': round(monthly_income - total_spent, 2)
            },
            'warnings': warnings
        }

    # ==================== ONBOARDING ====================

    def get_onboarding_status(self) -> Dict[str, Any]:
        """Check if onboarding is needed and what steps are complete.

        Returns status of each setup step and whether onboarding should be shown.
        """
        # Check each setup step
        has_accounts = self.conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0] > 0
        has_categories = self.conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0] > 0
        has_transactions = self.conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] > 0

        # Check if income is set
        income_set = False
        try:
            income = self.get_budget_setting('monthly_income')
            income_set = income is not None and float(income) > 0
        except:
            pass

        # Check for subscriptions/bills
        has_bills = False
        try:
            has_bills = self.conn.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0] > 0
        except:
            pass

        # Check if onboarding was completed (stored in budget_settings)
        onboarding_complete = False
        try:
            complete = self.get_budget_setting('onboarding_complete')
            onboarding_complete = complete == '1' or complete == 'true'
        except:
            pass

        # Determine if onboarding should be shown
        # Show if: not completed AND (no accounts OR no income set)
        needs_onboarding = not onboarding_complete and (not has_accounts or not income_set)

        return {
            'needs_onboarding': needs_onboarding,
            'onboarding_complete': onboarding_complete,
            'steps': {
                'income_set': income_set,
                'has_accounts': has_accounts,
                'has_categories': has_categories,
                'has_bills': has_bills,
                'has_transactions': has_transactions
            },
            'progress': {
                'completed': sum([income_set, has_accounts, has_categories, has_bills]),
                'total': 4  # income, accounts, categories, bills
            }
        }

    def complete_onboarding(self) -> bool:
        """Mark onboarding as complete."""
        self.update_budget_setting('onboarding_complete', '1')
        return True

    def reset_onboarding(self) -> bool:
        """Reset onboarding status (for re-running setup)."""
        self.update_budget_setting('onboarding_complete', '0')
        return True
