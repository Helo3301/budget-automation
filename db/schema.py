"""SQLite schema definitions for the budget automation system."""

SCHEMA_SQL = """
-- Accounts table (banks, credit cards, etc.)
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    institution TEXT,  -- Bank name: Chase, Ally, etc.
    account_type TEXT NOT NULL DEFAULT 'checking',  -- checking, savings, credit, investment
    last_four TEXT,  -- Last 4 digits for identification
    color TEXT DEFAULT '#3B82F6',  -- Color for charts (hex)
    initial_balance REAL DEFAULT 0,  -- Starting balance when account was added
    balance_as_of_date TEXT,  -- Date when initial_balance was recorded (ISO format)
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    amount REAL NOT NULL,
    merchant TEXT NOT NULL,
    description TEXT,
    category_id INTEGER,
    account_id INTEGER,
    is_recurring INTEGER DEFAULT 0,
    recurring_group_id INTEGER,
    is_anomaly INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(id),
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);

-- Categories table
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    keywords TEXT,
    budget_amount REAL DEFAULT 0,  -- Monthly budget for this category
    color TEXT DEFAULT '#6B7280',  -- Color for charts (hex)
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Categorization log - stores Claude's explanations
CREATE TABLE IF NOT EXISTS categorization_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    confidence REAL,
    explanation TEXT,
    similar_transaction_ids TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (transaction_id) REFERENCES transactions(id),
    FOREIGN KEY (category_id) REFERENCES categories(id)
);

-- Categorization rules - patterns for auto-categorizing
CREATE TABLE IF NOT EXISTS categorization_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    rule_type TEXT NOT NULL,  -- 'merchant_contains', 'merchant_exact', 'amount_range', 'description_contains'
    pattern TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    priority INTEGER DEFAULT 0,  -- Higher priority rules apply first
    is_active INTEGER DEFAULT 1,
    notes TEXT,  -- User's explanation (e.g., "SCA membership dues")
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(id)
);

-- Savings goals table
CREATE TABLE IF NOT EXISTS savings_goals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    target_amount REAL NOT NULL,
    current_amount REAL DEFAULT 0,
    target_date TEXT,  -- ISO date when goal should be achieved
    color TEXT DEFAULT '#10B981',  -- Emerald green by default
    icon TEXT DEFAULT 'piggy-bank',  -- Icon name for UI
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT  -- When goal was achieved
);

-- Goal contributions - track deposits/withdrawals toward goals
CREATE TABLE IF NOT EXISTS goal_contributions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    goal_id INTEGER NOT NULL,
    amount REAL NOT NULL,  -- Positive for deposits, negative for withdrawals
    note TEXT,
    transaction_id INTEGER,  -- Optional link to actual transaction
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (goal_id) REFERENCES savings_goals(id) ON DELETE CASCADE,
    FOREIGN KEY (transaction_id) REFERENCES transactions(id)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_transactions_merchant ON transactions(merchant);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category_id);
CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_amount ON transactions(amount);
CREATE INDEX IF NOT EXISTS idx_categorization_log_txn ON categorization_log(transaction_id);
CREATE INDEX IF NOT EXISTS idx_rules_type ON categorization_rules(rule_type);
CREATE INDEX IF NOT EXISTS idx_rules_active ON categorization_rules(is_active);
CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(is_active);
CREATE INDEX IF NOT EXISTS idx_savings_goals_active ON savings_goals(is_active);
CREATE INDEX IF NOT EXISTS idx_goal_contributions_goal ON goal_contributions(goal_id);
"""

# Default category insert
DEFAULT_CATEGORIES_SQL = """
INSERT OR IGNORE INTO categories (name) VALUES
    ('Housing'),
    ('Transportation'),
    ('Food & Dining'),
    ('Utilities'),
    ('Healthcare'),
    ('Entertainment'),
    ('Shopping'),
    ('Personal Care'),
    ('Education'),
    ('Travel'),
    ('Subscriptions'),
    ('Income'),
    ('Transfer'),
    ('Other');
"""
