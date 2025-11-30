"""Configuration settings for the budget automation system."""
from pathlib import Path
from typing import Dict, Any

# Paths
DATA_DIR = Path.home() / ".budget_automation"
DB_PATH = DATA_DIR / "budget.db"
VECTOR_DB_PATH = DATA_DIR / "vectors"

# Embedding model
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# RAG settings
SIMILARITY_THRESHOLD = 0.85  # Skip Claude if confidence > this
TOP_K_SIMILAR = 5  # Number of similar transactions to retrieve
MIN_MATCHES_FOR_AUTO = 3  # Minimum matches above threshold for auto-categorize

# Recurring detection
RECURRING_MIN_OCCURRENCES = 2
RECURRING_INTERVAL_TOLERANCE_DAYS = 3
RECURRING_INTERVALS = [7, 14, 28, 30, 31, 90, 365]  # Weekly, biweekly, monthly, quarterly, yearly

# Anomaly detection
ANOMALY_IQR_MULTIPLIER = 1.5
ANOMALY_MEDIAN_MULTIPLIER = 3  # For new merchant detection

# Claude API
CLAUDE_MODEL = "claude-3-haiku-20240307"  # Cost-efficient for explanations

# Default categories
DEFAULT_CATEGORIES = [
    "Housing",
    "Transportation",
    "Food & Dining",
    "Utilities",
    "Healthcare",
    "Entertainment",
    "Shopping",
    "Personal Care",
    "Education",
    "Travel",
    "Subscriptions",
    "Income",
    "Transfer",
    "Other"
]

def ensure_data_dir() -> Path:
    """Ensure the data directory exists."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR
