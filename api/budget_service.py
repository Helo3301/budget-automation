"""Budget service - main orchestration layer."""
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

from budget_automation.config import (
    DB_PATH,
    VECTOR_DB_PATH,
    ensure_data_dir
)
from budget_automation.db.sqlite_store import SQLiteStore
from budget_automation.db.vector_store import VectorStore
from budget_automation.intelligence.embedder import LocalEmbedder
from budget_automation.intelligence.categorizer import RAGCategorizer
from budget_automation.intelligence.recurring_detector import RecurringDetector
from budget_automation.intelligence.anomaly_detector import AnomalyDetector
from budget_automation.ingestion.csv_parser import CSVParser


logger = logging.getLogger(__name__)


class BudgetService:
    """Main service for budget automation.

    Orchestrates importing, categorizing, and analyzing transactions.
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        vector_path: Optional[Path] = None
    ):
        """Initialize the budget service.

        Args:
            db_path: Path to SQLite database (default: ~/.budget_automation/budget.db)
            vector_path: Path to vector store (default: ~/.budget_automation/vectors)
        """
        ensure_data_dir()

        self.db_path = db_path or DB_PATH
        self.vector_path = vector_path or VECTOR_DB_PATH

        self.store = SQLiteStore(self.db_path)
        self.vector_store = VectorStore(self.vector_path)
        self.embedder = LocalEmbedder()
        self.categorizer = RAGCategorizer(
            self.store,
            self.vector_store,
            self.embedder
        )
        self.recurring_detector = RecurringDetector(self.store)
        self.anomaly_detector = AnomalyDetector(self.store)
        self.parser = CSVParser()

    def close(self):
        """Close all connections."""
        self.store.close()
        self.vector_store.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def import_file(
        self,
        file_path: Path,
        auto_categorize: bool = True,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """Import transactions from CSV/Excel file.

        Args:
            file_path: Path to the file
            auto_categorize: Whether to auto-categorize transactions
            progress_callback: Optional callback(current, total) for progress

        Returns:
            Dict with import statistics
        """
        logger.info(f"Importing file: {file_path}")

        # Parse the file
        transactions = self.parser.parse(file_path)
        total = len(transactions)

        if progress_callback:
            progress_callback(0, total)

        # Add transactions to database
        added_ids = self.store.add_transactions(transactions)
        added_count = len(added_ids)
        duplicate_count = total - added_count

        logger.info(f"Added {added_count} transactions, {duplicate_count} duplicates skipped")

        # Categorize new transactions
        categorized_count = 0
        if auto_categorize and added_ids:
            for i, txn_id in enumerate(added_ids):
                txn = self.store.get_transaction(txn_id)
                if txn and not txn.get("category_id"):
                    try:
                        self.categorizer.categorize_and_update({
                            "id": txn_id,
                            "merchant": txn["merchant"],
                            "description": txn.get("description"),
                            "amount": txn["amount"]
                        })
                        categorized_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to categorize transaction {txn_id}: {e}")

                if progress_callback:
                    progress_callback(i + 1, len(added_ids))

        return {
            "total_parsed": total,
            "added": added_count,
            "duplicates": duplicate_count,
            "categorized": categorized_count
        }

    def analyze_transactions(self) -> Dict[str, Any]:
        """Run analysis on all transactions.

        Detects recurring patterns and anomalies.

        Returns:
            Dict with analysis results
        """
        logger.info("Running transaction analysis")

        # Detect recurring
        recurring = self.recurring_detector.detect_and_mark()
        logger.info(f"Found {len(recurring)} recurring patterns")

        # Detect anomalies
        anomalies = self.anomaly_detector.detect_and_mark()
        logger.info(f"Found {len(anomalies)} anomalies")

        return {
            "recurring_patterns": recurring,
            "anomalies": anomalies
        }

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all transactions.

        Returns:
            Dict with transaction summary
        """
        all_txns = self.store.get_all_transactions()
        categories = self.store.get_all_categories()

        # Calculate totals
        total_income = sum(t["amount"] for t in all_txns if t["amount"] > 0)
        total_expenses = sum(abs(t["amount"]) for t in all_txns if t["amount"] < 0)

        # Category breakdown
        category_totals = {}
        for cat in categories:
            cat_txns = self.store.get_transactions_by_category(cat["id"])
            if cat_txns:
                category_totals[cat["name"]] = {
                    "count": len(cat_txns),
                    "total": sum(abs(t["amount"]) for t in cat_txns),
                    "average": sum(abs(t["amount"]) for t in cat_txns) / len(cat_txns)
                }

        # Recurring summary
        recurring_txns = [t for t in all_txns if t.get("is_recurring")]
        monthly_recurring = sum(abs(t["amount"]) for t in recurring_txns)

        # Anomalies
        anomaly_txns = [t for t in all_txns if t.get("is_anomaly")]

        return {
            "total_transactions": len(all_txns),
            "total_income": total_income,
            "total_expenses": total_expenses,
            "net": total_income - total_expenses,
            "category_breakdown": category_totals,
            "recurring_transactions": len(recurring_txns),
            "estimated_monthly_recurring": monthly_recurring,
            "anomalies": len(anomaly_txns),
            "uncategorized": len([t for t in all_txns if not t.get("category_id")])
        }

    def search_similar(
        self,
        query: str,
        k: int = 5
    ) -> List[Dict[str, Any]]:
        """Search for similar transactions using semantic search.

        Args:
            query: Search query (e.g., "coffee shops", "subscriptions")
            k: Number of results to return

        Returns:
            List of similar transactions
        """
        embedding = self.embedder.embed(query)
        similar = self.vector_store.search(embedding, k=k)

        # Enrich with full transaction data
        results = []
        for s in similar:
            txn = self.store.get_transaction(s["transaction_id"])
            if txn:
                results.append({
                    **txn,
                    "similarity_score": 1 - s.get("_distance", 0)
                })

        return results

    def get_uncategorized(self) -> List[Dict[str, Any]]:
        """Get all uncategorized transactions."""
        return self.store.get_uncategorized_transactions()

    def categorize_transaction(
        self,
        txn_id: int,
        category_name: str
    ) -> bool:
        """Manually categorize a transaction.

        Args:
            txn_id: Transaction ID
            category_name: Category name

        Returns:
            True if successful
        """
        categories = self.store.get_all_categories()
        cat = next((c for c in categories if c["name"].lower() == category_name.lower()), None)

        if not cat:
            return False

        self.store.update_transaction_category(txn_id, cat["id"])

        # Also update vector store
        txn = self.store.get_transaction(txn_id)
        if txn:
            embedding = self.embedder.embed_transaction(
                txn["merchant"],
                txn.get("description"),
                txn["amount"]
            )
            self.vector_store.add_embedding(
                txn_id,
                embedding,
                txn["merchant"],
                category_name,
                txn["amount"]
            )

        return True

    def get_categories(self) -> List[Dict[str, Any]]:
        """Get all available categories."""
        return self.store.get_all_categories()

    def get_transaction(self, txn_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific transaction."""
        return self.store.get_transaction(txn_id)

    def get_categorization_explanation(self, txn_id: int) -> Optional[str]:
        """Get the explanation for how a transaction was categorized."""
        log = self.store.get_categorization_log(txn_id)
        if log:
            return log.get("explanation")
        return None
