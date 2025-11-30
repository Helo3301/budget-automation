"""Anomaly detector using statistical analysis (IQR method)."""
from typing import List, Dict, Any
import statistics

from budget_automation.db.sqlite_store import SQLiteStore
from budget_automation.config import ANOMALY_IQR_MULTIPLIER, ANOMALY_MEDIAN_MULTIPLIER


class AnomalyDetector:
    """Detect unusual transactions using statistical methods."""

    def __init__(self, store: SQLiteStore):
        """Initialize with SQLite store.

        Args:
            store: SQLiteStore instance
        """
        self.store = store

    def detect(self) -> List[Dict[str, Any]]:
        """Detect anomalous transactions.

        Uses two methods:
        1. Per-category IQR: amount > Q3 + 1.5*IQR or < Q1 - 1.5*IQR
        2. New merchant: first-time merchant with amount > median * 3

        Returns:
            List of anomaly dicts with transaction_id, reason, amount
        """
        anomalies = []

        # Get all transactions
        all_txns = self.store.get_all_transactions()
        if not all_txns:
            return []

        # Calculate overall median for new merchant detection
        all_amounts = [abs(t["amount"]) for t in all_txns]
        overall_median = statistics.median(all_amounts) if all_amounts else 0

        # Get all known merchants
        known_merchants = set(self.store.get_all_merchants())

        # Check each transaction
        seen_merchants_in_session = set()
        for txn in all_txns:
            reasons = []

            # Check 1: Category-based amount anomaly
            if txn["category_id"]:
                stats = self.store.get_category_stats(txn["category_id"])
                if stats and (stats["q1"] > 0 or stats["q3"] > 0):
                    iqr = stats["q3"] - stats["q1"]
                    amount = abs(txn["amount"])

                    # If IQR is 0 (all same/similar amounts), use Q3 * multiplier
                    if iqr == 0:
                        # When all amounts are similar, flag if current amount is
                        # significantly higher than the typical amount (Q3)
                        upper_bound = stats["q3"] * (1 + ANOMALY_IQR_MULTIPLIER)
                        lower_bound = stats["q1"] * (1 - ANOMALY_IQR_MULTIPLIER)
                    else:
                        lower_bound = stats["q1"] - ANOMALY_IQR_MULTIPLIER * iqr
                        upper_bound = stats["q3"] + ANOMALY_IQR_MULTIPLIER * iqr

                    if amount > upper_bound:
                        reasons.append(f"Amount ${amount:.2f} exceeds category normal range (max ${upper_bound:.2f})")
                    elif amount < lower_bound and lower_bound > 0:
                        reasons.append(f"Amount ${amount:.2f} below category normal range (min ${lower_bound:.2f})")

            # Check 2: New merchant with high amount
            merchant = txn["merchant"]
            amount = abs(txn["amount"])
            threshold = overall_median * ANOMALY_MEDIAN_MULTIPLIER

            # If this is the first time we see this merchant and amount is high
            if merchant not in seen_merchants_in_session:
                seen_merchants_in_session.add(merchant)
                # Check if merchant has only this one transaction (truly new)
                merchant_txns = self.store.get_transactions_by_merchant(merchant)
                if len(merchant_txns) == 1 and amount > threshold and threshold > 0:
                    reasons.append(f"New merchant with high amount ${amount:.2f} (threshold ${threshold:.2f})")

            if reasons:
                anomalies.append({
                    "transaction_id": txn["id"],
                    "merchant": merchant,
                    "amount": txn["amount"],
                    "reason": "; ".join(reasons)
                })

        return anomalies

    def detect_and_mark(self) -> List[Dict[str, Any]]:
        """Detect anomalies and mark them in the database.

        Returns:
            List of anomaly dicts
        """
        anomalies = self.detect()

        for anomaly in anomalies:
            self.store.mark_transaction_anomaly(anomaly["transaction_id"])

        return anomalies
