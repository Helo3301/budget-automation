"""Recurring transaction detector using SQL and date pattern analysis."""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import statistics

from budget_automation.db.sqlite_store import SQLiteStore
from budget_automation.config import (
    RECURRING_MIN_OCCURRENCES,
    RECURRING_INTERVAL_TOLERANCE_DAYS,
    RECURRING_INTERVALS
)


class RecurringDetector:
    """Detect recurring transactions (subscriptions, bills) using date patterns."""

    def __init__(self, store: SQLiteStore):
        """Initialize with SQLite store.

        Args:
            store: SQLiteStore instance
        """
        self.store = store
        self._next_group_id = 1

    def detect(self) -> List[Dict[str, Any]]:
        """Detect recurring transaction patterns.

        Returns:
            List of recurring groups with merchant, amount, interval, transaction_ids
        """
        # Get groups by merchant and amount
        groups = self.store.get_merchant_amount_groups()

        recurring = []
        for group in groups:
            if group["count"] < RECURRING_MIN_OCCURRENCES:
                continue

            # Analyze date patterns
            dates = [datetime.strptime(d, "%Y-%m-%d") for d in group["dates"]]
            dates.sort()

            interval_info = self._analyze_intervals(dates)
            if interval_info:
                recurring.append({
                    "merchant": group["merchant"],
                    "amount": group["amount"],
                    "interval_days": interval_info["interval"],
                    "interval_type": interval_info["type"],
                    "transaction_ids": group["txn_ids"],
                    "count": group["count"]
                })

        return recurring

    def detect_and_mark(self) -> List[Dict[str, Any]]:
        """Detect recurring patterns and mark transactions in database.

        Returns:
            List of recurring groups
        """
        recurring = self.detect()

        for group in recurring:
            group_id = self._next_group_id
            self._next_group_id += 1

            for txn_id in group["transaction_ids"]:
                self.store.mark_transaction_recurring(txn_id, group_id)

        return recurring

    def _analyze_intervals(self, dates: List[datetime]) -> Optional[Dict[str, Any]]:
        """Analyze date intervals to determine if pattern is recurring.

        Args:
            dates: Sorted list of transaction dates

        Returns:
            Dict with interval and type, or None if not recurring
        """
        if len(dates) < 2:
            return None

        # Calculate intervals between consecutive dates
        intervals = []
        for i in range(1, len(dates)):
            delta = (dates[i] - dates[i-1]).days
            intervals.append(delta)

        if not intervals:
            return None

        # Calculate mean and std deviation
        mean_interval = statistics.mean(intervals)
        std_interval = statistics.stdev(intervals) if len(intervals) > 1 else 0

        # Check if intervals match known patterns within tolerance
        for known_interval in RECURRING_INTERVALS:
            tolerance = RECURRING_INTERVAL_TOLERANCE_DAYS
            # More tolerance for longer intervals
            if known_interval >= 365:
                tolerance = 7
            elif known_interval >= 28:
                tolerance = 3

            if abs(mean_interval - known_interval) <= tolerance:
                # Check consistency (low std deviation relative to interval)
                if std_interval <= tolerance:
                    return {
                        "interval": round(mean_interval),
                        "type": self._interval_to_type(known_interval)
                    }

        return None

    def _interval_to_type(self, interval: int) -> str:
        """Convert interval days to human-readable type."""
        if interval == 7:
            return "weekly"
        elif interval == 14:
            return "biweekly"
        elif interval in [28, 29, 30, 31]:
            return "monthly"
        elif interval == 90:
            return "quarterly"
        elif interval in [365, 366]:
            return "yearly"
        else:
            return f"every {interval} days"
