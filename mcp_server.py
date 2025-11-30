#!/usr/bin/env python3
"""MCP Server for Budget Automation - exposes budget data to Claude Code."""
import sys
from pathlib import Path
from typing import Optional

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastmcp import FastMCP

# Initialize MCP server
mcp = FastMCP(
    name="budget-automation",
    instructions="""You have access to a local budget tracking system with 2,368 transactions.

Use these tools to help the user understand their finances:
- get_summary: Overview of income, expenses, and net
- get_transactions: List transactions with optional filters
- get_uncategorized: Transactions needing manual categorization
- categorize_transaction: Assign a category to a transaction
- get_categories: List available categories
- get_recurring: Detected subscriptions and recurring payments
- get_anomalies: Unusual transactions flagged for review
- search_transactions: Semantic search for similar transactions

When discussing finances, be helpful and provide insights about spending patterns."""
)

# Lazy-load the budget service to avoid import issues at startup
_service = None

def get_service():
    """Get or create the budget service instance."""
    global _service
    if _service is None:
        from budget_automation.api.budget_service import BudgetService
        _service = BudgetService()
        _service.__enter__()
    return _service


@mcp.tool()
def get_summary() -> dict:
    """Get budget summary with income, expenses, net, and category breakdown.

    Returns overview of total transactions, income, expenses, net balance,
    recurring transaction count, anomaly count, and uncategorized count.
    """
    service = get_service()
    return service.get_summary()


@mcp.tool()
def get_categories() -> list:
    """Get list of all available budget categories.

    Returns list of categories like Housing, Food & Dining, Entertainment, etc.
    """
    service = get_service()
    return service.get_categories()


@mcp.tool()
def get_transactions(
    limit: int = 50,
    offset: int = 0,
    category: Optional[str] = None,
    sort_by: str = "date",
    sort_dir: str = "desc"
) -> dict:
    """Get paginated list of transactions with optional filtering.

    Args:
        limit: Max transactions to return (default 50)
        offset: Number of transactions to skip for pagination
        category: Filter by category name (e.g., "Food & Dining")
        sort_by: Sort field - "date", "amount", or "merchant"
        sort_dir: Sort direction - "asc" or "desc"

    Returns dict with 'transactions' list and 'total' count.
    """
    service = get_service()
    all_txns = service.store.get_all_transactions()

    # Filter by category if specified
    if category:
        categories = service.get_categories()
        cat_id = next((c["id"] for c in categories if c["name"] == category), None)
        if cat_id:
            all_txns = [t for t in all_txns if t.get("category_id") == cat_id]
        else:
            all_txns = []

    # Sort
    reverse = sort_dir == "desc"
    if sort_by == "date":
        all_txns.sort(key=lambda x: x.get("date", ""), reverse=reverse)
    elif sort_by == "amount":
        all_txns.sort(key=lambda x: x.get("amount", 0), reverse=reverse)
    elif sort_by == "merchant":
        all_txns.sort(key=lambda x: x.get("merchant", "").lower(), reverse=reverse)

    total = len(all_txns)
    transactions = all_txns[offset:offset + limit]

    return {"transactions": transactions, "total": total}


@mcp.tool()
def get_transaction(txn_id: int) -> dict:
    """Get details of a single transaction by ID.

    Args:
        txn_id: The transaction ID

    Returns transaction details or error if not found.
    """
    service = get_service()
    txn = service.get_transaction(txn_id)
    if txn is None:
        return {"error": f"Transaction {txn_id} not found"}
    return txn


@mcp.tool()
def get_uncategorized(limit: int = 50) -> list:
    """Get transactions that need manual categorization.

    Args:
        limit: Max transactions to return

    Returns list of uncategorized transactions.
    """
    service = get_service()
    return service.get_uncategorized()[:limit]


@mcp.tool()
def categorize_transaction(txn_id: int, category: str) -> dict:
    """Manually assign a category to a transaction.

    Args:
        txn_id: The transaction ID to categorize
        category: Category name (e.g., "Food & Dining", "Entertainment")

    Returns success status or error message.
    """
    service = get_service()

    # Validate category
    categories = service.get_categories()
    if not any(c["name"] == category for c in categories):
        valid = [c["name"] for c in categories]
        return {"success": False, "error": f"Invalid category. Valid options: {valid}"}

    success = service.categorize_transaction(txn_id, category)
    if success:
        return {"success": True, "message": f"Transaction {txn_id} categorized as '{category}'"}
    return {"success": False, "error": "Transaction not found"}


@mcp.tool()
def get_recurring() -> list:
    """Get detected recurring transactions and subscriptions.

    Returns list of recurring patterns with merchant, amount, and frequency.
    """
    service = get_service()
    all_txns = service.store.get_all_transactions()
    recurring = [t for t in all_txns if t.get("is_recurring")]

    # Group by merchant
    patterns = {}
    for t in recurring:
        merchant = t.get("merchant", "Unknown")
        if merchant not in patterns:
            patterns[merchant] = {
                "merchant": merchant,
                "amount": t.get("amount", 0),
                "count": 0,
                "transactions": []
            }
        patterns[merchant]["count"] += 1
        patterns[merchant]["transactions"].append({
            "id": t.get("id"),
            "date": t.get("date"),
            "amount": t.get("amount")
        })

    return list(patterns.values())


@mcp.tool()
def get_anomalies() -> list:
    """Get transactions flagged as anomalies (unusual amounts).

    Returns list of anomalous transactions with reason for flagging.
    """
    service = get_service()
    all_txns = service.store.get_all_transactions()
    return [t for t in all_txns if t.get("is_anomaly")]


@mcp.tool()
def search_transactions(query: str, limit: int = 10) -> list:
    """Semantic search for similar transactions.

    Args:
        query: Natural language search (e.g., "coffee shops", "monthly subscriptions")
        limit: Max results to return

    Returns list of matching transactions with similarity scores.
    """
    service = get_service()
    if not query or not query.strip():
        return []
    return service.search_similar(query.strip(), k=limit)


@mcp.tool()
def get_spending_by_category() -> dict:
    """Get total spending broken down by category.

    Returns dict mapping category names to total amounts and transaction counts.
    """
    service = get_service()
    summary = service.get_summary()
    return summary.get("category_breakdown", {})


@mcp.tool()
def get_top_merchants(limit: int = 20) -> list:
    """Get merchants with highest total spending.

    Args:
        limit: Number of top merchants to return

    Returns list of merchants with total amount and transaction count.
    """
    service = get_service()
    all_txns = service.store.get_all_transactions()

    # Aggregate by merchant
    merchants = {}
    for t in all_txns:
        merchant = t.get("merchant", "Unknown")
        if merchant not in merchants:
            merchants[merchant] = {"merchant": merchant, "total": 0, "count": 0}
        merchants[merchant]["total"] += t.get("amount", 0)
        merchants[merchant]["count"] += 1

    # Sort by absolute total (expenses are negative)
    sorted_merchants = sorted(merchants.values(), key=lambda x: abs(x["total"]), reverse=True)
    return sorted_merchants[:limit]


if __name__ == "__main__":
    mcp.run()
