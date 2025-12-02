"""FastAPI backend for Budget Automation web UI."""
import tempfile
import subprocess
import json
import uuid
import shutil
import re
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

import pandas as pd

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from budget_automation.api.budget_service import BudgetService


# Global service instance (for production use)
_service: Optional[BudgetService] = None


def get_service() -> BudgetService:
    """Dependency to get the budget service."""
    global _service
    if _service is None:
        _service = BudgetService()
        _service.__enter__()
    return _service


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage service lifecycle."""
    yield
    # Cleanup on shutdown
    global _service
    if _service is not None:
        _service.__exit__(None, None, None)
        _service = None


app = FastAPI(
    title="Budget Automation API",
    description="Local, privacy-first budget tracking with RAG-based categorization",
    version="1.0.0",
    lifespan=lifespan
)


# === Pydantic Models ===

class CategorizeRequest(BaseModel):
    category: str


class BulkCategorizeRequest(BaseModel):
    ids: List[int]
    category: str


class TransactionResponse(BaseModel):
    id: int
    date: str
    amount: float
    merchant: str
    description: Optional[str] = None
    category_id: Optional[int] = None
    category_name: Optional[str] = None
    is_recurring: bool = False
    is_anomaly: bool = False


class TransactionListResponse(BaseModel):
    transactions: List[dict]
    total: int


class ImportResponse(BaseModel):
    total_parsed: int
    added: int
    duplicates: int
    categorized: int


class RuleCreate(BaseModel):
    name: str
    rule_type: str  # 'merchant_contains', 'merchant_exact', 'description_contains', 'amount_range'
    pattern: str
    category: str
    priority: int = 0
    notes: Optional[str] = None


class RuleUpdate(BaseModel):
    name: Optional[str] = None
    rule_type: Optional[str] = None
    pattern: Optional[str] = None
    category: Optional[str] = None
    priority: Optional[int] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None


class ChatMessage(BaseModel):
    message: str
    context_txn_id: Optional[int] = None  # Transaction being discussed
    session_id: Optional[str] = None  # For conversation continuity
    onboarding_mode: bool = False  # Whether this is an onboarding conversation


class CategoryCreate(BaseModel):
    name: str
    keywords: Optional[str] = None  # Comma-separated keywords for auto-matching
    budget_amount: float = 0  # Monthly budget limit for this category


class CategoryUpdate(BaseModel):
    name: Optional[str] = None
    keywords: Optional[str] = None
    budget_amount: Optional[float] = None


class AccountCreate(BaseModel):
    name: str
    institution: Optional[str] = None  # Bank name: Chase, Ally, etc.
    account_type: str = "checking"  # checking, savings, credit, investment
    last_four: Optional[str] = None  # Last 4 digits
    color: str = "#3B82F6"  # Chart color
    initial_balance: float = 0  # Starting balance (or balance owed for credit cards)
    balance_as_of_date: Optional[str] = None  # Date when initial_balance was recorded (ISO format)


class AccountUpdate(BaseModel):
    name: Optional[str] = None
    institution: Optional[str] = None
    account_type: Optional[str] = None
    last_four: Optional[str] = None
    color: Optional[str] = None
    initial_balance: Optional[float] = None
    balance_as_of_date: Optional[str] = None


class RecurringCreate(BaseModel):
    name: str  # Display name (e.g., "Netflix", "Car Insurance")
    merchant: Optional[str] = None  # Merchant name for matching
    amount: float  # Expected amount (negative for expenses)
    frequency: str = "monthly"  # daily, weekly, biweekly, monthly, quarterly, yearly
    start_date: str  # First payment date (YYYY-MM-DD)
    category_id: Optional[int] = None
    account_id: Optional[int] = None
    notes: Optional[str] = None


class RecurringUpdate(BaseModel):
    name: Optional[str] = None
    merchant: Optional[str] = None
    amount: Optional[float] = None
    frequency: Optional[str] = None
    start_date: Optional[str] = None
    next_due_date: Optional[str] = None
    category_id: Optional[int] = None
    account_id: Optional[int] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None


class SplitItem(BaseModel):
    category_id: int
    amount: float
    description: Optional[str] = None


class TransactionSplitRequest(BaseModel):
    splits: List[SplitItem]


class ColumnMappingRequest(BaseModel):
    date_column: str
    amount_column: Optional[str] = None  # For single amount column
    merchant_column: str
    description_column: Optional[str] = None
    # For split debit/credit columns (like Capital One, PNC, etc.)
    debit_column: Optional[str] = None
    credit_column: Optional[str] = None


class ImportConfirmRequest(BaseModel):
    session_id: str
    column_mapping: ColumnMappingRequest
    account_id: Optional[int] = None
    auto_categorize: bool = False


# Store for import preview sessions
_import_sessions: Dict[str, Dict[str, Any]] = {}


# === API Endpoints ===

@app.get("/api/summary")
def get_summary(service: BudgetService = Depends(get_service)):
    """Get budget summary with category breakdown."""
    return service.get_summary()


@app.get("/api/categories")
def get_categories(
    year: Optional[int] = None,
    month: Optional[int] = None,
    service: BudgetService = Depends(get_service)
):
    """Get list of all categories with transaction counts and spending."""
    from datetime import datetime

    # Default to current month if not specified
    if year is None or month is None:
        now = datetime.now()
        year = now.year
        month = now.month

    categories = service.get_categories()
    # Add transaction count and spending to each category
    for cat in categories:
        cat["transaction_count"] = service.store.get_category_transaction_count(cat["id"])
        cat["spent"] = service.store.get_category_spending(cat["id"], year, month)
        # Calculate percentage of budget used
        budget = cat.get("budget_amount", 0) or 0
        if budget > 0:
            cat["budget_percent"] = min(100, (cat["spent"] / budget) * 100)
        else:
            cat["budget_percent"] = 0
    return categories


@app.post("/api/categories")
def create_category(category: CategoryCreate, service: BudgetService = Depends(get_service)):
    """Create a new category."""
    # Check if category name already exists
    existing = service.store.get_category_by_name(category.name)
    if existing:
        raise HTTPException(status_code=400, detail=f"Category '{category.name}' already exists")

    category_id = service.store.add_category(
        category.name,
        category.keywords,
        category.budget_amount
    )
    return {"id": category_id, "name": category.name, "success": True}


@app.get("/api/categories/{category_id}")
def get_category(category_id: int, service: BudgetService = Depends(get_service)):
    """Get a single category by ID."""
    cat = service.store.get_category(category_id)
    if not cat:
        raise HTTPException(status_code=404, detail="Category not found")
    cat["transaction_count"] = service.store.get_category_transaction_count(category_id)
    return cat


@app.put("/api/categories/{category_id}")
def update_category(
    category_id: int,
    updates: CategoryUpdate,
    service: BudgetService = Depends(get_service)
):
    """Update a category."""
    existing = service.store.get_category(category_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Category not found")

    # Check if new name conflicts with existing category
    if updates.name and updates.name != existing["name"]:
        conflict = service.store.get_category_by_name(updates.name)
        if conflict:
            raise HTTPException(status_code=400, detail=f"Category '{updates.name}' already exists")

    update_dict = {}
    if updates.name is not None:
        update_dict["name"] = updates.name
    if updates.keywords is not None:
        update_dict["keywords"] = updates.keywords
    if updates.budget_amount is not None:
        update_dict["budget_amount"] = updates.budget_amount

    if update_dict:
        service.store.update_category(category_id, **update_dict)

    return {"success": True}


@app.delete("/api/categories/{category_id}")
def delete_category(category_id: int, service: BudgetService = Depends(get_service)):
    """Delete a category. Fails if category has transactions assigned."""
    existing = service.store.get_category(category_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Category not found")

    txn_count = service.store.get_category_transaction_count(category_id)
    if txn_count > 0:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot delete category with {txn_count} transactions. Reassign transactions first."
        )

    service.store.delete_category(category_id)
    return {"success": True}


# === Account Endpoints ===

@app.get("/api/accounts")
def get_accounts(
    include_inactive: bool = Query(False),
    service: BudgetService = Depends(get_service)
):
    """Get list of all accounts with balances."""
    from datetime import datetime
    now = datetime.now()

    accounts = service.store.get_all_accounts(active_only=not include_inactive)

    # Add computed fields to each account
    for acct in accounts:
        acct["balance"] = service.store.get_account_balance(acct["id"])
        acct["transaction_count"] = service.store.get_account_transaction_count(acct["id"])
        acct["spending_this_month"] = service.store.get_account_spending(acct["id"], now.year, now.month)
        acct["income_this_month"] = service.store.get_account_income(acct["id"], now.year, now.month)

        # For credit cards, add available credit
        if acct.get("account_type") == "credit":
            acct["credit_limit"] = acct.get("initial_balance", 0) or 0
            acct["available_credit"] = service.store.get_credit_card_available(acct["id"])
            acct["balance_label"] = "Owed"
        else:
            acct["balance_label"] = "Balance"

    return accounts


@app.post("/api/accounts")
def create_account(account: AccountCreate, service: BudgetService = Depends(get_service)):
    """Create a new account."""
    # Check if account name already exists
    existing = service.store.get_account_by_name(account.name)
    if existing:
        raise HTTPException(status_code=400, detail=f"Account '{account.name}' already exists")

    # Validate account type
    valid_types = ["checking", "savings", "credit", "investment"]
    if account.account_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Invalid account_type. Must be one of: {valid_types}")

    account_id = service.store.add_account(
        name=account.name,
        institution=account.institution,
        account_type=account.account_type,
        last_four=account.last_four,
        color=account.color,
        initial_balance=account.initial_balance,
        balance_as_of_date=account.balance_as_of_date
    )
    return {"id": account_id, "name": account.name, "success": True}


@app.get("/api/accounts/{account_id}")
def get_account(account_id: int, service: BudgetService = Depends(get_service)):
    """Get a single account by ID with full details."""
    from datetime import datetime
    now = datetime.now()

    acct = service.store.get_account(account_id)
    if not acct:
        raise HTTPException(status_code=404, detail="Account not found")

    acct["balance"] = service.store.get_account_balance(account_id)
    acct["transaction_count"] = service.store.get_account_transaction_count(account_id)
    acct["spending_this_month"] = service.store.get_account_spending(account_id, now.year, now.month)
    acct["income_this_month"] = service.store.get_account_income(account_id, now.year, now.month)

    if acct.get("account_type") == "credit":
        acct["credit_limit"] = acct.get("initial_balance", 0) or 0
        acct["available_credit"] = service.store.get_credit_card_available(account_id)

    return acct


@app.put("/api/accounts/{account_id}")
def update_account(
    account_id: int,
    updates: AccountUpdate,
    service: BudgetService = Depends(get_service)
):
    """Update an account."""
    existing = service.store.get_account(account_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Account not found")

    # Check if new name conflicts
    if updates.name and updates.name != existing["name"]:
        conflict = service.store.get_account_by_name(updates.name)
        if conflict:
            raise HTTPException(status_code=400, detail=f"Account '{updates.name}' already exists")

    # Validate account type if updating
    if updates.account_type:
        valid_types = ["checking", "savings", "credit", "investment"]
        if updates.account_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Invalid account_type. Must be one of: {valid_types}")

    update_dict = {}
    if updates.name is not None:
        update_dict["name"] = updates.name
    if updates.institution is not None:
        update_dict["institution"] = updates.institution
    if updates.account_type is not None:
        update_dict["account_type"] = updates.account_type
    if updates.last_four is not None:
        update_dict["last_four"] = updates.last_four
    if updates.color is not None:
        update_dict["color"] = updates.color
    if updates.initial_balance is not None:
        update_dict["initial_balance"] = updates.initial_balance

    if update_dict:
        service.store.update_account(account_id, **update_dict)

    return {"success": True}


@app.delete("/api/accounts/{account_id}")
def delete_account(account_id: int, service: BudgetService = Depends(get_service)):
    """Soft delete an account (deactivate it)."""
    existing = service.store.get_account(account_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Account not found")

    service.store.delete_account(account_id)
    return {"success": True}


@app.get("/api/accounts/{account_id}/balance")
def get_account_balance(
    account_id: int,
    as_of_date: Optional[str] = Query(None, description="ISO date (YYYY-MM-DD) to calculate balance as of. Defaults to current."),
    service: BudgetService = Depends(get_service)
):
    """Get account balance as of a specific date.

    For checking/savings: returns the balance (positive = money available)
    For credit cards: returns the balance owed (positive = debt)

    If as_of_date is before the account's balance_as_of_date, returns an error
    since we can't calculate the balance before we know the starting point.
    """
    account = service.store.get_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    balance = service.store.get_balance_as_of_date(account_id, as_of_date)

    if balance is None:
        balance_date = account.get("balance_as_of_date")
        raise HTTPException(
            status_code=400,
            detail=f"Cannot calculate balance before {balance_date}. The starting balance was recorded on that date."
        )

    return {
        "account_id": account_id,
        "account_name": account["name"],
        "account_type": account["account_type"],
        "balance": round(balance, 2),
        "as_of_date": as_of_date or "current",
        "balance_as_of_date": account.get("balance_as_of_date"),
        "initial_balance": account.get("initial_balance", 0)
    }


@app.get("/api/accounts/{account_id}/transactions")
def get_account_transactions(
    account_id: int,
    limit: int = Query(100, ge=1, le=500),
    service: BudgetService = Depends(get_service)
):
    """Get transactions for a specific account."""
    existing = service.store.get_account(account_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Account not found")

    return service.store.get_transactions_by_account(account_id, limit)


@app.post("/api/transactions/{txn_id}/assign-account")
def assign_transaction_account(
    txn_id: int,
    account_id: Optional[int] = None,
    service: BudgetService = Depends(get_service)
):
    """Assign a transaction to an account."""
    txn = service.store.get_transaction(txn_id)
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")

    if account_id:
        acct = service.store.get_account(account_id)
        if not acct:
            raise HTTPException(status_code=404, detail="Account not found")

    service.store.update_transaction_account(txn_id, account_id)
    return {"success": True}


@app.get("/api/transactions")
def get_transactions(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    category: Optional[str] = None,
    search: Optional[str] = Query(None, description="Search in merchant and description"),
    sort_by: str = Query("date", pattern="^(date|amount|merchant)$"),
    sort_dir: str = Query("desc", pattern="^(asc|desc)$"),
    service: BudgetService = Depends(get_service)
):
    """Get paginated list of transactions with optional filtering."""
    # Get all transactions from store
    all_txns = service.store.get_all_transactions()

    # Filter by search term if specified (search in merchant and description)
    if search:
        search_lower = search.lower().strip()
        all_txns = [
            t for t in all_txns
            if search_lower in (t.get("merchant") or "").lower()
            or search_lower in (t.get("description") or "").lower()
        ]

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


@app.get("/api/transactions/{txn_id}")
def get_transaction(txn_id: int, service: BudgetService = Depends(get_service)):
    """Get a single transaction by ID."""
    txn = service.get_transaction(txn_id)
    if txn is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return txn


@app.get("/api/uncategorized")
def get_uncategorized(
    limit: int = Query(50, ge=1, le=500),
    service: BudgetService = Depends(get_service)
):
    """Get uncategorized transactions."""
    return service.get_uncategorized()[:limit]


@app.post("/api/transactions/{txn_id}/categorize")
def categorize_transaction(
    txn_id: int,
    request: CategorizeRequest,
    service: BudgetService = Depends(get_service)
):
    """Manually categorize a transaction."""
    # Check if category exists
    categories = service.get_categories()
    if not any(c["name"] == request.category for c in categories):
        raise HTTPException(status_code=400, detail=f"Invalid category: {request.category}")

    success = service.categorize_transaction(txn_id, request.category)
    if not success:
        raise HTTPException(status_code=404, detail="Transaction not found")

    return {"success": True}


@app.post("/api/transactions/bulk-categorize")
def bulk_categorize_transactions(
    request: BulkCategorizeRequest,
    service: BudgetService = Depends(get_service)
):
    """Bulk categorize multiple transactions at once."""
    if not request.ids:
        raise HTTPException(status_code=400, detail="No transaction IDs provided")

    # Check if category exists
    categories = service.get_categories()
    if not any(c["name"] == request.category for c in categories):
        raise HTTPException(status_code=400, detail=f"Invalid category: {request.category}")

    # Categorize each transaction
    success_count = 0
    failed_ids = []
    for txn_id in request.ids:
        try:
            if service.categorize_transaction(txn_id, request.category):
                success_count += 1
            else:
                failed_ids.append(txn_id)
        except Exception:
            failed_ids.append(txn_id)

    return {
        "success": True,
        "categorized": success_count,
        "failed": len(failed_ids),
        "failed_ids": failed_ids
    }


@app.get("/api/recurring")
def get_recurring(service: BudgetService = Depends(get_service)):
    """Get detected recurring transactions/subscriptions."""
    # Get from the last analysis or run a fresh one
    all_txns = service.store.get_all_transactions()
    recurring = [t for t in all_txns if t.get("is_recurring")]

    # Group by merchant for patterns
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
        patterns[merchant]["transactions"].append(t)

    return list(patterns.values())


@app.get("/api/anomalies")
def get_anomalies(service: BudgetService = Depends(get_service)):
    """Get detected anomalous transactions."""
    all_txns = service.store.get_all_transactions()
    return [t for t in all_txns if t.get("is_anomaly")]


@app.get("/api/search")
def search_transactions(
    q: str = Query(..., min_length=1),
    k: int = Query(10, ge=1, le=50),
    service: BudgetService = Depends(get_service)
):
    """Semantic search for similar transactions."""
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    return service.search_similar(q.strip(), k=k)


@app.post("/api/import")
async def import_file(
    file: UploadFile = File(...),
    auto_categorize: bool = Query(False),
    service: BudgetService = Depends(get_service)
):
    """Import transactions from CSV/Excel file."""
    # Save uploaded file to temp location
    suffix = Path(file.filename).suffix if file.filename else ".csv"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = Path(tmp.name)

    try:
        result = service.import_file(tmp_path, auto_categorize=auto_categorize)
        return result
    finally:
        tmp_path.unlink()  # Clean up temp file


# === Smart Import Wizard Endpoints ===

# Common column name patterns for auto-detection
DATE_COLUMNS = ["date", "transaction date", "trans date", "posted date", "trans_date", "posting date"]
AMOUNT_COLUMNS = ["amount", "trans_amt", "transaction amount", "debit/credit", "value"]
MERCHANT_COLUMNS = ["merchant", "payee", "description", "payee_name", "vendor", "name"]
DESC_COLUMNS = ["description", "memo", "notes", "details", "original description", "address"]


def _detect_column_mapping(columns: List[str]) -> Dict[str, Optional[str]]:
    """Auto-detect column mappings based on common names."""
    columns_lower = {str(c).lower(): c for c in columns}
    mapping = {"date": None, "amount": None, "merchant": None, "description": None}

    for col in DATE_COLUMNS:
        if col in columns_lower:
            mapping["date"] = columns_lower[col]
            break

    for col in AMOUNT_COLUMNS:
        if col in columns_lower:
            mapping["amount"] = columns_lower[col]
            break

    for col in MERCHANT_COLUMNS:
        if col in columns_lower:
            mapping["merchant"] = columns_lower[col]
            break

    for col in DESC_COLUMNS:
        if col in columns_lower and columns_lower[col] != mapping.get("merchant"):
            mapping["description"] = columns_lower[col]
            break

    return mapping


@app.post("/api/import/preview")
async def import_preview(file: UploadFile = File(...)):
    """Upload file for preview - returns comprehensive analysis including bank detection."""
    from ingestion.csv_parser import CSVParser

    # Generate session ID
    session_id = str(uuid.uuid4())

    # Save to persistent temp location
    suffix = Path(file.filename).suffix if file.filename else ".csv"
    import_dir = Path(tempfile.gettempdir()) / "budget_imports"
    import_dir.mkdir(exist_ok=True)
    tmp_path = import_dir / f"{session_id}{suffix}"

    content = await file.read()
    tmp_path.write_bytes(content)

    try:
        # Use enhanced CSV parser for analysis
        parser = CSVParser()
        analysis = parser.analyze_file(tmp_path, filename=file.filename)

        if "error" in analysis:
            tmp_path.unlink(missing_ok=True)
            raise HTTPException(status_code=400, detail=analysis["error"])

        # Get more sample rows for preview
        if suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(tmp_path)
        else:
            df = pd.read_csv(tmp_path)

        # Get sample rows (first 10)
        sample_rows = []
        for _, row in df.head(10).iterrows():
            sample_rows.append({str(col): str(val) if not pd.isna(val) else "" for col, val in row.items()})

        # Store session data with enhanced info
        _import_sessions[session_id] = {
            "file_path": str(tmp_path),
            "columns": analysis["columns"],
            "total_rows": analysis["total_rows"],
            "detected_mapping": analysis["detected_mapping"],
            "format": analysis["format"],
            "account_type": analysis["account_type"],
            "has_split_amounts": analysis["has_split_amounts"]
        }

        # Build response with all analysis info
        return {
            "session_id": session_id,
            "filename": file.filename,
            "columns": analysis["columns"],
            "sample_rows": sample_rows,
            "total_rows": analysis["total_rows"],
            # Enhanced detection info
            "detected_mapping": analysis["detected_mapping"],
            "format": analysis["format"],
            "account_type": analysis["account_type"],
            "has_split_amounts": analysis["has_split_amounts"],
            "analysis": analysis["analysis"]
        }

    except HTTPException:
        raise
    except Exception as e:
        # Clean up on error
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {str(e)}")


@app.post("/api/import/confirm")
async def import_confirm(
    request: ImportConfirmRequest,
    service: BudgetService = Depends(get_service)
):
    """Confirm import with custom column mapping."""
    session = _import_sessions.get(request.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Import session not found or expired")

    tmp_path = Path(session["file_path"])
    if not tmp_path.exists():
        del _import_sessions[request.session_id]
        raise HTTPException(status_code=404, detail="Import file expired, please upload again")

    try:
        # Read file again
        suffix = tmp_path.suffix.lower()
        if suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(tmp_path)
        else:
            with open(tmp_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()

            header_row = 0
            pandas_row = 0
            for line in lines[:20]:
                lower = line.lower().strip()
                if not lower:
                    continue
                if ',' in lower and any(col in lower for col in ["date", "amount", "merchant", "payee", "description"]):
                    header_row = pandas_row
                    break
                pandas_row += 1

            df = pd.read_csv(tmp_path, header=header_row)

        # Parse transactions using custom mapping
        mapping = request.column_mapping
        transactions = []
        errors = []

        # Helper function to parse amount values
        def parse_amount_value(val):
            """Parse an amount value, handling various formats."""
            if pd.isna(val) or val == '' or val is None:
                return 0.0
            if isinstance(val, (int, float)):
                return float(val)
            amount_str = re.sub(r'[$,]', '', str(val).strip())
            if not amount_str or amount_str == '-':
                return 0.0
            # Handle accounting format (negative in parentheses)
            if amount_str.startswith('(') and amount_str.endswith(')'):
                amount_str = '-' + amount_str[1:-1]
            try:
                return float(amount_str)
            except ValueError:
                return None

        # Check if we're using split debit/credit columns
        use_split_columns = mapping.debit_column and mapping.credit_column

        for idx, row in df.iterrows():
            try:
                date_val = row.get(mapping.date_column)
                merchant_val = row.get(mapping.merchant_column)
                desc_val = row.get(mapping.description_column) if mapping.description_column else None

                # Handle amount: either split columns or single amount column
                if use_split_columns:
                    debit_val = row.get(mapping.debit_column)
                    credit_val = row.get(mapping.credit_column)

                    debit = parse_amount_value(debit_val)
                    credit = parse_amount_value(credit_val)

                    if debit is None and credit is None:
                        errors.append(f"Row {idx+1}: Invalid debit/credit values")
                        continue

                    debit = debit or 0.0
                    credit = credit or 0.0

                    # Calculate amount: credit (income/refunds) - debit (spending)
                    # This makes expenses negative and income positive
                    amount = credit - debit

                    # Skip rows with no activity
                    if amount == 0 and debit == 0 and credit == 0:
                        continue
                else:
                    amount_val = row.get(mapping.amount_column)

                    # Skip if critical values missing
                    if pd.isna(amount_val):
                        continue

                    amount = parse_amount_value(amount_val)
                    if amount is None:
                        errors.append(f"Row {idx+1}: Invalid amount")
                        continue

                # Skip if date is missing
                if pd.isna(date_val):
                    continue

                # Normalize date
                if isinstance(date_val, (pd.Timestamp,)):
                    date_str = date_val.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_val).strip()
                    # Try common formats
                    for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y"]:
                        try:
                            from datetime import datetime
                            dt = datetime.strptime(date_str, fmt)
                            date_str = dt.strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue
                    else:
                        try:
                            dt = pd.to_datetime(date_str)
                            date_str = dt.strftime("%Y-%m-%d")
                        except:
                            errors.append(f"Row {idx+1}: Invalid date format")
                            continue

                merchant = str(merchant_val).strip() if not pd.isna(merchant_val) else "UNKNOWN"
                description = str(desc_val).strip() if desc_val and not pd.isna(desc_val) else None

                transactions.append({
                    "date": date_str,
                    "amount": amount,
                    "merchant": merchant,
                    "description": description
                })

            except Exception as e:
                errors.append(f"Row {idx+1}: {str(e)}")

        # Import transactions
        imported = 0
        duplicates = 0

        for txn in transactions:
            txn_id = service.store.add_transaction(
                date=txn["date"],
                amount=txn["amount"],
                merchant=txn["merchant"],
                description=txn["description"]
            )
            if txn_id is not None:
                imported += 1
                # Auto-categorize if requested
                if request.auto_categorize:
                    try:
                        service.categorizer.categorize_and_update({
                            "id": txn_id,
                            "merchant": txn["merchant"],
                            "description": txn.get("description"),
                            "amount": txn["amount"]
                        })
                    except:
                        pass
            else:
                duplicates += 1

        return {
            "success": True,
            "imported": imported,
            "duplicates": duplicates,
            "errors": errors[:10],  # Return first 10 errors
            "total_errors": len(errors)
        }

    finally:
        # Clean up
        tmp_path.unlink(missing_ok=True)
        if request.session_id in _import_sessions:
            del _import_sessions[request.session_id]


@app.delete("/api/import/session/{session_id}")
async def cancel_import(session_id: str):
    """Cancel an import session and clean up."""
    session = _import_sessions.get(session_id)
    if session:
        tmp_path = Path(session["file_path"])
        tmp_path.unlink(missing_ok=True)
        del _import_sessions[session_id]
    return {"success": True}


@app.get("/api/transactions/{txn_id}/explanation")
def get_explanation(txn_id: int, service: BudgetService = Depends(get_service)):
    """Get categorization explanation for a transaction."""
    txn = service.get_transaction(txn_id)
    if txn is None:
        raise HTTPException(status_code=404, detail="Transaction not found")

    explanation = service.get_categorization_explanation(txn_id)
    return {"explanation": explanation or "No explanation available"}


# === Rules API ===

@app.get("/api/rules")
def get_rules(
    active_only: bool = Query(True),
    service: BudgetService = Depends(get_service)
):
    """Get all categorization rules."""
    return service.store.get_all_rules(active_only=active_only)


@app.post("/api/rules")
def create_rule(rule: RuleCreate, service: BudgetService = Depends(get_service)):
    """Create a new categorization rule."""
    # Validate category
    categories = service.get_categories()
    cat = next((c for c in categories if c["name"] == rule.category), None)
    if not cat:
        raise HTTPException(status_code=400, detail=f"Invalid category: {rule.category}")

    # Validate rule type
    valid_types = ["merchant_contains", "merchant_exact", "description_contains", "amount_range"]
    if rule.rule_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Invalid rule_type. Must be one of: {valid_types}")

    rule_id = service.store.add_rule(
        name=rule.name,
        rule_type=rule.rule_type,
        pattern=rule.pattern,
        category_id=cat["id"],
        priority=rule.priority,
        notes=rule.notes
    )
    return {"id": rule_id, "success": True}


@app.get("/api/rules/{rule_id}")
def get_rule(rule_id: int, service: BudgetService = Depends(get_service)):
    """Get a single rule by ID."""
    rule = service.store.get_rule(rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule


@app.put("/api/rules/{rule_id}")
def update_rule(rule_id: int, updates: RuleUpdate, service: BudgetService = Depends(get_service)):
    """Update a rule."""
    existing = service.store.get_rule(rule_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Rule not found")

    update_dict = {}
    if updates.name is not None:
        update_dict["name"] = updates.name
    if updates.rule_type is not None:
        update_dict["rule_type"] = updates.rule_type
    if updates.pattern is not None:
        update_dict["pattern"] = updates.pattern
    if updates.category is not None:
        categories = service.get_categories()
        cat = next((c for c in categories if c["name"] == updates.category), None)
        if not cat:
            raise HTTPException(status_code=400, detail=f"Invalid category: {updates.category}")
        update_dict["category_id"] = cat["id"]
    if updates.priority is not None:
        update_dict["priority"] = updates.priority
    if updates.is_active is not None:
        update_dict["is_active"] = 1 if updates.is_active else 0
    if updates.notes is not None:
        update_dict["notes"] = updates.notes

    service.store.update_rule(rule_id, **update_dict)
    return {"success": True}


@app.delete("/api/rules/{rule_id}")
def delete_rule(rule_id: int, service: BudgetService = Depends(get_service)):
    """Delete (deactivate) a rule."""
    existing = service.store.get_rule(rule_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Rule not found")

    service.store.delete_rule(rule_id)
    return {"success": True}


@app.post("/api/rules/apply")
def apply_rules(service: BudgetService = Depends(get_service)):
    """Apply all active rules to uncategorized transactions."""
    updated = service.store.apply_rules_to_uncategorized()
    return {"updated": updated}


@app.get("/api/transactions/similar/{merchant}")
def find_similar_transactions(
    merchant: str,
    limit: int = Query(50, ge=1, le=200),
    service: BudgetService = Depends(get_service)
):
    """Find transactions with similar merchant names."""
    return service.store.find_similar_by_merchant(merchant, limit)


# === Chat API ===

@app.post("/api/chat")
async def chat_with_claude(msg: ChatMessage, service: BudgetService = Depends(get_service)):
    """Send a message to Claude with budget context."""
    # Check onboarding status
    onboarding_status = service.store.get_onboarding_status()
    is_onboarding = msg.onboarding_mode or onboarding_status.get('needs_onboarding', False)

    # Build context
    context_parts = []

    # Add onboarding context if needed
    if is_onboarding:
        steps = onboarding_status.get('steps', {})
        context_parts.append("=== ONBOARDING MODE ===")
        context_parts.append("This is a new user who needs help setting up their budget tracker.")
        context_parts.append(f"Setup Progress:")
        context_parts.append(f"  - Monthly income set: {'Yes' if steps.get('income_set') else 'No - NEEDS SETUP'}")
        context_parts.append(f"  - Accounts created: {'Yes' if steps.get('has_accounts') else 'No - NEEDS SETUP'}")
        context_parts.append(f"  - Categories exist: {'Yes' if steps.get('has_categories') else 'No - NEEDS SETUP'}")
        context_parts.append(f"  - Bills/subscriptions: {'Yes' if steps.get('has_bills') else 'No - optional'}")
        context_parts.append(f"  - Transactions imported: {'Yes' if steps.get('has_transactions') else 'No - optional'}")
        context_parts.append("")

    # Add transaction context if provided
    if msg.context_txn_id:
        txn = service.get_transaction(msg.context_txn_id)
        if txn:
            context_parts.append(f"The user is asking about this transaction:")
            context_parts.append(f"- Date: {txn.get('date')}")
            context_parts.append(f"- Merchant: {txn.get('merchant')}")
            context_parts.append(f"- Amount: ${abs(txn.get('amount', 0)):.2f}")
            if txn.get('description'):
                context_parts.append(f"- Description: {txn.get('description')}")
            if txn.get('category_name'):
                context_parts.append(f"- Current Category: {txn.get('category_name')}")
            else:
                context_parts.append(f"- Currently uncategorized")

            # Find similar transactions
            similar = service.store.find_similar_by_merchant(txn.get('merchant', ''), limit=5)
            if similar:
                context_parts.append(f"\nSimilar transactions from this merchant:")
                for s in similar[:5]:
                    context_parts.append(f"  - {s.get('date')}: ${abs(s.get('amount', 0)):.2f}")

    # Add budget summary
    summary = service.get_summary()
    context_parts.append(f"\nBudget Overview:")
    context_parts.append(f"- Total transactions: {summary.get('total_transactions', 0)}")
    context_parts.append(f"- Uncategorized: {summary.get('uncategorized', 0)}")

    # Get available categories
    categories = service.get_categories()
    context_parts.append(f"\nAvailable categories: {', '.join(c['name'] for c in categories)}")

    # Build the prompt
    system_context = "\n".join(context_parts)

    # Different prompt for onboarding vs normal mode
    if is_onboarding:
        full_prompt = f"""You are a friendly budget assistant helping a new user set up their budget tracker.

{system_context}

ONBOARDING INSTRUCTIONS:
Guide the user through setup in this order:
1. First, ask about their monthly income and set it
2. Then help them add their bank accounts/credit cards
3. Help them customize their budget categories
4. Optionally, help them add recurring bills/subscriptions
5. Help them import transactions by dragging a CSV file into the chat

IMPORTANT: Users can import transactions by dragging a CSV file directly into the chat window, or by clicking the upload button next to the text input. When they do, the system will automatically detect columns and show an import preview. Let them know about this feature when appropriate (especially after setting up accounts).

Be conversational and encouraging. Ask one question at a time. When the user provides info, use action blocks to save it.

User's message: {msg.message}

AVAILABLE ACTIONS - Use these action blocks to save user data:

To set monthly income:
```action
{{"action": "set_income", "amount": 5000}}
```

To create an account:
```action
{{"action": "create_account", "name": "Main Checking", "institution": "Chase", "account_type": "checking", "initial_balance": 1500}}
```

To create a category:
```action
{{"action": "create_category", "name": "Groceries", "budget_amount": 400}}
```

To create a bill/subscription:
```action
{{"action": "create_subscription", "name": "Netflix", "amount": -15.99, "frequency": "monthly"}}
```

To mark onboarding as complete (when all essential setup is done):
```action
{{"action": "complete_onboarding"}}
```

Keep responses concise and focused on the current setup step."""
    else:
        full_prompt = f"""You are a helpful budget assistant. You have access to the user's transaction data.

{system_context}

User's message: {msg.message}

If the user wants to categorize a transaction or create a rule, respond with a JSON action block like:
```action
{{"action": "categorize", "txn_id": 123, "category": "Food & Dining"}}
```
or
```action
{{"action": "create_rule", "name": "SCA Membership", "rule_type": "merchant_contains", "pattern": "sca", "category": "Entertainment", "notes": "Society for Creative Anachronism membership"}}
```

Otherwise, just respond helpfully about their finances."""

    # Call Claude via subprocess (claude CLI)
    try:
        result = subprocess.run(
            ["claude", "-p", full_prompt, "--output-format", "text"],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(Path(__file__).parent.parent)
        )
        response = result.stdout.strip() if result.returncode == 0 else f"Error: {result.stderr}"

        # Check for action blocks in response
        actions_executed = []
        if "```action" in response:
            import re
            from datetime import datetime
            action_matches = re.findall(r'```action\s*\n(.*?)\n```', response, re.DOTALL)
            for action_str in action_matches:
                try:
                    action = json.loads(action_str)
                    action_type = action.get("action")

                    if action_type == "categorize":
                        success = service.categorize_transaction(action["txn_id"], action["category"])
                        actions_executed.append({"type": "categorize", "success": success, **action})

                    elif action_type == "create_rule":
                        cat = next((c for c in categories if c["name"] == action["category"]), None)
                        if cat:
                            rule_id = service.store.add_rule(
                                name=action["name"],
                                rule_type=action["rule_type"],
                                pattern=action["pattern"],
                                category_id=cat["id"],
                                notes=action.get("notes")
                            )
                            actions_executed.append({"type": "create_rule", "rule_id": rule_id, **action})

                    # Onboarding actions
                    elif action_type == "set_income":
                        service.store.update_budget_setting('monthly_income', action["amount"])
                        actions_executed.append({"type": "set_income", "success": True, **action})

                    elif action_type == "create_account":
                        account_id = service.store.add_account(
                            name=action["name"],
                            institution=action.get("institution"),
                            account_type=action.get("account_type", "checking"),
                            last_four=action.get("last_four"),
                            color=action.get("color", "#3B82F6"),
                            initial_balance=action.get("initial_balance", 0)
                        )
                        actions_executed.append({"type": "create_account", "account_id": account_id, "success": True, **action})

                    elif action_type == "create_category":
                        category_id = service.store.add_category(
                            name=action["name"],
                            keywords=action.get("keywords"),
                            budget_amount=action.get("budget_amount", 0)
                        )
                        actions_executed.append({"type": "create_category", "category_id": category_id, "success": True, **action})

                    elif action_type == "create_subscription":
                        sub_id = service.store.create_recurring_transaction(
                            name=action["name"],
                            merchant=action.get("merchant"),
                            amount=action["amount"],
                            frequency=action.get("frequency", "monthly"),
                            start_date=action.get("start_date", datetime.now().strftime("%Y-%m-%d")),
                            category_id=action.get("category_id"),
                            account_id=action.get("account_id"),
                            notes=action.get("notes")
                        )
                        actions_executed.append({"type": "create_subscription", "subscription_id": sub_id, "success": True, **action})

                    elif action_type == "complete_onboarding":
                        service.store.complete_onboarding()
                        actions_executed.append({"type": "complete_onboarding", "success": True})

                except json.JSONDecodeError:
                    pass
                except Exception as e:
                    actions_executed.append({"type": action.get("action", "unknown"), "success": False, "error": str(e)})

        return {
            "response": response,
            "actions_executed": actions_executed
        }
    except subprocess.TimeoutExpired:
        return {"response": "Request timed out. Please try again.", "actions_executed": []}
    except FileNotFoundError:
        return {"response": "Claude CLI not found. Please ensure claude is installed.", "actions_executed": []}


# === Analytics API ===

@app.get("/api/analytics/monthly")
def get_monthly_analytics(
    months: int = Query(12, ge=1, le=24),
    service: BudgetService = Depends(get_service)
):
    """Get monthly income and expenses for the last N months."""
    return service.store.get_monthly_totals(months)


@app.get("/api/analytics/categories")
def get_category_analytics(
    year: Optional[int] = None,
    month: Optional[int] = None,
    service: BudgetService = Depends(get_service)
):
    """Get spending totals by category."""
    from datetime import datetime
    if year is None or month is None:
        now = datetime.now()
        year = now.year
        month = now.month
    return service.store.get_category_totals(year, month)


@app.get("/api/analytics/daily")
def get_daily_analytics(
    days: int = Query(30, ge=1, le=365),
    service: BudgetService = Depends(get_service)
):
    """Get daily spending for the last N days."""
    return service.store.get_daily_spending(days)


@app.get("/api/analytics/merchants")
def get_merchant_analytics(
    limit: int = Query(10, ge=1, le=50),
    days: int = Query(90, ge=1, le=365),
    service: BudgetService = Depends(get_service)
):
    """Get top merchants by spending."""
    return service.store.get_top_merchants(limit, days)


@app.get("/api/analytics/weekday")
def get_weekday_analytics(service: BudgetService = Depends(get_service)):
    """Get spending patterns by day of week."""
    return service.store.get_spending_by_day_of_week()


@app.get("/api/analytics/category/{category_id}/trend")
def get_category_trend(
    category_id: int,
    months: int = Query(6, ge=1, le=24),
    service: BudgetService = Depends(get_service)
):
    """Get spending trend for a specific category."""
    return service.store.get_category_trend(category_id, months)


# === Recurring Transactions / Subscriptions API ===

@app.get("/api/subscriptions")
def get_subscriptions(
    include_inactive: bool = Query(False),
    service: BudgetService = Depends(get_service)
):
    """Get all recurring transactions/subscriptions."""
    return service.store.get_recurring_transactions(include_inactive=include_inactive)


@app.get("/api/subscriptions/summary")
def get_subscriptions_summary(service: BudgetService = Depends(get_service)):
    """Get summary statistics for subscriptions."""
    return service.store.get_recurring_summary()


@app.get("/api/subscriptions/upcoming")
def get_upcoming_subscriptions(
    days: int = Query(7, ge=1, le=90),
    service: BudgetService = Depends(get_service)
):
    """Get subscriptions due within the next N days."""
    return service.store.get_upcoming_recurring(days)


@app.get("/api/subscriptions/overdue")
def get_overdue_subscriptions(service: BudgetService = Depends(get_service)):
    """Get subscriptions that are past due."""
    return service.store.get_overdue_recurring()


@app.post("/api/subscriptions")
def create_subscription(
    subscription: RecurringCreate,
    service: BudgetService = Depends(get_service)
):
    """Create a new recurring transaction/subscription."""
    # Validate frequency
    valid_frequencies = ['daily', 'weekly', 'biweekly', 'monthly', 'quarterly', 'yearly', 'annually']
    if subscription.frequency.lower() not in valid_frequencies:
        raise HTTPException(status_code=400, detail=f"Invalid frequency. Must be one of: {valid_frequencies}")

    # Validate category if provided
    if subscription.category_id:
        categories = service.get_categories()
        if not any(c["id"] == subscription.category_id for c in categories):
            raise HTTPException(status_code=400, detail="Invalid category_id")

    # Validate account if provided
    if subscription.account_id:
        account = service.store.get_account(subscription.account_id)
        if not account:
            raise HTTPException(status_code=400, detail="Invalid account_id")

    sub_id = service.store.create_recurring_transaction(
        name=subscription.name,
        merchant=subscription.merchant,
        amount=subscription.amount,
        frequency=subscription.frequency.lower(),
        start_date=subscription.start_date,
        category_id=subscription.category_id,
        account_id=subscription.account_id,
        notes=subscription.notes
    )
    return {"id": sub_id, "success": True}


@app.get("/api/subscriptions/{sub_id}")
def get_subscription(sub_id: int, service: BudgetService = Depends(get_service)):
    """Get a single subscription by ID."""
    sub = service.store.get_recurring_transaction(sub_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return sub


@app.put("/api/subscriptions/{sub_id}")
def update_subscription(
    sub_id: int,
    updates: RecurringUpdate,
    service: BudgetService = Depends(get_service)
):
    """Update a subscription."""
    existing = service.store.get_recurring_transaction(sub_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Subscription not found")

    # Validate frequency if updating
    if updates.frequency:
        valid_frequencies = ['daily', 'weekly', 'biweekly', 'monthly', 'quarterly', 'yearly', 'annually']
        if updates.frequency.lower() not in valid_frequencies:
            raise HTTPException(status_code=400, detail=f"Invalid frequency. Must be one of: {valid_frequencies}")

    # Build update dict
    update_dict = {}
    if updates.name is not None:
        update_dict["name"] = updates.name
    if updates.merchant is not None:
        update_dict["merchant"] = updates.merchant
    if updates.amount is not None:
        update_dict["amount"] = updates.amount
    if updates.frequency is not None:
        update_dict["frequency"] = updates.frequency.lower()
    if updates.start_date is not None:
        update_dict["start_date"] = updates.start_date
    if updates.next_due_date is not None:
        update_dict["next_due_date"] = updates.next_due_date
    if updates.category_id is not None:
        update_dict["category_id"] = updates.category_id
    if updates.account_id is not None:
        update_dict["account_id"] = updates.account_id
    if updates.is_active is not None:
        update_dict["is_active"] = 1 if updates.is_active else 0
    if updates.notes is not None:
        update_dict["notes"] = updates.notes

    service.store.update_recurring_transaction(sub_id, **update_dict)
    return {"success": True}


@app.delete("/api/subscriptions/{sub_id}")
def delete_subscription(sub_id: int, service: BudgetService = Depends(get_service)):
    """Delete a subscription."""
    existing = service.store.get_recurring_transaction(sub_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Subscription not found")

    service.store.delete_recurring_transaction(sub_id)
    return {"success": True}


class MarkPaidRequest(BaseModel):
    paid_date: Optional[str] = None
    amount_paid: Optional[float] = None
    payment_method: Optional[str] = None
    confirmation_number: Optional[str] = None
    notes: Optional[str] = None


@app.post("/api/subscriptions/{sub_id}/paid")
def mark_subscription_paid(
    sub_id: int,
    request: Optional[MarkPaidRequest] = None,
    service: BudgetService = Depends(get_service)
):
    """Mark a subscription as paid, record payment history, and advance the next due date."""
    existing = service.store.get_recurring_transaction(sub_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Subscription not found")

    # Extract parameters from request body if provided
    paid_date = request.paid_date if request else None
    amount_paid = request.amount_paid if request else None
    payment_method = request.payment_method if request else None
    confirmation_number = request.confirmation_number if request else None
    notes = request.notes if request else None

    payment_id = service.store.mark_recurring_paid(
        sub_id,
        paid_date=paid_date,
        amount_paid=amount_paid,
        payment_method=payment_method,
        confirmation_number=confirmation_number,
        notes=notes
    )
    if payment_id is None:
        raise HTTPException(status_code=500, detail="Failed to mark as paid")

    # Return the updated subscription with payment info
    updated = service.store.get_recurring_transaction(sub_id)
    updated['last_payment_id'] = payment_id
    return updated


# === Bill Payment History API ===

@app.get("/api/subscriptions/{sub_id}/payments")
def get_subscription_payments(
    sub_id: int,
    limit: int = Query(20, ge=1, le=100),
    service: BudgetService = Depends(get_service)
):
    """Get payment history for a subscription."""
    existing = service.store.get_recurring_transaction(sub_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return service.store.get_bill_payments(sub_id, limit=limit)


@app.get("/api/bill-payments")
def get_all_bill_payments(
    limit: int = Query(50, ge=1, le=200),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    service: BudgetService = Depends(get_service)
):
    """Get all recent bill payments across all subscriptions."""
    return service.store.get_all_bill_payments(limit=limit, start_date=start_date, end_date=end_date)


@app.get("/api/bill-payments/summary")
def get_bill_payments_summary(
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    service: BudgetService = Depends(get_service)
):
    """Get summary of bills paid in a specific month (defaults to current month)."""
    return service.store.get_monthly_bill_summary(year=year, month=month)


@app.get("/api/bills/due-soon")
def get_bills_due_soon(
    days: int = Query(7, ge=1, le=90),
    service: BudgetService = Depends(get_service)
):
    """Get bills due within the specified number of days with status indicators."""
    return service.store.get_bills_due_soon(days=days)


@app.delete("/api/bill-payments/{payment_id}")
def delete_bill_payment(
    payment_id: int,
    service: BudgetService = Depends(get_service)
):
    """Delete a bill payment record."""
    payment = service.store.get_bill_payment(payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    success = service.store.delete_bill_payment(payment_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete payment")
    return {"success": True}


# === Budget Overview API ===

class BudgetSettingsUpdate(BaseModel):
    monthly_income: Optional[float] = None
    savings_target_percent: Optional[float] = None
    emergency_fund_months: Optional[int] = None
    discretionary_warning_percent: Optional[float] = None


@app.get("/api/budget/settings")
def get_budget_settings(service: BudgetService = Depends(get_service)):
    """Get all budget settings (monthly income, savings targets, etc.)."""
    return service.store.get_all_budget_settings()


@app.put("/api/budget/settings")
def update_budget_settings(
    settings: BudgetSettingsUpdate,
    service: BudgetService = Depends(get_service)
):
    """Update budget settings."""
    updates = settings.model_dump(exclude_none=True)
    for key, value in updates.items():
        service.store.update_budget_setting(key, value)
    return service.store.get_all_budget_settings()


@app.get("/api/budget/overview")
def get_budget_overview(
    month: Optional[str] = Query(None, regex=r"^\d{4}-\d{2}$"),
    service: BudgetService = Depends(get_service)
):
    """
    Get comprehensive budget overview for a month.

    Returns:
    - Income (monthly)
    - Fixed costs (recurring bills)
    - Category budgets with spending
    - Discretionary budget and spending
    - Warnings/alerts
    """
    return service.store.get_budget_overview(month=month)


@app.get("/api/budget/fixed-costs")
def get_fixed_costs(service: BudgetService = Depends(get_service)):
    """Get all fixed costs (recurring bills) with monthly equivalents."""
    return service.store.get_fixed_costs_monthly()


# === Onboarding API ===

@app.get("/api/onboarding/status")
def get_onboarding_status(service: BudgetService = Depends(get_service)):
    """Get onboarding status - whether setup is needed and what steps are complete."""
    return service.store.get_onboarding_status()


@app.post("/api/onboarding/complete")
def complete_onboarding(service: BudgetService = Depends(get_service)):
    """Mark onboarding as complete."""
    service.store.complete_onboarding()
    return {"success": True, "message": "Onboarding marked as complete"}


@app.post("/api/onboarding/reset")
def reset_onboarding(service: BudgetService = Depends(get_service)):
    """Reset onboarding status (for re-running setup)."""
    service.store.reset_onboarding()
    return {"success": True, "message": "Onboarding has been reset"}


# Budgeting method templates with educational content
BUDGETING_METHODS = {
    "fifty_thirty_twenty": {
        "id": "fifty_thirty_twenty",
        "name": "50/30/20 Rule",
        "short_description": "Simple & balanced",
        "description": "The 50/30/20 rule divides your after-tax income into three buckets: 50% for needs (essentials), 30% for wants (lifestyle), and 20% for savings and debt repayment. This method is great for beginners because it's simple to follow and provides flexibility.",
        "best_for": ["Beginners", "Those who want simplicity", "People with stable income"],
        "pros": ["Easy to remember", "Flexible within categories", "Encourages saving"],
        "cons": ["May not work for high cost-of-living areas", "Doesn't track individual spending"],
        "allocation": {
            "needs": 50,
            "wants": 30,
            "savings": 20
        },
        "categories": [
            {"name": "Housing", "type": "needs", "percent": 25, "keywords": "rent,mortgage,property tax"},
            {"name": "Utilities", "type": "needs", "percent": 5, "keywords": "electric,gas,water,internet,phone"},
            {"name": "Groceries", "type": "needs", "percent": 10, "keywords": "grocery,supermarket,food"},
            {"name": "Transportation", "type": "needs", "percent": 10, "keywords": "gas,fuel,uber,lyft,transit,parking"},
            {"name": "Insurance", "type": "needs", "percent": 0, "keywords": "insurance,premium"},
            {"name": "Dining Out", "type": "wants", "percent": 5, "keywords": "restaurant,cafe,coffee,doordash,grubhub"},
            {"name": "Entertainment", "type": "wants", "percent": 5, "keywords": "netflix,spotify,movie,concert,game"},
            {"name": "Shopping", "type": "wants", "percent": 10, "keywords": "amazon,target,walmart,clothing"},
            {"name": "Personal Care", "type": "wants", "percent": 5, "keywords": "haircut,salon,gym,spa"},
            {"name": "Travel", "type": "wants", "percent": 5, "keywords": "hotel,airbnb,airline,vacation"},
            {"name": "Savings", "type": "savings", "percent": 15, "keywords": "transfer,savings"},
            {"name": "Debt Payment", "type": "savings", "percent": 5, "keywords": "loan,credit card payment"}
        ]
    },
    "zero_based": {
        "id": "zero_based",
        "name": "Zero-Based Budget",
        "short_description": "Every dollar has a job",
        "description": "With zero-based budgeting, you assign every dollar of income to a specific category until you have $0 left to allocate. This ensures complete control over your money and eliminates wasteful spending. Popular method taught by Dave Ramsey.",
        "best_for": ["Detail-oriented people", "Those paying off debt", "People who want maximum control"],
        "pros": ["Complete spending awareness", "Great for debt payoff", "No money 'leaks'"],
        "cons": ["Requires more time", "Can feel restrictive", "Needs regular adjustments"],
        "allocation": {
            "giving": 10,
            "saving": 10,
            "housing": 25,
            "utilities": 10,
            "food": 10,
            "transportation": 10,
            "health": 5,
            "insurance": 10,
            "personal": 5,
            "recreation": 5
        },
        "categories": [
            {"name": "Giving/Charity", "type": "fixed", "percent": 10, "keywords": "donation,charity,tithe,church"},
            {"name": "Emergency Fund", "type": "savings", "percent": 5, "keywords": "emergency,savings"},
            {"name": "Retirement", "type": "savings", "percent": 5, "keywords": "401k,ira,retirement"},
            {"name": "Housing", "type": "fixed", "percent": 25, "keywords": "rent,mortgage,hoa"},
            {"name": "Utilities", "type": "fixed", "percent": 5, "keywords": "electric,gas,water,trash,internet"},
            {"name": "Phone", "type": "fixed", "percent": 2, "keywords": "phone,cell,mobile,verizon,att"},
            {"name": "Groceries", "type": "variable", "percent": 8, "keywords": "grocery,supermarket"},
            {"name": "Restaurants", "type": "variable", "percent": 4, "keywords": "restaurant,dining,takeout"},
            {"name": "Gas & Transportation", "type": "variable", "percent": 8, "keywords": "gas,fuel,parking,tolls"},
            {"name": "Car Payment", "type": "fixed", "percent": 5, "keywords": "car payment,auto loan"},
            {"name": "Car Insurance", "type": "fixed", "percent": 3, "keywords": "car insurance,auto insurance"},
            {"name": "Health Insurance", "type": "fixed", "percent": 5, "keywords": "health insurance,medical"},
            {"name": "Medical", "type": "variable", "percent": 2, "keywords": "doctor,pharmacy,medical"},
            {"name": "Clothing", "type": "variable", "percent": 3, "keywords": "clothing,clothes,shoes"},
            {"name": "Personal", "type": "variable", "percent": 3, "keywords": "haircut,personal,toiletries"},
            {"name": "Entertainment", "type": "variable", "percent": 5, "keywords": "entertainment,netflix,spotify,fun"},
            {"name": "Debt Payoff", "type": "debt", "percent": 2, "keywords": "loan,debt,credit card"}
        ]
    },
    "pay_yourself_first": {
        "id": "pay_yourself_first",
        "name": "Pay Yourself First",
        "short_description": "Savings-focused",
        "description": "This method prioritizes saving by automatically setting aside a portion of income before paying bills or spending. The idea is simple: treat savings as a non-negotiable expense. Great for building wealth over time.",
        "best_for": ["Wealth builders", "People who struggle to save", "Those with stable expenses"],
        "pros": ["Builds savings automatically", "Simple concept", "Flexible spending"],
        "cons": ["Requires income stability", "May not help with overspending", "Less detailed tracking"],
        "allocation": {
            "savings_first": 20,
            "everything_else": 80
        },
        "categories": [
            {"name": "Savings (Auto)", "type": "savings", "percent": 10, "keywords": "auto transfer,savings"},
            {"name": "Investments", "type": "savings", "percent": 10, "keywords": "investment,brokerage,stocks"},
            {"name": "Fixed Bills", "type": "fixed", "percent": 40, "keywords": "rent,mortgage,utilities,insurance"},
            {"name": "Daily Living", "type": "variable", "percent": 25, "keywords": "groceries,gas,food,transport"},
            {"name": "Discretionary", "type": "wants", "percent": 15, "keywords": "entertainment,shopping,dining"}
        ]
    },
    "envelope": {
        "id": "envelope",
        "name": "Envelope System",
        "short_description": "Cash-based categories",
        "description": "The envelope system allocates cash to specific spending categories. When an envelope is empty, you stop spending in that category. While traditionally cash-based, this digital version tracks virtual 'envelopes' for each category.",
        "best_for": ["Visual learners", "Overspenders", "Those who need hard limits"],
        "pros": ["Prevents overspending", "Highly visual", "Creates awareness"],
        "cons": ["Requires discipline", "Many categories to track", "Less flexible"],
        "allocation": {
            "by_category": True
        },
        "categories": [
            {"name": "Groceries", "type": "envelope", "percent": 12, "keywords": "grocery,food,supermarket"},
            {"name": "Gas", "type": "envelope", "percent": 8, "keywords": "gas,fuel"},
            {"name": "Dining Out", "type": "envelope", "percent": 5, "keywords": "restaurant,coffee,cafe"},
            {"name": "Entertainment", "type": "envelope", "percent": 5, "keywords": "movies,games,fun"},
            {"name": "Clothing", "type": "envelope", "percent": 4, "keywords": "clothes,shoes,apparel"},
            {"name": "Personal Care", "type": "envelope", "percent": 3, "keywords": "haircut,beauty,salon"},
            {"name": "Gifts", "type": "envelope", "percent": 3, "keywords": "gift,present,birthday"},
            {"name": "Household", "type": "envelope", "percent": 5, "keywords": "home,supplies,cleaning"},
            {"name": "Medical", "type": "envelope", "percent": 5, "keywords": "doctor,pharmacy,health"},
            {"name": "Savings", "type": "savings", "percent": 15, "keywords": "savings,transfer"},
            {"name": "Fixed Expenses", "type": "fixed", "percent": 35, "keywords": "rent,utilities,insurance,bills"}
        ]
    },
    "minimalist": {
        "id": "minimalist",
        "name": "Minimalist Budget",
        "short_description": "Just the essentials",
        "description": "A simplified approach with only a few categories. Perfect for people who find detailed budgets overwhelming. Focus on the big picture: bills, spending money, and savings.",
        "best_for": ["Busy people", "Those overwhelmed by detail", "People who want low maintenance"],
        "pros": ["Very simple", "Low maintenance", "Easy to stick to"],
        "cons": ["Less detailed insights", "May miss spending patterns", "Less control"],
        "allocation": {
            "bills": 50,
            "spending": 30,
            "savings": 20
        },
        "categories": [
            {"name": "Bills & Essentials", "type": "fixed", "percent": 50, "keywords": "rent,mortgage,utilities,insurance,groceries,gas"},
            {"name": "Spending Money", "type": "variable", "percent": 30, "keywords": "shopping,dining,entertainment,personal"},
            {"name": "Savings & Goals", "type": "savings", "percent": 20, "keywords": "savings,investment,transfer"}
        ]
    }
}


class OnboardingSetupRequest(BaseModel):
    budgeting_method: str
    monthly_income: float
    accounts: Optional[List[dict]] = None
    category_adjustments: Optional[Dict[str, float]] = None  # category_name -> budget_amount
    savings_target_percent: float = 20
    emergency_fund_months: int = 6


@app.get("/api/onboarding/methods")
def get_budgeting_methods():
    """Get all available budgeting methods with descriptions."""
    methods = []
    for method_id, method in BUDGETING_METHODS.items():
        methods.append({
            "id": method["id"],
            "name": method["name"],
            "short_description": method["short_description"],
            "description": method["description"],
            "best_for": method["best_for"],
            "pros": method["pros"],
            "cons": method["cons"]
        })
    return methods


@app.get("/api/onboarding/methods/{method_id}")
def get_budgeting_method_details(method_id: str):
    """Get detailed info for a specific budgeting method including category templates."""
    if method_id not in BUDGETING_METHODS:
        raise HTTPException(status_code=404, detail="Budgeting method not found")
    return BUDGETING_METHODS[method_id]


@app.get("/api/onboarding/methods/{method_id}/preview")
def preview_budget_allocation(
    method_id: str,
    monthly_income: float = Query(..., description="Monthly after-tax income")
):
    """Preview how income would be allocated with a specific budgeting method."""
    if method_id not in BUDGETING_METHODS:
        raise HTTPException(status_code=404, detail="Budgeting method not found")

    method = BUDGETING_METHODS[method_id]
    categories_preview = []

    for cat in method["categories"]:
        budget = round(monthly_income * (cat["percent"] / 100), 2)
        categories_preview.append({
            "name": cat["name"],
            "type": cat["type"],
            "percent": cat["percent"],
            "budget_amount": budget,
            "keywords": cat.get("keywords", "")
        })

    # Calculate totals by type
    totals = {}
    for cat in categories_preview:
        cat_type = cat["type"]
        if cat_type not in totals:
            totals[cat_type] = 0
        totals[cat_type] += cat["budget_amount"]

    return {
        "method": method["name"],
        "monthly_income": monthly_income,
        "categories": categories_preview,
        "totals_by_type": totals
    }


@app.post("/api/onboarding/setup")
def apply_onboarding_setup(
    request: OnboardingSetupRequest,
    service: BudgetService = Depends(get_service)
):
    """Apply the complete onboarding setup - creates categories, accounts, and budget settings."""
    if request.budgeting_method not in BUDGETING_METHODS:
        raise HTTPException(status_code=400, detail="Invalid budgeting method")

    method = BUDGETING_METHODS[request.budgeting_method]
    results = {"categories_created": 0, "accounts_created": 0}

    # Update budget settings (one at a time)
    service.store.update_budget_setting("monthly_income", request.monthly_income)
    service.store.update_budget_setting("savings_target_percent", request.savings_target_percent)
    service.store.update_budget_setting("emergency_fund_months", request.emergency_fund_months)
    service.store.update_budget_setting("budgeting_method", request.budgeting_method)

    # Create categories from template
    for cat in method["categories"]:
        # Check for custom adjustment
        budget = round(request.monthly_income * (cat["percent"] / 100), 2)
        if request.category_adjustments and cat["name"] in request.category_adjustments:
            budget = request.category_adjustments[cat["name"]]

        try:
            service.store.add_category(
                name=cat["name"],
                keywords=cat.get("keywords"),
                budget_amount=budget
            )
            results["categories_created"] += 1
        except Exception:
            # Category might already exist - update it instead
            existing = service.store.get_category_by_name(cat["name"])
            if existing:
                service.store.update_category(
                    existing["id"],
                    keywords=cat.get("keywords"),
                    budget_amount=budget
                )

    # Create accounts if provided
    if request.accounts:
        for acc in request.accounts:
            try:
                service.store.add_account(
                    name=acc["name"],
                    institution=acc.get("institution"),
                    account_type=acc.get("account_type", "checking"),
                    last_four=acc.get("last_four"),
                    color=acc.get("color", "#3B82F6"),
                    initial_balance=acc.get("initial_balance", 0)
                )
                results["accounts_created"] += 1
            except Exception:
                pass  # Skip if account already exists

    # Mark onboarding as complete
    service.store.complete_onboarding()

    return {
        "success": True,
        "message": f"Setup complete! Created {results['categories_created']} categories and {results['accounts_created']} accounts.",
        "results": results,
        "method_applied": method["name"]
    }


@app.get("/api/onboarding/tips")
def get_money_management_tips():
    """Get educational tips about money management."""
    return {
        "emergency_fund": {
            "title": "Emergency Fund",
            "description": "An emergency fund is money set aside for unexpected expenses like medical bills, car repairs, or job loss.",
            "recommendation": "Aim to save 3-6 months of essential expenses.",
            "tips": [
                "Start small - even $500 can cover minor emergencies",
                "Keep it in a high-yield savings account for easy access",
                "Only use it for true emergencies, not planned expenses"
            ]
        },
        "debt_payoff": {
            "title": "Debt Payoff Strategies",
            "description": "There are two popular methods for paying off debt.",
            "methods": {
                "avalanche": {
                    "name": "Debt Avalanche",
                    "description": "Pay off highest interest debt first",
                    "pros": "Saves the most money on interest",
                    "cons": "May take longer to see progress"
                },
                "snowball": {
                    "name": "Debt Snowball",
                    "description": "Pay off smallest balance first",
                    "pros": "Quick wins build motivation",
                    "cons": "May pay more interest overall"
                }
            }
        },
        "savings_goals": {
            "title": "Setting Savings Goals",
            "description": "Having specific goals makes saving easier and more rewarding.",
            "common_goals": [
                {"name": "Emergency Fund", "typical_target": "3-6 months expenses", "priority": "High"},
                {"name": "Vacation", "typical_target": "$1,000-5,000", "priority": "Medium"},
                {"name": "New Car", "typical_target": "$5,000-15,000", "priority": "Medium"},
                {"name": "Home Down Payment", "typical_target": "10-20% of home price", "priority": "Long-term"},
                {"name": "Retirement", "typical_target": "15% of income yearly", "priority": "Ongoing"}
            ]
        },
        "spending_awareness": {
            "title": "Spending Awareness",
            "description": "Understanding where your money goes is the first step to financial health.",
            "tips": [
                "Track every expense for at least one month",
                "Review subscriptions regularly - cancel unused ones",
                "Wait 24-48 hours before impulse purchases over $50",
                "Use the 'cost per use' mindset for big purchases"
            ]
        }
    }


@app.post("/api/onboarding/csv-upload")
async def upload_csv_for_onboarding(file: UploadFile = File(...)):
    """Upload CSV and get column headers for user to map.

    Step 1 of CSV import: User uploads file, we return columns and sample data.
    User then selects which columns are Date, Amount, Description.
    """
    # Generate session ID
    session_id = str(uuid.uuid4())

    # Save to temp location
    suffix = Path(file.filename).suffix if file.filename else ".csv"
    import_dir = Path(tempfile.gettempdir()) / "budget_imports"
    import_dir.mkdir(exist_ok=True)
    tmp_path = import_dir / f"{session_id}{suffix}"

    content = await file.read()
    tmp_path.write_bytes(content)

    try:
        # Read the CSV to get columns and sample data
        if suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(tmp_path)
        else:
            df = pd.read_csv(tmp_path)

        if df.empty:
            tmp_path.unlink(missing_ok=True)
            raise HTTPException(status_code=400, detail="CSV file is empty")

        # Get column names (strip whitespace) and rename in DataFrame
        df.columns = [str(c).strip() for c in df.columns]
        columns = df.columns.tolist()

        # Get sample rows (first 5)
        sample_rows = []
        for _, row in df.head(5).iterrows():
            sample_rows.append({col: str(row[col]) for col in columns})

        # Store session
        _import_sessions[session_id] = {
            "file_path": str(tmp_path),
            "columns": columns,
            "total_rows": len(df)
        }

        return {
            "session_id": session_id,
            "filename": file.filename,
            "columns": columns,
            "total_rows": len(df),
            "sample_rows": sample_rows
        }

    except HTTPException:
        raise
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"Failed to read CSV: {str(e)}")


class ColumnMapping(BaseModel):
    session_id: str
    date_column: str
    amount_column: str
    description_column: str


@app.post("/api/onboarding/analyze-csv")
async def analyze_csv_for_onboarding(mapping: ColumnMapping):
    """Analyze CSV with user-provided column mappings.

    Step 2 of CSV import: User has selected columns, now we analyze.
    Returns income and spending breakdown.
    """
    from collections import defaultdict
    from datetime import datetime

    session = _import_sessions.get(mapping.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found. Please upload CSV again.")

    tmp_path = Path(session["file_path"])
    if not tmp_path.exists():
        raise HTTPException(status_code=404, detail="CSV file not found. Please upload again.")

    try:
        # Read CSV
        suffix = tmp_path.suffix.lower()
        if suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(tmp_path)
        else:
            df = pd.read_csv(tmp_path)

        # Strip column name whitespace for matching
        df.columns = [str(c).strip() for c in df.columns]

        date_col = mapping.date_column
        amount_col = mapping.amount_column
        desc_col = mapping.description_column

        # Parse transactions
        transactions = []
        for _, row in df.iterrows():
            try:
                # Get amount - handle various formats
                amt_str = str(row[amount_col]).replace("$", "").replace(",", "").strip()
                if amt_str.startswith("(") and amt_str.endswith(")"):
                    amt = -float(amt_str[1:-1])
                elif amt_str:
                    amt = float(amt_str)
                else:
                    continue

                merchant = str(row.get(desc_col, "")).upper()
                date_str = str(row.get(date_col, ""))

                if amt != 0:
                    transactions.append({
                        "amount": amt,
                        "merchant": merchant,
                        "date": date_str
                    })
            except (ValueError, TypeError):
                continue

        # Calculate income (positive amounts)
        income_txns = [t for t in transactions if t["amount"] > 0]
        total_income = sum(t["amount"] for t in income_txns)

        # Calculate spending (negative amounts)
        expense_txns = [t for t in transactions if t["amount"] < 0]
        total_spending = abs(sum(t["amount"] for t in expense_txns))

        # Estimate months from date range
        num_months = 1
        try:
            dates = []
            for t in transactions:
                if t["date"]:
                    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y", "%Y/%m/%d"]:
                        try:
                            dates.append(datetime.strptime(t["date"].split()[0], fmt))
                            break
                        except:
                            continue
            if dates:
                date_range = (max(dates) - min(dates)).days
                num_months = max(1, date_range / 30)
                min_date = min(dates).strftime("%Y-%m-%d")
                max_date = max(dates).strftime("%Y-%m-%d")
            else:
                min_date = max_date = "N/A"
        except:
            min_date = max_date = "N/A"

        estimated_monthly_income = round(total_income / num_months, 2)
        estimated_monthly_spending = round(total_spending / num_months, 2)

        # Categorize spending by merchant keywords
        category_keywords = {
            "Housing": ["rent", "mortgage", "hoa", "property", "landlord"],
            "Utilities": ["electric", "gas", "water", "internet", "comcast", "verizon", "att", "t-mobile", "phone"],
            "Groceries": ["grocery", "kroger", "publix", "walmart", "target", "costco", "aldi", "trader joe", "whole foods", "safeway", "fred meyer", "winco"],
            "Dining Out": ["restaurant", "cafe", "coffee", "starbucks", "mcdonald", "chipotle", "doordash", "grubhub", "uber eats", "pizza", "jack in the box", "taco", "wendy", "burger"],
            "Transportation": ["gas", "shell", "exxon", "chevron", "bp", "uber", "lyft", "parking", "toll", "fuel"],
            "Shopping": ["amazon", "ebay", "etsy", "best buy", "home depot", "lowes", "clothing", "nike", "nordstrom"],
            "Entertainment": ["netflix", "spotify", "hulu", "disney", "hbo", "movie", "theater", "concert", "ticket"],
            "Healthcare": ["pharmacy", "cvs", "walgreens", "doctor", "medical", "hospital", "dental", "health"],
            "Insurance": ["insurance", "geico", "progressive", "allstate", "state farm"],
            "Subscriptions": ["subscription", "membership", "gym", "patreon", "apple.com", "google"],
            "Travel": ["airbnb", "hotel", "airline", "flight", "booking", "expedia", "vrbo"]
        }

        spending_by_category = defaultdict(float)
        uncategorized = 0

        for t in expense_txns:
            categorized = False
            merchant = t["merchant"].lower()
            for category, keywords in category_keywords.items():
                if any(kw in merchant for kw in keywords):
                    spending_by_category[category] += abs(t["amount"])
                    categorized = True
                    break
            if not categorized:
                uncategorized += abs(t["amount"])

        if uncategorized > 0:
            spending_by_category["Other"] = uncategorized

        # Convert to monthly averages
        monthly_spending = {
            cat: round(amt / num_months, 2)
            for cat, amt in spending_by_category.items()
        }

        return {
            "session_id": mapping.session_id,
            "total_transactions": len(transactions),
            "date_range": {"start": min_date, "end": max_date, "months": round(num_months, 1)},
            "income": {
                "total": round(total_income, 2),
                "count": len(income_txns),
                "estimated_monthly": estimated_monthly_income
            },
            "spending": {
                "total": round(total_spending, 2),
                "count": len(expense_txns),
                "estimated_monthly": estimated_monthly_spending,
                "by_category": monthly_spending
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to analyze: {str(e)}")


@app.post("/api/onboarding/import-csv")
def import_csv_transactions(
    mapping: ColumnMapping,
    service: BudgetService = Depends(get_service)
):
    """Import transactions from CSV into the database.

    Step 3 of CSV import: User has reviewed analysis, now import transactions.
    """
    from datetime import datetime

    session = _import_sessions.get(mapping.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found. Please upload CSV again.")

    tmp_path = Path(session["file_path"])
    if not tmp_path.exists():
        raise HTTPException(status_code=404, detail="CSV file not found. Please upload again.")

    try:
        # Read CSV
        suffix = tmp_path.suffix.lower()
        if suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(tmp_path)
        else:
            df = pd.read_csv(tmp_path)

        # Strip column name whitespace
        df.columns = [str(c).strip() for c in df.columns]

        date_col = mapping.date_column
        amount_col = mapping.amount_column
        desc_col = mapping.description_column

        imported = 0
        skipped = 0
        errors = 0

        for _, row in df.iterrows():
            try:
                # Parse amount
                amt_str = str(row[amount_col]).replace("$", "").replace(",", "").strip()
                if amt_str.startswith("(") and amt_str.endswith(")"):
                    amt = -float(amt_str[1:-1])
                elif amt_str:
                    amt = float(amt_str)
                else:
                    skipped += 1
                    continue

                if amt == 0:
                    skipped += 1
                    continue

                merchant = str(row.get(desc_col, "")).strip()
                date_str = str(row.get(date_col, "")).strip()

                # Parse date to standard format
                parsed_date = None
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d/%m/%Y", "%Y/%m/%d"]:
                    try:
                        parsed_date = datetime.strptime(date_str.split()[0], fmt)
                        break
                    except:
                        continue

                if not parsed_date:
                    # Try to use as-is if it looks like a date
                    parsed_date = date_str
                else:
                    parsed_date = parsed_date.strftime("%Y-%m-%d")

                # Add transaction (will skip duplicates)
                result = service.store.add_transaction(
                    date=str(parsed_date),
                    amount=amt,
                    merchant=merchant,
                    description=merchant[:100] if len(merchant) > 100 else merchant
                )

                if result:
                    imported += 1
                else:
                    skipped += 1  # Duplicate

            except Exception as e:
                errors += 1
                continue

        # Clean up temp file
        try:
            tmp_path.unlink(missing_ok=True)
            del _import_sessions[mapping.session_id]
        except:
            pass

        return {
            "success": True,
            "imported": imported,
            "skipped": skipped,
            "errors": errors,
            "total_processed": imported + skipped + errors
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to import: {str(e)}")


# === Admin API ===

@app.post("/api/admin/reset")
def reset_all_data(service: BudgetService = Depends(get_service)):
    """Reset all data - delete transactions, accounts, rules, and reset categories to defaults.

    This is a destructive operation and cannot be undone.
    """
    counts = service.store.reset_all_data()
    return {
        "success": True,
        "message": "All data has been reset",
        "deleted": counts
    }


# === Transaction Splitting Endpoints ===

@app.get("/api/transactions/{txn_id}/splits")
def get_transaction_splits(txn_id: int, service: BudgetService = Depends(get_service)):
    """Get all splits for a transaction."""
    return service.store.get_transaction_splits(txn_id)


@app.post("/api/transactions/{txn_id}/splits")
def create_transaction_splits(
    txn_id: int,
    request: TransactionSplitRequest,
    service: BudgetService = Depends(get_service)
):
    """Split a transaction across multiple categories.

    The sum of split amounts must equal the original transaction amount.
    """
    splits = [{"category_id": s.category_id, "amount": s.amount, "description": s.description}
              for s in request.splits]
    return service.store.create_transaction_splits(txn_id, splits)


@app.delete("/api/transactions/{txn_id}/splits")
def delete_transaction_splits(txn_id: int, service: BudgetService = Depends(get_service)):
    """Remove all splits from a transaction (unsplit it)."""
    return service.store.delete_transaction_splits(txn_id)


# === Savings Goals API ===

class SavingsGoalCreate(BaseModel):
    name: str
    target_amount: float
    description: Optional[str] = None
    target_date: Optional[str] = None
    color: str = '#10B981'
    icon: str = 'piggy-bank'


class SavingsGoalUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    target_amount: Optional[float] = None
    target_date: Optional[str] = None
    color: Optional[str] = None
    icon: Optional[str] = None
    is_active: Optional[bool] = None


class GoalContributionCreate(BaseModel):
    amount: float
    note: Optional[str] = None
    transaction_id: Optional[int] = None


@app.get("/api/goals")
def get_savings_goals(
    active_only: bool = Query(True),
    service: BudgetService = Depends(get_service)
):
    """Get all savings goals."""
    return service.store.get_all_savings_goals(active_only=active_only)


@app.post("/api/goals")
def create_savings_goal(
    request: SavingsGoalCreate,
    service: BudgetService = Depends(get_service)
):
    """Create a new savings goal."""
    goal_id = service.store.create_savings_goal(
        name=request.name,
        target_amount=request.target_amount,
        description=request.description,
        target_date=request.target_date,
        color=request.color,
        icon=request.icon
    )
    return service.store.get_savings_goal(goal_id)


@app.get("/api/goals/{goal_id}")
def get_savings_goal(
    goal_id: int,
    service: BudgetService = Depends(get_service)
):
    """Get a specific savings goal."""
    goal = service.store.get_savings_goal(goal_id)
    if not goal:
        raise HTTPException(status_code=404, detail="Goal not found")
    return goal


@app.put("/api/goals/{goal_id}")
def update_savings_goal(
    goal_id: int,
    request: SavingsGoalUpdate,
    service: BudgetService = Depends(get_service)
):
    """Update a savings goal."""
    success = service.store.update_savings_goal(
        goal_id=goal_id,
        name=request.name,
        description=request.description,
        target_amount=request.target_amount,
        target_date=request.target_date,
        color=request.color,
        icon=request.icon,
        is_active=request.is_active
    )
    if not success:
        raise HTTPException(status_code=404, detail="Goal not found or no changes made")
    return service.store.get_savings_goal(goal_id)


@app.delete("/api/goals/{goal_id}")
def delete_savings_goal(
    goal_id: int,
    service: BudgetService = Depends(get_service)
):
    """Delete a savings goal."""
    success = service.store.delete_savings_goal(goal_id)
    if not success:
        raise HTTPException(status_code=404, detail="Goal not found")
    return {"success": True}


@app.get("/api/goals/{goal_id}/contributions")
def get_goal_contributions(
    goal_id: int,
    limit: int = Query(50),
    service: BudgetService = Depends(get_service)
):
    """Get contributions for a savings goal."""
    return service.store.get_goal_contributions(goal_id, limit=limit)


@app.post("/api/goals/{goal_id}/contributions")
def add_goal_contribution(
    goal_id: int,
    request: GoalContributionCreate,
    service: BudgetService = Depends(get_service)
):
    """Add a contribution to a savings goal."""
    # Verify goal exists
    goal = service.store.get_savings_goal(goal_id)
    if not goal:
        raise HTTPException(status_code=404, detail="Goal not found")

    contribution_id = service.store.add_goal_contribution(
        goal_id=goal_id,
        amount=request.amount,
        note=request.note,
        transaction_id=request.transaction_id
    )
    # Return updated goal
    return service.store.get_savings_goal(goal_id)


@app.delete("/api/goals/{goal_id}/contributions/{contribution_id}")
def delete_goal_contribution(
    goal_id: int,
    contribution_id: int,
    service: BudgetService = Depends(get_service)
):
    """Delete a contribution from a savings goal."""
    success = service.store.delete_goal_contribution(contribution_id)
    if not success:
        raise HTTPException(status_code=404, detail="Contribution not found")
    return service.store.get_savings_goal(goal_id)


# Mount static files for frontend (if directory exists)
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/")
    def serve_index():
        """Serve the main index.html."""
        return FileResponse(static_dir / "index.html")
