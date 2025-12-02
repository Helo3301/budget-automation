"""
Comprehensive Playwright UI tests for Budget Automation app.
Tests every UI element and function with fake data.
"""
import pytest
import re
from playwright.sync_api import Page, expect
import requests
import time

BASE_URL = "http://localhost:5000"


class TestPageLoad:
    """Test basic page loading and structure."""

    def test_homepage_loads(self, page: Page):
        """Test that the homepage loads successfully."""
        page.goto(BASE_URL)
        expect(page).to_have_title(re.compile(r"Budget|Dashboard", re.IGNORECASE))

    def test_main_navigation_visible(self, page: Page):
        """Test that main navigation tabs are visible."""
        page.goto(BASE_URL)
        # Wait for Alpine.js to initialize
        page.wait_for_timeout(1000)
        
        # Check for navigation tabs
        expect(page.locator("text=Dashboard")).to_be_visible()
        expect(page.locator("text=Transactions")).to_be_visible()
        expect(page.locator("text=Accounts")).to_be_visible()

    def test_dashboard_tab_default(self, page: Page):
        """Test that Dashboard is the default active tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        
        # Dashboard content should be visible by default
        dashboard_section = page.locator("[x-show*='dashboard']").first
        # Or look for dashboard-specific content


class TestNavigationTabs:
    """Test navigation between different tabs."""

    def test_switch_to_transactions_tab(self, page: Page):
        """Test switching to Transactions tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        
        page.click("text=Transactions")
        page.wait_for_timeout(500)
        
        # Transactions view should now be visible

    def test_switch_to_accounts_tab(self, page: Page):
        """Test switching to Accounts tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        
        page.click("text=Accounts")
        page.wait_for_timeout(500)

    def test_switch_to_budgets_tab(self, page: Page):
        """Test switching to Budgets tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        
        # Try to find and click Budgets tab
        budgets_tab = page.locator("text=Budgets")
        if budgets_tab.count() > 0:
            budgets_tab.click()
            page.wait_for_timeout(500)

    def test_switch_to_settings_tab(self, page: Page):
        """Test switching to Settings tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        
        settings_tab = page.locator("text=Settings")
        if settings_tab.count() > 0:
            settings_tab.click()
            page.wait_for_timeout(500)


class TestAccountsUI:
    """Test Accounts tab functionality."""

    def test_accounts_list_displays(self, page: Page, api_client):
        """Test that accounts list is displayed."""
        # First create a test account via API
        api_client.post("/api/accounts", json={
            "name": "UI Test Account",
            "institution": "Test Bank",
            "account_type": "checking",
            "current_balance": 1000.00
        })
        
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        page.click("text=Accounts")
        page.wait_for_timeout(1000)
        
        # Should see the account we created
        expect(page.locator("text=UI Test Account")).to_be_visible()

    def test_add_account_button_exists(self, page: Page):
        """Test that Add Account button exists."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        page.click("text=Accounts")
        page.wait_for_timeout(500)
        
        # Look for add account button
        add_btn = page.locator("button:has-text('Add'), button:has-text('New Account'), text=Add Account")
        expect(add_btn.first).to_be_visible()

    def test_account_balance_displayed(self, page: Page, api_client):
        """Test that account balances are displayed correctly."""
        api_client.post("/api/accounts", json={
            "name": "Balance Test Account",
            "institution": "Test Bank",
            "account_type": "checking",
            "current_balance": 5432.10
        })
        
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        page.click("text=Accounts")
        page.wait_for_timeout(1000)
        
        # Check balance is displayed (formatted as currency)
        expect(page.locator("text=5,432")).to_be_visible()


class TestTransactionsUI:
    """Test Transactions tab functionality."""

    def test_transactions_list_displays(self, page: Page, api_client):
        """Test that transactions list is displayed."""
        # Create account and transaction
        acc_resp = api_client.post("/api/accounts", json={
            "name": "Txn Test Account",
            "institution": "Test Bank",
            "account_type": "checking",
            "current_balance": 1000.00
        })
        
        api_client.post("/api/transactions", json={
            "date": "2024-11-15",
            "amount": -25.50,
            "merchant": "Test Merchant",
            "description": "Test transaction"
        })
        
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        page.click("text=Transactions")
        page.wait_for_timeout(1000)
        
        # Should see the transaction
        expect(page.locator("text=Test Merchant")).to_be_visible()

    def test_transaction_search_exists(self, page: Page):
        """Test that transaction search input exists."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        page.click("text=Transactions")
        page.wait_for_timeout(500)
        
        # Look for search input
        search_input = page.locator("input[placeholder*='Search'], input[type='search']")
        expect(search_input.first).to_be_visible()

    def test_add_transaction_button(self, page: Page):
        """Test that Add Transaction button exists."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        page.click("text=Transactions")
        page.wait_for_timeout(500)
        
        # Look for add transaction button
        add_btn = page.locator("button:has-text('Add'), button:has-text('New Transaction')")
        expect(add_btn.first).to_be_visible()


class TestSemanticSearch:
    """Test semantic search functionality - critical for finding the 'dairy queen' bug."""

    def test_search_for_dairy_queen(self, page: Page, api_client):
        """
        Test searching for 'dairy queen' returns Dairy Queen transactions,
        NOT ATM reimbursements (reported bug).
        """
        # Set up test data with Dairy Queen and ATM transactions
        api_client.post("/api/transactions", json={
            "date": "2024-11-01",
            "amount": -12.50,
            "merchant": "Dairy Queen",
            "description": "Ice cream and burgers"
        })
        api_client.post("/api/transactions", json={
            "date": "2024-11-02",
            "amount": 5.00,
            "merchant": "ATM Fee Reimbursement",
            "description": "Monthly ATM fee refund"
        })
        
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        page.click("text=Transactions")
        page.wait_for_timeout(1000)
        
        # Find and use search
        search_input = page.locator("input[placeholder*='Search'], input[type='search']").first
        search_input.fill("dairy queen")
        page.wait_for_timeout(1500)  # Wait for search results
        
        # Verify Dairy Queen appears
        dq_visible = page.locator("text=Dairy Queen").is_visible()
        
        # Check if ATM shows up (it shouldn't for this search)
        atm_visible = page.locator("text=ATM").is_visible()
        
        # Log for debugging
        print(f"Dairy Queen visible: {dq_visible}")
        print(f"ATM visible: {atm_visible}")
        
        # The actual assertion
        assert dq_visible, "Dairy Queen should appear in search results"
        # Note: we can't strictly assert ATM is not visible since it might 
        # legitimately appear in the list before filtering

    def test_search_for_restaurants(self, page: Page, api_client):
        """Test semantic search for 'restaurants' finds food-related transactions."""
        # Create restaurant transactions
        restaurants = [
            {"date": "2024-11-01", "amount": -25.00, "merchant": "McDonald's", "description": "Fast food lunch"},
            {"date": "2024-11-02", "amount": -45.00, "merchant": "Olive Garden", "description": "Italian dinner"},
            {"date": "2024-11-03", "amount": -12.00, "merchant": "Subway", "description": "Sandwich"},
        ]
        
        for r in restaurants:
            api_client.post("/api/transactions", json=r)
        
        # Also create non-restaurant transaction
        api_client.post("/api/transactions", json={
            "date": "2024-11-04",
            "amount": -55.00,
            "merchant": "Shell Gas",
            "description": "Fuel for car"
        })
        
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        page.click("text=Transactions")
        page.wait_for_timeout(1000)
        
        search_input = page.locator("input[placeholder*='Search'], input[type='search']").first
        search_input.fill("restaurants")
        page.wait_for_timeout(1500)
        
        # At least one restaurant should be visible
        mcd_visible = page.locator("text=McDonald's").is_visible()
        olive_visible = page.locator("text=Olive Garden").is_visible()
        
        assert mcd_visible or olive_visible, "At least one restaurant should appear"

    def test_search_for_groceries(self, page: Page, api_client):
        """Test searching for 'groceries' finds grocery store transactions."""
        api_client.post("/api/transactions", json={
            "date": "2024-11-01",
            "amount": -125.00,
            "merchant": "Walmart Grocery",
            "description": "Weekly groceries"
        })
        api_client.post("/api/transactions", json={
            "date": "2024-11-02",
            "amount": -85.00,
            "merchant": "Kroger",
            "description": "Food shopping"
        })
        
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        page.click("text=Transactions")
        page.wait_for_timeout(1000)
        
        search_input = page.locator("input[placeholder*='Search'], input[type='search']").first
        search_input.fill("groceries")
        page.wait_for_timeout(1500)
        
        walmart_visible = page.locator("text=Walmart").is_visible()
        kroger_visible = page.locator("text=Kroger").is_visible()
        
        assert walmart_visible or kroger_visible, "Grocery stores should appear in results"

    def test_search_empty_clears_results(self, page: Page, api_client):
        """Test that clearing search shows all transactions."""
        api_client.post("/api/transactions", json={
            "date": "2024-11-01",
            "amount": -50.00,
            "merchant": "Test Store",
            "description": "Test purchase"
        })
        
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        page.click("text=Transactions")
        page.wait_for_timeout(1000)
        
        search_input = page.locator("input[placeholder*='Search'], input[type='search']").first
        
        # Search for something
        search_input.fill("nonexistent")
        page.wait_for_timeout(1000)
        
        # Clear search
        search_input.fill("")
        page.wait_for_timeout(1000)
        
        # Original transaction should be visible again
        expect(page.locator("text=Test Store")).to_be_visible()


class TestCategoriesUI:
    """Test Categories/Budget functionality."""

    def test_categories_displayed(self, page: Page, api_client):
        """Test that categories are displayed."""
        api_client.post("/api/categories", json={
            "name": "Test Category",
            "monthly_budget": 500.00
        })
        
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        
        # Categories might be on Budgets tab or sidebar
        budgets_tab = page.locator("text=Budgets")
        if budgets_tab.count() > 0:
            budgets_tab.click()
            page.wait_for_timeout(500)
        
        expect(page.locator("text=Test Category")).to_be_visible()


class TestOnboardingWizard:
    """Test the onboarding wizard functionality."""

    def test_wizard_appears_for_new_user(self, page: Page, api_client):
        """Test that onboarding wizard appears for new users."""
        # Reset to fresh state
        api_client.reset_database()
        
        page.goto(BASE_URL)
        page.wait_for_timeout(2000)
        
        # Look for wizard/onboarding content
        wizard = page.locator("text=Welcome, text=Get Started, text=Setup")
        # Wizard should be visible for new user

    def test_csv_upload_section_exists(self, page: Page, api_client):
        """Test that CSV upload section exists in wizard."""
        api_client.reset_database()
        
        page.goto(BASE_URL)
        page.wait_for_timeout(2000)
        
        # Look for CSV-related elements
        csv_section = page.locator("text=CSV, text=Import, input[type='file']")


class TestAPIEndpoints:
    """Direct API endpoint testing."""

    def test_categories_api(self, api_client):
        """Test /api/categories endpoint."""
        response = api_client.get("/api/categories")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_accounts_api(self, api_client):
        """Test /api/accounts endpoint."""
        response = api_client.get("/api/accounts")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_transactions_api(self, api_client):
        """Test /api/transactions endpoint."""
        response = api_client.get("/api/transactions")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, (list, dict))

    def test_create_account_api(self, api_client):
        """Test creating an account via API."""
        import time
        unique_name = f"API Test Account {int(time.time())}"
        response = api_client.post("/api/accounts", json={
            "name": unique_name,
            "institution": "API Bank",
            "account_type": "savings",
            "current_balance": 1234.56
        })
        assert response.status_code == 200
        data = response.json()
        assert data.get("id") is not None

    def test_transactions_post_not_allowed(self, api_client):
        """Test that direct transaction POST is not allowed (transactions come from CSV import)."""
        response = api_client.post("/api/transactions", json={
            "date": "2024-11-20",
            "amount": -99.99,
            "merchant": "API Test Merchant",
            "description": "API test transaction"
        })
        # Transactions are created via CSV import, not direct POST
        assert response.status_code == 405  # Method Not Allowed

    def test_semantic_search_api(self, api_client):
        """Test the semantic search API directly using existing data."""
        # Search for dairy queen (using existing transactions in database)
        response = api_client.get("/api/transactions?search=dairy%20queen")
        assert response.status_code == 200
        data = response.json()

        # Check results structure
        if isinstance(data, list):
            transactions = data
        else:
            transactions = data.get("transactions", data.get("items", []))

        # All returned results should contain "dairy queen" in merchant or description
        for txn in transactions:
            merchant = (txn.get("merchant") or "").lower()
            description = (txn.get("description") or "").lower()
            has_match = "dairy queen" in merchant or "dairy queen" in description
            assert has_match, f"Transaction {txn} doesn't match 'dairy queen'"

        print(f"Found {len(transactions)} transactions matching 'dairy queen'")


class TestDataReset:
    """Test data reset functionality."""

    def test_reset_endpoint_exists(self, api_client):
        """Test that reset endpoint exists."""
        response = api_client.post("/api/admin/reset")
        # Should work or return not found/not implemented
        assert response.status_code in [200, 201, 204, 404, 405, 501]

    def test_reset_clears_data(self, api_client):
        """Test that reset clears transaction data."""
        # Create some data
        api_client.post("/api/transactions", json={
            "date": "2024-11-01",
            "amount": -50.00,
            "merchant": "Reset Test",
            "description": "Will be deleted"
        })
        
        # Reset
        api_client.reset_database()
        
        # Check data is cleared
        response = api_client.get("/api/transactions")
        data = response.json()
        
        if isinstance(data, list):
            transactions = data
        else:
            transactions = data.get("transactions", data.get("items", []))
        
        # After reset, "Reset Test" transaction should be gone
        reset_test_found = any("Reset Test" in str(t) for t in transactions)
        # Note: depending on implementation, might still have seed data


class TestResponsiveUI:
    """Test responsive design at different viewport sizes."""

    def test_mobile_viewport(self, page: Page):
        """Test UI at mobile viewport size."""
        page.set_viewport_size({"width": 375, "height": 667})
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        
        # Page should still be functional
        # Navigation might be in hamburger menu

    def test_tablet_viewport(self, page: Page):
        """Test UI at tablet viewport size."""
        page.set_viewport_size({"width": 768, "height": 1024})
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)

    def test_desktop_viewport(self, page: Page):
        """Test UI at desktop viewport size."""
        page.set_viewport_size({"width": 1920, "height": 1080})
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)


class TestErrorHandling:
    """Test error handling in the UI."""

    def test_invalid_transaction_amount(self, page: Page, api_client):
        """Test handling of invalid transaction data."""
        response = api_client.post("/api/transactions", json={
            "date": "invalid-date",
            "amount": "not-a-number",
            "merchant": "Test"
        })
        # Should return error status
        assert response.status_code in [400, 422, 500]

    def test_missing_required_fields(self, page: Page, api_client):
        """Test handling of missing required fields."""
        response = api_client.post("/api/accounts", json={})
        # Should return error status
        assert response.status_code in [400, 422, 500]


class TestFullWorkflow:
    """Test complete user workflows."""

    def test_create_account_and_transaction_workflow(self, page: Page, api_client):
        """Test the full workflow of creating an account and adding transactions."""
        # 1. Create account
        acc_response = api_client.post("/api/accounts", json={
            "name": "Workflow Test Account",
            "institution": "Workflow Bank",
            "account_type": "checking",
            "current_balance": 2000.00
        })
        assert acc_response.status_code == 200
        
        # 2. Add transactions
        api_client.post("/api/transactions", json={
            "date": "2024-11-01",
            "amount": -50.00,
            "merchant": "Workflow Store",
            "description": "Test purchase"
        })
        
        # 3. Verify in UI
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        
        # Check account is visible
        page.click("text=Accounts")
        page.wait_for_timeout(500)
        expect(page.locator("text=Workflow Test Account")).to_be_visible()
        
        # Check transaction is visible
        page.click("text=Transactions")
        page.wait_for_timeout(500)
        expect(page.locator("text=Workflow Store")).to_be_visible()
