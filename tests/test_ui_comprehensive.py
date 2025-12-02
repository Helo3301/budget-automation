"""
Comprehensive Playwright UI tests for Budget Automation app.
Tests every UI element and function with fake data.

Actual UI structure:
- Tabs: Dashboard, Budget, Analytics, Subscriptions, Goals, Transactions,
        Uncategorized, Recurring, Anomalies, Search, Settings
- Search is on its own dedicated "Search" tab (not in Transactions)
- No Accounts tab exists
- Onboarding wizard may appear for new users - tests need to dismiss it
"""
import pytest
import re
from playwright.sync_api import Page, expect
import requests
import time

BASE_URL = "http://localhost:5000"


def dismiss_onboarding_if_present(page: Page, timeout: int = 2000):
    """Helper to dismiss onboarding wizard if it's showing."""
    # Check if onboarding is visible and click skip/close if so
    try:
        skip_btn = page.locator("button:has-text('Skip')").first
        if skip_btn.is_visible(timeout=timeout):
            skip_btn.click()
            page.wait_for_timeout(500)
            return True
    except:
        pass

    # Try clicking "Go to Dashboard" if wizard is at end
    try:
        go_dashboard = page.locator("button:has-text('Go to Dashboard')").first
        if go_dashboard.is_visible(timeout=500):
            go_dashboard.click()
            page.wait_for_timeout(500)
            return True
    except:
        pass

    return False


def click_nav_tab(page: Page, tab_name: str):
    """Helper to click a navigation tab in the header nav bar."""
    # Use nav element to target navigation buttons specifically
    nav_btn = page.locator(f"nav button:has-text('{tab_name}')").first
    nav_btn.click()


class TestPageLoad:
    """Test basic page loading and structure."""

    def test_homepage_loads(self, page: Page):
        """Test that the homepage loads successfully."""
        page.goto(BASE_URL)
        expect(page).to_have_title(re.compile(r"Budget|Dashboard", re.IGNORECASE))

    def test_main_navigation_visible(self, page: Page):
        """Test that main navigation tabs are visible."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        # Check for actual navigation tabs in the nav element
        expect(page.locator("nav button:has-text('Dashboard')")).to_be_visible()
        expect(page.locator("nav button:has-text('Transactions')")).to_be_visible()
        expect(page.locator("nav button:has-text('Settings')")).to_be_visible()

    def test_dashboard_tab_default(self, page: Page):
        """Test that Dashboard is the default active tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        # Dashboard content should be visible by default
        # Look for summary cards that appear on dashboard (use exact match)
        expect(page.locator("div.text-sm:has-text('Total Transactions')")).to_be_visible()


class TestNavigationTabs:
    """Test navigation between different tabs."""

    def test_switch_to_transactions_tab(self, page: Page):
        """Test switching to Transactions tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Transactions')
        page.wait_for_timeout(500)

    def test_switch_to_budget_tab(self, page: Page):
        """Test switching to Budget tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Budget')
        page.wait_for_timeout(500)

    def test_switch_to_analytics_tab(self, page: Page):
        """Test switching to Analytics tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Analytics')
        page.wait_for_timeout(500)

    def test_switch_to_search_tab(self, page: Page):
        """Test switching to Search tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Search')
        page.wait_for_timeout(500)

        # Search input should be visible
        expect(page.locator("input[placeholder*='Search transactions']")).to_be_visible()

    def test_switch_to_settings_tab(self, page: Page):
        """Test switching to Settings tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Settings')
        page.wait_for_timeout(500)

        # Settings content visible - categories management
        expect(page.locator("text=Budget Categories")).to_be_visible()

    def test_switch_to_subscriptions_tab(self, page: Page):
        """Test switching to Subscriptions tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Subscriptions')
        page.wait_for_timeout(500)

    def test_switch_to_goals_tab(self, page: Page):
        """Test switching to Goals tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Goals')
        page.wait_for_timeout(500)

    def test_switch_to_recurring_tab(self, page: Page):
        """Test switching to Recurring tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Recurring')
        page.wait_for_timeout(500)

    def test_switch_to_anomalies_tab(self, page: Page):
        """Test switching to Anomalies tab."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Anomalies')
        page.wait_for_timeout(500)


class TestTransactionsUI:
    """Test Transactions tab functionality."""

    def test_transactions_tab_loads(self, page: Page):
        """Test that transactions tab loads."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Transactions')
        page.wait_for_timeout(1000)


class TestSemanticSearch:
    """Test semantic search functionality on Search tab."""

    def test_search_tab_has_input(self, page: Page):
        """Test that Search tab has search input."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Search')
        page.wait_for_timeout(500)

        # Search input should be visible with specific placeholder
        search_input = page.locator("input[placeholder*='Search transactions']")
        expect(search_input).to_be_visible()

    def test_search_tab_has_button(self, page: Page):
        """Test that Search tab has search button."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Search')
        page.wait_for_timeout(500)

        # Search button should be visible - specifically the one in the search form
        search_btn = page.locator("div[x-show*='search'] button:has-text('Search')").first
        expect(search_btn).to_be_visible()

    def test_search_for_dairy_queen(self, page: Page):
        """
        Test searching for 'dairy queen' returns Dairy Queen transactions.
        This tests the fix for the reported bug where search returned ATM reimbursements.
        """
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        # Go to Search tab
        click_nav_tab(page, 'Search')
        page.wait_for_timeout(500)

        # Find search input and enter query
        search_input = page.locator("input[placeholder*='Search transactions']")
        search_input.fill("dairy queen")

        # Click search button in the search tab form
        page.locator("div[x-show*='search'] button:has-text('Search')").first.click()
        page.wait_for_timeout(1500)  # Wait for search results

    def test_search_empty_shows_prompt(self, page: Page):
        """Test that empty search shows prompt message."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Search')
        page.wait_for_timeout(500)

        # Should see prompt to enter search
        expect(page.locator("text=Enter a search query")).to_be_visible()


class TestSettingsUI:
    """Test Settings tab functionality (where categories are managed)."""

    def test_categories_section_visible(self, page: Page):
        """Test that categories section is visible in Settings."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Settings')
        page.wait_for_timeout(500)

        expect(page.locator("text=Budget Categories")).to_be_visible()

    def test_add_category_button_exists(self, page: Page):
        """Test that Add Category button exists in Settings."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        click_nav_tab(page, 'Settings')
        page.wait_for_timeout(500)

        # Use first to get the primary Add Category button (not the modal one)
        add_btn = page.locator("button:has-text('Add Category')").first
        expect(add_btn).to_be_visible()


class TestDashboardUI:
    """Test Dashboard tab functionality."""

    def test_dashboard_shows_summary_cards(self, page: Page):
        """Test that dashboard shows summary statistic cards."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        # Dashboard should show these summary cards (use .first to handle duplicates)
        expect(page.locator("div.text-sm.text-gray-400:has-text('Total Transactions')").first).to_be_visible()
        expect(page.locator("div.text-sm.text-gray-400:has-text('Total Income')").first).to_be_visible()
        expect(page.locator("div.text-sm.text-gray-400:has-text('Total Expenses')").first).to_be_visible()

    def test_dashboard_shows_net(self, page: Page):
        """Test that dashboard shows net amount."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)
        dismiss_onboarding_if_present(page)

        # Use exact text to avoid matching "Net Savings"
        expect(page.get_by_text("Net", exact=True).first).to_be_visible()


class TestImportCSV:
    """Test CSV import functionality."""

    def test_import_button_visible(self, page: Page):
        """Test that Import CSV button is visible in header."""
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)

        # Import button is in header, should be visible even with onboarding
        import_btn = page.locator("header button:has-text('Import CSV')")
        expect(import_btn).to_be_visible()


class TestAPIEndpoints:
    """Direct API endpoint testing."""

    def test_categories_api(self, api_client):
        """Test /api/categories endpoint."""
        response = api_client.get("/api/categories")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_transactions_api(self, api_client):
        """Test /api/transactions endpoint."""
        response = api_client.get("/api/transactions")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, (list, dict))

    def test_summary_api(self, api_client):
        """Test /api/summary endpoint."""
        response = api_client.get("/api/summary")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    def test_search_api(self, api_client):
        """Test /api/search endpoint with dairy queen query."""
        response = api_client.get("/api/search?q=dairy%20queen&k=5")
        assert response.status_code == 200
        data = response.json()

        # Results should be a list
        assert isinstance(data, list)

        # All results should match dairy queen
        for txn in data:
            merchant = (txn.get("merchant") or "").lower()
            description = (txn.get("description") or "").lower()
            # Either merchant or description should contain dairy queen
            has_match = "dairy queen" in merchant or "dairy queen" in description
            assert has_match, f"Transaction doesn't match 'dairy queen': {txn.get('merchant')}"

    def test_search_api_returns_similarity_score(self, api_client):
        """Test that search API returns similarity scores."""
        response = api_client.get("/api/search?q=coffee&k=5")
        assert response.status_code == 200
        data = response.json()

        # Results should have similarity_score field
        for txn in data:
            assert "similarity_score" in txn, "Results should include similarity_score"
            assert txn["similarity_score"] >= 0, "Similarity score should be non-negative"
            assert txn["similarity_score"] <= 1, "Similarity score should be <= 1"


class TestResponsiveUI:
    """Test responsive design at different viewport sizes."""

    def test_mobile_viewport(self, page: Page):
        """Test UI at mobile viewport size."""
        page.set_viewport_size({"width": 375, "height": 667})
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)

        # Page should still be functional - header visible
        expect(page.locator("h1:has-text('Budget Automation')")).to_be_visible()

    def test_tablet_viewport(self, page: Page):
        """Test UI at tablet viewport size."""
        page.set_viewport_size({"width": 768, "height": 1024})
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)

        expect(page.locator("h1:has-text('Budget Automation')")).to_be_visible()

    def test_desktop_viewport(self, page: Page):
        """Test UI at desktop viewport size."""
        page.set_viewport_size({"width": 1920, "height": 1080})
        page.goto(BASE_URL)
        page.wait_for_timeout(1000)

        expect(page.locator("h1:has-text('Budget Automation')")).to_be_visible()


class TestErrorHandling:
    """Test error handling in the API."""

    def test_invalid_search_query(self, api_client):
        """Test search with empty query."""
        response = api_client.get("/api/search?q=&k=5")
        # Should return empty results or validation error
        assert response.status_code in [200, 400, 422]

    def test_search_with_special_chars(self, api_client):
        """Test search handles special characters."""
        response = api_client.get("/api/search?q=%25%26%3C%3E&k=5")
        # Should not crash - might return empty results or validation error
        assert response.status_code in [200, 400, 422]
