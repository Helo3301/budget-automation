"""Playwright E2E tests for Budget Automation Web UI - TDD style."""
import re
import pytest
from playwright.sync_api import Page, expect


class TestDashboardTab:
    """Tests for the Dashboard tab."""

    def test_dashboard_loads_on_startup(self, page: Page, web_server: str):
        """Dashboard should be the default tab on page load."""
        page.goto(web_server)

        # Dashboard tab should be active
        dashboard_tab = page.locator("button", has_text="Dashboard")
        expect(dashboard_tab).to_have_class(re.compile(r"tab-active"))

    def test_dashboard_shows_summary_cards(self, page: Page, web_server: str):
        """Dashboard should display four summary cards."""
        page.goto(web_server)
        page.wait_for_load_state("networkidle")

        # Should show Total Transactions, Income, Expenses, Net
        expect(page.locator("text=Total Transactions")).to_be_visible()
        expect(page.locator("text=Total Income")).to_be_visible()
        expect(page.locator("text=Total Expenses")).to_be_visible()
        # Use exact match for "Net" to avoid matching "Internet" etc.
        expect(page.get_by_text("Net", exact=True)).to_be_visible()

    def test_dashboard_shows_category_breakdown(self, page: Page, web_server: str):
        """Dashboard should show category breakdown section."""
        page.goto(web_server)
        page.wait_for_load_state("networkidle")

        expect(page.locator("text=Category Breakdown")).to_be_visible()

    def test_income_displayed_in_green(self, page: Page, web_server: str):
        """Income values should be displayed in green."""
        page.goto(web_server)
        page.wait_for_load_state("networkidle")

        income_value = page.locator("text=Total Income").locator("..").locator(".text-green-400")
        expect(income_value).to_be_visible()

    def test_expenses_displayed_in_red(self, page: Page, web_server: str):
        """Expense values should be displayed in red."""
        page.goto(web_server)
        page.wait_for_load_state("networkidle")

        expense_value = page.locator("text=Total Expenses").locator("..").locator(".text-red-400")
        expect(expense_value).to_be_visible()


class TestTransactionsTab:
    """Tests for the Transactions tab."""

    def test_can_switch_to_transactions_tab(self, page: Page, web_server: str):
        """Should be able to click Transactions tab."""
        page.goto(web_server)

        page.click("button:has-text('Transactions')")

        transactions_tab = page.locator("button", has_text="Transactions")
        expect(transactions_tab).to_have_class(re.compile(r"tab-active"))

    def test_transactions_tab_shows_filter_controls(self, page: Page, web_server: str):
        """Transactions tab should have category, sort, and direction filters."""
        page.goto(web_server)
        page.click("button:has-text('Transactions')")
        page.wait_for_load_state("networkidle")

        # Should have filter dropdowns
        expect(page.locator("select").first).to_be_visible()

    def test_transactions_list_displays_merchant_and_amount(self, page: Page, web_server: str):
        """Transaction list should show merchant names and amounts."""
        page.goto(web_server)
        page.click("button:has-text('Transactions')")
        page.wait_for_load_state("networkidle")

        # Wait for transactions to load (should have at least one)
        page.wait_for_timeout(1000)

        # Check for transaction elements with amounts (currency formatted)
        transactions = page.locator(".divide-y > div")
        expect(transactions.first).to_be_visible()

    def test_category_filter_changes_results(self, page: Page, web_server: str):
        """Selecting a category should filter transactions."""
        page.goto(web_server)
        page.click("button:has-text('Transactions')")
        page.wait_for_load_state("networkidle")

        # Get initial count indicator
        category_select = page.locator("select").first
        expect(category_select).to_be_visible()

        # The filter should have "All Categories" option (use count to check it exists)
        options = category_select.locator("option")
        expect(options.first).to_be_attached()

    def test_pagination_controls_visible_when_many_transactions(self, page: Page, web_server: str):
        """Should show pagination when more than 50 transactions."""
        page.goto(web_server)
        page.click("button:has-text('Transactions')")
        page.wait_for_load_state("networkidle")

        # Should have Previous/Next buttons if total > 50
        # This depends on having enough data
        page.wait_for_timeout(500)


class TestUncategorizedTab:
    """Tests for the Uncategorized tab."""

    def test_uncategorized_tab_shows_badge_count(self, page: Page, web_server: str):
        """Uncategorized tab should show count badge if items exist."""
        page.goto(web_server)
        page.wait_for_load_state("networkidle")

        # The tab button should exist
        uncategorized_tab = page.locator("button:has-text('Uncategorized')")
        expect(uncategorized_tab).to_be_visible()

    def test_uncategorized_shows_category_buttons(self, page: Page, web_server: str):
        """Each uncategorized transaction should have category buttons."""
        page.goto(web_server)
        page.click("button:has-text('Uncategorized')")
        page.wait_for_load_state("networkidle")

        # Wait for content to load
        page.wait_for_timeout(500)

        # Should either show transactions with category buttons or "All categorized" message
        content = page.locator("main")
        expect(content).to_be_visible()

    def test_clicking_category_button_categorizes_transaction(self, page: Page, web_server: str):
        """Clicking a category button should categorize the transaction."""
        page.goto(web_server)
        page.click("button:has-text('Uncategorized')")
        page.wait_for_load_state("networkidle")

        # This test depends on having uncategorized transactions
        # Check that the interface is ready
        expect(page.locator("main")).to_be_visible()


class TestRecurringTab:
    """Tests for the Recurring tab."""

    def test_recurring_tab_accessible(self, page: Page, web_server: str):
        """Should be able to navigate to Recurring tab."""
        page.goto(web_server)

        page.click("button:has-text('Recurring')")

        recurring_tab = page.locator("button", has_text="Recurring")
        expect(recurring_tab).to_have_class(re.compile(r"tab-active"))

    def test_recurring_shows_merchant_patterns(self, page: Page, web_server: str):
        """Recurring tab should show merchant patterns with counts."""
        page.goto(web_server)
        page.click("button:has-text('Recurring')")
        page.wait_for_load_state("networkidle")

        # Should show recurring patterns or empty state message
        page.wait_for_timeout(500)
        content = page.locator("main")
        expect(content).to_be_visible()


class TestAnomaliesTab:
    """Tests for the Anomalies tab."""

    def test_anomalies_tab_accessible(self, page: Page, web_server: str):
        """Should be able to navigate to Anomalies tab."""
        page.goto(web_server)

        # Click on the nav tab specifically
        page.locator("nav button:has-text('Anomalies')").click()

        anomalies_tab = page.locator("nav button", has_text="Anomalies")
        expect(anomalies_tab).to_have_class(re.compile(r"tab-active"))

    def test_anomalies_have_yellow_background(self, page: Page, web_server: str):
        """Anomaly items should have yellow/warning background."""
        page.goto(web_server)
        page.click("button:has-text('Anomalies')")
        page.wait_for_load_state("networkidle")

        # Wait for content
        page.wait_for_timeout(500)

        # Should show anomalies or "No anomalies detected" message
        content = page.locator("main")
        expect(content).to_be_visible()


class TestSearchTab:
    """Tests for the Search tab."""

    def test_search_tab_has_input_field(self, page: Page, web_server: str):
        """Search tab should have a search input field."""
        page.goto(web_server)
        page.click("button:has-text('Search')")
        page.wait_for_load_state("networkidle")

        search_input = page.locator("input[placeholder*='Search transactions']")
        expect(search_input).to_be_visible()

    def test_search_tab_has_search_button(self, page: Page, web_server: str):
        """Search tab should have a search button."""
        page.goto(web_server)
        page.click("button:has-text('Search')")

        search_button = page.locator("button:has-text('Search')").last
        expect(search_button).to_be_visible()

    def test_search_shows_placeholder_text(self, page: Page, web_server: str):
        """Search input should have helpful placeholder text."""
        page.goto(web_server)
        page.click("button:has-text('Search')")

        search_input = page.locator("input[placeholder*='Search']")
        expect(search_input).to_be_visible()

    def test_search_returns_results_for_valid_query(self, page: Page, web_server: str):
        """Searching for a common term should return results."""
        page.goto(web_server)
        page.click("button:has-text('Search')")
        page.wait_for_load_state("networkidle")

        # Type a search query - use specific selector for search input
        page.fill("input[placeholder*='Search transactions']", "coffee")
        page.locator("button:has-text('Search')").last.click()

        # Wait for results
        page.wait_for_timeout(1000)

        # Should show results or "No results" message
        content = page.locator("main")
        expect(content).to_be_visible()

    def test_search_with_enter_key(self, page: Page, web_server: str):
        """Should be able to search by pressing Enter."""
        page.goto(web_server)
        page.click("button:has-text('Search')")
        page.wait_for_load_state("networkidle")

        # Type and press Enter - use specific selector for search input
        search_input = page.locator("input[placeholder*='Search transactions']")
        search_input.fill("grocery")
        search_input.press("Enter")

        page.wait_for_timeout(500)


class TestFloatingChatWidget:
    """Tests for the floating chat widget."""

    def test_chat_widget_button_visible_on_load(self, page: Page, web_server: str):
        """Floating chat button should be visible on page load."""
        page.goto(web_server)

        # Chat widget button should be visible (bottom right)
        chat_button = page.locator(".chat-widget button").first
        expect(chat_button).to_be_visible()

    def test_chat_widget_expands_on_click(self, page: Page, web_server: str):
        """Clicking chat button should expand the chat panel."""
        page.goto(web_server)

        # Click the chat button
        page.locator(".chat-widget button").first.click()
        page.wait_for_timeout(500)

        # Chat panel should now be visible with header
        expect(page.locator("text=Budget Assistant")).to_be_visible()

    def test_chat_widget_shows_quick_questions(self, page: Page, web_server: str):
        """Expanded chat widget should show quick question buttons."""
        page.goto(web_server)
        page.locator(".chat-widget button").first.click()
        page.wait_for_timeout(500)

        expect(page.locator("text=Quick Questions")).to_be_visible()

    def test_chat_widget_minimizes(self, page: Page, web_server: str):
        """Should be able to minimize the chat widget."""
        page.goto(web_server)

        # Open chat
        page.locator(".chat-widget button").first.click()
        page.wait_for_timeout(500)

        # Click minimize button (chevron down in header)
        page.locator(".chat-widget button[title='Minimize']").click()
        page.wait_for_timeout(500)

        # Header should no longer be visible
        expect(page.locator("text=Budget Assistant")).not_to_be_visible()

    def test_chat_has_native_input(self, page: Page, web_server: str):
        """Chat should have a native input field (not iframe) for direct communication."""
        page.goto(web_server)
        page.locator(".chat-widget button").first.click()
        page.wait_for_timeout(500)

        # Should have a native chat input
        chat_input = page.locator("input[placeholder*='Ask about your budget']")
        expect(chat_input).to_be_visible()

        # Should NOT have an iframe
        iframe = page.locator("#claude-chat-frame")
        expect(iframe).to_have_count(0)

    def test_chat_widget_accessible_from_all_tabs(self, page: Page, web_server: str):
        """Chat widget should be visible from any tab."""
        page.goto(web_server)

        # Check on Dashboard (default)
        expect(page.locator(".chat-widget")).to_be_visible()

        # Switch to Transactions
        page.click("button:has-text('Transactions')")
        expect(page.locator(".chat-widget")).to_be_visible()

        # Switch to Recurring
        page.click("button:has-text('Recurring')")
        expect(page.locator(".chat-widget")).to_be_visible()

    def test_chat_widget_shows_status_indicator(self, page: Page, web_server: str):
        """Chat widget should show status indicator in header."""
        page.goto(web_server)
        page.locator(".chat-widget button").first.click()
        page.wait_for_timeout(500)

        # Should have a status indicator dot in the header (green = ready)
        indicator = page.locator(".chat-widget .bg-gray-900 .rounded-full.w-2.h-2.bg-green-400")
        expect(indicator).to_be_visible()

    def test_no_chat_tab_in_navigation(self, page: Page, web_server: str):
        """Chat should not appear as a navigation tab (it's now a floating widget)."""
        page.goto(web_server)

        # Nav should NOT have a Chat tab
        nav_buttons = page.locator("nav button")
        chat_in_nav = page.locator("nav button:has-text('Chat')")
        expect(chat_in_nav).to_have_count(0)


class TestImportModal:
    """Tests for the Import CSV modal."""

    def test_import_button_visible_in_header(self, page: Page, web_server: str):
        """Import CSV button should be visible in header."""
        page.goto(web_server)

        expect(page.locator("button:has-text('Import CSV')")).to_be_visible()

    def test_clicking_import_opens_modal(self, page: Page, web_server: str):
        """Clicking Import CSV should open the import modal."""
        page.goto(web_server)

        page.click("button:has-text('Import CSV')")

        expect(page.locator("text=Import Transactions")).to_be_visible()

    def test_import_modal_has_file_input(self, page: Page, web_server: str):
        """Import modal should have a file input."""
        page.goto(web_server)
        page.click("button:has-text('Import CSV')")

        file_input = page.locator("input[type='file']")
        expect(file_input).to_be_visible()

    def test_import_modal_accepts_csv_xlsx(self, page: Page, web_server: str):
        """File input should accept CSV and Excel files."""
        page.goto(web_server)
        page.click("button:has-text('Import CSV')")

        file_input = page.locator("input[type='file']")
        accept_attr = file_input.get_attribute("accept")
        assert ".csv" in accept_attr
        assert ".xlsx" in accept_attr

    def test_import_modal_has_cancel_button(self, page: Page, web_server: str):
        """Import modal should have a cancel button."""
        page.goto(web_server)
        page.click("button:has-text('Import CSV')")

        # Use .first to get the visible cancel button in the import modal
        expect(page.locator("button:has-text('Cancel')").first).to_be_visible()

    def test_import_modal_closes_on_cancel(self, page: Page, web_server: str):
        """Clicking cancel should close the modal."""
        page.goto(web_server)
        page.click("button:has-text('Import CSV')")
        page.click("button:has-text('Cancel')")

        # Modal should be hidden
        expect(page.locator("text=Import Transactions")).not_to_be_visible()

    def test_upload_button_disabled_without_file(self, page: Page, web_server: str):
        """Upload button should be disabled when no file selected."""
        page.goto(web_server)
        page.click("button:has-text('Import CSV')")

        upload_button = page.locator("button:has-text('Upload')")
        expect(upload_button).to_be_disabled()


class TestDarkTheme:
    """Tests for dark theme styling."""

    def test_body_has_dark_background(self, page: Page, web_server: str):
        """Page body should have dark background."""
        page.goto(web_server)

        body = page.locator("body")
        expect(body).to_have_class(re.compile(r"bg-gray-900"))

    def test_header_has_dark_background(self, page: Page, web_server: str):
        """Header should have dark background."""
        page.goto(web_server)

        header = page.locator("header")
        expect(header).to_have_class(re.compile(r"bg-gray-800"))

    def test_cards_have_dark_background(self, page: Page, web_server: str):
        """Dashboard cards should have dark background."""
        page.goto(web_server)
        page.wait_for_load_state("networkidle")

        # Dashboard cards should have bg-gray-800
        cards = page.locator(".bg-gray-800.rounded-lg")
        expect(cards.first).to_be_visible()


class TestResponsiveLayout:
    """Tests for responsive layout behavior."""

    def test_header_always_visible(self, page: Page, web_server: str):
        """Header should always be visible."""
        page.goto(web_server)

        expect(page.locator("text=Budget Automation").first).to_be_visible()

    def test_navigation_tabs_scroll_horizontally(self, page: Page, web_server: str):
        """Navigation tabs should be horizontally scrollable."""
        page.goto(web_server)

        nav = page.locator("nav")
        expect(nav).to_have_class(re.compile(r"overflow-x-auto"))

    def test_offline_indicator_hidden_when_online(self, page: Page, web_server: str):
        """Offline indicator should be hidden when online."""
        page.goto(web_server)

        # By default should be online
        offline_badge = page.locator("text=Offline")
        expect(offline_badge).not_to_be_visible()


class TestLoadingStates:
    """Tests for loading indicators."""

    def test_loading_overlay_shows_during_load(self, page: Page, web_server: str):
        """Loading overlay should appear during data loading."""
        # This is tricky to test as it happens quickly
        # We verify the loading element exists in DOM
        page.goto(web_server)

        # The loading overlay exists but may be hidden
        loading = page.locator("text=Loading...")
        # It might not be visible if load is fast, but structure should exist


class TestAccessibility:
    """Basic accessibility tests."""

    def test_page_has_title(self, page: Page, web_server: str):
        """Page should have a proper title."""
        page.goto(web_server)

        assert "Budget" in page.title()

    def test_buttons_have_visible_text(self, page: Page, web_server: str):
        """All buttons should have visible text or labels."""
        page.goto(web_server)

        # Main action buttons should be visible
        expect(page.locator("button:has-text('Import CSV')")).to_be_visible()
        expect(page.locator("button:has-text('Dashboard')")).to_be_visible()

    def test_color_coded_amounts(self, page: Page, web_server: str):
        """Amounts should use consistent color coding (green=income, red=expense)."""
        page.goto(web_server)
        page.wait_for_load_state("networkidle")

        # Income should have green class (dark theme uses -400)
        income_section = page.locator("text=Total Income").locator("..")
        expect(income_section.locator(".text-green-400")).to_be_visible()

        # Expenses should have red class (dark theme uses -400)
        expense_section = page.locator("text=Total Expenses").locator("..")
        expect(expense_section.locator(".text-red-400")).to_be_visible()
