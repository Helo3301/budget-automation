// Reporting Center Component
function reportingCenter() {
    return {
        currentSlide: 0,
        autoPlay: true,
        interval: null,
        slides: [],

        init() {
            this.buildSlides();
            this.startAutoPlay();

            // Watch for data changes from parent
            this.$watch('$root.summary', () => this.buildSlides());
            this.$watch('$root.categories', () => this.buildSlides());
            this.$watch('$root.uncategorized', () => this.buildSlides());
            this.$watch('$root.budgetOverview', () => this.buildSlides());
        },

        buildSlides() {
            const app = this.$root;
            const slides = [];
            const formatCurrency = (amt) => new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(amt);

            // 1. Monthly Summary
            const net = app.summary?.net || 0;
            slides.push({
                type: 'Monthly Summary',
                icon: 'M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z',
                iconBg: net >= 0 ? 'bg-green-900' : 'bg-red-900',
                iconColor: net >= 0 ? 'text-green-400' : 'text-red-400',
                badgeClass: 'bg-blue-900 text-blue-300',
                title: net >= 0 ? 'You\'re in the green!' : 'Spending exceeds income',
                description: `This month: ${formatCurrency(app.summary?.total_income || 0)} income, ${formatCurrency(Math.abs(app.summary?.total_expenses || 0))} expenses`,
                value: formatCurrency(Math.abs(net)),
                valueClass: net >= 0 ? 'text-green-400' : 'text-red-400',
                subValue: net >= 0 ? 'net savings' : 'net deficit'
            });

            // 2. Uncategorized Alert (if any)
            const uncatCount = app.summary?.uncategorized || 0;
            if (uncatCount > 0) {
                slides.push({
                    type: 'Action Required',
                    icon: 'M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z',
                    iconBg: 'bg-yellow-900',
                    iconColor: 'text-yellow-400',
                    badgeClass: 'bg-yellow-900 text-yellow-300',
                    title: `${uncatCount} transactions need categorizing`,
                    description: 'Categorize your transactions for accurate budget tracking',
                    value: uncatCount,
                    valueClass: 'text-yellow-400',
                    subValue: 'uncategorized',
                    actionText: 'Review now',
                    action: () => { app.tab = 'uncategorized'; }
                });
            }

            // 3. Budget Alerts - Over Budget
            const overBudget = (app.categories || []).filter(c => c.budget_amount > 0 && c.budget_percent >= 100);
            if (overBudget.length > 0) {
                const worst = overBudget.sort((a, b) => b.budget_percent - a.budget_percent)[0];
                slides.push({
                    type: 'Budget Alert',
                    icon: 'M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z',
                    iconBg: 'bg-red-900',
                    iconColor: 'text-red-400',
                    badgeClass: 'bg-red-900 text-red-300',
                    title: `${worst.name} is over budget!`,
                    description: `Spent ${formatCurrency(worst.spent)} of ${formatCurrency(worst.budget_amount)} budget`,
                    value: Math.round(worst.budget_percent) + '%',
                    valueClass: 'text-red-400',
                    subValue: `${formatCurrency(worst.spent - worst.budget_amount)} over`,
                    progress: worst.budget_percent,
                    progressClass: 'bg-red-500',
                    actionText: 'View details',
                    action: () => { app.tab = 'settings'; }
                });
            }

            // 4. Budget Warnings - Approaching limit (80-99%)
            const nearLimit = (app.categories || []).filter(c => c.budget_amount > 0 && c.budget_percent >= 80 && c.budget_percent < 100);
            if (nearLimit.length > 0) {
                const closest = nearLimit.sort((a, b) => b.budget_percent - a.budget_percent)[0];
                slides.push({
                    type: 'Budget Warning',
                    icon: 'M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z',
                    iconBg: 'bg-yellow-900',
                    iconColor: 'text-yellow-400',
                    badgeClass: 'bg-yellow-900 text-yellow-300',
                    title: `${closest.name} approaching limit`,
                    description: `${formatCurrency(closest.budget_amount - closest.spent)} remaining this month`,
                    value: Math.round(closest.budget_percent) + '%',
                    valueClass: 'text-yellow-400',
                    subValue: 'of budget used',
                    progress: closest.budget_percent,
                    progressClass: 'bg-yellow-500',
                    actionText: 'View budget',
                    action: () => { app.tab = 'settings'; }
                });
            }

            // 5. Top Spending Category
            const categoriesWithSpending = (app.categories || []).filter(c => c.spent > 0);
            if (categoriesWithSpending.length > 0) {
                const topCat = categoriesWithSpending.sort((a, b) => b.spent - a.spent)[0];
                slides.push({
                    type: 'Top Spending',
                    icon: 'M13 7h8m0 0v8m0-8l-8 8-4-4-6 6',
                    iconBg: 'bg-purple-900',
                    iconColor: 'text-purple-400',
                    badgeClass: 'bg-purple-900 text-purple-300',
                    title: `${topCat.name} leads spending`,
                    description: `Your highest spending category this month`,
                    value: formatCurrency(topCat.spent),
                    valueClass: 'text-purple-400',
                    subValue: `${topCat.transaction_count} transactions`,
                    actionText: 'View transactions',
                    action: () => { app.txnFilter.category = topCat.name; app.tab = 'transactions'; app.loadTransactions(); }
                });
            }

            // 6. Healthy Budget Status
            const healthyBudgets = (app.categories || []).filter(c => c.budget_amount > 0 && c.budget_percent < 50);
            if (healthyBudgets.length > 0) {
                slides.push({
                    type: 'On Track',
                    icon: 'M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z',
                    iconBg: 'bg-green-900',
                    iconColor: 'text-green-400',
                    badgeClass: 'bg-green-900 text-green-300',
                    title: `${healthyBudgets.length} budgets on track`,
                    description: 'These categories are under 50% spent',
                    value: healthyBudgets.length,
                    valueClass: 'text-green-400',
                    subValue: 'healthy budgets',
                    actionText: 'View all budgets',
                    action: () => { app.tab = 'settings'; }
                });
            }

            // 7. Transaction Count
            const totalTxn = app.summary?.total_transactions || 0;
            slides.push({
                type: 'Activity',
                icon: 'M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2',
                iconBg: 'bg-blue-900',
                iconColor: 'text-blue-400',
                badgeClass: 'bg-blue-900 text-blue-300',
                title: 'Transaction Overview',
                description: 'Your financial activity at a glance',
                value: totalTxn,
                valueClass: 'text-blue-400',
                subValue: 'total transactions',
                actionText: 'Browse all',
                action: () => { app.tab = 'transactions'; }
            });

            // 8. Discretionary Budget Alert (from budget overview)
            const budgetOverview = app.budgetOverview;
            if (budgetOverview && budgetOverview.income?.is_set) {
                const disc = budgetOverview.discretionary;
                // Alert if discretionary spending is over budget
                if (disc?.is_over) {
                    slides.push({
                        type: 'Spending Alert',
                        icon: 'M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z',
                        iconBg: 'bg-red-900',
                        iconColor: 'text-red-400',
                        badgeClass: 'bg-red-900 text-red-300',
                        title: 'Discretionary budget exceeded!',
                        description: `You've overspent by ${formatCurrency(Math.abs(disc.remaining))} - dipping into assets`,
                        value: formatCurrency(disc.spent),
                        valueClass: 'text-red-400',
                        subValue: `of ${formatCurrency(disc.budget)} budget`,
                        progress: Math.min(150, disc.percent_used),
                        progressClass: 'bg-red-500',
                        actionText: 'View budget',
                        action: () => { app.tab = 'budget'; }
                    });
                }
                // Warning if discretionary spending is high (80%+)
                else if (disc?.percent_used >= 80) {
                    slides.push({
                        type: 'Budget Warning',
                        icon: 'M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z',
                        iconBg: 'bg-yellow-900',
                        iconColor: 'text-yellow-400',
                        badgeClass: 'bg-yellow-900 text-yellow-300',
                        title: 'Discretionary budget getting tight',
                        description: `Only ${formatCurrency(disc.remaining)} remaining of your discretionary budget`,
                        value: Math.round(disc.percent_used) + '%',
                        valueClass: 'text-yellow-400',
                        subValue: 'of discretionary used',
                        progress: disc.percent_used,
                        progressClass: 'bg-yellow-500',
                        actionText: 'View budget',
                        action: () => { app.tab = 'budget'; }
                    });
                }
                // Good status if under 50%
                else if (disc?.budget > 0 && disc?.percent_used < 50) {
                    slides.push({
                        type: 'Budget Status',
                        icon: 'M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z',
                        iconBg: 'bg-emerald-900',
                        iconColor: 'text-emerald-400',
                        badgeClass: 'bg-emerald-900 text-emerald-300',
                        title: 'Discretionary budget healthy',
                        description: `${formatCurrency(disc.remaining)} remaining for discretionary spending`,
                        value: Math.round(100 - disc.percent_used) + '%',
                        valueClass: 'text-emerald-400',
                        subValue: 'budget remaining',
                        progress: disc.percent_used,
                        progressClass: 'bg-emerald-500',
                        actionText: 'View details',
                        action: () => { app.tab = 'budget'; }
                    });
                }
            }

            // 9. Budget Setup Prompt (if no budgets set)
            const budgetsSet = (app.categories || []).filter(c => c.budget_amount > 0).length;
            if (budgetsSet === 0 && !budgetOverview?.income?.is_set) {
                slides.push({
                    type: 'Get Started',
                    icon: 'M12 6v6m0 0v6m0-6h6m-6 0H6',
                    iconBg: 'bg-indigo-900',
                    iconColor: 'text-indigo-400',
                    badgeClass: 'bg-indigo-900 text-indigo-300',
                    title: 'Set up your budget',
                    description: 'Configure your income and spending limits to track your finances',
                    actionText: 'Configure now',
                    action: () => { app.tab = 'budget'; }
                });
            }

            this.slides = slides;

            // Reset to first slide if current is out of bounds
            if (this.currentSlide >= slides.length) {
                this.currentSlide = 0;
            }
        },

        nextSlide() {
            this.currentSlide = (this.currentSlide + 1) % this.slides.length;
            this.resetAutoPlay();
        },

        prevSlide() {
            this.currentSlide = (this.currentSlide - 1 + this.slides.length) % this.slides.length;
            this.resetAutoPlay();
        },

        goToSlide(index) {
            this.currentSlide = index;
            this.resetAutoPlay();
        },

        startAutoPlay() {
            if (this.interval) clearInterval(this.interval);
            if (this.autoPlay) {
                this.interval = setInterval(() => {
                    this.currentSlide = (this.currentSlide + 1) % this.slides.length;
                }, 6000); // 6 seconds per slide
            }
        },

        resetAutoPlay() {
            if (this.autoPlay) {
                this.startAutoPlay();
            }
        },

        toggleAutoPlay() {
            this.autoPlay = !this.autoPlay;
            if (this.autoPlay) {
                this.startAutoPlay();
            } else {
                clearInterval(this.interval);
            }
        }
    };
}

// Budget Automation Web App
function app() {
    return {
        // State
        tab: 'dashboard',
        loading: false,
        offline: !navigator.onLine,

        // Chat widget state
        chatOpen: false,
        chatContext: null,
        showQuickActions: true,
        chatMessages: [],
        chatInput: '',
        chatLoading: false,

        // Data
        summary: {},
        categories: [],
        transactions: [],
        txnTotal: 0,
        txnOffset: 0,
        txnFilter: { category: '', sortBy: 'date', sortDir: 'desc' },
        uncategorized: [],
        recurring: [],
        anomalies: [],
        searchQuery: '',
        searchResults: [],

        // Import
        showImport: false,
        selectedFile: null,
        uploading: false,
        importResult: null,

        // Analytics
        analyticsData: {
            monthly: [],
            categories: [],
            daily: [],
            merchants: [],
            weekday: []
        },
        analyticsCharts: {},
        analyticsLoading: false,
        analyticsTimeRange: 12, // months

        // Category management
        showCategoryModal: false,
        editingCategory: null,
        newCategoryName: '',
        newCategoryKeywords: '',
        newCategoryBudget: 0,

        // Account management
        accounts: [],
        showAccountModal: false,

        // Admin
        showResetConfirm: false,
        resetConfirmText: '',

        // Subscriptions
        subscriptions: [],
        subscriptionsSummary: {},
        showSubscriptionModal: false,
        editingSubscription: null,
        newSubName: '',
        newSubMerchant: '',
        newSubAmount: 0,
        newSubFrequency: 'monthly',
        newSubStartDate: '',
        newSubCategoryId: null,
        newSubAccountId: null,
        newSubNotes: '',

        // Bill Payment Tracking
        showPaymentModal: false,
        paymentSubscription: null,
        paymentForm: {
            amount_paid: '',
            payment_method: '',
            confirmation_number: '',
            notes: ''
        },
        showPaymentHistoryModal: false,
        paymentHistorySubscription: null,
        paymentHistory: [],
        billsDueSoon: [],

        // Transaction Splitting
        showSplitModal: false,
        splittingTransaction: null,
        splits: [],
        splitRemaining: 0,

        // Import Wizard
        showImportWizard: false,
        importStep: 1, // 1=upload, 2=mapping, 3=preview, 4=importing
        importSession: null,
        importFilename: '',
        importColumns: [],
        importSampleRows: [],
        importTotalRows: 0,
        importMapping: {
            date_column: '',
            amount_column: '',
            merchant_column: '',
            description_column: ''
        },
        importAccountId: null,
        importAutoCategorize: false,
        importResult: null,
        importLoading: false,

        // Savings Goals
        savingsGoals: [],
        showGoalModal: false,
        editingGoal: null,
        goalForm: {
            name: '',
            description: '',
            target_amount: '',
            target_date: '',
            color: '#10B981',
            icon: 'piggy-bank'
        },
        showContributionModal: false,
        contributionGoal: null,
        contributionAmount: '',
        contributionNote: '',
        goalContributions: [],

        // Budget Overview
        budgetOverview: null,
        budgetSettings: {
            monthly_income: 0,
            savings_target_percent: 20,
            emergency_fund_months: 6,
            discretionary_warning_percent: 80
        },
        showBudgetSettingsModal: false,
        budgetLoading: false,

        // Onboarding
        onboardingStatus: null,
        onboardingMode: false,

        // Chat file upload
        chatFileDropActive: false,
        chatPendingFile: null,
        chatImportStatus: null, // 'uploading', 'mapping', 'importing', 'done', 'error'
        chatImportSession: null,
        chatImportPreview: null,
        showChatFilePrompt: false, // Show file upload prompt in chat

        editingAccount: null,
        newAccountName: '',
        newAccountInstitution: '',
        newAccountType: 'checking',
        newAccountLastFour: '',
        newAccountColor: '#3B82F6',
        newAccountInitialBalance: 0,

        // Initialize
        async init() {
            // Track online/offline status
            window.addEventListener('online', () => this.offline = false);
            window.addEventListener('offline', () => this.offline = true);

            // Register service worker for offline support
            if ('serviceWorker' in navigator) {
                try {
                    await navigator.serviceWorker.register('/static/sw.js');
                } catch (e) {
                    console.log('SW registration failed:', e);
                }
            }

            // Load initial data
            await this.loadAll();

            // Check if onboarding is needed and auto-trigger chat
            await this.checkOnboarding();
        },

        // Check onboarding status
        async checkOnboarding() {
            try {
                this.onboardingStatus = await this.api('/onboarding/status');
                if (this.onboardingStatus.needs_onboarding) {
                    this.onboardingMode = true;
                    this.startOnboardingChat();
                }
            } catch (e) {
                console.error('Failed to check onboarding status:', e);
            }
        },

        // Start onboarding chat with welcome message
        startOnboardingChat() {
            this.chatOpen = true;
            this.chatMessages = [{
                role: 'assistant',
                content: "Welcome to Budget Tracker! I'm here to help you get set up.\n\nWe'll configure your income, accounts, and categories. You can also drag-and-drop CSV files from your bank to import transactions.\n\nLet's start with your monthly income. How much do you earn per month (after taxes)?"
            }];
        },

        // API helper
        async api(endpoint, options = {}) {
            try {
                const response = await fetch(`/api${endpoint}`, {
                    headers: { 'Content-Type': 'application/json', ...options.headers },
                    ...options
                });
                if (!response.ok) throw new Error(`API error: ${response.status}`);
                return await response.json();
            } catch (error) {
                console.error('API error:', error);
                throw error;
            }
        },

        // Load all data
        async loadAll() {
            this.loading = true;
            try {
                await Promise.all([
                    this.loadSummary(),
                    this.loadCategories(),
                    this.loadTransactions(),
                    this.loadUncategorized(),
                    this.loadRecurring(),
                    this.loadAnomalies(),
                    this.loadAccounts(),
                    this.loadSubscriptions(),
                    this.loadSavingsGoals(),
                    this.loadBillsDueSoon(),
                    this.loadBudgetOverview()
                ]);
            } finally {
                this.loading = false;
            }
        },

        // Load summary
        async loadSummary() {
            this.summary = await this.api('/summary');
        },

        // Load categories
        async loadCategories() {
            this.categories = await this.api('/categories');
        },

        // Load transactions with filters
        async loadTransactions() {
            const params = new URLSearchParams({
                limit: '50',
                offset: this.txnOffset.toString(),
                sort_by: this.txnFilter.sortBy,
                sort_dir: this.txnFilter.sortDir
            });
            if (this.txnFilter.category) {
                params.set('category', this.txnFilter.category);
            }
            const result = await this.api(`/transactions?${params}`);
            this.transactions = result.transactions;
            this.txnTotal = result.total;
        },

        // Load uncategorized
        async loadUncategorized() {
            this.uncategorized = await this.api('/uncategorized?limit=100');
        },

        // Load recurring patterns
        async loadRecurring() {
            this.recurring = await this.api('/recurring');
        },

        // Load anomalies
        async loadAnomalies() {
            this.anomalies = await this.api('/anomalies');
        },

        // Load accounts
        async loadAccounts() {
            this.accounts = await this.api('/accounts');
        },

        // Categorize a transaction
        async categorize(txnId, category) {
            try {
                await this.api(`/transactions/${txnId}/categorize`, {
                    method: 'POST',
                    body: JSON.stringify({ category })
                });
                // Refresh data
                await Promise.all([
                    this.loadSummary(),
                    this.loadUncategorized(),
                    this.loadTransactions()
                ]);
            } catch (error) {
                alert('Failed to categorize transaction');
            }
        },

        // Search
        async doSearch() {
            if (!this.searchQuery.trim()) return;
            this.loading = true;
            try {
                this.searchResults = await this.api(`/search?q=${encodeURIComponent(this.searchQuery)}`);
            } finally {
                this.loading = false;
            }
        },

        // File handling
        handleFileSelect(event) {
            this.selectedFile = event.target.files[0];
            this.importResult = null;
        },

        async uploadFile() {
            if (!this.selectedFile) return;
            this.uploading = true;
            try {
                const formData = new FormData();
                formData.append('file', this.selectedFile);

                const response = await fetch('/api/import', {
                    method: 'POST',
                    body: formData
                });

                if (!response.ok) throw new Error('Upload failed');
                this.importResult = await response.json();

                // Refresh all data
                await this.loadAll();
            } catch (error) {
                alert('Failed to import file: ' + error.message);
            } finally {
                this.uploading = false;
            }
        },

        // Format currency
        formatCurrency(amount) {
            return new Intl.NumberFormat('en-US', {
                style: 'currency',
                currency: 'USD'
            }).format(amount);
        },

        // Category management functions
        openAddCategory() {
            this.editingCategory = null;
            this.newCategoryName = '';
            this.newCategoryKeywords = '';
            this.newCategoryBudget = 0;
            this.showCategoryModal = true;
        },

        openEditCategory(cat) {
            this.editingCategory = cat;
            this.newCategoryName = cat.name;
            this.newCategoryKeywords = cat.keywords || '';
            this.newCategoryBudget = cat.budget_amount || 0;
            this.showCategoryModal = true;
        },

        closeCategoryModal() {
            this.showCategoryModal = false;
            this.editingCategory = null;
            this.newCategoryName = '';
            this.newCategoryKeywords = '';
            this.newCategoryBudget = 0;
        },

        async saveCategory() {
            if (!this.newCategoryName.trim()) return;

            try {
                if (this.editingCategory) {
                    // Update existing category
                    await this.api(`/categories/${this.editingCategory.id}`, {
                        method: 'PUT',
                        body: JSON.stringify({
                            name: this.newCategoryName.trim(),
                            keywords: this.newCategoryKeywords.trim() || null,
                            budget_amount: this.newCategoryBudget || 0
                        })
                    });
                } else {
                    // Create new category
                    await this.api('/categories', {
                        method: 'POST',
                        body: JSON.stringify({
                            name: this.newCategoryName.trim(),
                            keywords: this.newCategoryKeywords.trim() || null,
                            budget_amount: this.newCategoryBudget || 0
                        })
                    });
                }

                this.closeCategoryModal();
                await this.loadCategories();
            } catch (error) {
                alert('Failed to save category: ' + error.message);
            }
        },

        async deleteCategory(cat) {
            if (cat.transaction_count > 0) {
                alert(`Cannot delete "${cat.name}" - it has ${cat.transaction_count} transactions assigned.`);
                return;
            }

            if (!confirm(`Are you sure you want to delete "${cat.name}"?`)) {
                return;
            }

            try {
                await this.api(`/categories/${cat.id}`, { method: 'DELETE' });
                await this.loadCategories();
            } catch (error) {
                alert('Failed to delete category: ' + error.message);
            }
        },

        // Chat functions
        askAboutTransaction(txn) {
            // Set the transaction as context and open chat
            this.chatContext = txn;
            this.chatOpen = true;
            this.showQuickActions = false;
            // Focus the chat input
            this.$nextTick(() => {
                const input = document.querySelector('#chat-messages + .p-3 input');
                if (input) input.focus();
            });
        },

        quickQuestion(query) {
            // Open chat with a quick question and send it immediately
            this.chatContext = null;
            this.chatOpen = true;
            this.showQuickActions = false;
            if (query) {
                this.sendChatMessage(query);
            }
        },

        // Send a message to the chat API
        async sendChatMessage(message) {
            if (!message || !message.trim()) return;

            const userMessage = message.trim();
            this.chatInput = '';
            this.showQuickActions = false;

            // Add user message to chat
            this.chatMessages.push({
                role: 'user',
                content: userMessage,
                timestamp: new Date().toISOString()
            });

            // Scroll to bottom
            this.$nextTick(() => {
                const container = document.getElementById('chat-messages');
                if (container) container.scrollTop = container.scrollHeight;
            });

            this.chatLoading = true;
            try {
                const response = await fetch('/api/chat', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        message: userMessage,
                        context_txn_id: this.chatContext?.id || null,
                        onboarding_mode: this.onboardingMode
                    })
                });

                if (!response.ok) throw new Error('Chat request failed');
                const data = await response.json();

                // Add assistant response
                this.chatMessages.push({
                    role: 'assistant',
                    content: data.response,
                    timestamp: new Date().toISOString(),
                    actions: data.actions_executed || []
                });

                // If actions were executed, refresh data
                if (data.actions_executed && data.actions_executed.length > 0) {
                    await this.loadAll();

                    // Check if onboarding was completed
                    const completedOnboarding = data.actions_executed.some(
                        a => a.type === 'complete_onboarding' && a.success
                    );
                    if (completedOnboarding) {
                        this.onboardingMode = false;
                        this.onboardingStatus = await this.api('/onboarding/status');
                    }
                }

                // Scroll to bottom
                this.$nextTick(() => {
                    const container = document.getElementById('chat-messages');
                    if (container) container.scrollTop = container.scrollHeight;
                });
            } catch (error) {
                console.error('Chat error:', error);
                this.chatMessages.push({
                    role: 'assistant',
                    content: 'Sorry, I encountered an error. Please try again.',
                    timestamp: new Date().toISOString(),
                    error: true
                });
            } finally {
                this.chatLoading = false;
            }
        },

        // Clear the chat history
        clearChat() {
            this.chatMessages = [];
            this.chatContext = null;
            this.showQuickActions = true;
        },

        // Clear the transaction context
        clearChatContext() {
            this.chatContext = null;
        },

        // Chat file upload methods
        handleChatDragOver(e) {
            e.preventDefault();
            this.chatFileDropActive = true;
        },

        handleChatDragLeave(e) {
            e.preventDefault();
            this.chatFileDropActive = false;
        },

        async handleChatFileDrop(e) {
            e.preventDefault();
            this.chatFileDropActive = false;

            const files = e.dataTransfer?.files || e.target?.files;
            if (!files || files.length === 0) return;

            const file = files[0];
            const validTypes = ['.csv', '.xlsx', '.xls'];
            const ext = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));

            if (!validTypes.includes(ext)) {
                this.chatMessages.push({
                    role: 'assistant',
                    content: `Sorry, I can only accept CSV or Excel files (.csv, .xlsx, .xls). You uploaded "${file.name}".`,
                    timestamp: new Date().toISOString()
                });
                return;
            }

            // Show uploading status in chat
            this.chatMessages.push({
                role: 'user',
                content: `Uploading file: ${file.name}`,
                timestamp: new Date().toISOString(),
                isFileUpload: true
            });

            this.chatImportStatus = 'uploading';

            try {
                const formData = new FormData();
                formData.append('file', file);

                const response = await fetch('/api/import/preview', {
                    method: 'POST',
                    body: formData
                });

                if (!response.ok) throw new Error('Upload failed');

                const preview = await response.json();
                this.chatImportSession = preview.session_id;
                this.chatImportPreview = preview;
                this.chatImportStatus = 'mapping';

                // Auto-detect and use the mapping
                const mapping = preview.detected_mapping;
                const hasGoodMapping = mapping.date && mapping.amount && mapping.merchant;

                if (hasGoodMapping) {
                    // Good auto-detection - ask for confirmation
                    this.chatMessages.push({
                        role: 'assistant',
                        content: `I found ${preview.total_rows} transactions in "${preview.filename}". I detected these columns:\n\n` +
                            `- Date: ${mapping.date}\n` +
                            `- Amount: ${mapping.amount}\n` +
                            `- Merchant: ${mapping.merchant}\n` +
                            (mapping.description ? `- Description: ${mapping.description}\n` : '') +
                            `\nWould you like me to import these transactions?`,
                        timestamp: new Date().toISOString(),
                        importPreview: true,
                        previewData: preview
                    });
                    this.showChatFilePrompt = true;
                } else {
                    // Couldn't auto-detect - show available columns
                    this.chatMessages.push({
                        role: 'assistant',
                        content: `I found ${preview.total_rows} rows in "${preview.filename}", but I couldn't automatically detect the column mapping.\n\n` +
                            `Available columns: ${preview.columns.join(', ')}\n\n` +
                            `Please tell me which column contains the date, amount, and merchant/description.`,
                        timestamp: new Date().toISOString()
                    });
                }

            } catch (error) {
                console.error('Upload error:', error);
                this.chatImportStatus = 'error';
                this.chatMessages.push({
                    role: 'assistant',
                    content: `Sorry, there was an error processing your file: ${error.message}`,
                    timestamp: new Date().toISOString(),
                    error: true
                });
            }
        },

        async confirmChatImport(autoCategorize = false) {
            if (!this.chatImportSession || !this.chatImportPreview) return;

            this.chatImportStatus = 'importing';
            this.showChatFilePrompt = false;

            const mapping = this.chatImportPreview.detected_mapping;

            try {
                const response = await fetch('/api/import/confirm', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        session_id: this.chatImportSession,
                        column_mapping: {
                            date_column: mapping.date,
                            amount_column: mapping.amount,
                            merchant_column: mapping.merchant,
                            description_column: mapping.description
                        },
                        auto_categorize: autoCategorize
                    })
                });

                if (!response.ok) throw new Error('Import failed');

                const result = await response.json();
                this.chatImportStatus = 'done';

                this.chatMessages.push({
                    role: 'assistant',
                    content: `Import complete!\n\n` +
                        `- Imported: ${result.imported} transactions\n` +
                        `- Duplicates skipped: ${result.duplicates}\n` +
                        (result.total_errors > 0 ? `- Errors: ${result.total_errors}\n` : '') +
                        `\nYour transactions are now ready to categorize!`,
                    timestamp: new Date().toISOString(),
                    actions: [{ type: 'import_complete', imported: result.imported }]
                });

                // Refresh data
                await this.loadAll();

                // Reset import state
                this.chatImportSession = null;
                this.chatImportPreview = null;

            } catch (error) {
                console.error('Import error:', error);
                this.chatImportStatus = 'error';
                this.chatMessages.push({
                    role: 'assistant',
                    content: `Sorry, there was an error importing your transactions: ${error.message}`,
                    timestamp: new Date().toISOString(),
                    error: true
                });
            }
        },

        cancelChatImport() {
            if (this.chatImportSession) {
                fetch(`/api/import/session/${this.chatImportSession}`, { method: 'DELETE' });
            }
            this.chatImportSession = null;
            this.chatImportPreview = null;
            this.chatImportStatus = null;
            this.showChatFilePrompt = false;

            this.chatMessages.push({
                role: 'assistant',
                content: 'Import cancelled. You can drop another file when you\'re ready.',
                timestamp: new Date().toISOString()
            });
        },

        triggerChatFileInput() {
            document.getElementById('chat-file-input').click();
        },

        // Account management functions
        openAddAccount() {
            this.editingAccount = null;
            this.newAccountName = '';
            this.newAccountInstitution = '';
            this.newAccountType = 'checking';
            this.newAccountLastFour = '';
            this.newAccountColor = '#3B82F6';
            this.newAccountInitialBalance = 0;
            this.showAccountModal = true;
        },

        openEditAccount(account) {
            this.editingAccount = account;
            this.newAccountName = account.name;
            this.newAccountInstitution = account.institution || '';
            this.newAccountType = account.account_type || 'checking';
            this.newAccountLastFour = account.last_four || '';
            this.newAccountColor = account.color || '#3B82F6';
            this.newAccountInitialBalance = account.initial_balance || 0;
            this.showAccountModal = true;
        },

        closeAccountModal() {
            this.showAccountModal = false;
            this.editingAccount = null;
            this.newAccountName = '';
            this.newAccountInstitution = '';
            this.newAccountType = 'checking';
            this.newAccountLastFour = '';
            this.newAccountColor = '#3B82F6';
            this.newAccountInitialBalance = 0;
        },

        async saveAccount() {
            if (!this.newAccountName.trim()) return;

            try {
                const accountData = {
                    name: this.newAccountName.trim(),
                    institution: this.newAccountInstitution.trim() || null,
                    account_type: this.newAccountType,
                    last_four: this.newAccountLastFour.trim() || null,
                    color: this.newAccountColor,
                    initial_balance: this.newAccountInitialBalance || 0
                };

                if (this.editingAccount) {
                    await this.api(`/accounts/${this.editingAccount.id}`, {
                        method: 'PUT',
                        body: JSON.stringify(accountData)
                    });
                } else {
                    await this.api('/accounts', {
                        method: 'POST',
                        body: JSON.stringify(accountData)
                    });
                }

                this.closeAccountModal();
                await this.loadAccounts();
            } catch (error) {
                alert('Failed to save account: ' + error.message);
            }
        },

        async deleteAccount(account) {
            if (!confirm(`Are you sure you want to delete "${account.name}"? This will unassign all transactions from this account.`)) {
                return;
            }

            try {
                await this.api(`/accounts/${account.id}`, { method: 'DELETE' });
                await this.loadAccounts();
            } catch (error) {
                alert('Failed to delete account: ' + error.message);
            }
        },

        // Get display color based on account type
        getAccountTypeLabel(type) {
            const labels = {
                'checking': 'Checking',
                'savings': 'Savings',
                'credit': 'Credit Card',
                'investment': 'Investment'
            };
            return labels[type] || type;
        },

        getAccountTypeIcon(type) {
            const icons = {
                'checking': 'M3 10h18M7 15h1m4 0h1m-7 4h12a3 3 0 003-3V8a3 3 0 00-3-3H6a3 3 0 00-3 3v8a3 3 0 003 3z',
                'savings': 'M17 9V7a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2m2 4h10a2 2 0 002-2v-6a2 2 0 00-2-2H9a2 2 0 00-2 2v6a2 2 0 002 2zm7-5a2 2 0 11-4 0 2 2 0 014 0z',
                'credit': 'M3 10h18M7 15h1m4 0h1m-7 4h12a3 3 0 003-3V8a3 3 0 00-3-3H6a3 3 0 00-3 3v8a3 3 0 003 3z',
                'investment': 'M13 7h8m0 0v8m0-8l-8 8-4-4-6 6'
            };
            return icons[type] || icons['checking'];
        },

        // Calculate total balance across all accounts
        getTotalBalance() {
            return this.accounts.reduce((sum, acc) => sum + (acc.balance || 0), 0);
        },

        // Calculate total available credit
        getTotalAvailableCredit() {
            return this.accounts
                .filter(acc => acc.account_type === 'credit')
                .reduce((sum, acc) => sum + (acc.available_credit || 0), 0);
        },

        // Calculate net worth (assets minus credit card debt)
        getNetWorth() {
            let assets = 0;
            let liabilities = 0;
            this.accounts.forEach(acc => {
                if (acc.account_type === 'credit') {
                    liabilities += acc.balance || 0;
                } else {
                    assets += acc.balance || 0;
                }
            });
            return assets - liabilities;
        },

        // Admin: Reset Data
        openResetConfirm() {
            this.resetConfirmText = '';
            this.showResetConfirm = true;
        },

        closeResetConfirm() {
            this.showResetConfirm = false;
            this.resetConfirmText = '';
        },

        async confirmResetData() {
            if (this.resetConfirmText !== 'RESET') {
                alert('Please type RESET to confirm');
                return;
            }

            try {
                await this.api('/admin/reset', { method: 'POST' });
                this.closeResetConfirm();
                // Reload all data
                await this.loadAll();
                alert('All data has been reset successfully.');
            } catch (error) {
                alert('Failed to reset data: ' + error.message);
            }
        },

        // Analytics functions
        async loadAnalytics() {
            this.analyticsLoading = true;
            try {
                const [monthly, categories, daily, merchants, weekday] = await Promise.all([
                    this.api(`/analytics/monthly?months=${this.analyticsTimeRange}`),
                    this.api('/analytics/categories'),
                    this.api('/analytics/daily?days=30'),
                    this.api('/analytics/merchants?limit=10'),
                    this.api('/analytics/weekday')
                ]);

                this.analyticsData = { monthly, categories, daily, merchants, weekday };

                // Wait for DOM to be ready then render charts
                this.$nextTick(() => {
                    this.renderCharts();
                });
            } catch (error) {
                console.error('Failed to load analytics:', error);
            } finally {
                this.analyticsLoading = false;
            }
        },

        renderCharts() {
            this.renderMonthlyChart();
            this.renderCategoryChart();
            this.renderDailyChart();
            this.renderMerchantsChart();
            this.renderWeekdayChart();
        },

        destroyChart(name) {
            if (this.analyticsCharts[name]) {
                this.analyticsCharts[name].destroy();
                this.analyticsCharts[name] = null;
            }
        },

        renderMonthlyChart() {
            this.destroyChart('monthly');
            const canvas = document.getElementById('chart-monthly');
            if (!canvas || !this.analyticsData.monthly.length) return;

            const ctx = canvas.getContext('2d');
            const data = this.analyticsData.monthly;

            this.analyticsCharts.monthly = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: data.map(d => d.month),
                    datasets: [
                        {
                            label: 'Income',
                            data: data.map(d => d.income),
                            backgroundColor: 'rgba(74, 222, 128, 0.7)',
                            borderColor: 'rgb(74, 222, 128)',
                            borderWidth: 1
                        },
                        {
                            label: 'Expenses',
                            data: data.map(d => d.expenses),
                            backgroundColor: 'rgba(248, 113, 113, 0.7)',
                            borderColor: 'rgb(248, 113, 113)',
                            borderWidth: 1
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            labels: { color: '#9ca3af' }
                        },
                        title: {
                            display: true,
                            text: 'Monthly Income vs Expenses',
                            color: '#f3f4f6',
                            font: { size: 14 }
                        }
                    },
                    scales: {
                        x: {
                            ticks: { color: '#9ca3af' },
                            grid: { color: 'rgba(75, 85, 99, 0.3)' }
                        },
                        y: {
                            ticks: {
                                color: '#9ca3af',
                                callback: (value) => '$' + value.toLocaleString()
                            },
                            grid: { color: 'rgba(75, 85, 99, 0.3)' }
                        }
                    }
                }
            });
        },

        renderCategoryChart() {
            this.destroyChart('category');
            const canvas = document.getElementById('chart-category');
            if (!canvas || !this.analyticsData.categories.length) return;

            const ctx = canvas.getContext('2d');
            const data = this.analyticsData.categories.filter(d => d.total > 0);

            const colors = [
                'rgba(59, 130, 246, 0.8)',  // blue
                'rgba(239, 68, 68, 0.8)',   // red
                'rgba(34, 197, 94, 0.8)',   // green
                'rgba(168, 85, 247, 0.8)',  // purple
                'rgba(249, 115, 22, 0.8)',  // orange
                'rgba(236, 72, 153, 0.8)',  // pink
                'rgba(20, 184, 166, 0.8)',  // teal
                'rgba(234, 179, 8, 0.8)',   // yellow
                'rgba(99, 102, 241, 0.8)',  // indigo
                'rgba(107, 114, 128, 0.8)'  // gray
            ];

            this.analyticsCharts.category = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: data.map(d => d.category_name || 'Uncategorized'),
                    datasets: [{
                        data: data.map(d => d.total),
                        backgroundColor: colors.slice(0, data.length),
                        borderColor: '#1f2937',
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'right',
                            labels: { color: '#9ca3af', padding: 12 }
                        },
                        title: {
                            display: true,
                            text: 'Spending by Category',
                            color: '#f3f4f6',
                            font: { size: 14 }
                        },
                        tooltip: {
                            callbacks: {
                                label: (context) => {
                                    const value = context.raw;
                                    const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                    const pct = ((value / total) * 100).toFixed(1);
                                    return `$${value.toLocaleString()} (${pct}%)`;
                                }
                            }
                        }
                    }
                }
            });
        },

        renderDailyChart() {
            this.destroyChart('daily');
            const canvas = document.getElementById('chart-daily');
            if (!canvas || !this.analyticsData.daily.length) return;

            const ctx = canvas.getContext('2d');
            const data = this.analyticsData.daily;

            this.analyticsCharts.daily = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: data.map(d => d.date),
                    datasets: [{
                        label: 'Daily Spending',
                        data: data.map(d => d.total),
                        borderColor: 'rgb(59, 130, 246)',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        fill: true,
                        tension: 0.3,
                        pointRadius: 3,
                        pointHoverRadius: 6
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            labels: { color: '#9ca3af' }
                        },
                        title: {
                            display: true,
                            text: 'Daily Spending (Last 30 Days)',
                            color: '#f3f4f6',
                            font: { size: 14 }
                        }
                    },
                    scales: {
                        x: {
                            ticks: { color: '#9ca3af', maxRotation: 45 },
                            grid: { color: 'rgba(75, 85, 99, 0.3)' }
                        },
                        y: {
                            ticks: {
                                color: '#9ca3af',
                                callback: (value) => '$' + value.toLocaleString()
                            },
                            grid: { color: 'rgba(75, 85, 99, 0.3)' }
                        }
                    }
                }
            });
        },

        renderMerchantsChart() {
            this.destroyChart('merchants');
            const canvas = document.getElementById('chart-merchants');
            if (!canvas || !this.analyticsData.merchants.length) return;

            const ctx = canvas.getContext('2d');
            const data = this.analyticsData.merchants;

            this.analyticsCharts.merchants = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: data.map(d => d.merchant.length > 20 ? d.merchant.slice(0, 20) + '...' : d.merchant),
                    datasets: [{
                        label: 'Total Spent',
                        data: data.map(d => d.total),
                        backgroundColor: 'rgba(168, 85, 247, 0.7)',
                        borderColor: 'rgb(168, 85, 247)',
                        borderWidth: 1
                    }]
                },
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        title: {
                            display: true,
                            text: 'Top 10 Merchants (Last 90 Days)',
                            color: '#f3f4f6',
                            font: { size: 14 }
                        },
                        tooltip: {
                            callbacks: {
                                label: (context) => {
                                    const item = data[context.dataIndex];
                                    return `$${context.raw.toLocaleString()} (${item.count} transactions)`;
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            ticks: {
                                color: '#9ca3af',
                                callback: (value) => '$' + value.toLocaleString()
                            },
                            grid: { color: 'rgba(75, 85, 99, 0.3)' }
                        },
                        y: {
                            ticks: { color: '#9ca3af' },
                            grid: { display: false }
                        }
                    }
                }
            });
        },

        renderWeekdayChart() {
            this.destroyChart('weekday');
            const canvas = document.getElementById('chart-weekday');
            if (!canvas || !this.analyticsData.weekday.length) return;

            const ctx = canvas.getContext('2d');
            const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
            const data = this.analyticsData.weekday.sort((a, b) => a.day_of_week - b.day_of_week);

            this.analyticsCharts.weekday = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: data.map(d => dayNames[d.day_of_week]),
                    datasets: [{
                        label: 'Avg. Spending',
                        data: data.map(d => d.avg_spending),
                        backgroundColor: 'rgba(20, 184, 166, 0.7)',
                        borderColor: 'rgb(20, 184, 166)',
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        title: {
                            display: true,
                            text: 'Average Spending by Day of Week',
                            color: '#f3f4f6',
                            font: { size: 14 }
                        },
                        tooltip: {
                            callbacks: {
                                label: (context) => {
                                    const item = data[context.dataIndex];
                                    return `Avg: $${context.raw.toFixed(2)} (${item.count} days)`;
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            ticks: { color: '#9ca3af' },
                            grid: { color: 'rgba(75, 85, 99, 0.3)' }
                        },
                        y: {
                            ticks: {
                                color: '#9ca3af',
                                callback: (value) => '$' + value.toLocaleString()
                            },
                            grid: { color: 'rgba(75, 85, 99, 0.3)' }
                        }
                    }
                }
            });
        },

        // === Subscription Management ===
        async loadSubscriptions() {
            try {
                const [subs, summary] = await Promise.all([
                    this.api('/subscriptions'),
                    this.api('/subscriptions/summary')
                ]);
                this.subscriptions = subs;
                this.subscriptionsSummary = summary;
            } catch (error) {
                console.error('Failed to load subscriptions:', error);
            }
        },

        openAddSubscription() {
            this.editingSubscription = null;
            this.newSubName = '';
            this.newSubMerchant = '';
            this.newSubAmount = 0;
            this.newSubFrequency = 'monthly';
            this.newSubStartDate = new Date().toISOString().split('T')[0];
            this.newSubCategoryId = null;
            this.newSubAccountId = null;
            this.newSubNotes = '';
            this.showSubscriptionModal = true;
        },

        openEditSubscription(sub) {
            this.editingSubscription = sub;
            this.newSubName = sub.name;
            this.newSubMerchant = sub.merchant || '';
            this.newSubAmount = Math.abs(sub.amount);
            this.newSubFrequency = sub.frequency;
            this.newSubStartDate = sub.start_date;
            this.newSubCategoryId = sub.category_id;
            this.newSubAccountId = sub.account_id;
            this.newSubNotes = sub.notes || '';
            this.showSubscriptionModal = true;
        },

        closeSubscriptionModal() {
            this.showSubscriptionModal = false;
            this.editingSubscription = null;
        },

        async saveSubscription() {
            if (!this.newSubName.trim() || !this.newSubAmount) return;

            try {
                const subData = {
                    name: this.newSubName.trim(),
                    merchant: this.newSubMerchant.trim() || null,
                    amount: -Math.abs(this.newSubAmount), // Always negative for expenses
                    frequency: this.newSubFrequency,
                    start_date: this.newSubStartDate,
                    category_id: this.newSubCategoryId || null,
                    account_id: this.newSubAccountId || null,
                    notes: this.newSubNotes.trim() || null
                };

                if (this.editingSubscription) {
                    await this.api(`/subscriptions/${this.editingSubscription.id}`, {
                        method: 'PUT',
                        body: JSON.stringify(subData)
                    });
                } else {
                    await this.api('/subscriptions', {
                        method: 'POST',
                        body: JSON.stringify(subData)
                    });
                }

                this.closeSubscriptionModal();
                await this.loadSubscriptions();
            } catch (error) {
                alert('Failed to save subscription: ' + error.message);
            }
        },

        async deleteSubscription(sub) {
            if (!confirm(`Are you sure you want to delete "${sub.name}"?`)) return;

            try {
                await this.api(`/subscriptions/${sub.id}`, { method: 'DELETE' });
                await this.loadSubscriptions();
            } catch (error) {
                alert('Failed to delete subscription: ' + error.message);
            }
        },

        // Opens the payment modal to record payment details
        openPaymentModal(sub) {
            this.paymentSubscription = sub;
            this.paymentForm = {
                amount_paid: Math.abs(sub.amount).toString(),
                payment_method: '',
                confirmation_number: '',
                notes: ''
            };
            this.showPaymentModal = true;
        },

        closePaymentModal() {
            this.showPaymentModal = false;
            this.paymentSubscription = null;
        },

        async submitPayment() {
            if (!this.paymentSubscription) return;

            try {
                const payload = {
                    amount_paid: parseFloat(this.paymentForm.amount_paid) || null,
                    payment_method: this.paymentForm.payment_method || null,
                    confirmation_number: this.paymentForm.confirmation_number || null,
                    notes: this.paymentForm.notes || null
                };

                await this.api(`/subscriptions/${this.paymentSubscription.id}/paid`, {
                    method: 'POST',
                    body: JSON.stringify(payload)
                });

                this.closePaymentModal();
                await this.loadSubscriptions();
                await this.loadBillsDueSoon();
            } catch (error) {
                alert('Failed to record payment: ' + error.message);
            }
        },

        // Quick mark as paid without details (for backwards compatibility)
        async markSubscriptionPaidQuick(sub) {
            try {
                await this.api(`/subscriptions/${sub.id}/paid`, { method: 'POST' });
                await this.loadSubscriptions();
                await this.loadBillsDueSoon();
            } catch (error) {
                alert('Failed to mark as paid: ' + error.message);
            }
        },

        async openPaymentHistoryModal(sub) {
            this.paymentHistorySubscription = sub;
            this.paymentHistory = [];
            this.showPaymentHistoryModal = true;
            await this.loadPaymentHistory(sub.id);
        },

        closePaymentHistoryModal() {
            this.showPaymentHistoryModal = false;
            this.paymentHistorySubscription = null;
            this.paymentHistory = [];
        },

        async loadPaymentHistory(subId) {
            try {
                this.paymentHistory = await this.api(`/subscriptions/${subId}/payments`);
            } catch (error) {
                console.error('Failed to load payment history:', error);
                this.paymentHistory = [];
            }
        },

        async loadBillsDueSoon() {
            try {
                this.billsDueSoon = await this.api('/bills/due-soon?days=14');
            } catch (error) {
                console.error('Failed to load bills due soon:', error);
                this.billsDueSoon = [];
            }
        },

        async deletePaymentRecord(paymentId) {
            if (!confirm('Are you sure you want to delete this payment record?')) return;

            try {
                await this.api(`/bill-payments/${paymentId}`, { method: 'DELETE' });
                if (this.paymentHistorySubscription) {
                    await this.loadPaymentHistory(this.paymentHistorySubscription.id);
                }
                await this.loadSubscriptions();
            } catch (error) {
                alert('Failed to delete payment: ' + error.message);
            }
        },

        getBillStatusClass(status) {
            switch (status) {
                case 'overdue': return 'bg-red-600 text-red-100';
                case 'due_soon': return 'bg-yellow-600 text-yellow-100';
                case 'upcoming': return 'bg-blue-600 text-blue-100';
                default: return 'bg-gray-600 text-gray-300';
            }
        },

        getBillStatusLabel(bill) {
            if (bill.status === 'overdue') {
                return `${Math.abs(Math.floor(bill.days_until_due))}d overdue`;
            } else if (bill.days_until_due <= 0) {
                return 'Due today';
            } else {
                return `Due in ${Math.ceil(bill.days_until_due)}d`;
            }
        },

        async toggleSubscriptionActive(sub) {
            try {
                await this.api(`/subscriptions/${sub.id}`, {
                    method: 'PUT',
                    body: JSON.stringify({ is_active: !sub.is_active })
                });
                await this.loadSubscriptions();
            } catch (error) {
                alert('Failed to update subscription: ' + error.message);
            }
        },

        getFrequencyLabel(freq) {
            const labels = {
                'daily': 'Daily',
                'weekly': 'Weekly',
                'biweekly': 'Bi-weekly',
                'monthly': 'Monthly',
                'quarterly': 'Quarterly',
                'yearly': 'Yearly',
                'annually': 'Annually'
            };
            return labels[freq] || freq;
        },

        getSubscriptionStatus(sub) {
            if (!sub.is_active) return { label: 'Paused', class: 'bg-gray-600 text-gray-300' };
            if (!sub.next_due_date) return { label: 'Unknown', class: 'bg-gray-600 text-gray-300' };

            const today = new Date();
            today.setHours(0, 0, 0, 0);
            const dueDate = new Date(sub.next_due_date);

            const diffTime = dueDate - today;
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

            if (diffDays < 0) return { label: `${Math.abs(diffDays)}d overdue`, class: 'bg-red-600 text-red-100' };
            if (diffDays === 0) return { label: 'Due today', class: 'bg-yellow-600 text-yellow-100' };
            if (diffDays <= 7) return { label: `Due in ${diffDays}d`, class: 'bg-yellow-600 text-yellow-100' };
            return { label: `Due in ${diffDays}d`, class: 'bg-green-600 text-green-100' };
        },

        getMonthlyEquivalent(sub) {
            const amount = Math.abs(sub.amount);
            switch (sub.frequency) {
                case 'daily': return amount * 30;
                case 'weekly': return amount * 4.33;
                case 'biweekly': return amount * 2.17;
                case 'monthly': return amount;
                case 'quarterly': return amount / 3;
                case 'yearly':
                case 'annually': return amount / 12;
                default: return amount;
            }
        },

        // === Transaction Splitting Functions ===

        async openSplitModal(txn) {
            this.splittingTransaction = txn;
            this.splits = [];

            // Load existing splits if any
            try {
                const existing = await fetch(`/api/transactions/${txn.id}/splits`).then(r => r.json());
                if (existing.length > 0) {
                    this.splits = existing.map(s => ({
                        category_id: s.category_id,
                        amount: s.amount,
                        description: s.description || ''
                    }));
                } else {
                    // Start with two empty splits
                    this.splits = [
                        { category_id: txn.category_id || null, amount: Math.abs(txn.amount), description: '' },
                        { category_id: null, amount: 0, description: '' }
                    ];
                }
            } catch (e) {
                // Start with two empty splits
                this.splits = [
                    { category_id: txn.category_id || null, amount: Math.abs(txn.amount), description: '' },
                    { category_id: null, amount: 0, description: '' }
                ];
            }

            this.updateSplitRemaining();
            this.showSplitModal = true;
        },

        closeSplitModal() {
            this.showSplitModal = false;
            this.splittingTransaction = null;
            this.splits = [];
        },

        addSplit() {
            this.splits.push({ category_id: null, amount: 0, description: '' });
        },

        removeSplit(index) {
            if (this.splits.length > 2) {
                this.splits.splice(index, 1);
                this.updateSplitRemaining();
            }
        },

        updateSplitRemaining() {
            if (!this.splittingTransaction) return;
            const total = Math.abs(this.splittingTransaction.amount);
            const used = this.splits.reduce((sum, s) => sum + (parseFloat(s.amount) || 0), 0);
            this.splitRemaining = Math.round((total - used) * 100) / 100;
        },

        autoFillLastSplit() {
            if (this.splits.length > 0 && this.splitRemaining !== 0) {
                const lastSplit = this.splits[this.splits.length - 1];
                lastSplit.amount = Math.round((parseFloat(lastSplit.amount || 0) + this.splitRemaining) * 100) / 100;
                this.updateSplitRemaining();
            }
        },

        async saveSplits() {
            if (!this.splittingTransaction) return;

            // Validate all splits have categories
            const validSplits = this.splits.filter(s => s.category_id && s.amount > 0);
            if (validSplits.length < 2) {
                alert('Please add at least 2 valid splits with categories and amounts');
                return;
            }

            // Check remaining is zero
            if (Math.abs(this.splitRemaining) > 0.01) {
                alert(`Split amounts must equal the transaction total. Remaining: $${this.splitRemaining.toFixed(2)}`);
                return;
            }

            try {
                const response = await fetch(`/api/transactions/${this.splittingTransaction.id}/splits`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ splits: validSplits })
                });

                const result = await response.json();
                if (result.success) {
                    this.closeSplitModal();
                    await this.loadTransactions();
                } else {
                    alert(result.error || 'Failed to save splits');
                }
            } catch (e) {
                console.error('Error saving splits:', e);
                alert('Failed to save splits');
            }
        },

        async unsplitTransaction(txnId) {
            if (!confirm('Remove all splits from this transaction?')) return;

            try {
                const response = await fetch(`/api/transactions/${txnId}/splits`, { method: 'DELETE' });
                const result = await response.json();
                if (result.success) {
                    await this.loadTransactions();
                }
            } catch (e) {
                console.error('Error unsplitting:', e);
            }
        },

        // === Import Wizard Functions ===

        openImportWizard() {
            this.showImportWizard = true;
            this.importStep = 1;
            this.importSession = null;
            this.importFilename = '';
            this.importColumns = [];
            this.importSampleRows = [];
            this.importTotalRows = 0;
            this.importMapping = {
                date_column: '',
                amount_column: '',
                merchant_column: '',
                description_column: ''
            };
            this.importAccountId = null;
            this.importAutoCategorize = false;
            this.importResult = null;
            this.importLoading = false;
        },

        closeImportWizard() {
            // Cancel session if exists
            if (this.importSession) {
                fetch(`/api/import/session/${this.importSession}`, { method: 'DELETE' });
            }
            this.showImportWizard = false;
        },

        async handleImportFile(event) {
            const file = event.target.files[0];
            if (!file) return;

            this.importLoading = true;
            const formData = new FormData();
            formData.append('file', file);

            try {
                const response = await fetch('/api/import/preview', {
                    method: 'POST',
                    body: formData
                });

                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Failed to parse file');
                }

                const data = await response.json();
                this.importSession = data.session_id;
                this.importFilename = data.filename;
                this.importColumns = data.columns;
                this.importSampleRows = data.sample_rows;
                this.importTotalRows = data.total_rows;

                // Set detected mapping
                const detected = data.detected_mapping;
                this.importMapping = {
                    date_column: detected.date || '',
                    amount_column: detected.amount || '',
                    merchant_column: detected.merchant || '',
                    description_column: detected.description || ''
                };

                this.importStep = 2;
            } catch (e) {
                alert('Error: ' + e.message);
            } finally {
                this.importLoading = false;
                event.target.value = ''; // Reset file input
            }
        },

        goToPreview() {
            // Validate required mappings
            if (!this.importMapping.date_column) {
                alert('Please select a Date column');
                return;
            }
            if (!this.importMapping.amount_column) {
                alert('Please select an Amount column');
                return;
            }
            if (!this.importMapping.merchant_column) {
                alert('Please select a Merchant column');
                return;
            }
            this.importStep = 3;
        },

        getPreviewValue(row, field) {
            const colName = this.importMapping[field + '_column'];
            if (!colName) return '-';
            return row[colName] || '-';
        },

        async confirmImport() {
            this.importLoading = true;
            this.importStep = 4;

            try {
                const response = await fetch('/api/import/confirm', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        session_id: this.importSession,
                        column_mapping: this.importMapping,
                        account_id: this.importAccountId || null,
                        auto_categorize: this.importAutoCategorize
                    })
                });

                const result = await response.json();
                this.importResult = result;
                this.importSession = null; // Session is cleaned up

                // Reload transactions
                await this.loadTransactions();
            } catch (e) {
                this.importResult = { success: false, error: e.message };
            } finally {
                this.importLoading = false;
            }
        },

        resetImportWizard() {
            this.importStep = 1;
            this.importSession = null;
            this.importFilename = '';
            this.importColumns = [];
            this.importSampleRows = [];
            this.importTotalRows = 0;
            this.importMapping = {
                date_column: '',
                amount_column: '',
                merchant_column: '',
                description_column: ''
            };
            this.importResult = null;
        },

        // ==================== BUDGET OVERVIEW ====================

        async loadBudgetOverview() {
            this.budgetLoading = true;
            try {
                const [overview, settings] = await Promise.all([
                    this.api('/budget/overview'),
                    this.api('/budget/settings')
                ]);
                this.budgetOverview = overview;
                this.budgetSettings = settings;
            } catch (e) {
                console.error('Failed to load budget overview:', e);
            } finally {
                this.budgetLoading = false;
            }
        },

        openBudgetSettingsModal() {
            this.showBudgetSettingsModal = true;
        },

        closeBudgetSettingsModal() {
            this.showBudgetSettingsModal = false;
        },

        async saveBudgetSettings() {
            try {
                await this.api('/budget/settings', {
                    method: 'PUT',
                    body: JSON.stringify(this.budgetSettings)
                });
                this.closeBudgetSettingsModal();
                await this.loadBudgetOverview();
            } catch (e) {
                console.error('Failed to save budget settings:', e);
                alert('Failed to save settings');
            }
        },

        getBudgetHealthClass(overview) {
            if (!overview) return 'text-gray-400';
            const disc = overview.discretionary;
            if (disc.is_over) return 'text-red-400';
            if (disc.percent_used >= 80) return 'text-yellow-400';
            return 'text-green-400';
        },

        getBudgetHealthText(overview) {
            if (!overview) return 'Loading...';
            const disc = overview.discretionary;
            if (disc.is_over) return 'Over Budget';
            if (disc.percent_used >= 80) return 'Getting Tight';
            if (disc.percent_used >= 50) return 'On Track';
            return 'Healthy';
        },

        getWarningClass(type) {
            switch (type) {
                case 'danger': return 'bg-red-900 border-red-700 text-red-200';
                case 'warning': return 'bg-yellow-900 border-yellow-700 text-yellow-200';
                case 'info': return 'bg-blue-900 border-blue-700 text-blue-200';
                default: return 'bg-gray-700 border-gray-600 text-gray-300';
            }
        },

        // ==================== SAVINGS GOALS ====================

        async loadSavingsGoals() {
            try {
                const response = await this.api('/api/goals');
                this.savingsGoals = response || [];
            } catch (e) {
                console.error('Failed to load savings goals:', e);
                this.savingsGoals = [];
            }
        },

        calculateGoalProgress(goal) {
            if (!goal.target_amount || goal.target_amount <= 0) return 0;
            return Math.min(100, Math.round((goal.current_amount / goal.target_amount) * 100));
        },

        getDaysRemaining(targetDate) {
            if (!targetDate) return null;
            const target = new Date(targetDate);
            const today = new Date();
            const diffTime = target - today;
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
            return diffDays;
        },

        getMonthlyNeeded(goal) {
            if (!goal.target_date) return null;
            const remaining = goal.target_amount - goal.current_amount;
            if (remaining <= 0) return 0;
            const daysRemaining = this.getDaysRemaining(goal.target_date);
            if (!daysRemaining || daysRemaining <= 0) return remaining;
            const monthsRemaining = daysRemaining / 30;
            return Math.ceil(remaining / monthsRemaining);
        },

        openGoalModal(goal = null) {
            this.editingGoal = goal;
            if (goal) {
                this.goalForm = {
                    name: goal.name,
                    description: goal.description || '',
                    target_amount: goal.target_amount,
                    target_date: goal.target_date || '',
                    color: goal.color || '#10B981',
                    icon: goal.icon || 'piggy-bank'
                };
            } else {
                this.goalForm = {
                    name: '',
                    description: '',
                    target_amount: '',
                    target_date: '',
                    color: '#10B981',
                    icon: 'piggy-bank'
                };
            }
            this.showGoalModal = true;
        },

        closeGoalModal() {
            this.showGoalModal = false;
            this.editingGoal = null;
        },

        async saveGoal() {
            if (!this.goalForm.name || !this.goalForm.target_amount) {
                alert('Please enter a goal name and target amount');
                return;
            }

            const payload = {
                name: this.goalForm.name,
                description: this.goalForm.description || null,
                target_amount: parseFloat(this.goalForm.target_amount),
                target_date: this.goalForm.target_date || null,
                color: this.goalForm.color,
                icon: this.goalForm.icon
            };

            try {
                if (this.editingGoal) {
                    await this.api(`/api/goals/${this.editingGoal.id}`, 'PUT', payload);
                } else {
                    await this.api('/api/goals', 'POST', payload);
                }
                await this.loadSavingsGoals();
                this.closeGoalModal();
            } catch (e) {
                alert('Failed to save goal: ' + e.message);
            }
        },

        async deleteGoal(goalId) {
            if (!confirm('Are you sure you want to delete this savings goal? All contributions will also be deleted.')) {
                return;
            }
            try {
                await this.api(`/api/goals/${goalId}`, 'DELETE');
                await this.loadSavingsGoals();
            } catch (e) {
                alert('Failed to delete goal: ' + e.message);
            }
        },

        openContributionModal(goal) {
            this.contributionGoal = goal;
            this.contributionAmount = '';
            this.contributionNote = '';
            this.loadGoalContributions(goal.id);
            this.showContributionModal = true;
        },

        closeContributionModal() {
            this.showContributionModal = false;
            this.contributionGoal = null;
            this.goalContributions = [];
        },

        async loadGoalContributions(goalId) {
            try {
                const response = await this.api(`/api/goals/${goalId}/contributions`);
                this.goalContributions = response || [];
            } catch (e) {
                console.error('Failed to load contributions:', e);
                this.goalContributions = [];
            }
        },

        async addContribution() {
            if (!this.contributionAmount) {
                alert('Please enter an amount');
                return;
            }

            const amount = parseFloat(this.contributionAmount);
            if (isNaN(amount) || amount === 0) {
                alert('Please enter a valid amount');
                return;
            }

            try {
                await this.api(`/api/goals/${this.contributionGoal.id}/contributions`, 'POST', {
                    amount: amount,
                    note: this.contributionNote || null
                });
                this.contributionAmount = '';
                this.contributionNote = '';
                await this.loadGoalContributions(this.contributionGoal.id);
                await this.loadSavingsGoals();
                // Update the local goal object
                const updatedGoal = this.savingsGoals.find(g => g.id === this.contributionGoal.id);
                if (updatedGoal) {
                    this.contributionGoal = updatedGoal;
                }
            } catch (e) {
                alert('Failed to add contribution: ' + e.message);
            }
        },

        async deleteContribution(contributionId) {
            if (!confirm('Delete this contribution?')) return;
            try {
                await this.api(`/api/goals/${this.contributionGoal.id}/contributions/${contributionId}`, 'DELETE');
                await this.loadGoalContributions(this.contributionGoal.id);
                await this.loadSavingsGoals();
                const updatedGoal = this.savingsGoals.find(g => g.id === this.contributionGoal.id);
                if (updatedGoal) {
                    this.contributionGoal = updatedGoal;
                }
            } catch (e) {
                alert('Failed to delete contribution: ' + e.message);
            }
        },

        getGoalIcon(iconName) {
            const icons = {
                'piggy-bank': '🐷',
                'home': '🏠',
                'car': '🚗',
                'vacation': '✈️',
                'education': '🎓',
                'emergency': '🚨',
                'gift': '🎁',
                'health': '💊',
                'wedding': '💒',
                'baby': '👶',
                'retirement': '🏖️',
                'tech': '💻',
                'other': '🎯'
            };
            return icons[iconName] || '🎯';
        }
    };
}
