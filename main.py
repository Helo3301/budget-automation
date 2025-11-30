#!/usr/bin/env python3
"""Budget Automation CLI - Local RAG-based transaction categorization."""
import argparse
import sys
import logging
from pathlib import Path
from typing import Optional

from budget_automation.api.budget_service import BudgetService


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )


def cmd_import(args):
    """Import transactions from CSV/Excel file."""
    file_path = Path(args.file)
    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        return 1

    with BudgetService() as service:
        def progress(current, total):
            pct = (current / total * 100) if total > 0 else 0
            print(f"\rProcessing: {current}/{total} ({pct:.0f}%)", end="", flush=True)

        result = service.import_file(
            file_path,
            auto_categorize=not args.no_categorize,
            progress_callback=progress if not args.quiet else None
        )

        print()  # New line after progress
        print(f"\nImport complete:")
        print(f"  Total parsed:  {result['total_parsed']}")
        print(f"  Added:         {result['added']}")
        print(f"  Duplicates:    {result['duplicates']}")
        print(f"  Categorized:   {result['categorized']}")

    return 0


def cmd_analyze(args):
    """Analyze transactions for patterns."""
    with BudgetService() as service:
        result = service.analyze_transactions()

        print("Analysis complete:")
        print(f"\nRecurring patterns found: {len(result['recurring_patterns'])}")
        for r in result["recurring_patterns"][:10]:
            print(f"  - {r['merchant']}: ${abs(r['amount']):.2f} ({r.get('interval_type', 'recurring')})")

        print(f"\nAnomalies found: {len(result['anomalies'])}")
        for a in result["anomalies"][:10]:
            print(f"  - {a['merchant']}: ${abs(a['amount']):.2f} - {a['reason']}")

    return 0


def cmd_summary(args):
    """Show summary of all transactions."""
    with BudgetService() as service:
        summary = service.get_summary()

        print("=" * 50)
        print("BUDGET SUMMARY")
        print("=" * 50)
        print(f"\nTotal transactions: {summary['total_transactions']}")
        print(f"Total income:       ${summary['total_income']:,.2f}")
        print(f"Total expenses:     ${summary['total_expenses']:,.2f}")
        print(f"Net:                ${summary['net']:,.2f}")
        print(f"\nRecurring:          {summary['recurring_transactions']}")
        print(f"Anomalies:          {summary['anomalies']}")
        print(f"Uncategorized:      {summary['uncategorized']}")

        if summary["category_breakdown"]:
            print("\n" + "-" * 50)
            print("CATEGORY BREAKDOWN")
            print("-" * 50)
            sorted_cats = sorted(
                summary["category_breakdown"].items(),
                key=lambda x: x[1]["total"],
                reverse=True
            )
            for name, data in sorted_cats:
                print(f"  {name:20s}  {data['count']:4d} txns  ${data['total']:10,.2f}")

    return 0


def cmd_search(args):
    """Search for similar transactions."""
    with BudgetService() as service:
        results = service.search_similar(args.query, k=args.limit)

        if not results:
            print("No matching transactions found.")
            return 0

        print(f"Found {len(results)} similar transactions:\n")
        for r in results:
            score = r.get("similarity_score", 0)
            print(f"  [{score:.2f}] {r['date']} | {r['merchant']:20s} | ${r['amount']:10.2f}")
            if r.get("description"):
                print(f"         {r['description'][:50]}")

    return 0


def cmd_uncategorized(args):
    """List uncategorized transactions."""
    with BudgetService() as service:
        txns = service.get_uncategorized()

        if not txns:
            print("All transactions are categorized!")
            return 0

        print(f"Found {len(txns)} uncategorized transactions:\n")
        for t in txns[:args.limit]:
            print(f"  ID {t['id']:5d} | {t['date']} | {t['merchant']:20s} | ${t['amount']:10.2f}")

    return 0


def cmd_categorize(args):
    """Manually categorize a transaction."""
    with BudgetService() as service:
        if args.category:
            success = service.categorize_transaction(args.txn_id, args.category)
            if success:
                print(f"Transaction {args.txn_id} categorized as '{args.category}'")
            else:
                print(f"Error: Category '{args.category}' not found")
                print("Available categories:")
                for c in service.get_categories():
                    print(f"  - {c['name']}")
                return 1
        else:
            # Show transaction and ask for category
            txn = service.get_transaction(args.txn_id)
            if not txn:
                print(f"Transaction {args.txn_id} not found")
                return 1

            print(f"Transaction {args.txn_id}:")
            print(f"  Date:     {txn['date']}")
            print(f"  Merchant: {txn['merchant']}")
            print(f"  Amount:   ${txn['amount']:.2f}")
            print(f"  Desc:     {txn.get('description', 'N/A')}")
            print("\nAvailable categories:")
            for c in service.get_categories():
                print(f"  - {c['name']}")

    return 0


def cmd_explain(args):
    """Show explanation for a transaction's categorization."""
    with BudgetService() as service:
        txn = service.get_transaction(args.txn_id)
        if not txn:
            print(f"Transaction {args.txn_id} not found")
            return 1

        print(f"Transaction {args.txn_id}:")
        print(f"  Date:     {txn['date']}")
        print(f"  Merchant: {txn['merchant']}")
        print(f"  Amount:   ${txn['amount']:.2f}")

        explanation = service.get_categorization_explanation(args.txn_id)
        if explanation:
            print(f"\nExplanation: {explanation}")
        else:
            print("\nNo categorization explanation available.")

    return 0


def cmd_categories(args):
    """List all categories."""
    with BudgetService() as service:
        categories = service.get_categories()

        print("Available categories:")
        for c in categories:
            print(f"  - {c['name']}")

    return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Budget Automation - Local RAG-based transaction categorization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  budget import transactions.csv       Import transactions from CSV
  budget analyze                        Detect recurring patterns and anomalies
  budget summary                        Show budget summary
  budget search "coffee"               Find similar transactions
  budget uncategorized                  List transactions needing categories
  budget categorize 123 "Food & Dining" Manually categorize a transaction
  budget explain 123                    Show why a transaction was categorized
"""
    )

    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Import command
    import_parser = subparsers.add_parser("import", help="Import transactions from file")
    import_parser.add_argument("file", help="CSV or Excel file to import")
    import_parser.add_argument("--no-categorize", action="store_true",
                               help="Don't auto-categorize imported transactions")
    import_parser.add_argument("-q", "--quiet", action="store_true", help="Suppress progress output")
    import_parser.set_defaults(func=cmd_import)

    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze transactions")
    analyze_parser.set_defaults(func=cmd_analyze)

    # Summary command
    summary_parser = subparsers.add_parser("summary", help="Show budget summary")
    summary_parser.set_defaults(func=cmd_summary)

    # Search command
    search_parser = subparsers.add_parser("search", help="Search for similar transactions")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("-n", "--limit", type=int, default=10, help="Max results")
    search_parser.set_defaults(func=cmd_search)

    # Uncategorized command
    uncat_parser = subparsers.add_parser("uncategorized", help="List uncategorized transactions")
    uncat_parser.add_argument("-n", "--limit", type=int, default=20, help="Max results")
    uncat_parser.set_defaults(func=cmd_uncategorized)

    # Categorize command
    cat_parser = subparsers.add_parser("categorize", help="Categorize a transaction")
    cat_parser.add_argument("txn_id", type=int, help="Transaction ID")
    cat_parser.add_argument("category", nargs="?", help="Category name")
    cat_parser.set_defaults(func=cmd_categorize)

    # Explain command
    explain_parser = subparsers.add_parser("explain", help="Show categorization explanation")
    explain_parser.add_argument("txn_id", type=int, help="Transaction ID")
    explain_parser.set_defaults(func=cmd_explain)

    # Categories command
    cats_parser = subparsers.add_parser("categories", help="List all categories")
    cats_parser.set_defaults(func=cmd_categories)

    args = parser.parse_args()
    setup_logging(args.verbose)

    if not args.command:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
