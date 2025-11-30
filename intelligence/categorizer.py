"""RAG-based categorizer using local embeddings and Claude API."""
from typing import Dict, Any, List, Optional
import numpy as np
from collections import Counter

from budget_automation.db.sqlite_store import SQLiteStore
from budget_automation.db.vector_store import VectorStore
from budget_automation.intelligence.embedder import LocalEmbedder
from budget_automation.config import (
    SIMILARITY_THRESHOLD,
    TOP_K_SIMILAR,
    MIN_MATCHES_FOR_AUTO
)


class RAGCategorizer:
    """Categorize transactions using RAG context and optional Claude API."""

    def __init__(
        self,
        store: SQLiteStore,
        vector_store: VectorStore,
        embedder: LocalEmbedder,
        claude_client: Optional[Any] = None
    ):
        """Initialize the categorizer.

        Args:
            store: SQLiteStore for transaction data
            vector_store: VectorStore for embeddings
            embedder: LocalEmbedder for generating embeddings
            claude_client: Optional Anthropic client (injected for testing)
        """
        self.store = store
        self.vector_store = vector_store
        self.embedder = embedder
        self.claude_client = claude_client
        self._category_map = None

    def get_available_categories(self) -> List[Dict[str, Any]]:
        """Get list of available categories."""
        return self.store.get_all_categories()

    def _get_category_map(self) -> Dict[int, str]:
        """Get mapping of category ID to name."""
        if self._category_map is None:
            categories = self.get_available_categories()
            self._category_map = {c["id"]: c["name"] for c in categories}
        return self._category_map

    def _get_category_id_by_name(self, name: str) -> Optional[int]:
        """Get category ID by name."""
        cat_map = self._get_category_map()
        for cat_id, cat_name in cat_map.items():
            if cat_name.lower() == name.lower():
                return cat_id
        return None

    def categorize(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Categorize a transaction using RAG context.

        Args:
            transaction: Dict with merchant, description, amount

        Returns:
            Dict with category_id, confidence, explanation, method
        """
        # Build RAG context
        context = self._build_rag_context(transaction)

        # Check if we can auto-categorize
        auto_result = self._try_auto_categorize(context)
        if auto_result:
            return auto_result

        # Return needs_review instead of calling Claude API
        # User will manually categorize in the UI
        return {
            "category_id": None,
            "category_name": None,
            "confidence": 0.0,
            "explanation": "Needs manual review - not enough similar transactions to auto-categorize.",
            "method": "needs_review",
            "similar_transactions": context["similar_transactions"],
            "suggestions": self._get_suggestions(context)
        }

    def _get_suggestions(self, context: Dict[str, Any]) -> List[str]:
        """Get category suggestions based on similar transactions."""
        distribution = context.get("category_distribution", {})
        if not distribution:
            return []
        # Return top 3 most common categories from similar transactions
        sorted_cats = sorted(distribution.items(), key=lambda x: x[1], reverse=True)
        return [cat for cat, _ in sorted_cats[:3]]

    def categorize_and_update(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Categorize and update the transaction in database.

        Args:
            transaction: Dict with id, merchant, description, amount

        Returns:
            Categorization result
        """
        result = self.categorize(transaction)

        # Update transaction category
        self.store.update_transaction_category(
            transaction["id"],
            result["category_id"]
        )

        # Add to vector store
        embedding = self.embedder.embed_transaction(
            transaction["merchant"],
            transaction.get("description"),
            transaction["amount"]
        )

        cat_map = self._get_category_map()
        category_name = cat_map.get(result["category_id"], "Other")

        self.vector_store.add_embedding(
            transaction["id"],
            embedding,
            transaction["merchant"],
            category_name,
            transaction["amount"]
        )

        # Log categorization
        similar_ids = [s["transaction_id"] for s in result.get("similar_transactions", [])]
        self.store.add_categorization_log(
            transaction_id=transaction["id"],
            category_id=result["category_id"],
            confidence=result["confidence"],
            explanation=result["explanation"],
            similar_transaction_ids=similar_ids
        )

        return result

    def _build_rag_context(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Build context with similar transactions for RAG.

        Args:
            transaction: Transaction to find context for

        Returns:
            Dict with similar_transactions and category_distribution
        """
        # Generate embedding for the transaction
        embedding = self.embedder.embed_transaction(
            transaction["merchant"],
            transaction.get("description"),
            transaction["amount"]
        )

        # Search for similar transactions
        similar = self.vector_store.search(embedding, k=TOP_K_SIMILAR)

        # Build context
        context = {
            "similar_transactions": similar,
            "category_distribution": self._get_category_distribution(similar),
            "embedding": embedding
        }

        return context

    def _get_category_distribution(self, similar: List[Dict[str, Any]]) -> Dict[str, int]:
        """Get distribution of categories in similar transactions."""
        if not similar:
            return {}

        categories = [s.get("category", "Unknown") for s in similar]
        return dict(Counter(categories))

    def _try_auto_categorize(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Try to auto-categorize without API call.

        Returns result if confident, None otherwise.
        """
        similar = context["similar_transactions"]
        if len(similar) < MIN_MATCHES_FOR_AUTO:
            return None

        # Check if enough similar transactions agree on category
        distribution = context["category_distribution"]
        if not distribution:
            return None

        # Find most common category
        most_common = max(distribution.items(), key=lambda x: x[1])
        category_name, count = most_common

        # Calculate confidence based on agreement ratio
        agreement_ratio = count / len(similar)

        if agreement_ratio >= SIMILARITY_THRESHOLD and count >= MIN_MATCHES_FOR_AUTO:
            category_id = self._get_category_id_by_name(category_name)
            if category_id is None:
                return None

            return {
                "category_id": category_id,
                "category_name": category_name,
                "confidence": agreement_ratio,
                "explanation": f"Auto-categorized based on {count} similar transactions all categorized as {category_name}.",
                "method": "auto",
                "similar_transactions": similar
            }

        return None

    def _categorize_with_claude(
        self,
        transaction: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Categorize using Claude API with RAG context.

        Args:
            transaction: Transaction to categorize
            context: RAG context with similar transactions

        Returns:
            Categorization result
        """
        # Build prompt for Claude
        prompt = self._build_claude_prompt(transaction, context)

        # Call Claude
        result = self._call_claude(prompt)

        # Get category ID
        category_id = self._get_category_id_by_name(result["category"])
        if category_id is None:
            # Default to "Other" if category not found
            category_id = self._get_category_id_by_name("Other")

        return {
            "category_id": category_id,
            "category_name": result["category"],
            "confidence": result.get("confidence", 0.5),
            "explanation": result.get("explanation", "Categorized by Claude."),
            "method": "claude",
            "similar_transactions": context["similar_transactions"]
        }

    def _build_claude_prompt(
        self,
        transaction: Dict[str, Any],
        context: Dict[str, Any]
    ) -> str:
        """Build prompt for Claude API."""
        categories = self.get_available_categories()
        category_names = [c["name"] for c in categories]

        similar = context["similar_transactions"]

        prompt = f"""Categorize this transaction:
Merchant: {transaction['merchant']}
Description: {transaction.get('description', 'N/A')}
Amount: ${abs(transaction['amount']):.2f}

Available categories: {', '.join(category_names)}

"""
        if similar:
            prompt += "Similar past transactions:\n"
            for s in similar[:5]:
                prompt += f"- {s.get('merchant', 'Unknown')}: ${abs(s.get('amount', 0)):.2f} -> {s.get('category', 'Unknown')}\n"

        prompt += """
Respond with JSON: {"category": "<category name>", "confidence": <0-1>, "explanation": "<1-2 sentences>"}"""

        return prompt

    def _call_claude(self, prompt: str) -> Dict[str, Any]:
        """Call Claude API for categorization.

        This method should be mocked in tests.
        """
        if self.claude_client is None:
            # Lazy import for production
            try:
                import anthropic
                from budget_automation.config import CLAUDE_MODEL
                self.claude_client = anthropic.Anthropic()
            except Exception:
                # Return default if API not available
                return {
                    "category": "Other",
                    "confidence": 0.5,
                    "explanation": "Claude API not available, defaulted to Other."
                }

        try:
            import json
            from budget_automation.config import CLAUDE_MODEL

            response = self.claude_client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}]
            )

            # Parse JSON response
            text = response.content[0].text
            # Extract JSON from response
            if "{" in text:
                json_str = text[text.index("{"):text.rindex("}")+1]
                return json.loads(json_str)

        except Exception as e:
            pass

        return {
            "category": "Other",
            "confidence": 0.5,
            "explanation": f"Error calling Claude API, defaulted to Other."
        }
