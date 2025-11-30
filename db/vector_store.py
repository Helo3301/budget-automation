"""LanceDB vector store for transaction embeddings."""
import lancedb
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional
import pyarrow as pa


class VectorStore:
    """LanceDB storage for transaction embeddings with semantic search."""

    TABLE_NAME = "transaction_embeddings"

    def __init__(self, db_path: Path, dim: int = 384):
        """Initialize the vector store.

        Args:
            db_path: Path to LanceDB directory
            dim: Embedding dimension (default 384 for all-MiniLM-L6-v2)
        """
        self.db_path = db_path
        self.dim = dim
        self.db = lancedb.connect(str(db_path))
        self.table = self._get_or_create_table()

    def _get_or_create_table(self):
        """Get existing table or create new one."""
        if self.TABLE_NAME in self.db.table_names():
            return self.db.open_table(self.TABLE_NAME)

        # Create empty table with schema
        schema = pa.schema([
            pa.field("transaction_id", pa.int64()),
            pa.field("vector", pa.list_(pa.float32(), self.dim)),
            pa.field("merchant", pa.string()),
            pa.field("category", pa.string()),
            pa.field("amount", pa.float64()),
        ])

        # Create with empty data matching schema
        return self.db.create_table(self.TABLE_NAME, schema=schema)

    def close(self) -> None:
        """Close the database connection."""
        # LanceDB doesn't need explicit close, but we keep the method for API consistency
        pass

    def add_embedding(
        self,
        transaction_id: int,
        embedding: np.ndarray,
        merchant: str,
        category: str,
        amount: float
    ) -> None:
        """Add a single embedding with metadata."""
        data = [{
            "transaction_id": transaction_id,
            "vector": embedding.tolist(),
            "merchant": merchant,
            "category": category,
            "amount": amount
        }]
        self.table.add(data)

    def add_embeddings(self, embeddings: List[Dict[str, Any]]) -> None:
        """Add multiple embeddings in batch."""
        data = []
        for emb in embeddings:
            data.append({
                "transaction_id": emb["transaction_id"],
                "vector": emb["embedding"].tolist() if isinstance(emb["embedding"], np.ndarray) else emb["embedding"],
                "merchant": emb["merchant"],
                "category": emb["category"],
                "amount": emb["amount"]
            })
        if data:
            self.table.add(data)

    def search(self, query_embedding: np.ndarray, k: int = 5) -> List[Dict[str, Any]]:
        """Search for similar embeddings.

        Args:
            query_embedding: Query vector
            k: Number of results to return

        Returns:
            List of dicts with transaction_id, merchant, category, amount, and distance
        """
        if self.count() == 0:
            return []

        results = self.table.search(query_embedding.tolist()).limit(k).to_list()

        return results

    def delete_embedding(self, transaction_id: int) -> None:
        """Delete embedding by transaction ID."""
        self.table.delete(f"transaction_id = {transaction_id}")

    def get_embedding(self, transaction_id: int) -> Optional[Dict[str, Any]]:
        """Get embedding by transaction ID."""
        results = self.table.search().where(
            f"transaction_id = {transaction_id}", prefilter=True
        ).limit(1).to_list()

        if results:
            return results[0]
        return None

    def update_category(self, transaction_id: int, new_category: str) -> None:
        """Update category for an embedding."""
        # LanceDB doesn't have direct update, so we need to delete and re-add
        existing = self.get_embedding(transaction_id)
        if existing:
            self.delete_embedding(transaction_id)
            self.add_embedding(
                transaction_id=transaction_id,
                embedding=np.array(existing["vector"], dtype=np.float32),
                merchant=existing["merchant"],
                category=new_category,
                amount=existing["amount"]
            )

    def count(self) -> int:
        """Get count of embeddings in store."""
        return self.table.count_rows()
