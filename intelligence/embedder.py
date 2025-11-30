"""Local embedder using sentence-transformers."""
import numpy as np
from typing import List, Optional


class LocalEmbedder:
    """Singleton embedder using sentence-transformers for local embeddings.

    Uses all-MiniLM-L6-v2 model (22M params, 384 dims) for efficiency.
    Model is lazy-loaded on first embed call.
    """

    _instance = None
    _model = None
    MODEL_NAME = "all-MiniLM-L6-v2"
    DIM = 384

    def __new__(cls):
        """Singleton pattern - return existing instance if available."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _ensure_model_loaded(self) -> None:
        """Lazy-load the model on first use."""
        if LocalEmbedder._model is None:
            from sentence_transformers import SentenceTransformer
            LocalEmbedder._model = SentenceTransformer(self.MODEL_NAME)

    def embed(self, text: str) -> np.ndarray:
        """Embed a single text string.

        Args:
            text: Text to embed

        Returns:
            384-dimensional normalized embedding
        """
        self._ensure_model_loaded()
        embedding = LocalEmbedder._model.encode(
            text,
            convert_to_numpy=True,
            normalize_embeddings=True
        )
        return embedding.astype(np.float32)

    def embed_batch(
        self,
        texts: List[str],
        batch_size: int = 32
    ) -> List[np.ndarray]:
        """Embed multiple texts in batch for efficiency.

        Args:
            texts: List of texts to embed
            batch_size: Batch size for encoding

        Returns:
            List of 384-dimensional normalized embeddings
        """
        self._ensure_model_loaded()
        embeddings = LocalEmbedder._model.encode(
            texts,
            batch_size=batch_size,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False
        )
        return [e.astype(np.float32) for e in embeddings]

    def embed_transaction(
        self,
        merchant: str,
        description: Optional[str] = None,
        amount: Optional[float] = None
    ) -> np.ndarray:
        """Embed a transaction in standard format.

        Args:
            merchant: Merchant name
            description: Transaction description
            amount: Transaction amount

        Returns:
            384-dimensional normalized embedding
        """
        # Build transaction text
        parts = [merchant]
        if description:
            parts.append(description)
        if amount is not None:
            # Add amount bucket for semantic meaning
            abs_amount = abs(amount)
            if abs_amount < 10:
                parts.append("small purchase")
            elif abs_amount < 50:
                parts.append("medium purchase")
            elif abs_amount < 200:
                parts.append("large purchase")
            else:
                parts.append("major expense")

        text = " ".join(parts)
        return self.embed(text)
