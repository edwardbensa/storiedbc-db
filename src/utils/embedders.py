"""Embedding models"""

# Imports
from threading import Lock
import torch
import numpy as np
from sentence_transformers import SentenceTransformer
from src.config import hf_token


class GemmaEmbedder:
    """Thread-safe, lazy-loaded embedding model for ETL pipelines and inference."""

    _instance = None
    _lock = Lock()

    def __init__(self):
        self._model = None
        self._device = "cuda" if torch.cuda.is_available() else "cpu"

    @classmethod
    def instance(cls):
        """Instance with thread lock."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def load(self):
        """Load model."""
        if self._model is None:
            with self._lock:
                if self._model is None:
                    self._model = SentenceTransformer(
                        "google/embeddinggemma-300m",
                        token=hf_token,
                        device=self._device
                    )
        return self._model

    def vectorise_one(self, text: str, as_numpy=False):
        """Vectorise text."""
        model = self.load()
        vec = model.encode_document(text)

        if as_numpy:
            return np.asarray(vec)
        return np.asarray(vec).tolist()

    def vectorise_many(self, texts: list[str], as_numpy=False):
        """Vectorise a list of text."""
        model = self.load()
        vecs = model.encode(texts, show_progress_bar=True)

        if as_numpy:
            return np.asarray(vecs)
        return [np.asarray(v).tolist() for v in vecs]
