import time
from typing import List

import numpy as np

from base_adapter import VectorDBAdapter

COLLECTION = "bench_products"


class ChromaAdapter(VectorDBAdapter):
    name = "ChromaDB"

    def __init__(self, persist_path: str = "./chroma_bench_data"):
        import chromadb

        self.client = chromadb.PersistentClient(path=persist_path)
        self.collection = None

    def setup(self, dim: int):
        try:
            self.client.delete_collection(COLLECTION)
        except Exception:
            pass
        self.collection = self.client.create_collection(
            name=COLLECTION, metadata={"hnsw:space": "cosine"}
        )

    def insert(self, ids: List[str], vectors: np.ndarray, payloads: List[dict]) -> None:
        batch_size = 500
        for i in range(0, len(ids), batch_size):
            self.collection.add(
                ids=ids[i : i + batch_size],
                embeddings=vectors[i : i + batch_size].tolist(),
                metadatas=payloads[i : i + batch_size],
            )

    def search(self, query_vector: np.ndarray, top_k: int = 10) -> List[str]:
        res = self.collection.query(query_embeddings=[query_vector.tolist()], n_results=top_k)
        return res["ids"][0]

    def filtered_search(self, query_vector: np.ndarray, filters: dict, top_k: int = 10) -> List[str]:
        where = {
            "$and": [
                {"category": {"$eq": filters["category"]}},
                {"price": {"$lte": filters["price_lte"]}},
                {"in_stock": {"$eq": True}},
            ]
        }
        res = self.collection.query(
            query_embeddings=[query_vector.tolist()], n_results=top_k, where=where
        )
        return res["ids"][0]

    def teardown(self):
        try:
            self.client.delete_collection(COLLECTION)
        except Exception:
            pass
