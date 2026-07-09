import os
import time
from typing import List

import numpy as np

from base_adapter import VectorDBAdapter

INDEX_NAME = "bench-products"


class PineconeAdapter(VectorDBAdapter):
    """
    Pinecone은 서버리스 인덱스라 생성 후 ready 상태가 될 때까지 대기 시간이 필요함.
    환경변수 PINECONE_API_KEY 필요.
    """

    name = "Pinecone"

    def __init__(self, cloud: str = "aws", region: str = "us-east-1"):
        from pinecone import Pinecone, ServerlessSpec

        api_key = os.environ.get("PINECONE_API_KEY")
        if not api_key:
            raise RuntimeError("PINECONE_API_KEY 환경변수가 설정되어 있지 않습니다.")
        self.pc = Pinecone(api_key=api_key)
        self.cloud = cloud
        self.region = region
        self.ServerlessSpec = ServerlessSpec
        self.index = None

    def setup(self, dim: int):
        existing = [idx["name"] for idx in self.pc.list_indexes()]
        if INDEX_NAME in existing:
            self.pc.delete_index(INDEX_NAME)
            time.sleep(5)

        self.pc.create_index(
            name=INDEX_NAME,
            dimension=dim,
            metric="cosine",
            spec=self.ServerlessSpec(cloud=self.cloud, region=self.region),
        )
        # 인덱스가 ready 상태가 될 때까지 대기
        while not self.pc.describe_index(INDEX_NAME).status["ready"]:
            time.sleep(1)
        self.index = self.pc.Index(INDEX_NAME)

    def insert(self, ids: List[str], vectors: np.ndarray, payloads: List[dict]) -> None:
        batch_size = 200
        for i in range(0, len(ids), batch_size):
            vecs = [
                {"id": ids[j], "values": vectors[j].tolist(), "metadata": payloads[j]}
                for j in range(i, min(i + batch_size, len(ids)))
            ]
            self.index.upsert(vectors=vecs)
        # 서버리스는 upsert 후 검색 가능해지기까지 약간의 propagation 지연이 있음
        time.sleep(5)

    def search(self, query_vector: np.ndarray, top_k: int = 10) -> List[str]:
        res = self.index.query(vector=query_vector.tolist(), top_k=top_k)
        return [m["id"] for m in res["matches"]]

    def filtered_search(self, query_vector: np.ndarray, filters: dict, top_k: int = 10) -> List[str]:
        metadata_filter = {
            "category": {"$eq": filters["category"]},
            "price": {"$lte": filters["price_lte"]},
            "in_stock": {"$eq": True},
        }
        res = self.index.query(
            vector=query_vector.tolist(), top_k=top_k, filter=metadata_filter
        )
        return [m["id"] for m in res["matches"]]

    def teardown(self):
        try:
            self.pc.delete_index(INDEX_NAME)
        except Exception:
            pass
