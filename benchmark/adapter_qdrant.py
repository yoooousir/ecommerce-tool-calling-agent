import uuid
from typing import List, Optional

import numpy as np
from qdrant_client import QdrantClient, models

from base_adapter import VectorDBAdapter

COLLECTION = "bench_products"
# 문자열 ID를 결정적(deterministic)으로 UUID로 변환하기 위한 고정 네임스페이스.
# 같은 문자열이면 항상 같은 UUID가 나오므로 재삽입해도 일관성 유지됨.
ID_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


def _to_qdrant_id(original_id: str) -> str:
    """Qdrant는 point ID로 부호 없는 정수 또는 UUID만 허용하므로 문자열 ID를 UUID로 변환"""
    return str(uuid.uuid5(ID_NAMESPACE, str(original_id)))


class QdrantAdapter(VectorDBAdapter):
    name = "Qdrant"

    def __init__(self, url: str = "http://localhost:6333", api_key: Optional[str] = None):
        self.client = QdrantClient(url=url, api_key=api_key, timeout=60)

    def setup(self, dim: int):
        if self.client.collection_exists(COLLECTION):
            self.client.delete_collection(COLLECTION)
        self.client.create_collection(
            collection_name=COLLECTION,
            vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE),
        )
        for field_name, schema in [
            ("category", models.PayloadSchemaType.KEYWORD),
            ("price", models.PayloadSchemaType.INTEGER),
            ("in_stock", models.PayloadSchemaType.BOOL),
        ]:
            self.client.create_payload_index(
                collection_name=COLLECTION, field_name=field_name, field_schema=schema
            )

    def insert(self, ids: List[str], vectors: np.ndarray, payloads: List[dict]) -> None:
        batch_size = 500
        for i in range(0, len(ids), batch_size):
            points = [
                models.PointStruct(
                    id=_to_qdrant_id(ids[j]),
                    vector=vectors[j].tolist(),
                    # 원본 ID를 payload에 함께 저장해서 검색 결과에서 다시 꺼내 씀
                    payload={**payloads[j], "_original_id": ids[j]},
                )
                for j in range(i, min(i + batch_size, len(ids)))
            ]
            self.client.upsert(collection_name=COLLECTION, points=points, wait=True)

    def search(self, query_vector: np.ndarray, top_k: int = 10) -> List[str]:
        res = self.client.query_points(
            collection_name=COLLECTION, query=query_vector.tolist(), limit=top_k, with_payload=True
        )
        return [p.payload["_original_id"] for p in res.points]

    def filtered_search(self, query_vector: np.ndarray, filters: dict, top_k: int = 10) -> List[str]:
        qfilter = models.Filter(
            must=[
                models.FieldCondition(
                    key="category", match=models.MatchValue(value=filters["category"])
                ),
                models.FieldCondition(key="price", range=models.Range(lte=filters["price_lte"])),
                models.FieldCondition(key="in_stock", match=models.MatchValue(value=True)),
            ]
        )
        res = self.client.query_points(
            collection_name=COLLECTION,
            query=query_vector.tolist(),
            query_filter=qfilter,
            limit=top_k,
            with_payload=True,
        )
        return [p.payload["_original_id"] for p in res.points]

    def teardown(self):
        if self.client.collection_exists(COLLECTION):
            self.client.delete_collection(COLLECTION)