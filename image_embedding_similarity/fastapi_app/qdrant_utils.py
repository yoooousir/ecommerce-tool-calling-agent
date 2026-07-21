"""
Qdrant 접속/컬렉션/검색 유틸.
FastAPI, Airflow 양쪽에서 동일하게 사용합니다.
EC2 Qdrant 주소는 환경변수로 주입 (QDRANT_HOST, QDRANT_PORT).
"""
from dotenv import load_dotenv
load_dotenv()

import os

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from embedder import VECTOR_SIZE

QDRANT_HOST = os.getenv("QDRANT_HOST", "EC2_PUBLIC_IP_HERE")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "products_images")


def get_client() -> QdrantClient:
    return QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)


def ensure_collection(client: QdrantClient) -> None:
    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in existing:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
        )


def upsert_points(client: QdrantClient, points: list[PointStruct]) -> None:
    if points:
        client.upsert(collection_name=COLLECTION_NAME, points=points)


def search_similar(client: QdrantClient, vector: list[float], top_k: int = 5):
    return client.search(
        collection_name=COLLECTION_NAME,
        query_vector=vector,
        limit=top_k,
    )
