"""
벤치마크용 데이터 생성기.
실제 CLIP 임베딩(512차원, L2 정규화)과 통계적으로 유사한 랜덤 벡터 +
쇼핑 상품과 유사한 메타데이터(price, category, brand, in_stock)를 생성.

실제 프로젝트 임베딩을 쓰고 싶다면 load_real_embeddings()를 사용해
run_pipeline.py에서 만든 벡터를 그대로 재사용할 수 있음.
"""

import random
import string
from dataclasses import dataclass, field
from typing import List

import numpy as np

CATEGORIES = ["shoes", "bags", "clothing", "electronics", "beauty", "home", "sports"]
BRANDS = ["nike", "adidas", "newbalance", "samsung", "lg", "unbranded", "zara"]


@dataclass
class BenchmarkDataset:
    ids: List[str]
    vectors: np.ndarray  # (N, dim), L2 정규화됨
    payloads: List[dict]
    query_vectors: np.ndarray  # (Q, dim), 검색 쿼리용 별도 샘플
    query_filters: List[dict] = field(default_factory=list)


def generate_dataset(
    n_vectors: int = 10_000,
    n_queries: int = 200,
    dim: int = 512,
    seed: int = 42,
) -> BenchmarkDataset:
    rng = np.random.default_rng(seed)

    def random_unit_vectors(n):
        v = rng.normal(size=(n, dim)).astype(np.float32)
        v /= np.linalg.norm(v, axis=1, keepdims=True)
        return v

    vectors = random_unit_vectors(n_vectors)
    query_vectors = random_unit_vectors(n_queries)

    ids = [f"prod_{i:07d}" for i in range(n_vectors)]
    payloads = []
    for i in range(n_vectors):
        payloads.append(
            {
                "title": f"product-{''.join(random.choices(string.ascii_lowercase, k=6))}",
                "category": rng.choice(CATEGORIES),
                "brand": rng.choice(BRANDS),
                "price": int(rng.integers(5_000, 300_000)),
                "in_stock": bool(rng.random() > 0.15),
            }
        )

    # 쿼리마다 랜덤 필터 조합 부여 (필터 성능 측정용)
    query_filters = []
    for _ in range(n_queries):
        query_filters.append(
            {
                "category": rng.choice(CATEGORIES),
                "price_lte": int(rng.integers(50_000, 300_000)),
                "in_stock_only": True,
            }
        )

    return BenchmarkDataset(
        ids=ids,
        vectors=vectors,
        payloads=payloads,
        query_vectors=query_vectors,
        query_filters=query_filters,
    )


def load_real_embeddings(npz_path: str) -> BenchmarkDataset:
    """
    run_pipeline.py에서 저장한 실제 CLIP 임베딩(.npz)을 불러와 벤치마크에 사용.
    np.savez(npz_path, ids=..., vectors=..., payloads=... (json string list))
    """
    import json

    data = np.load(npz_path, allow_pickle=True)
    ids = data["ids"].tolist()
    vectors = data["vectors"]
    payloads = [json.loads(p) for p in data["payloads"]]

    n_queries = min(200, len(ids))
    rng = np.random.default_rng(0)
    query_idx = rng.choice(len(ids), size=n_queries, replace=False)
    query_vectors = vectors[query_idx]

    query_filters = [
        {"category": payloads[i]["category"], "price_lte": 300_000, "in_stock_only": True}
        for i in query_idx
    ]

    return BenchmarkDataset(
        ids=ids,
        vectors=vectors,
        payloads=payloads,
        query_vectors=query_vectors,
        query_filters=query_filters,
    )
