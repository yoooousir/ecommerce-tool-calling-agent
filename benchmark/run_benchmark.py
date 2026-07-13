"""
벡터 DB 정량 비교 벤치마크.

측정 항목:
  1. 삽입 처리량 (vectors/sec)
  2. 순수 벡터 검색 지연시간 (p50/p95, ms)
  3. 필터+벡터 검색 지연시간 (p50/p95, ms) 및 필터로 인한 저하율
  4. Recall@10 (brute-force 정답 대비 정확도)
  5. 동시 요청 처리량 (concurrent QPS, 10 workers)

사용 예:
  python run_benchmark.py --dbs chroma qdrant --n-vectors 10000
  python run_benchmark.py --dbs chroma qdrant pinecone --n-vectors 5000  # pinecone은 PINECONE_API_KEY 필요
"""

import argparse
import json
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import numpy as np
from dotenv import find_dotenv, load_dotenv

from cost_model import estimate_pinecone_cost, estimate_self_hosted_cost
from dataset import generate_dataset, load_real_embeddings


def load_env_file(explicit_path: str = None):
    """
    .env 파일을 로드해 환경변수로 등록.
    - explicit_path가 있으면 그걸 우선 사용 (예: ../airflow/.env 처럼 형제 폴더에 있는 경우)
    - 없으면 현재 폴더부터 상위 폴더로 올라가며 자동 탐색 (find_dotenv)
    """
    if explicit_path:
        if not os.path.isfile(explicit_path):
            print(f"[경고] 지정한 --env-file 경로를 찾을 수 없음: {explicit_path}")
            return
        load_dotenv(explicit_path)
        print(f"[.env 로드됨: {explicit_path}]")
        return

    found = find_dotenv(usecwd=True)
    if found:
        load_dotenv(found)
        print(f"[.env 로드됨: {found}]")
    else:
        print("[.env 파일을 찾지 못함 — --env-file로 직접 지정하거나 환경변수를 셸에 미리 설정해야 함]")


def compute_ground_truth(vectors: np.ndarray, query_vectors: np.ndarray, top_k: int = 10) -> List[List[int]]:
    """코사인 유사도 brute-force로 정답 top-k 인덱스 계산 (recall 측정 기준)"""
    # 벡터가 이미 정규화되어 있다는 가정 하에 내적 = 코사인 유사도
    sims = query_vectors @ vectors.T  # (Q, N)
    return np.argsort(-sims, axis=1)[:, :top_k].tolist()


def recall_at_k(retrieved_ids: List[str], ground_truth_ids: List[str]) -> float:
    gt_set = set(ground_truth_ids)
    hit = sum(1 for rid in retrieved_ids if rid in gt_set)
    return hit / len(ground_truth_ids)


def percentile(values: List[float], p: float) -> float:
    return float(np.percentile(values, p))


def benchmark_adapter(adapter, dataset, dim: int, top_k: int = 10, cost_kwargs: Dict = None) -> Dict:
    result = {"db": adapter.name}
    cost_kwargs = cost_kwargs or {}

    # 1. 삽입 처리량
    adapter.setup(dim=dim)
    t0 = time.perf_counter()
    adapter.insert(dataset.ids, dataset.vectors, dataset.payloads)
    insert_elapsed = time.perf_counter() - t0
    result["insert_throughput_vec_per_sec"] = round(len(dataset.ids) / insert_elapsed, 1)
    result["insert_total_sec"] = round(insert_elapsed, 2)

    # 정답셋 계산 (필터 없는 순수 벡터 검색 기준)
    gt_indices = compute_ground_truth(dataset.vectors, dataset.query_vectors, top_k=top_k)
    gt_ids = [[dataset.ids[i] for i in row] for row in gt_indices]

    # 2. 순수 벡터 검색 지연시간 + recall
    latencies = []
    recalls = []
    for i, qvec in enumerate(dataset.query_vectors):
        t0 = time.perf_counter()
        retrieved = adapter.search(qvec, top_k=top_k)
        latencies.append((time.perf_counter() - t0) * 1000)
        recalls.append(recall_at_k(retrieved, gt_ids[i]))

    result["search_p50_ms"] = round(percentile(latencies, 50), 2)
    result["search_p95_ms"] = round(percentile(latencies, 95), 2)
    result["recall_at_10"] = round(statistics.mean(recalls), 3)

    # 3. 필터+벡터 검색 지연시간
    filtered_latencies = []
    for i, qvec in enumerate(dataset.query_vectors):
        t0 = time.perf_counter()
        adapter.filtered_search(qvec, dataset.query_filters[i], top_k=top_k)
        filtered_latencies.append((time.perf_counter() - t0) * 1000)

    result["filtered_search_p50_ms"] = round(percentile(filtered_latencies, 50), 2)
    result["filtered_search_p95_ms"] = round(percentile(filtered_latencies, 95), 2)
    result["filter_slowdown_pct"] = round(
        (result["filtered_search_p50_ms"] / result["search_p50_ms"] - 1) * 100, 1
    )

    # 4. 동시 요청 처리량 (concurrent QPS)
    n_concurrent_queries = min(100, len(dataset.query_vectors))
    queries = list(dataset.query_vectors[:n_concurrent_queries])

    def _one_query(qv):
        adapter.search(qv, top_k=top_k)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(executor.map(_one_query, queries))
    elapsed = time.perf_counter() - t0
    result["concurrent_qps"] = round(n_concurrent_queries / elapsed, 1)

    # 5. 예상 월 비용 (실제 인프라를 띄우지 않고, 측정된 벡터 수/차원 기준으로 계산)
    n_vectors = len(dataset.ids)
    monthly_upserts = cost_kwargs.get("monthly_upserts", n_vectors)  # 기본값: 한 달에 전체 재색인 1회 가정
    monthly_queries = cost_kwargs.get("monthly_queries", 100_000)
    avg_metadata_bytes = cost_kwargs.get("avg_metadata_bytes", 200)

    if adapter.name == "Pinecone":
        cost = estimate_pinecone_cost(
            n_vectors=n_vectors,
            dim=dim,
            avg_metadata_bytes=avg_metadata_bytes,
            monthly_upserts=monthly_upserts,
            monthly_queries=monthly_queries,
        )
    else:
        cost = estimate_self_hosted_cost(
            db_name=adapter.name,
            n_vectors=n_vectors,
            dim=dim,
            avg_metadata_bytes=avg_metadata_bytes,
        )

    result["estimated_monthly_cost_usd"] = cost.monthly_cost_usd
    result["cost_breakdown"] = cost.breakdown

    adapter.teardown()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dbs", nargs="+", choices=["chroma", "qdrant", "pinecone"], required=True)
    parser.add_argument("--n-vectors", type=int, default=10_000)
    parser.add_argument("--n-queries", type=int, default=100)
    parser.add_argument("--dim", type=int, default=512)
    parser.add_argument("--real-embeddings-npz", default=None, help="실제 CLIP 임베딩(.npz)을 쓰려면 경로 지정")
    parser.add_argument("--qdrant-url", default="http://localhost:6333")
    parser.add_argument("--output", default="benchmark_results.json")
    parser.add_argument(
        "--monthly-upserts",
        type=int,
        default=None,
        help="월간 예상 upsert(신규/갱신) 건수. 기본값: 전체 벡터 수(월 1회 전체 재색인 가정)",
    )
    parser.add_argument(
        "--monthly-queries",
        type=int,
        default=100_000,
        help="월간 예상 검색 쿼리 수 (기본 10만 건)",
    )
    parser.add_argument(
        "--avg-metadata-bytes",
        type=int,
        default=200,
        help="레코드당 평균 메타데이터 크기(바이트). 상품명/가격/브랜드 등 payload 크기",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="PINECONE_API_KEY 등이 담긴 .env 경로. 예: ../airflow/.env (지정 안 하면 자동 탐색)",
    )
    args = parser.parse_args()

    load_env_file(args.env_file)

    if args.real_embeddings_npz:
        dataset = load_real_embeddings(args.real_embeddings_npz)
        dim = dataset.vectors.shape[1]
    else:
        dataset = generate_dataset(n_vectors=args.n_vectors, n_queries=args.n_queries, dim=args.dim)
        dim = args.dim

    cost_kwargs = {
        "monthly_upserts": (
            args.monthly_upserts if args.monthly_upserts is not None else len(dataset.ids)
        ),
        "monthly_queries": args.monthly_queries,
        "avg_metadata_bytes": args.avg_metadata_bytes,
    }

    results = []

    if "chroma" in args.dbs:
        from adapter_chroma import ChromaAdapter

        print(f"\n=== ChromaDB 벤치마크 시작 (n={len(dataset.ids)}) ===")
        results.append(benchmark_adapter(ChromaAdapter(), dataset, dim, cost_kwargs=cost_kwargs))

    if "qdrant" in args.dbs:
        from adapter_qdrant import QdrantAdapter

        print(f"\n=== Qdrant 벤치마크 시작 (n={len(dataset.ids)}) ===")
        results.append(
            benchmark_adapter(QdrantAdapter(url=args.qdrant_url), dataset, dim, cost_kwargs=cost_kwargs)
        )

    if "pinecone" in args.dbs:
        from adapter_pinecone import PineconeAdapter

        print(f"\n=== Pinecone 벤치마크 시작 (n={len(dataset.ids)}) ===")
        results.append(benchmark_adapter(PineconeAdapter(), dataset, dim, cost_kwargs=cost_kwargs))

    print("\n" + json.dumps(results, indent=2, ensure_ascii=False))
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print_comparison_table(results)


def print_comparison_table(results: List[Dict]):
    if not results:
        return
    cols = [
        "db",
        "insert_throughput_vec_per_sec",
        "search_p50_ms",
        "search_p95_ms",
        "filtered_search_p50_ms",
        "filter_slowdown_pct",
        "recall_at_10",
        "concurrent_qps",
        "estimated_monthly_cost_usd",
    ]
    header = " | ".join(f"{c:>26}" for c in cols)
    print("\n" + header)
    print("-" * len(header))
    for r in results:
        row = " | ".join(f"{str(r.get(c, '-')):>26}" for c in cols)
        print(row)

    print("\n--- 비용 상세 breakdown ---")
    for r in results:
        print(f"\n[{r['db']}] 예상 월 비용: ${r['estimated_monthly_cost_usd']}")
        for k, v in r.get("cost_breakdown", {}).items():
            print(f"  - {k}: {v}")


if __name__ == "__main__":
    main()