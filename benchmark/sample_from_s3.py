"""
로컬 sqlite mart(메타데이터) + S3(이미지)에서 N개를 랜덤 샘플링해
실제 CLIP 텍스트+이미지 임베딩을 생성 → run_benchmark.py --real-embeddings-npz 용 .npz로 저장.

전제:
  - sqlite mart 테이블에 image_key 컬럼이 있고, 그 값이 S3 오브젝트 키와 일치
  - 필수 컬럼: product_id(or rowid), title, image_key, price, brand, category, in_stock

멀티모달 융합 방식:
  텍스트 임베딩과 이미지 임베딩을 각각 정규화된 CLIP 벡터로 만든 뒤 평균 → 재정규화.
  둘 다 같은 CLIP 벡터공간에 있으므로 "제목과 이미지 둘 다 반영된" 단일 상품 벡터가 됨.
  (run_pipeline.py에서는 text_vector/image_vector를 따로 저장하지만,
   벤치마크는 단일 벡터 비교가 목적이라 여기선 융합해서 하나로 씀)
"""

import argparse
import json
import os
import random
import sqlite3
import sys

import boto3
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def sample_products(sqlite_path: str, table: str, n: int, seed: int = 42):
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"""
        SELECT product_id, title, image_key, price, brand, category, in_stock
        FROM {table}
        WHERE image_key IS NOT NULL
        """
    ).fetchall()
    conn.close()

    if len(rows) < n:
        print(f"⚠️  마트에 유효한 행이 {len(rows)}개뿐이라 전체를 사용합니다 (요청: {n}개)")
        n = len(rows)

    rng = random.Random(seed)
    sampled = rng.sample(rows, n)
    return sampled


def fetch_image_bytes(rows, bucket: str):
    s3 = boto3.client("s3")
    images = []
    failed_indices = []
    for i, row in enumerate(rows):
        try:
            obj = s3.get_object(Bucket=bucket, Key=row["image_key"])
            images.append(obj["Body"].read())
        except Exception as e:
            print(f"  ⚠️ 이미지 로드 실패 (id={row['product_id']}, key={row['image_key']}): {e}")
            images.append(None)
            failed_indices.append(i)
    return images, failed_indices


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite-path", required=True)
    parser.add_argument("--table", default="mart_products")
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--n", type=int, default=3000)
    parser.add_argument("--output", default="real_embeddings.npz")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    print(f"1) sqlite mart에서 {args.n}개 샘플링 중...")
    rows = sample_products(args.sqlite_path, args.table, args.n, seed=args.seed)
    print(f"   -> {len(rows)}개 샘플링 완료")

    print(f"2) S3({args.s3_bucket})에서 이미지 다운로드 중...")
    images, failed_indices = fetch_image_bytes(rows, args.s3_bucket)

    # 이미지 다운로드 실패한 항목은 제외
    valid_idx = [i for i in range(len(rows)) if i not in failed_indices]
    if failed_indices:
        print(f"   -> {len(failed_indices)}개 실패, {len(valid_idx)}개로 진행")

    rows = [rows[i] for i in valid_idx]
    images = [images[i] for i in valid_idx]

    from embeddings import MultimodalEmbedder

    embedder = MultimodalEmbedder()

    titles = [r["title"] for r in rows]
    print(f"3) 텍스트 임베딩 생성 중 ({len(titles)}개)...")
    text_vectors = embedder.encode_text(titles)

    print(f"4) 이미지 임베딩 생성 중 ({len(images)}개)...")
    image_vectors = embedder.encode_image(images)

    print("5) 텍스트+이미지 벡터 융합 중 (평균 후 재정규화)...")
    fused = text_vectors + image_vectors
    fused /= np.linalg.norm(fused, axis=1, keepdims=True)

    ids = [str(r["product_id"]) for r in rows]
    payloads = [
        json.dumps(
            {
                "title": r["title"],
                "category": r["category"],
                "brand": r["brand"],
                "price": int(r["price"]) if r["price"] is not None else 0,
                "in_stock": bool(r["in_stock"]),
            },
            ensure_ascii=False,
        )
        for r in rows
    ]

    np.savez(
        args.output,
        ids=np.array(ids, dtype=object),
        vectors=fused.astype(np.float32),
        payloads=np.array(payloads, dtype=object),
    )
    print(f"\n저장 완료: {args.output} ({len(ids)}개 벡터, {fused.shape[1]}차원)")


if __name__ == "__main__":
    main()
