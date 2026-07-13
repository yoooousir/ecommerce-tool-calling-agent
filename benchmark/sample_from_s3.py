"""
로컬 products.db(sqlite, 정형 메타데이터) + S3 parquet(description, image_url)을
product_id 기준으로 join한 뒤 N개를 랜덤 샘플링해 CLIP 텍스트+이미지 임베딩 생성.

[데이터 소스 구조 — collect_naver_shopping.py v2 기준]
  - 로컬 SQLite (products.db):
      raw 'products' 테이블: id, title, link, lprice, hprice, mall_name, maker, brand,
                              category1~4, search_keyword, collected_at
      dbt 'stg_products' / 'dim_products' (같은 db 파일에 있다면): product_id, title,
                              low_price, high_price, brand, category_l1~l2, price_bucket 등
      → image_url, description은 이 테이블들에 없음.

  - S3 parquet (naver_shopping/{year}/{month}/{day}/products.parquet):
      컬럼: id, description, image_url
      → 날짜별로 파티셔닝되어 있으므로 prefix 아래 모든 parquet을 모아 id 기준 병합.

⚠️ 주의: collect_naver_shopping.py의 upload_to_s3()가 image_url을 빈 문자열("")
placeholder로 채우는 코드로 되어 있습니다. 실제 크롤러가 image_url을 채우도록
연동되지 않았다면, 이 스크립트 실행 시 "유효한 image_url이 없습니다" 경고가 뜰 수 있습니다.
그 경우 크롤러 쪽 upload_to_s3()에서 image_url을 실제 값으로 채우는 수정이 먼저 필요합니다.
"""

import argparse
import io
import json
import os
import random
import sqlite3
import sys

import boto3
import numpy as np
import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# 우선순위: dbt 최종 마트 > staging > raw
TABLE_PRIORITY = ["dim_products", "stg_products", "products"]

# 실제 스키마(v2 dbt 모델 + raw 테이블)에서 나올 수 있는 컬럼명 후보
COLUMN_CANDIDATES = {
    "id": ["product_id", "id"],
    "title": ["title"],
    "price": ["low_price", "lprice", "price"],
    "brand": ["brand"],
    "category": ["category_l1", "category1", "category"],
}


def list_sqlite_tables(conn) -> list:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return [r[0] for r in rows]


def resolve_table(conn, table_arg: str) -> str:
    tables = list_sqlite_tables(conn)
    if table_arg:
        if table_arg not in tables:
            raise SystemExit(f"'{table_arg}' 테이블이 없습니다. 존재하는 테이블: {tables}")
        return table_arg

    for candidate in TABLE_PRIORITY:
        if candidate in tables:
            print(f"메타데이터 테이블로 '{candidate}'을(를) 자동 선택했습니다.")
            return candidate

    raise SystemExit(
        f"dim_products/stg_products/products 중 어떤 것도 찾지 못했습니다. "
        f"--table로 직접 지정하세요. 존재하는 테이블: {tables}"
    )


def resolve_columns(conn, table: str) -> dict:
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    print(f"'{table}' 테이블의 실제 컬럼: {cols}")

    resolved = {}
    for field, candidates in COLUMN_CANDIDATES.items():
        match = next((c for c in candidates if c in cols), None)
        resolved[field] = match
        if match is None and field in ("id", "title"):
            raise SystemExit(f"필수 컬럼 '{field}'을(를) 찾지 못했습니다. (테이블 컬럼: {cols})")
    return resolved


def load_metadata(sqlite_path: str, table_arg: str) -> pd.DataFrame:
    conn = sqlite3.connect(sqlite_path)
    table = resolve_table(conn, table_arg)
    cols = resolve_columns(conn, table)

    select_cols = ", ".join(f"{c} AS {field}" for field, c in cols.items() if c is not None)
    df = pd.read_sql(f"SELECT {select_cols} FROM {table}", conn)
    conn.close()
    return df


def list_parquet_keys(bucket: str, prefix: str) -> list:
    s3 = boto3.client("s3")
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def load_s3_parquets(bucket: str, prefix: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    keys = list_parquet_keys(bucket, prefix)
    if not keys:
        raise SystemExit(f"s3://{bucket}/{prefix} 아래에 parquet 파일이 없습니다.")

    print(f"S3에서 parquet {len(keys)}개 발견, 다운로드 중...")
    dfs = []
    for key in sorted(keys):  # 날짜순 정렬 (파티션 경로가 년/월/일이라 문자열 정렬로 충분)
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    # 같은 id가 여러 날짜 파티션에 중복될 수 있으므로, 가장 마지막(최신) 것만 유지
    combined = combined.drop_duplicates(subset="id", keep="last")
    return combined


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite-path", required=True, help="예: ../airflow/data/products.db")
    parser.add_argument("--table", default=None, help="지정 안 하면 dim_products > stg_products > products 순으로 자동 탐지")
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--s3-prefix", default="naver_shopping", help="parquet이 있는 S3 prefix (기본: naver_shopping)")
    parser.add_argument("--n", type=int, default=200)
    parser.add_argument("--output", default="real_embeddings.npz")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    print("1) 로컬 sqlite에서 정형 메타데이터 로드 중...")
    meta_df = load_metadata(args.sqlite_path, args.table)
    print(f"   -> {len(meta_df)}건 로드")

    print("2) S3 parquet에서 description/image_url 로드 중...")
    s3_df = load_s3_parquets(args.s3_bucket, args.s3_prefix)
    print(f"   -> {len(s3_df)}건 로드 (컬럼: {list(s3_df.columns)})")

    non_empty_urls = s3_df["image_url"].fillna("").str.strip().ne("").sum()
    print(f"   -> image_url이 채워진 행: {non_empty_urls}/{len(s3_df)}")
    if non_empty_urls == 0:
        raise SystemExit(
            "⚠️ S3 parquet의 image_url이 전부 비어 있습니다.\n"
            "collect_naver_shopping.py의 upload_to_s3()가 image_url을 placeholder(\"\")로 "
            "채우고 있어서 그렇습니다. 크롤러 쪽에서 실제 image_url을 parquet에 채우도록 "
            "먼저 수정한 뒤 다시 시도해주세요."
        )

    print("3) product_id 기준으로 join 중...")
    merged = meta_df.merge(s3_df, left_on="id", right_on="id", how="inner")
    merged = merged[merged["image_url"].fillna("").str.strip() != ""]
    print(f"   -> join 후 이미지 있는 행: {len(merged)}건")

    if merged.empty:
        raise SystemExit("join 결과가 비어 있습니다. id 컬럼 매칭을 확인해주세요.")

    n = min(args.n, len(merged))
    if n < args.n:
        print(f"⚠️  유효한 행이 {len(merged)}개뿐이라 전체를 사용합니다 (요청: {args.n}개)")
    sampled = merged.sample(n=n, random_state=args.seed).reset_index(drop=True)

    print("4) 이미지 다운로드 중...")
    images, valid_mask = [], []
    for url in sampled["image_url"]:
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            images.append(resp.content)
            valid_mask.append(True)
        except Exception as e:
            print(f"  ⚠️ 다운로드 실패 ({url}): {e}")
            images.append(None)
            valid_mask.append(False)

    sampled = sampled[valid_mask].reset_index(drop=True)
    images = [img for img, ok in zip(images, valid_mask) if ok]
    print(f"   -> {len(sampled)}개 이미지 다운로드 성공")

    if sampled.empty:
        raise SystemExit("모든 이미지 다운로드에 실패했습니다.")

    from embeddings import MultimodalEmbedder

    embedder = MultimodalEmbedder()

    titles = sampled["title"].fillna("").tolist()
    print(f"5) 텍스트 임베딩 생성 중 ({len(titles)}개)...")
    text_vectors = embedder.encode_text(titles)

    print(f"6) 이미지 임베딩 생성 중 ({len(images)}개)...")
    image_vectors = embedder.encode_image(images)

    print("7) 텍스트+이미지 벡터 융합 중 (평균 후 재정규화)...")
    fused = text_vectors + image_vectors
    fused /= np.linalg.norm(fused, axis=1, keepdims=True)

    ids = sampled["id"].astype(str).tolist()
    payloads = [
        json.dumps(
            {
                "title": row["title"],
                "category": row.get("category", "unknown"),
                "brand": row.get("brand", "unknown"),
                "price": int(row["price"]) if pd.notnull(row.get("price")) else 0,
                "in_stock": True,  # 스키마에 재고 컬럼이 없어 기본값 처리
            },
            ensure_ascii=False,
        )
        for _, row in sampled.iterrows()
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