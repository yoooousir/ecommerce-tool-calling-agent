"""
DAG: naver_shopping_daily_image_embedding

동작 순서:
1) S3KeySensor로 s3://ecommerce-tool-calling-agent/naver_shopping/YYYY/MM/DD/products.parquet 감지
   (여기서 YYYY/MM/DD는 해당 DAG 실행일(data_interval_end) 기준)
2) 파일이 생기면 parquet을 로컬(worker)로 다운로드
3) parquet에서 image_url 컬럼 추출 -> 각 이미지를 다운로드하여 임시 저장
4) CLIP(clip-ViT-B-32)로 임베딩 계산
5) EC2 Qdrant에 upsert

필요 설정:
- Airflow Connection: aws_default (S3 접근용 IAM 자격증명)
- Airflow Variable 또는 환경변수: QDRANT_HOST, QDRANT_PORT, QDRANT_COLLECTION
- Airflow worker에 설치 필요: pandas, pyarrow, boto3, pillow, requests,
  sentence-transformers, qdrant-client, apache-airflow-providers-amazon
"""
from __future__ import annotations

import io
import os
import uuid
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from PIL import Image

S3_BUCKET = "ecommerce-tool-calling-agent"
# S3KeySensor의 bucket_key는 템플릿 필드라 Jinja 그대로 사용 가능
S3_KEY_TEMPLATE = "naver_shopping/{{ data_interval_end.strftime('%Y/%m/%d') }}/products.parquet"

QDRANT_HOST = os.getenv("QDRANT_HOST", "EC2_PUBLIC_IP_HERE")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "products_images")
VECTOR_SIZE = 512

default_args = {
    "owner": "linda",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _build_s3_key(context) -> str:
    data_interval_end = context["data_interval_end"]
    return f"naver_shopping/{data_interval_end.strftime('%Y/%m/%d')}/products.parquet"


def download_parquet(**context):
    import boto3

    ds = context["ds_nodash"]
    key = _build_s3_key(context)

    local_dir = f"/tmp/naver_shopping_{ds}"
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, "products.parquet")

    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, key, local_path)
    print(f"다운로드 완료: s3://{S3_BUCKET}/{key} -> {local_path}")
    return local_path


def extract_and_download_images(**context):
    ti = context["ti"]
    ds = context["ds_nodash"]
    parquet_path = ti.xcom_pull(task_ids="download_parquet")

    df = pd.read_parquet(parquet_path)
    if "image_url" not in df.columns:
        raise ValueError("products.parquet에 image_url 컬럼이 없습니다.")

    img_dir = f"/tmp/naver_shopping_{ds}/images"
    os.makedirs(img_dir, exist_ok=True)

    records = []
    for _, row in df.iterrows():
        image_url = row.get("image_url")
        if not isinstance(image_url, str) or not image_url:
            continue

        product_id = str(row.get("product_id", uuid.uuid4()))
        title = row.get("title", "")

        try:
            resp = requests.get(image_url, timeout=10)
            resp.raise_for_status()
            image = Image.open(io.BytesIO(resp.content)).convert("RGB")
            local_img_path = os.path.join(img_dir, f"{product_id}.jpg")
            image.save(local_img_path)
            records.append(
                {
                    "product_id": product_id,
                    "title": title,
                    "image_url": image_url,
                    "local_path": local_img_path,
                }
            )
        except Exception as e:
            print(f"이미지 다운로드 실패, 스킵: {image_url} ({e})")
            continue

    meta_path = f"/tmp/naver_shopping_{ds}/image_meta.parquet"
    pd.DataFrame(records).to_parquet(meta_path)
    print(f"이미지 {len(records)}개 다운로드 완료, 메타: {meta_path}")
    return meta_path


def embed_and_upsert(**context):
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, PointStruct, VectorParams
    from sentence_transformers import SentenceTransformer

    ti = context["ti"]
    meta_path = ti.xcom_pull(task_ids="extract_and_download_images")
    df = pd.read_parquet(meta_path)

    if df.empty:
        print("임베딩할 이미지가 없습니다. 종료합니다.")
        return

    model = SentenceTransformer("clip-ViT-B-32")
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in existing:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
        )

    points = []
    for _, row in df.iterrows():
        try:
            image = Image.open(row["local_path"]).convert("RGB")
            vector = model.encode(
                image, convert_to_numpy=True, normalize_embeddings=True
            ).tolist()
            points.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=vector,
                    payload={
                        "product_id": row["product_id"],
                        "title": row["title"],
                        "image_url": row["image_url"],
                    },
                )
            )
        except Exception as e:
            print(f"임베딩 실패, 스킵: {row.get('image_url')} ({e})")
            continue

    if points:
        client.upsert(collection_name=COLLECTION_NAME, points=points)
        print(f"Qdrant upsert 완료: {len(points)}개 포인트 -> collection '{COLLECTION_NAME}'")
    else:
        print("upsert할 포인트가 없습니다.")


with DAG(
    dag_id="naver_shopping_daily_image_embedding",
    default_args=default_args,
    description="S3 일별 products.parquet 감지 -> 이미지 다운로드 -> CLIP 임베딩 -> Qdrant upsert",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["naver_shopping", "qdrant", "clip", "embedding"],
) as dag:

    wait_for_file = S3KeySensor(
        task_id="wait_for_products_parquet",
        bucket_name=S3_BUCKET,
        bucket_key=S3_KEY_TEMPLATE,
        aws_conn_id="aws_default",
        poke_interval=300,      # 5분마다 확인
        timeout=60 * 60 * 6,    # 6시간 안에 안 나타나면 타임아웃
        mode="reschedule",      # 슬롯 점유하지 않고 대기 (worker 자원 절약)
    )

    download_task = PythonOperator(
        task_id="download_parquet",
        python_callable=download_parquet,
    )

    extract_task = PythonOperator(
        task_id="extract_and_download_images",
        python_callable=extract_and_download_images,
    )

    embed_task = PythonOperator(
        task_id="embed_and_upsert",
        python_callable=embed_and_upsert,
    )

    wait_for_file >> download_task >> extract_task >> embed_task
