"""
DAG: naver_shopping_daily_image_embedding (v3 - 성능 개선)

v2 대비 변경 사항 (속도 개선 목적):
1) 이미지 다운로드를 ThreadPoolExecutor로 병렬화한다.
   - 네트워크 I/O 대기 구간에서는 GIL이 풀리므로, 스레드 병렬화만으로도
     순차 다운로드 대비 체감 속도가 크게 향상된다 (대략 워커 수에 비례).
2) CLIP 임베딩을 한 장씩이 아니라 배치(batch_size=32) 단위로 계산한다.
   - sentence-transformers/torch는 배치 연산에 최적화되어 있어,
     한 장씩 encode() 호출하는 것보다 배치로 묶어 호출하는 편이 훨씬 빠르다.
3) GPU(CUDA)가 있으면 자동으로 사용한다 (torch.cuda.is_available()).
   - 이 노트북/서버에 NVIDIA GPU + 드라이버가 설정되어 있다면 자동으로 감지되어
     CPU 대비 임베딩 속도가 크게 빨라진다. 없으면 자동으로 CPU로 폴백.
4) 체크포인트(JSONL append) 단위를 "건별" -> "배치별"로 변경.
   - 매 건마다 flush+fsync 하던 것을 배치(다운로드 배치 크기)당 한 번으로 줄여
     디스크 I/O 오버헤드를 낮췄다. 크래시 시 최대 한 배치(기본 32~64건) 분량만
     재처리하면 되므로 안전성은 거의 그대로 유지된다.
5) Qdrant upsert에 wait=False를 사용해 서버 커밋 확인을 기다리지 않고
   다음 배치를 바로 전송한다 (처리량 향상). 최종 완료 후 points_count로
   전체 건수를 검증하는 것을 권장한다.

필요 설정 (v2와 동일):
- Airflow Connection: aws_default
- 환경변수: QDRANT_HOST, QDRANT_PORT, QDRANT_COLLECTION
- 추가 설치: torch(이미 sentence-transformers 의존성으로 설치됨)
"""
from __future__ import annotations

import io
import json
import os
import shutil
import uuid
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from PIL import Image

S3_BUCKET = "ecommerce-tool-calling-agent"
S3_KEY_TEMPLATE = "naver_shopping/{{ data_interval_end.strftime('%Y/%m/%d') }}/products.parquet"

QDRANT_HOST = os.getenv("QDRANT_HOST", "EC2_PUBLIC_IP_HERE")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "products_images")
VECTOR_SIZE = 512

BASE_DATA_DIR = "/opt/airflow/data/image_embedding_similarity"

# ---- 성능 관련 설정값 (환경에 맞게 조절 가능) ----
DOWNLOAD_MAX_WORKERS = int(os.getenv("EMBED_DOWNLOAD_WORKERS", "16"))
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "32"))
UPSERT_BATCH_SIZE = int(os.getenv("QDRANT_UPSERT_BATCH_SIZE", "256"))

default_args = {
    "owner": "linda",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _work_dir(ds_nodash: str) -> str:
    path = os.path.join(BASE_DATA_DIR, ds_nodash)
    os.makedirs(path, exist_ok=True)
    return path


def _build_s3_key(context) -> str:
    data_interval_end = context["data_interval_end"]
    return f"naver_shopping/{data_interval_end.strftime('%Y/%m/%d')}/products.parquet"


def download_parquet(**context):
    import boto3

    ds = context["ds_nodash"]
    key = _build_s3_key(context)
    work_dir = _work_dir(ds)
    local_path = os.path.join(work_dir, "products.parquet")

    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, key, local_path)
    print(f"다운로드 완료: s3://{S3_BUCKET}/{key} -> {local_path}")
    return local_path


def _download_one(product_id: str, title: str, image_url: str):
    """스레드풀에서 실행되는 단일 이미지 다운로드 함수.
    CLIP은 내부적으로 224x224로 리사이즈하므로, 원본 해상도를 그대로
    메모리에 들고 있을 필요가 없다. 디코딩 직후 적당한 크기로 축소해
    스레드당 메모리 사용량을 줄인다.
    """
    resp = requests.get(image_url, timeout=10)
    resp.raise_for_status()
    image = Image.open(io.BytesIO(resp.content)).convert("RGB")
    image.thumbnail((336, 336))  # CLIP 입력 크기(224)보다 약간 여유 있게
    return product_id, title, image_url, image


def embed_incremental(**context):
    """
    1) 남은 항목들을 스레드풀로 병렬 다운로드
    2) EMBED_BATCH_SIZE개씩 묶어서 CLIP 배치 임베딩
    3) 배치 단위로 JSONL에 append (체크포인트)
    이미 처리된 product_id는 스킵하여 재시도 시 중복 다운로드를 방지한다.
    """
    import torch
    from sentence_transformers import SentenceTransformer

    ti = context["ti"]
    ds = context["ds_nodash"]
    work_dir = _work_dir(ds)

    parquet_path = ti.xcom_pull(task_ids="download_parquet")
    df = pd.read_parquet(parquet_path)

    if "image_url" not in df.columns:
        raise ValueError("products.parquet에 image_url 컬럼이 없습니다.")

    embeddings_path = os.path.join(work_dir, "embeddings.jsonl")

    # 이미 처리된 product_id 체크포인트 로드
    done_ids = set()
    if os.path.exists(embeddings_path):
        with open(embeddings_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    done_ids.add(json.loads(line)["product_id"])
                except (json.JSONDecodeError, KeyError):
                    continue
        print(f"이미 처리된 항목 {len(done_ids)}개 발견, 이어서 진행합니다.")

    # 처리 대상 목록 구성 (이미 끝난 건 제외)
    todo = []
    for _, row in df.iterrows():
        image_url = row.get("image_url")
        if not isinstance(image_url, str) or not image_url:
            continue
        product_id = str(row.get("product_id", uuid.uuid4()))
        if product_id in done_ids:
            continue
        todo.append((product_id, row.get("title", ""), image_url))

    print(f"이번에 처리할 항목: {len(todo)}건 (전체 {len(df)}건 중)")

    if not todo:
        print("처리할 항목이 없습니다.")
        return embeddings_path

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"임베딩 디바이스: {device}")
    model = SentenceTransformer("clip-ViT-B-32", device=device)

    processed = 0
    failed = 0
    total_todo = len(todo)
    already_done = len(done_ids)
    grand_total = already_done + total_todo  # 전체 데이터셋 기준 총 건수

    # 진행률 로그를 매 건마다 찍으면 너무 시끄러우므로, 대략 이 정도 간격으로만 출력
    LOG_EVERY = max(EMBED_BATCH_SIZE, 50)
    last_logged_at = 0

    with open(embeddings_path, "a", encoding="utf-8") as out_f:
        # DOWNLOAD_MAX_WORKERS개의 스레드로 동시에 다운로드
        with ThreadPoolExecutor(max_workers=DOWNLOAD_MAX_WORKERS) as executor:
            futures = {
                executor.submit(_download_one, pid, title, url): (pid, title, url)
                for pid, title, url in todo
            }

            batch_meta = []   # [(product_id, title, image_url), ...]
            batch_images = []  # [PIL.Image, ...]

            def flush_batch():
                nonlocal processed
                if not batch_images:
                    return
                vectors = model.encode(
                    batch_images,
                    batch_size=EMBED_BATCH_SIZE,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                    show_progress_bar=False,
                )
                for (pid, title, url), vector in zip(batch_meta, vectors):
                    record = {
                        "product_id": pid,
                        "title": title,
                        "image_url": url,
                        "vector": vector.tolist(),
                    }
                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                out_f.flush()
                os.fsync(out_f.fileno())
                processed += len(batch_images)
                batch_meta.clear()
                batch_images.clear()
                gc.collect()

            for future in as_completed(futures):
                pid, title, url = futures.pop(future)  # 처리 즉시 참조 제거 (메모리 누수 방지)
                try:
                    _, _, _, image = future.result()
                    batch_meta.append((pid, title, url))
                    batch_images.append(image)
                except Exception as e:
                    print(f"다운로드/처리 실패, 스킵: {url} ({e})")
                    failed += 1
                    continue
                finally:
                    del future  # Future 객체가 캐시하고 있는 결과 참조도 명시적으로 해제

                if len(batch_images) >= EMBED_BATCH_SIZE:
                    flush_batch()

                # 진행률 로그: 완료(성공+실패) 건수가 LOG_EVERY만큼 늘어날 때마다 출력
                done_so_far = processed + failed
                if done_so_far - last_logged_at >= LOG_EVERY:
                    current_total = already_done + processed
                    pct = current_total / grand_total * 100 if grand_total else 0
                    print(
                        f"[진행률] 이번 실행 {done_so_far}/{total_todo}건 처리 "
                        f"(성공 {processed}, 실패 {failed}) | "
                        f"전체 기준 {current_total}/{grand_total}건 ({pct:.1f}%)"
                    )
                    last_logged_at = done_so_far

            # 남은 배치 처리
            flush_batch()

    final_total = already_done + processed
    final_pct = final_total / grand_total * 100 if grand_total else 0
    print(
        f"이번 실행 완료: 신규 처리 {processed}건, 실패 {failed}건 -> {embeddings_path}\n"
        f"[최종 진행률] 전체 {final_total}/{grand_total}건 ({final_pct:.1f}%)"
    )
    return embeddings_path


def upsert_to_qdrant(**context):
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, PointStruct, VectorParams

    ti = context["ti"]
    embeddings_path = ti.xcom_pull(task_ids="embed_incremental")

    if not embeddings_path or not os.path.exists(embeddings_path):
        print("임베딩 파일이 없습니다. 업서트할 데이터가 없어 종료합니다.")
        return

    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in existing:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
        )

    # 전체 건수를 먼저 세어서 진행률 계산에 사용 (파일이 커도 텍스트 라인 카운트라 빠름)
    with open(embeddings_path, "r", encoding="utf-8") as f:
        grand_total = sum(1 for line in f if line.strip())

    batch: list[PointStruct] = []
    total_upserted = 0

    with open(embeddings_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)

            batch.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=record["vector"],
                    payload={
                        "product_id": record["product_id"],
                        "title": record["title"],
                        "image_url": record["image_url"],
                    },
                )
            )

            if len(batch) >= UPSERT_BATCH_SIZE:
                # wait=False: 서버 커밋 확인을 기다리지 않고 다음 배치를 바로 전송 (처리량 향상)
                client.upsert(collection_name=COLLECTION_NAME, points=batch, wait=False)
                total_upserted += len(batch)
                pct = total_upserted / grand_total * 100 if grand_total else 0
                print(f"[진행률] Qdrant upsert {total_upserted}/{grand_total}건 ({pct:.1f}%)")
                batch = []

    if batch:
        client.upsert(collection_name=COLLECTION_NAME, points=batch, wait=False)
        total_upserted += len(batch)

    print(f"Qdrant upsert 요청 완료: 총 {total_upserted}/{grand_total}개 포인트 -> collection '{COLLECTION_NAME}'")
    print("주의: wait=False라 서버 반영에 약간의 지연이 있을 수 있습니다. "
          "필요시 points_count로 최종 검증하세요.")


def cleanup_work_dir(**context):
    ds = context["ds_nodash"]
    work_dir = os.path.join(BASE_DATA_DIR, ds)
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
        print(f"작업 디렉토리 삭제 완료: {work_dir}")
    else:
        print(f"삭제할 디렉토리가 없습니다: {work_dir}")


with DAG(
    dag_id="naver_shopping_daily_image_embedding",
    default_args=default_args,
    description="S3 일별 products.parquet 감지 -> 병렬 다운로드+배치 CLIP 임베딩 -> Qdrant upsert -> 정리",
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
        poke_interval=300,
        timeout=60 * 60 * 6,
        mode="reschedule",
    )

    download_task = PythonOperator(
        task_id="download_parquet",
        python_callable=download_parquet,
    )

    embed_task = PythonOperator(
        task_id="embed_incremental",
        python_callable=embed_incremental,
    )

    upsert_task = PythonOperator(
        task_id="upsert_to_qdrant",
        python_callable=upsert_to_qdrant,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_work_dir",
        python_callable=cleanup_work_dir,
    )

    wait_for_file >> download_task >> embed_task >> upsert_task >> cleanup_task