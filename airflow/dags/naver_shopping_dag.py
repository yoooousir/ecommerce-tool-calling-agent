"""
naver_shopping_dag.py
============================================================
네이버 쇼핑 데이터 파이프라인 Airflow DAG (v2)

[v2 변경사항]
  - download_images Task 제거 (코드는 크롤러에 유지)
  - upload_to_s3 Task 추가
    → SQLite 적재 완료 후 id/description/image_url 을 parquet으로 S3에 업로드

[Task 의존성]
  crawl_and_load → dbt_run → dbt_test → upload_to_s3

[필요 환경변수]
  NAVER_CLIENT_ID, NAVER_CLIENT_SECRET  : 네이버 API 인증
  NAVER_DATA_DIR                        : SQLite 저장 경로
  S3_BUCKET_NAME                        : S3 버킷 이름
  S3_KEY_PREFIX                         : S3 키 프리픽스 (기본: naver_shopping/parquet)
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION : AWS 자격증명
============================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "naver_shop_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="naver_shopping_pipeline",
    description="네이버 쇼핑 수집 → SQLite 적재 → dbt 정제 → S3 parquet 업로드",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 29),
    catchup=False,
    tags=["naver", "ecommerce", "dbt", "s3"],
)

DBT_PROJECT_DIR  = "/opt/airflow/dbt/naver_shop"
DBT_PROFILES_DIR = "/opt/airflow/dbt/naver_shop"


# ----------------------------------------------------------------------
# Task 1: 크롤링 + SQLite 적재
# ----------------------------------------------------------------------

def _crawl_and_load(**context):
    """
    네이버 쇼핑 API로 상품 수집 후 SQLite에 적재.
    v2: description / image_url / local_image_path 컬럼 없음.
    """
    from collect_naver_shopping_prod import run_crawl_and_load
    return run_crawl_and_load()


crawl_and_load = PythonOperator(
    task_id="crawl_and_load",
    python_callable=_crawl_and_load,
    dag=dag,
)


# ----------------------------------------------------------------------
# Task 2: dbt run
# ----------------------------------------------------------------------

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=(
        f"/home/airflow/.local/bin/dbt run "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}"
    ),
    dag=dag,
)


# ----------------------------------------------------------------------
# Task 3: dbt test
# ----------------------------------------------------------------------

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=(
        f"/home/airflow/.local/bin/dbt test "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}"
    ),
    dag=dag,
)


# ----------------------------------------------------------------------
# Task 4: S3 parquet 업로드 (download_images 대체)
# ----------------------------------------------------------------------

def _upload_to_s3(**context):
    """
    SQLite에서 메타데이터를 읽어 description 재조합 후
    id / description / image_url 3개 컬럼을 parquet으로 S3에 업로드.

    [왜 dbt_test 이후에 실행하는가]
      dbt_test가 데이터 품질을 검증한 뒤에만 S3로 업로드하도록 순서를 강제.
      품질 미달 데이터가 S3(다운스트림 파이프라인의 입력)로 흘러가는 것을 방지.
    """
    from collect_naver_shopping_prod import run_upload_to_s3
    s3_path = run_upload_to_s3()
    # XCom으로 업로드된 S3 경로를 반환 → 다음 Task나 모니터링에서 확인 가능
    return s3_path


upload_to_s3 = PythonOperator(
    task_id="upload_to_s3",
    python_callable=_upload_to_s3,
    dag=dag,
)


# ----------------------------------------------------------------------
# Task 의존성
# crawl_and_load → dbt_run → dbt_test → upload_to_s3
# ----------------------------------------------------------------------

crawl_and_load >> dbt_run >> dbt_test >> upload_to_s3