"""
naver_shopping_dag.py
============================================================
네이버 쇼핑 데이터 파이프라인을 오케스트레이션하는 Airflow DAG.

[전체 흐름]
  1. crawl_and_load   : 63개 키워드로 네이버 쇼핑 API를 호출해 상품을 수집하고
                         SQLite(products.db)의 raw products 테이블에 적재한다.
                         (collect_naver_shopping_prod.run_crawl_and_load 호출)

  2. dbt_run           : dbt가 staging(stg_products) → marts(dim_products,
                         category_summary) 순서로 모델을 빌드한다.
                         raw 데이터를 정제하고, 가격구간/카테고리경로 같은
                         비즈니스 로직을 적용해 최종 분석/검색용 테이블을 만든다.

  3. dbt_test          : dbt test로 데이터 품질을 검증한다 (product_id 유일성,
                         null 여부, price_bucket 허용값 등). 여기서 실패하면
                         하류 Task(이미지 다운로드)가 실행되지 않도록 막아,
                         품질이 깨진 데이터가 그대로 서비스에 흘러가는 것을 방지한다.

  4. download_images   : dim_products에 적재된 상품들의 이미지를 실제로
                         다운로드해 로컬에 저장한다 (CLIP 임베딩 준비 단계).
                         시간이 오래 걸리는 작업이라 별도 Task로 분리했고,
                         dbt_test를 통과한 이후에만 실행되도록 의존성을 건다.

[Task 의존성 그래프]
  crawl_and_load >> dbt_run >> dbt_test >> download_images

[전제 조건]
  - 이 DAG는 Docker Compose로 띄운 Airflow 환경에서 실행되는 것을 전제로 한다.
  - collect_naver_shopping_prod.py, dbt/naver_shop/ 디렉토리가
    Airflow 컨테이너 내부의 PYTHONPATH 또는 dags/ 폴더 하위에서 import 가능해야 한다.
    (docker-compose.yml에서 볼륨 마운트로 연결, 아래 별도 안내 참고)
  - 환경변수 NAVER_CLIENT_ID / NAVER_CLIENT_SECRET / NAVER_DATA_DIR 가
    Airflow 컨테이너에 전달되어 있어야 한다 (.env 파일 또는 docker-compose
    environment 설정을 통해 주입).
============================================================
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ----------------------------------------------------------------------
# DAG 기본 설정
# ----------------------------------------------------------------------

default_args = {
    "owner": "naver_shop_pipeline",
    "retries": 1,                          # Task 실패 시 1회 자동 재시도
    "retry_delay": timedelta(minutes=5),   # 재시도 전 5분 대기 (API 일시 장애 등을 고려)
}

dag = DAG(
    dag_id="naver_shopping_pipeline",
    description="네이버 쇼핑 API 수집 → SQLite 적재 → dbt 정제/변환 → 이미지 다운로드 파이프라인",
    default_args=default_args,
    schedule_interval="@daily",   # 매일 1회 실행. 필요시 None으로 바꿔 수동 트리거만 사용 가능
    start_date=datetime(2026, 6, 29),
    catchup=False,                # 과거 미실행분을 한꺼번에 소급 실행하지 않음
    tags=["naver", "ecommerce", "dbt"],
)


# ----------------------------------------------------------------------
# Task 1: 크롤링 + SQLite 적재
# ----------------------------------------------------------------------

def _crawl_and_load(**context):
    """
    collect_naver_shopping_prod.py의 run_crawl_and_load()를 호출하는 래퍼 함수.
    PythonOperator는 Python 함수를 직접 Task로 등록할 수 있어, 별도 서브프로세스
    없이 Airflow 워커 프로세스 안에서 바로 실행된다.

    XCom(Airflow의 Task 간 데이터 전달 메커니즘)에 최종 적재 건수를 남겨,
    Airflow UI에서 이번 실행에 몇 건이 쌓였는지 바로 확인할 수 있게 한다.
    """
    # DAG 파일이 아닌 별도 모듈에서 함수를 가져오기 위해 import는 함수 내부에서 수행
    # (Airflow가 DAG 파일을 주기적으로 파싱하는데, 무거운 import를 최상단에 두면
    #  스케줄러 성능에 영향을 줄 수 있어 Task 본문 안에서 지연 import하는 것이 권장됨)
    from collect_naver_shopping_prod import run_crawl_and_load

    total_count = run_crawl_and_load()
    return total_count  # 반환값은 자동으로 XCom에 저장됨


crawl_and_load = PythonOperator(
    task_id="crawl_and_load",
    python_callable=_crawl_and_load,
    dag=dag,
)


# ----------------------------------------------------------------------
# Task 2: dbt run (staging → marts 모델 빌드)
# ----------------------------------------------------------------------

# dbt 프로젝트 경로. docker-compose.yml에서 ./dbt/naver_shop을
# 컨테이너 내부의 /opt/airflow/dbt/naver_shop 으로 마운트한다고 가정
DBT_PROJECT_DIR = "/opt/airflow/dbt/naver_shop"
DBT_PROFILES_DIR = "/opt/airflow/dbt/naver_shop"  # profiles.yml도 같은 경로에 위치

dbt_run = BashOperator(
    task_id="dbt_run",
    # BashOperator로 dbt CLI를 직접 호출. --project-dir / --profiles-dir로
    # 프로젝트와 접속 설정 위치를 명시적으로 지정해 Airflow 컨테이너 어디서
    # 실행되든 동일하게 동작하도록 한다.
    bash_command=(
        f"dbt run "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}"
    ),
    dag=dag,
)


# ----------------------------------------------------------------------
# Task 3: dbt test (데이터 품질 검증)
# ----------------------------------------------------------------------

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=(
        f"dbt test "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}"
    ),
    dag=dag,
)


# ----------------------------------------------------------------------
# Task 4: 이미지 다운로드 (CLIP 임베딩 준비)
# ----------------------------------------------------------------------

def _download_images(**context):
    """
    collect_naver_shopping_prod.py의 run_download_images()를 호출하는 래퍼.
    전체 1만 건 이미지를 한 번에 다 받으면 시간이 오래 걸리므로,
    DAG 운영 초기에는 limit을 두어 일부만 받고 점진적으로 늘려가는 것을 권장.
    (limit=None으로 바꾸면 전체 다운로드)
    """
    from collect_naver_shopping_prod import run_download_images

    run_download_images(limit=200)  # 운영 안정화 전까지는 200건으로 제한 (필요시 조정)


download_images = PythonOperator(
    task_id="download_images",
    python_callable=_download_images,
    dag=dag,
)


# ----------------------------------------------------------------------
# Task 의존성 정의
# crawl_and_load(수집/적재) → dbt_run(정제/변환) → dbt_test(품질검증) → download_images(이미지)
# 품질 검증을 통과한 데이터에 대해서만 이미지 다운로드(다음 단계인 CLIP 임베딩의 입력)를
# 진행하도록 순서를 강제해, 잘못된 데이터가 하류로 전파되는 것을 막는다.
# ----------------------------------------------------------------------

crawl_and_load >> dbt_run >> dbt_test >> download_images
