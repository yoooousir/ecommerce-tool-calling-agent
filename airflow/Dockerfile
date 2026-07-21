# Dockerfile
# ============================================================
# 공식 Airflow 2.9.0 이미지를 베이스로,
# dbt-core + dbt-sqlite를 미리 설치한 커스텀 이미지.
#
# [왜 Dockerfile을 따로 만드는가]
# docker exec로 컨테이너 안에 직접 설치한 패키지는
# 컨테이너 재시작 시 사라진다.
# Dockerfile로 이미지를 빌드해두면 재시작해도 dbt가 항상 유지된다.
#
# [빌드 방법]
#   cd airflow/
#   docker compose build
#   docker compose up -d
# ============================================================

FROM apache/airflow:2.9.0-python3.11

# root 권한으로 파일 복사 (USER 전환 전에 수행)
COPY requirements-airflow.txt /requirements-airflow.txt

# airflow 유저로 패키지 설치 (root 권한 불필요)
USER airflow

# dbt-core / dbt-sqlite 버전을 고정해서 설치
# (버전을 고정하지 않으면 빌드할 때마다 버전이 달라져 예기치 않은 충돌이 생길 수 있음)
# requests는 크롤러(collect_naver_shopping.py)가 사용하는 HTTP 클라이언트
# RUN pip install --no-cache-dir \
#     "dbt-core==1.9.0" \
#     "dbt-sqlite==1.9.1" \
#     "requests==2.34.2" \
#     "pandas==2.2.2" \
#     "pyarrow==16.1.0" \
#     "boto3==1.34.144" \
#     "pillow==10.4.0" \
#     "qdrant-client==1.10.1" \
#     "sentence-transformers==3.0.1" \
#     "apache-airflow-providers-amazon==8.26.0"

RUN pip install --no-cache-dir uv && \
    uv pip install --system --no-cache -r /requirements-airflow.txt