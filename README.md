# ecommerce-tool-calling-agent

네이버 쇼핑 검색 API 기반 상품 데이터 파이프라인. AI 쇼핑 에이전트(Tool Calling + 멀티모달 검색)
프로젝트의 데이터 적재/정제 단계이다.

## 전체 파이프라인 개요

```
네이버 쇼핑 검색 API
        │  (63개 키워드, 키워드당 페이지네이션 반복 호출)
        ▼
collect_naver_shopping_prod.py
        │  (수집 → 중복제거 → SQLite 적재)
        ▼
SQLite (products.db) — raw 레이어
        │
        ▼  dbt run
stg_products (staging, view)
        │  (결측치/이상치 정제, 컬럼명 표준화)
        ▼  dbt run
dim_products / category_summary (marts, table)
        │  (가격구간화, 카테고리 경로 결합, 품질 집계)
        ▼  dbt test
        │  (product_id 유일성, null 검증, price_bucket 허용값 검증)
        ▼
collect_naver_shopping_prod.run_download_images()
        │  (이미지 URL → 로컬 파일 다운로드, CLIP 임베딩 준비)
        ▼
이미지 파일 + dim_products 테이블 (벡터DB 적재 및 에이전트 검색 도구의 입력)
```

이 전체 흐름은 Airflow DAG(`airflow/dags/naver_shopping_dag.py`)로 오케스트레이션되며,
Task 순서는 `crawl_and_load → dbt_run → dbt_test → download_images` 이다.

## 디렉토리 구조

```
.
├── collect_naver_shopping_prod.py   # 운영용 크롤러 (63개 키워드, 실제 DB 적재 활성화)
├── collect_naver_shopping.py        # 테스트용 크롤러 (5개 키워드, 미리보기 2건만 출력+이미지다운로드)
├── airflow/
│   ├── docker-compose.yml           # Docker Desktop에서 Airflow를 띄우는 설정
│   ├── requirements-airflow.txt     # Airflow 컨테이너에 추가 설치할 패키지 (requests, dbt 등)
│   ├── dags/
│   │   └── naver_shopping_dag.py    # 전체 파이프라인을 오케스트레이션하는 DAG
│   └── data/                        # (실행 시 생성) SQLite DB 파일 등 산출물 저장 위치
└── dbt/
    └── naver_shop/
        ├── dbt_project.yml          # dbt 프로젝트 설정 (staging=view, marts=table)
        ├── profiles.yml             # SQLite 접속 설정 (dbt-sqlite 어댑터)
        └── models/
            ├── staging/
            │   ├── sources.yml      # raw products 테이블을 dbt 소스로 등록
            │   └── stg_products.sql # 1차 정제 (결측치/이상치 제거, 컬럼 표준화)
            └── marts/
                ├── dim_products.sql      # 최종 상품 카탈로그 (가격구간, 카테고리경로 추가)
                ├── category_summary.sql  # 카테고리별 수집 현황 요약
                └── schema.yml             # 컬럼 문서 + dbt test 정의
```

## 데이터 스키마 문서

### 1. Raw 레이어 — `products` (SQLite, collect_naver_shopping_prod.py가 직접 적재)

네이버 쇼핑 검색 API 응답을 가공 없이 그대로 받아 적재하는 원본 테이블.

| 컬럼명 | 타입 | 설명 |
|---|---|---|
| `id` | INTEGER (PK, AUTOINCREMENT) | 상품 고유 ID |
| `title` | TEXT | 상품명 (HTML 태그 제거됨) |
| `link` | TEXT (UNIQUE) | 상품 상세 페이지 URL. 중복 적재 방지 기준 |
| `image_url` | TEXT | 네이버가 제공하는 원본 상품 이미지 URL |
| `local_image_path` | TEXT (nullable) | 로컬에 다운로드한 이미지 파일 경로. `download_images()` 실행 전까지 NULL |
| `description` | TEXT | 텍스트 임베딩용 의사 설명문. title/brand/maker/category1~4/mallName을 조합해 생성 (네이버 API가 상세설명을 제공하지 않아 메타데이터로 대체) |
| `lprice` | INTEGER | 최저가 (원) |
| `hprice` | INTEGER | 최고가 (원) |
| `mall_name` | TEXT | 판매 쇼핑몰명 |
| `maker` | TEXT | 제조사 |
| `brand` | TEXT | 브랜드 |
| `category1` ~ `category4` | TEXT | 네이버 분류 카테고리 (대분류 → 소분류) |
| `search_keyword` | TEXT | 수집 시 사용한 검색 키워드 |
| `collected_at` | TEXT (ISO 8601) | 수집 시각 |

**제약조건**: `link UNIQUE` — 동일 상품이 여러 키워드/페이지에서 중복 수집되어도 `INSERT OR IGNORE`로 1건만 유지됨

### 2. Staging 레이어 — `stg_products` (dbt, view)

raw `products`를 1:1로 정제한 뷰. 비즈니스 로직은 적용하지 않고, 다음만 처리:
- 컬럼명을 분석 친화적으로 변경 (`lprice` → `low_price`, `category1` → `category_l1` 등)
- 결측치/이상치 제거: `low_price > 0` AND `title`이 비어있지 않은 행만 통과

| 컬럼명 | 원본 컬럼 | 설명 |
|---|---|---|
| `product_id` | `id` | 상품 고유 ID |
| `title` | `title` | 상품명 (trim 처리) |
| `link` | `link` | 상품 URL |
| `image_url` | `image_url` | 원본 이미지 URL |
| `local_image_path` | `local_image_path` | 로컬 이미지 경로 |
| `description` | `description` | 의사 설명문 |
| `low_price` | `lprice` | 최저가 |
| `high_price` | `hprice` | 최고가 |
| `mall_name` | `mall_name` | 판매처 (trim 처리) |
| `maker` | `maker` | 제조사 (trim 처리) |
| `brand` | `brand` | 브랜드 (trim 처리) |
| `category_l1` ~ `category_l4` | `category1` ~ `category4` | 카테고리 (trim 처리) |
| `search_keyword` | `search_keyword` | 수집 키워드 |
| `collected_at` | `collected_at` | 수집 시각 |

### 3. Marts 레이어 — `dim_products` (dbt, table)

쇼핑 에이전트의 RDBMS 조건 검색(`search_by_text` 도구)이 실제로 조회하는 최종 상품 카탈로그.
`stg_products`에 비즈니스 로직을 추가로 적용한다.

| 컬럼명 | 설명 |
|---|---|
| `product_id` | 상품 고유 ID |
| `title`, `link`, `image_url`, `local_image_path`, `description` | staging과 동일 |
| `low_price`, `high_price` | 최저가/최고가 |
| `mall_name`, `maker`, `brand` | staging과 동일 |
| `category_path` | `category_l1 > category_l2 > category_l3 > category_l4` 형태로 결합한 사람이 읽기 좋은 경로 문자열 (빈 하위 카테고리는 자동 생략) |
| `category_l1`, `category_l2` | 검색 필터링에 자주 쓰이는 상위 카테고리 (별도 컬럼으로 유지) |
| `price_bucket` | 가격 구간: `budget`(3만원 미만) / `mid`(3만원~10만원 미만) / `premium`(10만원 이상). 에이전트가 "저렴한 것 위주" 같은 모호한 자연어 조건을 처리할 때 활용 |
| `has_image` | 로컬에 이미지가 다운로드되어 있으면 1, 아니면 0. 멀티모달 검색 대상 포함 여부 판단용 |
| `search_keyword`, `collected_at` | staging과 동일 |

**dbt test 적용 항목** (`models/marts/schema.yml`):
- `product_id`: unique, not_null
- `link`: unique, not_null
- `low_price`: not_null
- `price_bucket`: accepted_values(`budget`, `mid`, `premium`)

### 4. Marts 레이어 — `category_summary` (dbt, table)

카테고리(대분류)별 수집 현황을 요약한 테이블. 데이터 품질 점검(특정 카테고리 과소 수집 여부)
및 추후 비용/사용량 대시보드의 카테고리 분포 패널에 활용한다.

| 컬럼명 | 설명 |
|---|---|
| `category_l1` | 대분류 카테고리명 |
| `product_count` | 해당 카테고리 상품 수 |
| `avg_low_price` | 평균 최저가 |
| `min_price`, `max_price` | 최저가의 최솟값/최댓값 |
| `image_downloaded_count` | 이미지가 다운로드된 상품 수 |
| `image_coverage_pct` | 이미지 다운로드 비율 (%) |

## 수집 대상 키워드 (63개)

| 카테고리 | 키워드 |
|---|---|
| 패션/액세서리 (15) | 운동화, 후드티, 청바지, 패딩, 원피스, 가방, 지갑, 벨트, 모자, 선글라스, 시계, 반지, 목걸이, 귀걸이, 팔찌 |
| 전자기기 (10) | 노트북, 무선마우스, 기계식키보드, 모니터, 이어폰, 스마트워치, 게이밍의자, 캠코더, 드론, 블루투스스피커 |
| 가전 (9) | 냉장고, 세탁기, 청소기, 로봇청소기, 제습기, 가습기, 에어컨, TV, 식기세척기 |
| 주방용품 (17) | 전기밥솥, 전기오븐, 전기주전자, 커피머신, 믹서기, 에어프라이어, 전자레인지, 수저세트, 주방칼, 도마, 냄비, 프라이팬, 와인잔, 컵, 접시, 그릇, 텀블러 |
| 뷰티 (12) | 샴푸, 바디워시, 선크림, 마스크팩, 향수, 립스틱, 아이섀도우, 파운데이션, 마스카라, 헤어드라이기, 고데기, 네일아트 |

목표 수집 건수: 전체 10,000건 (키워드당 약 158건, `target_total // len(KEYWORDS)`로 자동 계산)

## 실행 방법

### 사전 준비

1. [네이버 개발자센터](https://developers.naver.com)에서 애플리케이션 등록 (사용 API: 검색)
2. 발급받은 Client ID/Secret을 `airflow/.env` 파일에 작성:
   ```
   NAVER_CLIENT_ID=발급받은_ID
   NAVER_CLIENT_SECRET=발급받은_SECRET
   ```

### 단독 스크립트로 실행 (Airflow 없이 빠르게 테스트)

```bash
export NAVER_CLIENT_ID="발급받은_ID"
export NAVER_CLIENT_SECRET="발급받은_SECRET"
pip install requests --break-system-packages

python3 collect_naver_shopping_prod.py
```

### Airflow(Docker)로 전체 파이프라인 실행

```bash
cd airflow

# 최초 1회: Airflow 메타데이터 DB 초기화 + 관리자 계정 생성
docker compose up airflow-init

# Airflow 전체 서비스 기동
docker compose up -d

# 브라우저에서 http://localhost:8080 접속 (계정: airflow / airflow)
# naver_shopping_pipeline 대그를 수동 트리거
```

### dbt만 별도로 실행 (디버깅용)

```bash
cd dbt/naver_shop
export NAVER_DATA_DIR="../../naver_shopping_data"   # products.db가 있는 경로
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## 향후 확장 고려사항

- **RDBMS 전환**: 향후 AWS 배포(RDS)를 고려해 SQLite → PostgreSQL 전환 시 `dbt-sqlite` → `dbt-postgres`로 어댑터만 교체하면 `models/` 내 SQL은 대부분 그대로 재사용 가능
- **pgvector 활용**: PostgreSQL 전환 시 별도 벡터DB(Chroma/Qdrant) 대신 pgvector 확장으로 정형 데이터와 임베딩을 한 테이블에서 통합 관리할 수 있음
- **이미지 임베딩 적재**: `dim_products`의 `has_image=1`인 상품들을 대상으로 CLIP 임베딩을 생성해 벡터DB(or pgvector)에 적재하는 후속 DAG 추가 예정