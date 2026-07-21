# 이미지 임베딩 유사도 검색 파이프라인

## 구조
```
image_embedding_similarity/
├── ec2_qdrant/                # EC2에 올릴 Qdrant docker-compose
├── fastapi_app/               # 로컬에서 실행할 이미지 업로드 + 유사도검색 UI/API
└── airflow_dag/dags/          # S3 트리거 -> 임베딩 -> Qdrant 적재 DAG
```

## 폴더 위치 관련
`crawling/` 상위 폴더 밑에 형제 폴더로 `image_embedding_similarity/`를 두시는 것 자체는 문제 없습니다.
코드 안에 다른 폴더(`crawling` 등)를 import하거나 상대경로로 참조하는 부분이 전혀 없어서, 이 폴더는
독립적으로 어디에 둬도 동작에는 영향이 없습니다.

다만 한 가지만 주의하세요:

- **`fastapi_app/`, `ec2_qdrant/` 는 폴더 위치가 자유**입니다. `crawling/image_embedding_similarity/fastapi_app`
  이런 식으로 두고 그 안에서 `uvicorn main:app`을 실행하면 그대로 동작합니다.
- **`airflow_dag/dags/naver_shopping_daily_embedding_dag.py` 만은 예외**입니다. Airflow는 본인이 설정한
  `dags_folder` (보통 `$AIRFLOW_HOME/dags`, 또는 docker-compose로 Airflow를 돌리신다면 `docker-compose.yml`의
  `volumes`에 매핑된 dags 경로)를 스캔해서 DAG를 인식하기 때문에, 이 파일 하나는 **그 dags 폴더 안으로 복사(or 심볼릭 링크)**
  해줘야 Airflow UI에 뜹니다. `crawling/image_embedding_similarity/airflow_dag/dags/` 안에 그냥 둬도 Airflow가
  거길 스캔하도록 `dags_folder`를 거기로 지정해뒀다면 그대로 두셔도 됩니다.

정리하면: **`crawling/image_embedding_similarity/`로 통째로 넣으시면 되고, DAG 파일만 Airflow가 실제로 스캔하는
dags 경로와 일치하는지 한 번 확인**해주시면 됩니다.

## 전체 흐름
1. **EC2**: Qdrant 벡터 DB만 띄워둠 (저장소 역할)
2. **Airflow**: 매일 S3 `naver_shopping/YYYY/MM/DD/products.parquet` 파일 생성을 감지 →
   image_url 다운로드 → CLIP 임베딩 → EC2 Qdrant에 upsert
3. **로컬 FastAPI**: 사용자가 이미지 업로드 → 같은 CLIP 모델로 임베딩 → EC2 Qdrant에서
   유사 벡터 검색 → 결과(상품 이미지/제목/점수) 반환

**중요**: FastAPI와 Airflow 둘 다 반드시 같은 임베딩 모델(`clip-ViT-B-32`)을 사용해야
벡터 공간이 일치해서 유사도 검색이 의미가 있습니다. (embedder.py 로직을 그대로 재사용하세요)

---

## 1단계: EC2 Qdrant 실행
`ec2_qdrant/README.md` 참고. 완료 후 EC2 퍼블릭 IP를 확보해두세요.

## 2단계: 로컬 FastAPI 실행
```bash
cd fastapi_app
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

export QDRANT_HOST=<EC2_PUBLIC_IP>
export QDRANT_PORT=6333
export QDRANT_COLLECTION=products_images

uvicorn main:app --reload --port 8000
```
브라우저에서 `http://localhost:8000` 접속 → 이미지 업로드 → 유사 상품 확인.

(참고: 최초 컬렉션이 비어 있으면 검색 결과가 0건입니다. 3단계 DAG로 데이터를 먼저 적재하세요.)

## 3단계: Airflow DAG 배치
```bash
cp airflow_dag/dags/naver_shopping_daily_embedding_dag.py $AIRFLOW_HOME/dags/
pip install -r airflow_dag/requirements.txt
```

Airflow 웹서버/스케줄러 환경 변수에 아래를 추가 (docker-compose로 Airflow를 돌리고 계신다면
`environment:` 항목에 추가):
```
QDRANT_HOST=<EC2_PUBLIC_IP>
QDRANT_PORT=6333
QDRANT_COLLECTION=products_images
```

Airflow UI에서 **Admin > Connections**에 `aws_default` 커넥션을 등록하세요
(S3 읽기 권한이 있는 IAM Access Key/Secret, 또는 EC2/EKS라면 IAM Role 사용).

DAG(`naver_shopping_daily_image_embedding`)를 Unpause하면:
- 매일 자동으로 오늘 날짜 폴더에 `products.parquet`가 생기는지 5분 간격으로 확인
- 파일이 생기면 자동으로 다운로드 → 임베딩 → Qdrant 적재까지 한번에 실행

### 수동 테스트하고 싶을 때
특정 날짜에 대해 즉시 실행해보고 싶다면:
```bash
airflow dags trigger naver_shopping_daily_image_embedding -e 2026-07-11
```

---

## 트러블슈팅 체크리스트
- `S3KeySensor`가 계속 대기만 하고 안 넘어감 → S3 키 경로 확인
  (`aws s3 ls s3://ecommerce-tool-calling-agent/naver_shopping/2026/07/16/` 로 실제 파일 존재 여부 확인)
- Qdrant 접속 안 됨 → EC2 보안그룹에서 6333 포트가 Airflow/로컬 IP에 열려있는지 확인
- 이미지 다운로드 실패가 많음 → `image_url`이 만료 URL이거나 접근 제한(Referer 체크 등)일 수 있음,
  실패 건은 로그로 스킵 처리되도록 이미 구현되어 있음
- FastAPI 검색결과가 항상 비어있음 → 컬렉션명이 DAG/FastAPI 양쪽에서 동일한지
  (`QDRANT_COLLECTION` 환경변수) 확인
