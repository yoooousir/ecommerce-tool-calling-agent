# 벡터 DB 정량 비교 벤치마크

## 설치
```bash
pip install chromadb qdrant-client "pinecone[grpc]" numpy --break-system-packages
```

## 사전 준비
- **Qdrant**: 로컬 Docker로 띄워두기 (-d : 백그라운드)
  ```bash
  docker run -d -p 6333:6333 -v $(pwd)/qdrant_data:/qdrant/storage qdrant/qdrant:latest
  ```
- **Pinecone**: 계정 생성 후 API 키 발급, 환경변수 설정
  ```bash
  export PINECONE_API_KEY="your-key"
  ```
- **ChromaDB**: 별도 서버 불필요 (embedded 모드)

## 실행
```bash
cd benchmark

# 합성 데이터로 3사 비교 (1만 개 벡터, 512차원 = CLIP 차원)
python run_benchmark.py --dbs chroma qdrant pinecone --n-vectors 10000

# Pinecone 계정이 없다면 둘만 비교
python run_benchmark.py --dbs chroma qdrant --n-vectors 10000

# 실제 프로젝트 CLIP 임베딩으로 테스트하고 싶다면
# (run_pipeline.py 쪽에서 np.savez로 ids/vectors/payloads 저장해두고 경로 지정)
python run_benchmark.py --dbs chroma qdrant --real-embeddings-npz ./my_embeddings.npz

# 실제 서비스 규모를 가정한 비용 계산 (월간 검색량/갱신량을 직접 지정)
python run_benchmark.py --dbs chroma qdrant pinecone \
  --n-vectors 100000 \
  --monthly-queries 500000 \
  --monthly-upserts 20000 \
  --avg-metadata-bytes 300
```

## 비용 계산 방식 (`cost_model.py`)
DB 클러스터를 실제로 띄우지 않고도, 벤치마크에 사용한 벡터 수/차원과 예상 트래픽만으로 월 비용을 추정합니다.

- **Pinecone**: 실제 요금 문서 기준 종량제(Storage $0.33/GB, Write Unit $2/1M, Read Unit $8.25/1M, 월 최소 $50)를 그대로 계산식에 반영. `--monthly-upserts`, `--monthly-queries`로 예상 트래픽 조절 가능.
- **Qdrant / ChromaDB (자체 호스팅 가정)**: HNSW 인덱스가 요구하는 메모리량(벡터 차원×4바이트 + 메타데이터 + 그래프 오버헤드 1.5배)을 계산해, 그 메모리를 감당할 수 있는 가장 저렴한 AWS EC2 온디맨드 인스턴스(r6g 계열)를 자동 매핑하고, EBS 디스크 비용을 더함.

⚠️ 두 요율표(Pinecone 요금, AWS EC2 요금) 모두 스크립트 안에 상수로 박아뒀는데, 시간이 지나면 바뀌므로 실제 의사결정 전에는 `pinecone.io/pricing/estimate`와 `aws.amazon.com/ec2/pricing`에서 최신 값으로 갱신하는 걸 추천합니다. 포트폴리오 문서에도 "산정 기준일"을 같이 적어두시는 게 좋습니다.

결과 예시(`--n-vectors 10000` 기준):
- Qdrant/Chroma 자체호스팅: 최소 인스턴스(t4g.small)로 충분해 월 $14 내외
- Pinecone: 트래픽이 적어도 월 최소과금 $50이 적용됨

→ 소규모(수만 개 벡터, 월 검색량 낮음) 단계에서는 자체 호스팅이 비용상 유리하고, 트래픽/데이터가 커질수록 관리 부담 대비 Pinecone의 이점이 커진다는 걸 숫자로 보여줄 수 있습니다.

## 결과 해석
#### 성능 지표
- `insert_throughput_vec_per_sec`: 초당 몇 개의 벡터를 삽입할 수 있는지. 전체 벡터 수 / 삽입에 걸린 시간으로 계산. 클수록 좋음 — 대량 상품 색인 시 소요 시간에 직결
- `insert_total_sec`: 전체 벡터를 삽입하는 데 걸린 총 시간(초)
- `search_p50_ms`: 필터 없는 순수 벡터 검색 1건의 소요시간 중앙값(밀리초), 95번째 백분위 수. 작을수록 좋음 — 사용자 체감 검색 속도
- `search_p95_ms`: 필터 없는 순수 벡터 검색의 95번째 백분위 수. 이 값이 p50과 많이 벌어지면 가끔 튀는 경우가 있다는 뜻
- `recall_at_10`: 정확도 지표. brute-force(전수조사)로 계산한 '진짜 정답 top-10'과, 실제 DB가 ANN(근사) 알고리즘으로 찾은 top-10을 비교해서 몇 %나 일치하는지.
- `filtered_search_p50_ms` / `filtered_search_p95_ms`: 메타데이터 필터(가격이하, 카테고리일치, 재고있음 등)를 같이 걸었을 때의 지연시간.
- `filter_slowdown_pct`: 필터 걸었을 때 지연시간이 몇 % 늘어나는지 (filtered_search_p50_ms / search_p50_ms - 1) * 100. 음수면 오히려 필터 걸었을 때보다 빨라졌다는 뜻(그 db의 인덱스가 필터로 검색 대상 자체를 줄여줘서)
- `concurrent_qps`: 10개 스레드가 동시에 요청을 100건 날렸을 때 초당 처리 건수. 트래픽이 몰리는 상황(운영안정성)을 흉내낸 지표

#### 비용 지표
- `estimated_monthly_cost_usd`: 예상 월 비용(달러). Pinecone은 실제 종량제 요금표 기준 계산값, Chroma/Qdrant는 자체 호스팅 시 필요한 AWS EC2 인스턴스+EBS 비용 기준
- `cost_breakdown`: 위 비용이 어떻게 산출됐는지 세부 내역

결과는 `benchmark_results.json`에도 저장됩니다.

## 주의사항
- Pinecone은 서버리스 인덱스 특성상 upsert 후 즉시 검색 가능하지 않을 수 있어 삽입 후 propagation 대기(약 5초)를 넣어뒀습니다. 그래도 최초 실행 시 recall이 낮게 나오면 대기시간을 늘려보세요.
- 공정한 비교를 위해 세 DB 모두 동일한 데이터셋(같은 seed)과 동일한 top_k, 동일한 필터 조건을 사용합니다.
- 소규모(1만개) 테스트라 대규모(수천만개) 운영 시나리오와는 차이가 있을 수 있음을 포트폴리오 문서에 명시하시는 걸 추천합니다.