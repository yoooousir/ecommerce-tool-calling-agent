# EC2에 Qdrant 띄우기

## 1. EC2 준비
- 인스턴스: t3.medium 이상 권장 (CLIP 임베딩은 로컬/Airflow 쪽에서 계산하고, EC2는 벡터 저장/검색만 하므로 스펙 크게 필요 없음)
- 보안그룹 인바운드 규칙:
  - 6333 (REST), 6334 (gRPC) — 소스는 "내 로컬 IP" 또는 Airflow가 도는 서버의 IP/보안그룹만 허용 (전체 공개(0.0.0.0/0) 금지)
  - 22 (SSH) — 본인 IP만

## 2. Docker / Docker Compose 설치 (Ubuntu 기준)
```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```

## 3. Qdrant 실행
```bash
scp -i your-key.pem docker-compose.yml ubuntu@<EC2_PUBLIC_IP>:~/
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>
docker compose up -d
docker ps   # qdrant 컨테이너 확인
```

## 4. 정상 동작 확인
```bash
curl http://<EC2_PUBLIC_IP>:6333/collections
```
`{"result":{"collections":[]}, "status":"ok", ...}` 형태로 응답이 오면 정상입니다.

## 5. 로컬/Airflow 쪽에서 접속할 때
아래 환경변수로 EC2 주소를 지정합니다 (fastapi_app, airflow_dag 공통):
```bash
export QDRANT_HOST=<EC2_PUBLIC_IP>
export QDRANT_PORT=6333
export QDRANT_COLLECTION=products_images
```

> 데이터 볼륨(`./qdrant_storage`)이 EC2 로컬 디스크에 저장되므로, 인스턴스를 내렸다 올려도 EBS만 유지되면 데이터가 보존됩니다. 운영 환경이라면 EBS 스냅샷 백업을 걸어두는 걸 추천드려요.
