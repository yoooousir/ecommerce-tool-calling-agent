"""
벡터 DB 비용 계산 모듈.

- Pinecone: 서버리스 종량제(Read Unit / Write Unit / Storage / 월 최소과금)
  요율은 2026년 기준 공개된 Standard 플랜 값 사용. 실제 청구는 pinecone.io/pricing/estimate 로
  반드시 재검증할 것 — 요율은 수시로 바뀜.
- Qdrant / ChromaDB: 자체 호스팅을 가정하고, 인덱스가 요구하는 메모리량을 계산해
  그 메모리를 커버하는 가장 저렴한 AWS EC2 온디맨드 인스턴스를 매핑.
  (실제로는 EKS 노드 1대를 다른 워크로드와 공유할 수도 있으니, 이 값은 "온전히 이 DB만 위한
  전용 인스턴스" 기준의 상한선으로 해석할 것.)
"""

from dataclasses import dataclass

# --- Pinecone 요율 (2026 Standard plan 기준, 공개 자료 근사치) -----------------
PINECONE_RATE_PER_WU = 2.00 / 1_000_000        # $2 / 1M write units
PINECONE_RATE_PER_RU = 8.25 / 1_000_000        # $8.25 / 1M read units
PINECONE_RATE_PER_GB_MONTH = 0.33              # $0.33 / GB / month
PINECONE_MONTHLY_MINIMUM = 50.0                # Standard 플랜 월 최소 과금

# --- AWS EC2 온디맨드 요율 (us-east-1, 2026년 초 근사치, USD/hour) ------------
# 실제 청구는 aws.amazon.com/ec2/pricing 에서 재확인 필요. r6g(Graviton, 메모리 최적화) 기준.
EC2_INSTANCE_TABLE = [
    # (인스턴스 타입, vCPU, RAM_GB, 시간당 요금)
    ("t4g.small", 2, 2, 0.0168),
    ("t4g.medium", 2, 4, 0.0336),
    ("r6g.large", 2, 16, 0.1008),
    ("r6g.xlarge", 4, 32, 0.2016),
    ("r6g.2xlarge", 8, 64, 0.4032),
    ("r6g.4xlarge", 16, 128, 0.8064),
    ("r6g.8xlarge", 32, 256, 1.6128),
]

HOURS_PER_MONTH = 730


@dataclass
class CostEstimate:
    db: str
    monthly_cost_usd: float
    breakdown: dict


def _bytes_for_index(n_vectors: int, dim: int, avg_metadata_bytes: int, overhead_factor: float) -> int:
    """
    벡터 1개당: dim*4바이트(float32) + 메타데이터 + HNSW 그래프/인덱스 오버헤드.
    overhead_factor: HNSW 그래프 등 인덱스 자체가 차지하는 추가 배율 (Qdrant 실측 기준 통상 1.3~1.8배)
    """
    raw_vector_bytes = dim * 4
    per_vector_bytes = (raw_vector_bytes + avg_metadata_bytes) * overhead_factor
    return int(n_vectors * per_vector_bytes)


def estimate_pinecone_cost(
    n_vectors: int,
    dim: int,
    avg_metadata_bytes: int = 200,
    monthly_upserts: int = 0,
    monthly_queries: int = 0,
) -> CostEstimate:
    # 1. Storage
    index_bytes = _bytes_for_index(n_vectors, dim, avg_metadata_bytes, overhead_factor=1.0)
    index_gb = index_bytes / (1024 ** 3)
    storage_cost = index_gb * PINECONE_RATE_PER_GB_MONTH

    # 2. Write units: 1 WU per 1KB of upserted record, 최소 5 WU/요청
    #    배치 upsert라고 가정하고 레코드 단위로 근사 계산
    record_kb = (dim * 4 + avg_metadata_bytes) / 1024
    wu_per_record = max(5, record_kb)  # 문서 기준 최소 5WU/요청이지만 배치시 레코드당으로 근사
    write_units = monthly_upserts * wu_per_record
    write_cost = write_units * PINECONE_RATE_PER_WU

    # 3. Read units: namespace 크기(GB) 1GB당 1RU, 최소 0.25RU/쿼리
    ru_per_query = max(0.25, index_gb)
    read_units = monthly_queries * ru_per_query
    read_cost = read_units * PINECONE_RATE_PER_RU

    usage_total = storage_cost + write_cost + read_cost
    total = max(usage_total, PINECONE_MONTHLY_MINIMUM)

    return CostEstimate(
        db="Pinecone",
        monthly_cost_usd=round(total, 2),
        breakdown={
            "storage_gb": round(index_gb, 3),
            "storage_cost_usd": round(storage_cost, 2),
            "write_units": round(write_units, 1),
            "write_cost_usd": round(write_cost, 2),
            "read_units": round(read_units, 1),
            "read_cost_usd": round(read_cost, 2),
            "usage_subtotal_usd": round(usage_total, 2),
            "monthly_minimum_applied": usage_total < PINECONE_MONTHLY_MINIMUM,
        },
    )


def _pick_ec2_instance(required_ram_gb: float):
    for name, vcpu, ram, hourly in EC2_INSTANCE_TABLE:
        if ram >= required_ram_gb:
            return name, vcpu, ram, hourly
    # 표에 없을 만큼 크면 가장 큰 인스턴스를 여러 대 쓴다고 가정
    name, vcpu, ram, hourly = EC2_INSTANCE_TABLE[-1]
    n_nodes = -(-int(required_ram_gb) // ram)  # ceil division
    return f"{name} x{n_nodes}", vcpu * n_nodes, ram * n_nodes, hourly * n_nodes


def estimate_self_hosted_cost(
    db_name: str,
    n_vectors: int,
    dim: int,
    avg_metadata_bytes: int = 200,
    overhead_factor: float = 1.5,
    ebs_gb_price: float = 0.08,  # gp3 $/GB/month
) -> CostEstimate:
    """
    Qdrant/ChromaDB처럼 자체 호스팅하는 DB의 EC2 비용 추정.
    HNSW 인덱스는 대부분 인메모리로 상주해야 하므로, 필요한 RAM을 커버하는
    최소 인스턴스를 고른다. EBS는 데이터 영속화를 위한 디스크 비용.
    """
    index_bytes = _bytes_for_index(n_vectors, dim, avg_metadata_bytes, overhead_factor)
    required_ram_gb = index_bytes / (1024 ** 3)
    # OS/기타 프로세스를 위한 여유분 20% 추가
    required_ram_gb *= 1.2

    instance_name, vcpu, ram_gb, hourly = _pick_ec2_instance(required_ram_gb)
    compute_cost = hourly * HOURS_PER_MONTH

    disk_gb_needed = max(20, index_bytes / (1024 ** 3) * 1.3)  # 인덱스 + 스냅샷 여유
    disk_cost = disk_gb_needed * ebs_gb_price

    total = compute_cost + disk_cost

    return CostEstimate(
        db=db_name,
        monthly_cost_usd=round(total, 2),
        breakdown={
            "required_ram_gb": round(required_ram_gb, 2),
            "instance_type": instance_name,
            "instance_hourly_usd": hourly,
            "compute_cost_usd": round(compute_cost, 2),
            "ebs_gb": round(disk_gb_needed, 1),
            "ebs_cost_usd": round(disk_cost, 2),
        },
    )
