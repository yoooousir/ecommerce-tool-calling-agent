"""
공통 벤치마크 인터페이스.
각 DB 어댑터는 아래 4개 메서드만 구현하면 run_benchmark.py에서 동일하게 측정 가능.
"""

from abc import ABC, abstractmethod
from typing import Dict, List

import numpy as np


class VectorDBAdapter(ABC):
    name: str = "base"

    @abstractmethod
    def setup(self, dim: int):
        """컬렉션/인덱스 생성 (이미 있으면 초기화)"""

    @abstractmethod
    def insert(self, ids: List[str], vectors: np.ndarray, payloads: List[dict]) -> None:
        """벡터+메타데이터 삽입 (배치 처리는 어댑터 내부에서 처리)"""

    @abstractmethod
    def search(self, query_vector: np.ndarray, top_k: int = 10) -> List[str]:
        """순수 벡터 검색, 반환값은 id 리스트 (score 순)"""

    @abstractmethod
    def filtered_search(self, query_vector: np.ndarray, filters: dict, top_k: int = 10) -> List[str]:
        """메타데이터 필터 + 벡터 검색"""

    @abstractmethod
    def teardown(self):
        """컬렉션 삭제 등 정리"""
