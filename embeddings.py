"""
멀티모달 임베딩 모듈
- 텍스트(상품명, 설명)와 이미지(상품 썸네일)를 CLIP으로 동일 벡터 공간에 임베딩
- 하이브리드 검색을 위한 sparse(BM25 기반) 벡터도 함께 생성
"""

import io
import logging
from typing import List, Union

import numpy as np
import torch
from PIL import Image
from transformers import CLIPModel, CLIPProcessor

logger = logging.getLogger(__name__)


class MultimodalEmbedder:
    """
    CLIP 기반 텍스트/이미지 임베딩기.
    - 같은 벡터 공간에 텍스트/이미지를 투영하므로 "이 이미지와 비슷한 상품명"
      같은 cross-modal 검색도 가능.
    - 모델: openai/clip-vit-base-patch32 (512차원, 속도/정확도 균형)
      더 정확도가 필요하면 clip-vit-large-patch14 (768차원)로 교체.
    """

    def __init__(
        self,
        model_name: str = "openai/clip-vit-base-patch32",
        device: str = None,
        batch_size: int = 32,
    ):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Loading CLIP model '{model_name}' on {self.device}")

        self.model = CLIPModel.from_pretrained(model_name).to(self.device)
        self.model.eval()
        self.processor = CLIPProcessor.from_pretrained(model_name)
        self.batch_size = batch_size
        self.vector_dim = self.model.config.projection_dim

    @staticmethod
    def _extract_embedding_tensor(output):
        """
        transformers 버전에 따라 get_text_features()/get_image_features()가
        순수 텐서 대신 CLIPOutput류 객체를 반환하는 경우가 있어 방어적으로 처리.
        """
        if hasattr(output, "text_embeds"):
            return output.text_embeds
        if hasattr(output, "image_embeds"):
            return output.image_embeds
        if hasattr(output, "pooler_output"):
            return output.pooler_output
        return output  # 이미 순수 텐서인 정상 케이스

    @torch.no_grad()
    def encode_text(self, texts: Union[str, List[str]]) -> np.ndarray:
        """텍스트 리스트를 정규화된 임베딩 배열로 변환. shape: (N, dim)"""
        if isinstance(texts, str):
            texts = [texts]

        all_embeds = []
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i : i + self.batch_size]
            inputs = self.processor(
                text=batch, return_tensors="pt", padding=True, truncation=True, max_length=77
            ).to(self.device)
            feats = self._extract_embedding_tensor(self.model.get_text_features(**inputs))
            feats = feats / feats.norm(dim=-1, keepdim=True)
            all_embeds.append(feats.cpu().numpy())

        return np.concatenate(all_embeds, axis=0)

    @torch.no_grad()
    def encode_image(self, images: Union[List[bytes], List[str], List[Image.Image]]) -> np.ndarray:
        """
        이미지 리스트를 정규화된 임베딩 배열로 변환.
        images: 파일 경로 리스트, bytes 리스트, 또는 PIL Image 리스트 모두 허용
        """
        pil_images = [self._to_pil(img) for img in images]

        all_embeds = []
        for i in range(0, len(pil_images), self.batch_size):
            batch = pil_images[i : i + self.batch_size]
            inputs = self.processor(images=batch, return_tensors="pt").to(self.device)
            feats = self._extract_embedding_tensor(self.model.get_image_features(**inputs))
            feats = feats / feats.norm(dim=-1, keepdim=True)
            all_embeds.append(feats.cpu().numpy())

        return np.concatenate(all_embeds, axis=0)

    @staticmethod
    def _to_pil(img) -> Image.Image:
        if isinstance(img, Image.Image):
            return img.convert("RGB")
        if isinstance(img, (bytes, bytearray)):
            return Image.open(io.BytesIO(img)).convert("RGB")
        if isinstance(img, str):
            return Image.open(img).convert("RGB")
        raise TypeError(f"Unsupported image type: {type(img)}")


class SparseEncoder:
    """
    하이브리드 검색용 sparse 벡터 생성기 (BM25 기반).
    Qdrant는 dense(CLIP) + sparse(키워드) 벡터를 한 포인트에 같이 저장해
    RRF(Reciprocal Rank Fusion)로 재순위화하는 하이브리드 검색을 지원.
    """

    def __init__(self):
        from fastembed import SparseTextEmbedding

        # Qdrant 팀이 제공하는 경량 BM25 스타일 sparse 임베딩 모델
        self.model = SparseTextEmbedding(model_name="Qdrant/bm25")

    def encode(self, texts: List[str]):
        """
        반환: list of (indices, values) 튜플. Qdrant SparseVector 형식과 호환.
        """
        results = list(self.model.embed(texts))
        return [(r.indices.tolist(), r.values.tolist()) for r in results]