"""
CLIP 기반 이미지 임베딩 모듈.
sentence-transformers의 clip-ViT-B-32 사용 (출력 차원 512).
FastAPI, Airflow 양쪽에서 동일하게 사용합니다 (임베딩 공간을 일치시키기 위함).
"""
from PIL import Image
from sentence_transformers import SentenceTransformer

MODEL_NAME = "clip-ViT-B-32"
VECTOR_SIZE = 512


class ClipEmbedder:
    _instance = None

    def __init__(self, model_name: str = MODEL_NAME):
        self.model = SentenceTransformer(model_name)

    @classmethod
    def get_instance(cls) -> "ClipEmbedder":
        # 프로세스당 한 번만 모델 로드 (재사용)
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def embed_image(self, image: Image.Image) -> list[float]:
        image = image.convert("RGB")
        vector = self.model.encode(
            image, convert_to_numpy=True, normalize_embeddings=True
        )
        return vector.tolist()

    def embed_image_path(self, path: str) -> list[float]:
        image = Image.open(path)
        return self.embed_image(image)
