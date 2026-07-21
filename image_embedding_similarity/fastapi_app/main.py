"""
로컬에서 실행하는 FastAPI 서버.
- EC2 위 Qdrant에 원격 접속 (QDRANT_HOST 환경변수)
- 사용자가 이미지 업로드 -> CLIP 임베딩 -> Qdrant에서 유사 이미지 검색 -> 결과 반환
- 실행: uvicorn main:app --reload --port 8000
"""
import io

from fastapi import FastAPI, File, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image

from embedder import ClipEmbedder
from qdrant_utils import ensure_collection, get_client, search_similar

app = FastAPI(title="네이버쇼핑 이미지 유사도 검색 API")

# 서버 시작 시 1회: Qdrant 클라이언트 + CLIP 모델 로드
qdrant_client = get_client()
ensure_collection(qdrant_client)
embedder = ClipEmbedder.get_instance()

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/search")
async def search_image(
    file: UploadFile = File(...),
    top_k: int = Query(default=5, ge=1, le=50),
):
    contents = await file.read()
    try:
        image = Image.open(io.BytesIO(contents))
    except Exception:
        return JSONResponse(status_code=400, content={"error": "이미지 파일을 읽을 수 없습니다."})

    vector = embedder.embed_image(image)
    results = search_similar(qdrant_client, vector, top_k=top_k)

    payload = [
        {
            "score": round(r.score, 4),
            "product_id": r.payload.get("product_id"),
            "title": r.payload.get("title"),
            "image_url": r.payload.get("image_url"),
        }
        for r in results
    ]
    return JSONResponse(content={"results": payload})
