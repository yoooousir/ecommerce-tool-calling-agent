import os
import time
import json
import csv
import sqlite3
import requests
from pathlib import Path
from urllib.parse import quote
from datetime import datetime

CLIENT_ID = os.environ.get("NAVER_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")
API_URL = "https://openapi.naver.com/v1/search/shop.json"

OUTPUT_DIR = Path("./naver_shopping_data")
OUTPUT_DIR.mkdir(exist_ok=True)
IMAGE_DIR = OUTPUT_DIR / "images"
IMAGE_DIR.mkdir(exist_ok=True)

DB_PATH = OUTPUT_DIR / "products.db"

# 목표 수량을 채우기 위한 검색 키워드 목록 (카테고리를 다양하게 섞어야 1만건을 다양하게 채울 수 있음)
# 필요에 맞게 자유롭게 수정/추가하세요. 키워드당 최대 1000건까지 수집 가능.
KEYWORDS = [
    "운동화", "후드티", "청바지", "패딩", "원피스",
    # "가방", "지갑", "벨트", "모자", "선글라스", "시계", "반지", "목걸이", "귀걸이", "팔찌",
    #"노트북", "무선마우스", "기계식키보드", "모니터", "이어폰", "스마트워치", "게이밍의자", "캠코더", "드론", "블루투스스피커",
    #"냉장고", "세탁기", "청소기", "로봇청소기", "제습기", "가습기", "에어컨", "TV", "식기세척기",
    #"전기밥솥", "전기오븐", "전기주전자", "커피머신", "믹서기", "에어프라이어", "전자레인지", "수저세트", "주방칼", "도마", "냄비", "프라이팬", "와인잔", "컵", "접시", "그릇", "텀블러",
    #"샴푸", "바디워시", "선크림", "마스크팩", "향수", "립스틱", "아이섀도우", "파운데이션", "마스카라", "헤어드라이기", "고데기", "네일아트",
]

DISPLAY_PER_CALL = 100   # API 최대 허용치
MAX_START_PER_KEYWORD = 1000  # 네이버 API 정책상 키워드당 조회 가능한 최대 범위
SLEEP_SEC = 0.15  # 호출 간 딜레이 (과도한 호출 방지)


def fetch_page(keyword: str, start: int, display: int = DISPLAY_PER_CALL) -> dict:
    headers = {
        "X-Naver-Client-Id": CLIENT_ID,
        "X-Naver-Client-Secret": CLIENT_SECRET,
    }
    params = {
        "query": keyword, # 검색할 단어
        "display": display, # 한번 호출에 몇개 받을지 (api 최대 허용치)
        "start": start, # 몇번째부터 가져올지(페이지네이션용 - 1, 101, 201 ... 등 100씩 건너뛰며 호출)
        "sort": "sim",  # 정확도순.
    }
    resp = requests.get(API_URL, headers=headers, params=params, timeout=10)
    if resp.status_code != 200:
        print(f"  [WARN] status={resp.status_code} keyword={keyword} start={start} body={resp.text[:200]}")
        return {}
    return resp.json()


def collect_keyword(keyword: str, target_count: int = 500) -> list[dict]:
    """한 키워드에 대해 target_count(또는 API 한계)까지 수집"""
    items = [] #수집될 상품 데이터를 담을 리스트
    start = 1

    # 목표 건수를 채울때까지 반복하는 while문
    while len(items) < target_count and start <= MAX_START_PER_KEYWORD:
        remaining = target_count - len(items) #몇건 더 필요한지 계산
        display = min(DISPLAY_PER_CALL, remaining) #이번 호출에서 몇 건 요청할지 정함 -> 남은 필요량과 api 최대치 중 작은 것 택하기
        
        # start=1,   display=100  →  1번째 ~ 100번째 결과
        data = fetch_page(keyword, start, display) # fetch_page 호출하여 그 페이지의 결과를 받음
        page_items = data.get("items", []) # 응답에서 items 키의 값(상품 리스트)을 가져옴
        
        if not page_items: # 더 이상 결과가 없거나 에러날 경우 break
            break

        items.extend(page_items) # 수집된 상품 리스트에 이번 페이지의 상품들을 추가
        start += display # 다음 페이지를 위해 start 값을 증가시킴 (100, 혹은 remaining 만큼 건너뛰며 호출)
        time.sleep(SLEEP_SEC) # 0.15초 딜레이

    print(f"  [{keyword}] {len(items)}건 수집")
    return items


# 같은 상품이 여러 번 중복으로 들어왔을 때 한번씩만 남기고 걸러내는 함수(중복제거)
def dedup_by_link(all_items: list[dict]) -> list[dict]:
    seen = set() # 지금까지 본 상품의 link(상품 url)를 저장할 set
    deduped = [] # 중복 제거된 결과만 담을 새 리스트
    for it in all_items: # 받은 상품 리스트(all_items)를 하나씩 순회
        link = it.get("link") # 각 상품의 link(상품 url)를 가져옴 -> 이 상품이 유일한지를 판단하는 기준
        if link and link not in seen: # link가 존재하고, 지금까지 본 적이 없는 link라면
            seen.add(link) # 이제 봤다고 표시
            deduped.append(it) # 중복 제거된 결과 리스트에 추가
    return deduped # 중복 제거된 상품 리스트 반환

# sqlite 데이터베이스 파일에 연결해서 상품 데이터 저장할 테이블(표) 구조 정의
def init_db():
    # DB_PATH : ./naver_shopping_data/products.db
    # DB_PATH 에 연결하여 sqlite3.Connection 객체를 반환
    conn = sqlite3.connect(DB_PATH)
    # products 라는 이름으로 테이블 생성
    '''
    스키마 정의
    id : 상품 고유 ID (자동 증가)
    title : 상품명
    link : 상품 상세 페이지 URL (중복 방지를 위해 UNIQUE)
    image_url : 상품 이미지 URL
    local_image_path : 로컬에 다운로드한 이미지 파일 경로
    description : 메타데이터 조합 기반 의사 설명문 (텍스트 임베딩용)
    lprice : 최저가
    hprice : 최고가
    mall_name : 판매처 이름
    maker : 제조사
    brand : 브랜드
    category1~4 : 카테고리 정보 (대분류~소분류: 예: 패션의류 > 여성의류 > 원피스 > 미니원피스)
    search_keyword : 수집 시 사용한 검색 키워드 (어느 카데고리 의도로 수집했는지 기록용)
    collected_at : 수집 시각 (ISO 8601 형식)
    '''
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            link TEXT UNIQUE,
            image_url TEXT,
            local_image_path TEXT,
            description TEXT,
            lprice INTEGER,
            hprice INTEGER,
            mall_name TEXT,
            maker TEXT,
            brand TEXT,
            category1 TEXT,
            category2 TEXT,
            category3 TEXT,
            category4 TEXT,
            search_keyword TEXT,
            collected_at TEXT
        )
    """)
    conn.commit() # 커밋하여 테이블 생성 완료
    return conn # sqlite3.Connection 객체 반환 -> 이후 DB에 상품 데이터를 저장하거나 할때 같은 연결을 계속 재사용


def strip_html_tags(text: str) -> str:
    # 네이버 응답의 title에는 <b> 태그가 포함되어 있어 제거 필요
    return text.replace("<b>", "").replace("</b>", "")

def build_pseudo_description(item: dict) -> str:
    """
    네이버 쇼핑 검색 API는 상품 상세설명을 제공하지 않으므로,
    응답에 있는 메타데이터(제목/브랜드/제조사/카테고리/판매처)를 조합해
    텍스트 임베딩용 의사 설명문을 생성
    추후 LLM으로 자연스러운 문장으로 다듬는 단계(3번)의 입력으로 업그레이드 가능.
    """
    title = strip_html_tags(item.get("title", ""))
    parts = [
        title,
        item.get("brand", ""),
        item.get("maker", ""),
        item.get("category1", ""),
        item.get("category2", ""),
        item.get("category3", ""),
        item.get("category4", ""),
        item.get("mallName", ""),
    ]
    # 중복 제거(예: brand와 maker가 같은 경우, 카테고리가 비어있는 경우) + 빈 값 제거
    seen = set()
    deduped_parts = []
    for p in parts:
        p = (p or "").strip()
        if p and p not in seen:
            seen.add(p)
            deduped_parts.append(p)
    return " ".join(deduped_parts)


def _download_single_image(url: str, filename_stem: str) -> str | None:
    """단일 이미지 URL을 다운로드해 IMAGE_DIR에 저장하고, 저장된 경로를 반환 (실패 시 None)"""
    if not url:
        return None
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
        ext = url.split(".")[-1].split("?")[0][:4]
        if ext not in ("jpg", "jpeg", "png", "webp"):
            ext = "jpg"
        fname = IMAGE_DIR / f"{filename_stem}.{ext}"
        fname.write_bytes(r.content)
        return str(fname)
    except Exception as e:
        print(f"  [IMG ERROR] {url} -> {e}")
        return None


def download_preview_images(row_preview: dict, stem_prefix: str, idx: int) -> str | None:
    """
    [테스트용] DB를 거치지 않고, save_to_db()의 미리보기 단계에서 바로
    image_url을 다운로드한다. 키워드별 preview_limit 건수만큼만 호출됨.
    파일명: {검색키워드}_{순번}.{확장자}  예) 운동화_0.jpg
    """
    stem = f"{stem_prefix}_{idx}"
    path = _download_single_image(row_preview.get("image_url", ""), stem)
    if path:
        print(f"  [IMG OK] {row_preview.get('title','')[:30]} -> {path}")
    else:
        print(f"  [IMG FAIL] {row_preview.get('title','')[:30]} (url={row_preview.get('image_url','')[:60]})")
    return path


def save_to_db(conn, items: list[dict], keyword: str, preview_limit: int = 2):
    # items: collect_keyword()에서 수집한 상품 리스트
    # keyword: 어떤 검색 키워드로 수집했는지
    # now: 현재 시각을 ISO 8601 형식으로 한번만 구해둠
    # preview_limit: 콘솔이 넘치지 않도록 키워드당 출력+이미지다운로드할 샘플 건수 (기본 2건)
    now = datetime.now().isoformat()
    for idx, it in enumerate(items):
        title = strip_html_tags(it.get("title", ""))
        description = build_pseudo_description(it)
        row_preview = {
            "title": title,
            "link": it.get("link", ""),
            "image_url": it.get("image", ""),
            "description": description,
            "lprice": int(it.get("lprice") or 0),
            "hprice": int(it.get("hprice") or 0),
            "mall_name": it.get("mallName", ""),
            "maker": it.get("maker", ""),
            "brand": it.get("brand", ""),
            "category1": it.get("category1", ""),
            "category2": it.get("category2", ""),
            "category3": it.get("category3", ""),
            "category4": it.get("category4", ""),
            "search_keyword": keyword,
            "collected_at": now,
        }
        if idx < preview_limit:
            print(json.dumps(row_preview, ensure_ascii=False, indent=2))
            download_preview_images(row_preview, stem_prefix=keyword, idx=idx)

        # ---- 아래는 실제 DB 적재 부분 (테스트를 위해 주석 처리) ----
        # try:
        #     conn.execute("""
        #         INSERT OR IGNORE INTO products
        #         (title, link, image_url, description, lprice, hprice, mall_name, maker, brand,
        #          category1, category2, category3, category4, search_keyword, collected_at)
        #         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        #     """, (
        #         title,
        #         it.get("link", ""),
        #         it.get("image", ""),
        #         description,
        #         int(it.get("lprice") or 0),
        #         int(it.get("hprice") or 0),
        #         it.get("mallName", ""),
        #         it.get("maker", ""),
        #         it.get("brand", ""),
        #         it.get("category1", ""),
        #         it.get("category2", ""),
        #         it.get("category3", ""),
        #         it.get("category4", ""),
        #         keyword,
        #         now,
        #     ))
        # except Exception as e:
        #     print(f"  [DB ERROR] {e}")
    # conn.commit()


def download_images(conn, limit: int | None = None):
    """DB에 저장된 image_url을 실제로 다운로드하여 로컬에 저장 (CLIP 임베딩용)
    네이버 상품 이미지 서버는 보통 Referer 체크가 느슨하지만, 막힐 경우 헤더 추가 필요
    """
    cur = conn.execute("SELECT id, image_url FROM products WHERE local_image_path IS NULL")
    rows = cur.fetchall()
    if limit:
        rows = rows[:limit]

    success, fail = 0, 0
    for pid, url in rows:
        path = _download_single_image(url, str(pid))
        if path:
            conn.execute("UPDATE products SET local_image_path=? WHERE id=?", (path, pid))
            success += 1
        else:
            fail += 1
        time.sleep(0.05)
    conn.commit()
    print(f"이미지 다운로드 완료: 성공 {success}건, 실패 {fail}건")


def export_csv(conn, path=None):
    path = path or (OUTPUT_DIR / "products.csv")
    cur = conn.execute("SELECT * FROM products")
    cols = [d[0] for d in cur.description]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(cur.fetchall())
    print(f"CSV 내보내기 완료: {path}")


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit(
            "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수가 설정되지 않았습니다.\n"
            "  export NAVER_CLIENT_ID='발급받은 ID'\n"
            "  export NAVER_CLIENT_SECRET='발급받은 SECRET'"
        )

    conn = init_db()

    target_total = 10000
    per_keyword_target = max(100, target_total // len(KEYWORDS))
    print(f"키워드 {len(KEYWORDS)}개, 키워드당 목표 {per_keyword_target}건 (총 목표 {target_total}건)")

    all_collected = 0
    for kw in KEYWORDS:
        items = collect_keyword(kw, target_count=per_keyword_target)
        items = dedup_by_link(items)
        save_to_db(conn, items, kw)
        all_collected += len(items)
        if all_collected >= target_total:
            print(f"목표 {target_total}건 도달, 수집 중단")
            break

    cur = conn.execute("SELECT COUNT(*) FROM products")
    total_in_db = cur.fetchone()[0]
    print(f"\nDB 누적 저장 건수(중복 제거 후): {total_in_db}")

    # 이미지 다운로드는 시간이 오래 걸리므로 테스트 때는 주석 처리
    # download_images(conn)

    export_csv(conn)
    conn.close()


if __name__ == "__main__":
    main()