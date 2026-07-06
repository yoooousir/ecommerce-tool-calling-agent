"""
collect_naver_shopping.py
===========================
네이버 쇼핑 검색 오픈API를 이용해 상품 데이터를 수집하고,
로컬 SQLite 데이터베이스에 적재하는 운영(production) 버전 스크립트.

[테스트 버전과의 차이점]
  - 키워드 5개 → 65개 전체 활성화 (패션/전자/가전/주방/뷰티 5개 카테고리 골고루)
  - save_to_db()의 실제 INSERT 로직 활성화 (테스트 버전은 콘솔 출력 + 미리보기 이미지 2장만 받음)
  - Airflow DAG에서 각 단계(crawl/load/download_images)를 개별 Task로 호출할 수 있도록
    main()의 로직을 재사용 가능한 함수로 분리

[사전 준비]
  1. https://developers.naver.com 에서 애플리케이션 등록 (사용 API: 검색)
  2. 발급받은 Client ID / Secret을 환경변수로 등록
       export NAVER_CLIENT_ID="발급받은 ID"
       export NAVER_CLIENT_SECRET="발급받은 SECRET"
  3. pip install requests --break-system-packages (Airflow 컨테이너에서는 requirements.txt로 설치)

[데이터 흐름]
  네이버 검색 API
       │  (키워드별로 페이지네이션 반복 호출)
       ▼
  fetch_page()       : 1회 호출 = 최대 100건 응답(JSON)
       │
       ▼
  collect_keyword()  : 한 키워드에 대해 목표 건수까지 페이지를 넘기며 수집
       │
       ▼
  dedup_by_link()    : 같은 키워드 내에서 중복 상품(link 기준) 1차 제거
       │
       ▼
  save_to_db()        : SQLite products 테이블에 INSERT OR IGNORE로 적재 (2차 중복 방지)
       │                 + build_pseudo_description()으로 임베딩용 설명문 같이 생성
       ▼
  (선택) download_images() : DB에 저장된 image_url을 실제 파일로 다운로드 (CLIP 임베딩용)
       │
       ▼
  export_csv()        : 최종 적재 결과를 CSV로도 내보내 검수 가능하게 함
"""

import os
import time
import json
import csv
import sqlite3
import requests
from pathlib import Path
from urllib.parse import quote
from datetime import datetime

# ----------------------------------------------------------------------
# 환경 설정값
# ----------------------------------------------------------------------

# 네이버 개발자센터에서 발급받은 인증 정보. 코드에 직접 하드코딩하지 않고
# 환경변수로 주입받아, 실수로 GitHub 등에 키가 노출되는 사고를 방지한다.
CLIENT_ID = os.environ.get("NAVER_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")

# 네이버 쇼핑 검색 오픈API 엔드포인트 (공식 허용 경로, 비로그인 오픈 API)
API_URL = "https://openapi.naver.com/v1/search/shop.json"

# 데이터 저장 경로. 컨테이너(Airflow) 환경에서도 동일하게 동작하도록
# 상대경로 대신 환경변수로 베이스 경로를 오버라이드할 수 있게 함.
# (Airflow DAG에서는 DAG가 마운트한 볼륨 경로를 NAVER_DATA_DIR로 지정해서 사용)
OUTPUT_DIR = Path(os.environ.get("NAVER_DATA_DIR", "./naver_shopping_data"))
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
IMAGE_DIR = OUTPUT_DIR / "images"
IMAGE_DIR.mkdir(exist_ok=True, parents=True)

DB_PATH = OUTPUT_DIR / "products.db"

# ----------------------------------------------------------------------
# 수집 대상 키워드 (전체 65개 활성화)
# 패션/잡화(15) + 전자기기(10) + 가전(9) + 주방용품(17) + 뷰티(12) = 63개
#   ※ 처음 5개(운동화/후드티/청바지/패딩/원피스)는 테스트 단계에서부터 쓰던 키워드라 중복 포함
# 카테고리를 다양하게 섞어야, 멀티모달 검색/추천 데모 시 카테고리 편중 없이 보여줄 수 있음
# ----------------------------------------------------------------------
KEYWORDS = [
    # 패션/액세서리
    "운동화", "후드티", "청바지", "패딩", "원피스",
    "가방", "지갑", "벨트", "모자", "선글라스", "시계", "반지", "목걸이", "귀걸이", "팔찌",
    # 전자기기
    "노트북", "무선마우스", "기계식키보드", "모니터", "이어폰",
    "스마트워치", "게이밍의자", "캠코더", "드론", "블루투스스피커",
    # 가전
    "냉장고", "세탁기", "청소기", "로봇청소기", "제습기", "가습기", "에어컨", "TV", "식기세척기",
    # 주방용품
    "전기밥솥", "전기오븐", "전기주전자", "커피머신", "믹서기", "에어프라이어", "전자레인지",
    "수저세트", "주방칼", "도마", "냄비", "프라이팬", "와인잔", "컵", "접시", "그릇", "텀블러",
    # 뷰티
    "샴푸", "바디워시", "선크림", "마스크팩", "향수", "립스틱", "아이섀도우",
    "파운데이션", "마스카라", "헤어드라이기", "고데기", "네일아트",
]

# 네이버 쇼핑 검색 API의 호출 제약사항
DISPLAY_PER_CALL = 100        # 1회 호출당 최대 응답 건수 (API 정책상 최댓값)
MAX_START_PER_KEYWORD = 1000  # 한 키워드로 조회 가능한 최대 순위 범위 (그 이상은 API가 거부)
SLEEP_SEC = 0.15              # 호출 간 딜레이(초). 과도한 연속 호출로 인한 차단/부하를 방지

# 키워드당 목표 수집 건수는 기존 설계를 그대로 유지
# (전체 목표 10,000건을 키워드 개수로 나눈 값, 최소 100건은 보장)
TARGET_TOTAL = 10000


# ----------------------------------------------------------------------
# 1. API 호출 함수
# ----------------------------------------------------------------------

def fetch_page(keyword: str, start: int, display: int = DISPLAY_PER_CALL) -> dict:
    """
    네이버 쇼핑 검색 API에 단일 HTTP 요청을 보내고 JSON 응답을 반환한다.

    Args:
        keyword: 검색어 (예: "운동화")
        start: 검색 결과 중 몇 번째부터 가져올지 (1부터 시작, 페이지네이션용)
        display: 이번 호출에서 몇 건을 요청할지 (최대 100)

    Returns:
        성공 시 네이버 API의 JSON 응답을 dict로 반환.
        실패(인증오류/한도초과 등) 시 경고를 출력하고 빈 dict를 반환해
        호출부에서 안전하게 처리할 수 있도록 함.
    """
    headers = {
        "X-Naver-Client-Id": CLIENT_ID,
        "X-Naver-Client-Secret": CLIENT_SECRET,
    }
    params = {
        "query": keyword,   # 검색할 단어
        "display": display, # 한 번 호출에 몇 개 받을지 (API 최대 허용치 100)
        "start": start,     # 몇 번째부터 가져올지 (1, 101, 201 ... 식으로 display만큼 건너뛰며 호출)
        "sort": "sim",      # 정확도순 정렬 (date=최신순, asc/dsc=가격순 도 가능)
    }
    resp = requests.get(API_URL, headers=headers, params=params, timeout=10)
    if resp.status_code != 200:
        print(f"  [WARN] status={resp.status_code} keyword={keyword} start={start} body={resp.text[:200]}")
        return {}
    return resp.json()


def collect_keyword(keyword: str, target_count: int = 500) -> list[dict]:
    """
    한 키워드에 대해 target_count(또는 API가 허용하는 최대 범위인 1000건)까지
    fetch_page()를 반복 호출하며 결과를 누적 수집한다.

    Args:
        keyword: 검색어
        target_count: 이 키워드에서 목표로 하는 수집 건수

    Returns:
        수집된 상품 딕셔너리들의 리스트 (중복 제거는 아직 안 된 상태)
    """
    items = []  # 수집된 상품 데이터를 누적할 리스트
    start = 1   # 네이버 API는 1번째 결과부터 시작 (0이 아님)

    # 목표 건수를 채우거나, API가 허용하는 범위(1000)를 넘을 때까지 반복
    while len(items) < target_count and start <= MAX_START_PER_KEYWORD:
        remaining = target_count - len(items)          # 아직 몇 건 더 필요한지 계산
        display = min(DISPLAY_PER_CALL, remaining)      # 이번 호출 요청 건수 (남은 필요량과 API 최대치 중 작은 값)

        data = fetch_page(keyword, start, display)      # 실제 API 호출
        page_items = data.get("items", [])              # 응답에서 상품 리스트만 추출 (에러 시 빈 리스트)

        if not page_items:
            # 더 이상 결과가 없거나(검색결과 소진) API 에러였던 경우 → 반복 중단
            break

        items.extend(page_items)  # 이번 페이지 결과를 누적
        start += display          # 다음 호출은 이번에 받은 만큼 건너뛴 지점부터 시작
        time.sleep(SLEEP_SEC)     # 과도한 연속 호출 방지용 딜레이

    print(f"  [{keyword}] {len(items)}건 수집")
    return items


# ----------------------------------------------------------------------
# 2. 중복 제거
# ----------------------------------------------------------------------

def dedup_by_link(all_items: list[dict]) -> list[dict]:
    """
    같은 키워드 내에서 여러 페이지에 걸쳐 동일 상품이 중복 수집된 경우,
    link(상품 URL)를 기준으로 한 번씩만 남기고 걸러낸다.

    (참고) 키워드 간 중복(예: "운동화"와 "신발" 검색결과가 겹치는 경우)은
    여기서 못 잡고, DB의 link UNIQUE 제약 + INSERT OR IGNORE가 2차로 막아준다.
    """
    seen = set()      # 지금까지 본 link를 기록 (집합은 조회 속도가 O(1)로 빠름)
    deduped = []       # 중복 제거된 결과만 담을 리스트
    for it in all_items:
        link = it.get("link")
        if link and link not in seen:
            seen.add(link)
            deduped.append(it)
    return deduped


# ----------------------------------------------------------------------
# 3. SQLite 스키마 정의 및 초기화
# ----------------------------------------------------------------------

def init_db() -> sqlite3.Connection:
    """
    SQLite 데이터베이스 파일(DB_PATH)에 연결하고, products 테이블이 없으면 생성한다.
    이미 테이블이 있으면(재실행 시) 그대로 두고 연결만 반환 (IF NOT EXISTS).

    [스키마 설명]
      id               : 상품 고유 ID (자동 증가, PRIMARY KEY)
      title            : 상품명 (HTML 태그 제거된 순수 텍스트)
      link             : 상품 상세 페이지 URL (UNIQUE 제약으로 중복 적재 방지)
      image_url        : 네이버가 제공하는 원본 상품 이미지 URL
      local_image_path : 위 image_url을 실제로 다운로드해 저장한 로컬 파일 경로
                          (download_images() 실행 전까지는 NULL)
      description      : 텍스트 임베딩(벡터DB)용 의사 설명문.
                          네이버 API가 상세설명을 제공하지 않아, title/brand/maker/
                          category1~4/mallName을 조합해 build_pseudo_description()으로 생성
      lprice / hprice  : 최저가 / 최고가 (정수, 원화 기준)
      mall_name        : 판매 쇼핑몰명
      maker / brand    : 제조사 / 브랜드
      category1~4      : 네이버가 분류한 카테고리 (대분류→소분류 순)
      search_keyword   : 이 상품을 수집할 때 사용한 검색어 (수집 의도 추적용)
      collected_at     : 수집 시각 (ISO 8601 문자열)
    """
    conn = sqlite3.connect(DB_PATH)
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
    conn.commit()
    return conn


# ----------------------------------------------------------------------
# 4. 텍스트 정제 및 의사 설명문 생성
# ----------------------------------------------------------------------

def strip_html_tags(text: str) -> str:
    """네이버 API 응답의 title 필드에는 검색어 강조용 <b> 태그가 섞여 있어 제거한다."""
    return text.replace("<b>", "").replace("</b>", "")


def build_pseudo_description(item: dict) -> str:
    """
    네이버 쇼핑 검색 API는 상품 상세설명을 제공하지 않으므로,
    응답에 포함된 메타데이터(제목/브랜드/제조사/카테고리/판매처)를 조합해
    텍스트 임베딩(벡터 검색)용 의사 설명문을 생성한다.

    추후 필요 시 이 의사 설명문을 LLM에 입력해 더 자연스러운 한 문장으로
    다듬는 단계(증강)를 추가할 수 있도록 설계되어 있다.

    Args:
        item: 네이버 API가 반환한 상품 1건의 딕셔너리

    Returns:
        중복/빈 값이 제거된 공백 구분 텍스트 (예: "나이키 에어맥스 운동화 나이키 패션잡화 남성신발")
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
    # brand와 maker가 같은 값인 경우, 카테고리가 비어있는 경우 등을 정리
    seen = set()
    deduped_parts = []
    for p in parts:
        p = (p or "").strip()
        if p and p not in seen:
            seen.add(p)
            deduped_parts.append(p)
    return " ".join(deduped_parts)


# ----------------------------------------------------------------------
# 5. 이미지 다운로드 (CLIP 임베딩 준비용)
# ----------------------------------------------------------------------

def _download_single_image(url: str, filename_stem: str) -> str | None:
    """
    단일 이미지 URL을 다운로드하여 IMAGE_DIR에 저장하는 내부 헬퍼 함수.
    download_images()(DB 기반 일괄 다운로드)에서 공통으로 사용한다.

    Args:
        url: 다운로드할 이미지 URL
        filename_stem: 저장할 파일명(확장자 제외). 보통 상품 id를 사용

    Returns:
        성공 시 저장된 파일의 로컬 경로 문자열, 실패 시 None
    """
    if not url:
        return None
    headers = {"User-Agent": "Mozilla/5.0"}  # 일부 이미지 서버의 단순 차단을 피하기 위한 UA 헤더
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
        # URL 끝부분에서 확장자를 추출하되, 알 수 없는 형식이면 jpg로 기본 처리
        ext = url.split(".")[-1].split("?")[0][:4]
        if ext not in ("jpg", "jpeg", "png", "webp"):
            ext = "jpg"
        fname = IMAGE_DIR / f"{filename_stem}.{ext}"
        fname.write_bytes(r.content)
        return str(fname)
    except Exception as e:
        print(f"  [IMG ERROR] {url} -> {e}")
        return None


def download_images(conn: sqlite3.Connection, limit: int | None = None) -> None:
    """
    DB에 적재되어 있으나 아직 로컬 이미지가 없는(local_image_path IS NULL) 상품들의
    image_url을 실제로 다운로드해 IMAGE_DIR에 저장하고, 그 경로를 DB에 업데이트한다.

    1만 건 전체를 다운로드하면 시간이 오래 걸리므로, limit으로 일부만 먼저
    테스트하거나, Airflow에서 별도 Task로 분리해 비동기적으로 돌리는 것을 권장한다.

    Args:
        conn: SQLite 연결 객체
        limit: 다운로드할 최대 건수 (None이면 전체)
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
        time.sleep(0.05)  # 이미지 서버 부하 방지용 짧은 딜레이
    conn.commit()
    print(f"이미지 다운로드 완료: 성공 {success}건, 실패 {fail}건")


# ----------------------------------------------------------------------
# 6. DB 적재 (운영 버전 — 실제 INSERT 활성화)
# ----------------------------------------------------------------------

def save_to_db(conn: sqlite3.Connection, items: list[dict], keyword: str) -> None:
    """
    수집된 상품 리스트를 SQLite products 테이블에 실제로 적재한다.
    (테스트 버전에서는 이 INSERT 로직이 주석 처리되어 콘솔 출력만 했지만,
     운영 버전에서는 실제 적재가 활성화되어 있다.)

    Args:
        conn: SQLite 연결 객체
        items: collect_keyword() + dedup_by_link()를 거친 상품 리스트
        keyword: 이 상품들을 수집할 때 사용한 검색어 (search_keyword 컬럼에 기록)
    """
    now = datetime.now().isoformat()  # 이 배치 전체에 동일한 수집 시각을 기록
    inserted, skipped, errored = 0, 0, 0

    for it in items:
        title = strip_html_tags(it.get("title", ""))
        description = build_pseudo_description(it)
        try:
            cur = conn.execute("""
                INSERT OR IGNORE INTO products
                (title, link, image_url, description, lprice, hprice, mall_name, maker, brand,
                 category1, category2, category3, category4, search_keyword, collected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                title,
                it.get("link", ""),
                it.get("image", ""),
                description,
                # lprice/hprice가 빈 문자열이나 None으로 올 수 있어 int 변환 전 0으로 기본 처리
                int(it.get("lprice") or 0),
                int(it.get("hprice") or 0),
                it.get("mallName", ""),
                it.get("maker", ""),
                it.get("brand", ""),
                it.get("category1", ""),
                it.get("category2", ""),
                it.get("category3", ""),
                it.get("category4", ""),
                keyword,
                now,
            ))
            # INSERT OR IGNORE는 중복(link UNIQUE 충돌) 시 조용히 무시하므로,
            # rowcount로 실제 삽입 여부를 구분해 통계를 남긴다.
            if cur.rowcount == 0:
                skipped += 1
            else:
                inserted += 1
        except Exception as e:
            errored += 1
            print(f"  [DB ERROR] {e}")

    conn.commit()  # 키워드 단위로 한 번에 commit (매 INSERT마다 commit하면 느려짐)
    print(f"  [{keyword}] DB 적재 결과 — 신규 {inserted}건 / 중복스킵 {skipped}건 / 에러 {errored}건")


# ----------------------------------------------------------------------
# 7. CSV 내보내기 (검수용)
# ----------------------------------------------------------------------

def export_csv(conn: sqlite3.Connection, path: Path | None = None) -> None:
    """products 테이블 전체를 CSV로 내보내, 엑셀 등에서 빠르게 검수할 수 있게 한다."""
    path = path or (OUTPUT_DIR / "products.csv")
    cur = conn.execute("SELECT * FROM products")
    cols = [d[0] for d in cur.description]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:  # utf-8-sig: 엑셀 한글 깨짐 방지
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(cur.fetchall())
    print(f"CSV 내보내기 완료: {path}")


# ----------------------------------------------------------------------
# 8. Airflow에서 재사용할 수 있는 단계별 함수
#    (각 함수가 하나의 Airflow Task에 1:1로 대응되도록 설계)
# ----------------------------------------------------------------------

def run_crawl_and_load(keywords: list[str] | None = None, target_total: int = TARGET_TOTAL) -> int:
    """
    [Airflow Task 1: crawl_and_load 에 대응]
    전체 키워드를 순회하며 수집 + 중복제거 + DB 적재까지 한 번에 수행한다.

    Args:
        keywords: 수집할 키워드 리스트 (None이면 기본 KEYWORDS 전체 사용)
        target_total: 전체 목표 수집 건수

    Returns:
        최종 DB에 누적된 상품 건수 (중복 제거 후)
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit(
            "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수가 설정되지 않았습니다.\n"
            "  export NAVER_CLIENT_ID='발급받은 ID'\n"
            "  export NAVER_CLIENT_SECRET='발급받은 SECRET'"
        )

    keywords = keywords or KEYWORDS
    conn = init_db()

    # 키워드당 목표 건수 = 전체 목표를 키워드 수로 나눈 값 (최소 100건 보장)
    per_keyword_target = max(100, target_total // len(keywords))
    print(f"키워드 {len(keywords)}개, 키워드당 목표 {per_keyword_target}건 (총 목표 {target_total}건)")

    all_collected = 0
    for kw in keywords:
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

    export_csv(conn)
    conn.close()
    return total_in_db


def run_download_images(limit: int | None = None) -> None:
    """
    [Airflow Task 2: download_images 에 대응]
    DB에 적재된 상품들의 이미지를 실제로 다운로드한다.
    수집(run_crawl_and_load) Task가 끝난 뒤 별도 Task로 분리 실행하는 것을 권장
    (이미지 다운로드는 시간이 오래 걸리므로 크롤링/적재와 단계를 분리해
     실패 시 재시도 범위를 좁히고, 두 작업의 소요시간을 명확히 구분하기 위함)
    """
    conn = init_db()
    download_images(conn, limit=limit)
    conn.close()


# ----------------------------------------------------------------------
# 메인 진입점 (단독 스크립트로 실행할 때)
# ----------------------------------------------------------------------

def main():
    run_crawl_and_load()
    run_download_images()


if __name__ == "__main__":
    main()
