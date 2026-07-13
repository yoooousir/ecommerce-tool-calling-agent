"""
collect_naver_shopping.py
===========================
네이버 쇼핑 검색 오픈API를 이용해 상품 데이터를 수집하고,
로컬 SQLite + AWS S3(parquet)에 적재하는 운영(production) 버전 스크립트.

[v3 변경사항 — image_url 실제값 채우기]
  - 문제: v2에서는 image_url을 SQLite에 저장하지 않아서, upload_to_s3() 시점에
    복구할 방법이 없어 항상 빈 문자열("") placeholder만 parquet에 들어갔음.
  - 해결: products_media(id, image_url) 사이드 테이블을 새로 추가.
    - products 테이블(정형 필터링 데이터)의 스키마/역할은 그대로 유지 (v2 설계 의도 보존)
    - save_to_db()가 신규 삽입 시 image_url을 products_media에 같이 저장
    - 중복이라 스킵된 기존 상품(link 기준)도 products_media에 값이 없으면 backfill
    - upload_to_s3()는 이제 products_media를 join해서 실제 image_url을 parquet에 기록

[v2에서 이어지는 설계]
  - SQLite: 조건 검색(가격/카테고리 필터링)에 쓰이는 정형 데이터만 담당 (products 테이블)
  - S3 parquet: 텍스트 임베딩용 description + 이미지 URL처럼 크기가 크거나
    벡터DB 파이프라인에서 직접 읽을 데이터를 분리해 저장

[데이터 흐름]
  네이버 검색 API
       ↓
  collect_keyword() → dedup_by_link() → save_to_db() [SQLite: products + products_media]
                                      → upload_to_s3() [S3 parquet: id/description/image_url]
"""

import os
import io
import time
import csv
import sqlite3
import requests
import pandas as pd
import boto3
from pathlib import Path
from datetime import datetime

# ----------------------------------------------------------------------
# 환경 설정값
# ----------------------------------------------------------------------

CLIENT_ID     = os.environ.get("NAVER_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")
API_URL       = "https://openapi.naver.com/v1/search/shop.json"

# SQLite 저장 경로
OUTPUT_DIR = Path(os.environ.get("NAVER_DATA_DIR", "./naver_shopping_data"))
DB_PATH    = OUTPUT_DIR / "products.db"

# S3 설정 — 버킷 이름은 환경변수로 주입 (코드에 하드코딩하지 않음)
S3_BUCKET     = os.environ.get("S3_BUCKET_NAME", "")
S3_KEY_PREFIX = os.environ.get("S3_KEY_PREFIX", "naver_shopping")
# 최종 S3 경로: s3://{버킷}/naver_shopping/2026/07/02/products.parquet
# 년/월/일 파티션 구조로 저장해 날짜별 데이터 관리 및 조회가 용이

# 수집 파라미터
DISPLAY_PER_CALL      = 100
MAX_START_PER_KEYWORD = 1000
SLEEP_SEC             = 0.15
TARGET_TOTAL          = 10000

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


# ----------------------------------------------------------------------
# 1. API 호출
# ----------------------------------------------------------------------

def fetch_page(keyword: str, start: int, display: int = DISPLAY_PER_CALL) -> dict:
    """
    네이버 쇼핑 검색 API 단일 호출.
    start 파라미터로 페이지네이션 (1, 101, 201 ... 식으로 display만큼 증가).
    실패 시 빈 dict 반환 → 호출부에서 안전하게 처리.
    """
    headers = {
        "X-Naver-Client-Id": CLIENT_ID,
        "X-Naver-Client-Secret": CLIENT_SECRET,
    }
    params = {
        "query": keyword,
        "display": display,
        "start": start,
        "sort": "sim",  # 정확도순
    }
    resp = requests.get(API_URL, headers=headers, params=params, timeout=10)
    if resp.status_code != 200:
        print(f"  [WARN] status={resp.status_code} keyword={keyword} start={start}")
        return {}
    return resp.json()


def collect_keyword(keyword: str, target_count: int = 500) -> list[dict]:
    """
    한 키워드에 대해 target_count(또는 API 최대 1000건)까지
    fetch_page()를 반복 호출해 결과를 누적 수집.
    """
    items = []
    start = 1
    while len(items) < target_count and start <= MAX_START_PER_KEYWORD:
        remaining = target_count - len(items)
        display   = min(DISPLAY_PER_CALL, remaining)
        data       = fetch_page(keyword, start, display)
        page_items = data.get("items", [])
        if not page_items:
            break
        items.extend(page_items)
        start += display
        time.sleep(SLEEP_SEC)
    print(f"  [{keyword}] {len(items)}건 수집")
    return items


# ----------------------------------------------------------------------
# 2. 중복 제거
# ----------------------------------------------------------------------

def dedup_by_link(all_items: list[dict]) -> list[dict]:
    """
    동일 키워드 내에서 link(상품 URL) 기준으로 중복 제거.
    키워드 간 중복은 DB의 link UNIQUE 제약이 2차로 차단.
    """
    seen, deduped = set(), []
    for it in all_items:
        link = it.get("link")
        if link and link not in seen:
            seen.add(link)
            deduped.append(it)
    return deduped


# ----------------------------------------------------------------------
# 3. SQLite 스키마
#    - products: 정형 필터링 데이터 (v2 설계 그대로 유지)
#    - products_media: image_url만 담는 사이드 테이블 (v3 신규)
# ----------------------------------------------------------------------

def init_db() -> sqlite3.Connection:
    """
    SQLite 연결 및 테이블 생성 (없을 때만).

    products 테이블 (v2와 동일, 변경 없음):
      id, title, link, lprice, hprice, mall_name, maker, brand,
      category1~4, search_keyword, collected_at

    products_media 테이블 (v3 신규):
      id           — products.id를 그대로 참조 (FK 역할, 별도 제약은 걸지 않음)
      image_url    — 네이버 API 응답의 image 필드 원본 URL
      이 테이블은 dbt source로 노출하지 않고, upload_to_s3()에서만 내부적으로 join해 사용.
      → products 테이블은 여전히 "RDBMS 조건 검색용 정형 데이터만" 담는다는 v2 설계 유지.
    """
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            title            TEXT,
            link             TEXT UNIQUE,
            lprice           INTEGER,
            hprice           INTEGER,
            mall_name        TEXT,
            maker            TEXT,
            brand            TEXT,
            category1        TEXT,
            category2        TEXT,
            category3        TEXT,
            category4        TEXT,
            search_keyword   TEXT,
            collected_at     TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products_media (
            id          INTEGER PRIMARY KEY,
            image_url   TEXT
        )
    """)
    conn.commit()
    return conn


# ----------------------------------------------------------------------
# 4. 텍스트 정제 및 의사 설명문 생성 (S3 parquet 전용)
# ----------------------------------------------------------------------

def strip_html_tags(text: str) -> str:
    """네이버 API title 필드의 <b> 태그 제거."""
    return text.replace("<b>", "").replace("</b>", "")


def build_pseudo_description(item: dict) -> str:
    """
    title + brand + maker + category1~4 + mallName 조합으로
    텍스트 임베딩(벡터DB)용 의사 설명문 생성.
    SQLite에는 저장하지 않고 S3 parquet에만 포함된다.
    (네이버 쇼핑 검색 API는 장문 description을 제공하지 않으므로 의사 설명문으로 대체)
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
    seen, deduped_parts = set(), []
    for p in parts:
        p = (p or "").strip()
        if p and p not in seen:
            seen.add(p)
            deduped_parts.append(p)
    return " ".join(deduped_parts)


# ----------------------------------------------------------------------
# 5. SQLite 적재 (정형 데이터 + image_url 사이드 테이블)
# ----------------------------------------------------------------------

def save_to_db(conn: sqlite3.Connection, items: list[dict], keyword: str) -> None:
    """
    수집된 상품을 SQLite에 적재.

    [v3 변경]
      - 신규 삽입된 상품은 products_media에 image_url도 함께 저장.
      - link 중복으로 스킵된 기존 상품은, products_media에 image_url이
        아직 없는 경우에만 backfill (덮어쓰지는 않음 — INSERT OR IGNORE).
        이렇게 하면 v3 배포 이전에 수집돼서 media row가 없는 기존 데이터도
        같은 상품이 재수집(중복 링크로 재수신)될 때 자동으로 채워짐.
    """
    now = datetime.now().isoformat()
    inserted, skipped, errored = 0, 0, 0

    for it in items:
        title = strip_html_tags(it.get("title", ""))
        # 네이버 쇼핑 검색 API의 상품 이미지 URL 필드는 "image"
        image_url = it.get("image", "")
        link = it.get("link", "")

        try:
            cur = conn.execute("""
                INSERT OR IGNORE INTO products
                (title, link, lprice, hprice, mall_name, maker, brand,
                 category1, category2, category3, category4, search_keyword, collected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                title,
                link,
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

            if cur.rowcount == 0:
                # link UNIQUE 제약으로 스킵된 경우 → 기존 상품의 id를 찾아
                # products_media가 비어있으면 채워줌 (덮어쓰지 않음)
                skipped += 1
                existing = conn.execute(
                    "SELECT id FROM products WHERE link = ?", (link,)
                ).fetchone()
                if existing and image_url:
                    conn.execute(
                        "INSERT OR IGNORE INTO products_media (id, image_url) VALUES (?, ?)",
                        (existing[0], image_url),
                    )
            else:
                inserted += 1
                new_id = cur.lastrowid
                conn.execute(
                    "INSERT OR IGNORE INTO products_media (id, image_url) VALUES (?, ?)",
                    (new_id, image_url),
                )
        except Exception as e:
            errored += 1
            print(f"  [DB ERROR] {e}")

    conn.commit()
    print(f"  [{keyword}] DB 적재 — 신규 {inserted}건 / 중복스킵 {skipped}건 / 에러 {errored}건")


# ----------------------------------------------------------------------
# 6. S3 parquet 업로드 (id + description + image_url)
# ----------------------------------------------------------------------

def upload_to_s3(conn: sqlite3.Connection) -> str:
    """
    [Airflow Task 2: upload_to_s3 에 대응]

    products와 products_media를 id 기준으로 join해서
    description(재조합) + image_url(실제 값)을 parquet으로 S3에 업로드.

    [v3 변경]
      기존에는 image_url을 항상 빈 문자열("")로 채우는 placeholder였으나,
      products_media 테이블에서 실제 저장된 image_url을 읽어와 채운다.
      products_media에 값이 없는 경우(과거 데이터 등)에는 빈 문자열로 남는다.

    Returns:
        업로드된 S3 경로 (s3://버킷명/prefix/파일명)
    """
    if not S3_BUCKET:
        raise ValueError(
            "S3_BUCKET_NAME 환경변수가 설정되지 않았습니다.\n"
            "  export S3_BUCKET_NAME='버킷이름'"
        )

    # products + products_media(image_url) join
    cur = conn.execute("""
        SELECT
            p.id, p.title, p.mall_name, p.maker, p.brand,
            p.category1, p.category2, p.category3, p.category4,
            COALESCE(m.image_url, '') AS image_url
        FROM products p
        LEFT JOIN products_media m ON p.id = m.id
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    df_meta = pd.DataFrame(rows, columns=cols)

    missing_image = (df_meta["image_url"].fillna("").str.strip() == "").sum()
    if missing_image:
        print(f"  [INFO] image_url이 비어있는 상품 {missing_image}/{len(df_meta)}건 "
              f"(v3 배포 이전 수집 데이터일 가능성)")

    # description 재조합 (네이버 API에 장문 description이 없어 의사 설명문 생성)
    # def _rebuild_description(row) -> str:
    #     parts = [
    #         row["title"], row["brand"], row["maker"],
    #         row["category1"], row["category2"], row["category3"], row["category4"],
    #         row["mall_name"],
    #     ]
    #     seen, result = set(), []
    #     for p in parts:
    #         p = (p or "").strip()
    #         if p and p not in seen:
    #             seen.add(p)
    #             result.append(p)
    #     return " ".join(result)
    def _rebuild_description(row) -> str:
        parts = [
            row.get("title", ""), row.get("brand", ""), row.get("maker", ""),
            row.get("category1", ""), row.get("category2", ""),
            row.get("category3", ""), row.get("category4", ""),
            row.get("mall_name", ""),
        ]
        seen, result = set(), []
        for p in parts:
            # 어떤 이유로든 p가 순수 문자열이 아닌 경우(중복 컬럼으로 인한 Series, None, NaN 등)
            # 방어적으로 문자열로 강제 변환
            if not isinstance(p, str):
                p = "" if p is None or (isinstance(p, float) and pd.isna(p)) else str(p)
            p = p.strip()
            if p and p not in seen:
                seen.add(p)
                result.append(p)
        return " ".join(result)

    df_meta["description"] = df_meta.apply(_rebuild_description, axis=1)

    # S3에 올릴 parquet은 id / description / image_url 3개 컬럼
    df_s3 = df_meta[["id", "description", "image_url"]].copy()

    # 년/월/일 파티션 구조로 S3 키 생성
    # 예: naver_shopping/2026/07/02/products.parquet
    now_dt = datetime.now()
    s3_key = (
        f"{S3_KEY_PREFIX}/"
        f"{now_dt.year}/"
        f"{now_dt.month:02d}/"
        f"{now_dt.day:02d}/"
        f"products.parquet"
    )

    # 메모리 버퍼에 parquet 쓰기 → S3에 직접 업로드 (로컬 파일 저장 없음)
    buffer = io.BytesIO()
    df_s3.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3_client = boto3.client("s3")
    s3_client.upload_fileobj(buffer, S3_BUCKET, s3_key)

    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    print(f"S3 업로드 완료: {s3_path} ({len(df_s3)}건, image_url 채움 {len(df_s3) - missing_image}건)")
    return s3_path


# ----------------------------------------------------------------------
# 7. CSV 내보내기 (검수용)
# ----------------------------------------------------------------------

def export_csv(conn: sqlite3.Connection, path: Path | None = None) -> None:
    """products 테이블 전체를 CSV로 내보내 빠르게 검수."""
    path = path or (OUTPUT_DIR / "products.csv")
    cur  = conn.execute("SELECT * FROM products")
    cols = [d[0] for d in cur.description]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(cur.fetchall())
    print(f"CSV 내보내기 완료: {path}")


# ----------------------------------------------------------------------
# 8. 이미지 다운로드 (코드 유지, DAG Task에서는 제거됨)
# ----------------------------------------------------------------------

def _download_single_image(url: str, filename_stem: str) -> str | None:
    """단일 이미지 URL 다운로드 헬퍼. DAG Task에서는 사용하지 않음."""
    if not url:
        return None
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code != 200:
            return None
        ext = url.split(".")[-1].split("?")[0][:4]
        if ext not in ("jpg", "jpeg", "png", "webp"):
            ext = "jpg"
        image_dir = OUTPUT_DIR / "images"
        image_dir.mkdir(exist_ok=True, parents=True)
        fname = image_dir / f"{filename_stem}.{ext}"
        fname.write_bytes(r.content)
        return str(fname)
    except Exception as e:
        print(f"  [IMG ERROR] {url} -> {e}")
        return None


def run_download_images(limit: int | None = None) -> None:
    """
    이미지 다운로드 함수 (코드 유지, DAG Task에서는 제거됨).
    필요 시 단독 스크립트로 직접 호출 가능.
    v3부터는 products_media 테이블에서 image_url을 읽어올 수 있음.
    """
    print("[INFO] download_images는 DAG Task에서 제거되었습니다.")
    print("[INFO] 필요 시 products_media 테이블에서 image_url을 읽어 별도 실행하세요.")


# ----------------------------------------------------------------------
# 9. Airflow Task 대응 함수
# ----------------------------------------------------------------------

def run_crawl_and_load(keywords: list[str] | None = None,
                       target_total: int = TARGET_TOTAL) -> int:
    """
    [Airflow Task 1: crawl_and_load]
    수집 → 중복제거 → SQLite 적재까지 수행.
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit(
            "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수가 설정되지 않았습니다."
        )

    keywords = keywords or KEYWORDS
    conn     = init_db()

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


def run_upload_to_s3() -> str:
    """
    [Airflow Task 2: upload_to_s3]
    products + products_media를 join해 description/image_url을 채운 뒤
    parquet으로 S3에 업로드.
    """
    conn   = init_db()
    result = upload_to_s3(conn)
    conn.close()
    return result


# ----------------------------------------------------------------------
# 메인 진입점
# ----------------------------------------------------------------------

def main():
    run_crawl_and_load()
    # S3 업로드는 Airflow Task로 분리 실행 권장.
    # 단독 실행이 필요하면 아래 주석 해제:
    # run_upload_to_s3()


if __name__ == "__main__":
    main()