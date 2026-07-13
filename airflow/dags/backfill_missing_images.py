"""
backfill_missing_images.py
============================================================
products_media에 row가 없는(image_url을 못 채운) 기존 상품들을
"제목으로 재검색 → link 정확히 일치하는 것 찾기" 방식으로 복구.

일반 키워드 재크롤링은 키워드당 상위 N개만 가져오기 때문에,
예전에 수집됐지만 지금은 랭킹이 밀린 상품은 다시 안 잡힘.
반면 상품 제목 자체로 검색하면 그 상품이 최상위에 뜰 확률이 높아 훨씬 효율적.

collect_naver_shopping_prod.py와 같은 폴더(보통 airflow/dags/)에 두고 실행.
"""

import argparse
import sqlite3
import time
from urllib.parse import urlparse

from collect_naver_shopping_prod import DB_PATH, SLEEP_SEC, fetch_page


def normalize_link(url: str) -> str:
    """
    쿼리스트링/트래킹 파라미터를 제거하고 scheme+host+path만 남겨 비교.
    네이버 쇼핑 링크는 호출 시점마다 클릭 추적 파라미터(NaPm, trxid 등)가
    바뀌는 경우가 많아서, 완전 일치 비교는 거의 항상 실패한다.
    """
    p = urlparse(url or "")
    path = p.path.rstrip("/")
    return f"{p.scheme}://{p.netloc}{path}"


def get_missing_products(conn: sqlite3.Connection, limit: int = None) -> list:
    query = """
        SELECT p.id, p.title, p.link
        FROM products p
        LEFT JOIN products_media m ON p.id = m.id
        WHERE m.id IS NULL
    """
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query).fetchall()


def try_recover_image(title: str, link: str, display: int = 20, debug: bool = False) -> str | None:
    """
    상품 제목으로 검색해서 결과 중 link가 (정규화 기준) 일치하는 항목의 image를 반환.
    못 찾으면 None (단종/링크 변경/검색 결과에서 밀려남 등).
    """
    try:
        data = fetch_page(title, start=1, display=display)
    except Exception as e:
        print(f"  [API ERROR] title='{title[:30]}...': {e}")
        return None

    target = normalize_link(link)
    items = data.get("items", [])

    if debug:
        print(f"\n  [DEBUG] title='{title[:40]}'")
        print(f"  [DEBUG] 저장된 link (정규화): {target}")
        for it in items[:5]:
            print(f"  [DEBUG]   후보 link (정규화): {normalize_link(it.get('link', ''))}")

    for item in items:
        if normalize_link(item.get("link", "")) == target:
            return item.get("image", "")
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--limit", type=int, default=None, help="테스트용으로 일부만 처리")
    parser.add_argument("--commit-every", type=int, default=50)
    parser.add_argument("--debug", action="store_true", help="link 비교 과정을 자세히 출력 (처음 몇 건만 돌려볼 때)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    missing = get_missing_products(conn, args.limit)
    print(f"복구 대상: {len(missing)}건")

    recovered, not_found = 0, 0
    for i, (pid, title, link) in enumerate(missing, 1):
        image_url = try_recover_image(title, link, debug=args.debug)

        if image_url:
            conn.execute(
                "INSERT OR IGNORE INTO products_media (id, image_url) VALUES (?, ?)",
                (pid, image_url),
            )
            recovered += 1
        else:
            not_found += 1

        if i % args.commit_every == 0:
            conn.commit()
            print(f"  진행 {i}/{len(missing)} (복구 {recovered}, 미발견 {not_found})")

        time.sleep(SLEEP_SEC)

    conn.commit()
    conn.close()
    print(f"\n완료 — 복구 {recovered}건 / 미발견(단종·링크변경 추정) {not_found}건")


if __name__ == "__main__":
    main()