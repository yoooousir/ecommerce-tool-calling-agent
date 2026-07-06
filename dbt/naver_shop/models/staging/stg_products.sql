-- stg_products.sql (v2)
-- ============================================================
-- staging 레이어: raw products 테이블 1차 정제.
--
-- [v2 변경사항]
--   제거된 컬럼:
--     - description      → S3 parquet으로 분리
--     - image_url        → S3 parquet으로 분리
--     - local_image_path → download_images Task 제거로 불필요
--
-- 남은 컬럼은 모두 RDBMS 조건 검색(가격/카테고리 필터링)에
-- 직접 사용되는 정형 데이터이다.
-- ============================================================

with source as (

    select * from {{ source('naver_raw', 'products') }}

),

cleaned as (

    select
        id                              as product_id,
        trim(title)                     as title,
        link,
        lprice                          as low_price,
        hprice                          as high_price,
        trim(mall_name)                 as mall_name,
        trim(maker)                     as maker,
        trim(brand)                     as brand,
        trim(category1)                 as category_l1,
        trim(category2)                 as category_l2,
        trim(category3)                 as category_l3,
        trim(category4)                 as category_l4,
        search_keyword,
        collected_at

    from source
    -- 가격이 0 이하이거나 제목이 비어있는 비정상 행 제외
    where lprice > 0
      and title is not null
      and trim(title) != ''

)

select * from cleaned