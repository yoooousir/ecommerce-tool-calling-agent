-- stg_products.sql
-- ============================================================
-- staging 레이어: raw products 테이블을 가공 없이 "정제"만 하는 모델.
-- 여기서는 비즈니스 로직(카테고리 정규화, 가격대 구간화 등)을 넣지 않고,
-- 다음 두 가지만 처리한다:
--   1) 컬럼명을 분석하기 쉬운 snake_case로 통일 (이미 snake_case지만 명시적으로 alias)
--   2) 결측치/이상치 처리: 가격이 0이거나 음수인 행, 제목이 빈 행을 걸러냄
--      (네이버 API 응답에 가끔 가격 정보가 없는 상품이 섞여 있어 분석/검색 품질을 위해 제외)
--
-- materialized: view (dbt_project.yml에서 staging 레이어 전체에 설정됨)
--   → 매번 raw 테이블을 그대로 비추는 가벼운 뷰라서, 적재 직후 바로 최신값 반영
-- ============================================================

with source as (

    select * from {{ source('naver_raw', 'products') }}

),

cleaned as (

    select
        id                              as product_id,
        trim(title)                     as title,
        link,
        image_url,
        local_image_path,
        description,
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
    -- 가격 정보가 없거나(0 이하) 제목이 비어있는 비정상 행은 분석/검색 대상에서 제외
    where lprice > 0
      and title is not null
      and trim(title) != ''

)

select * from cleaned
