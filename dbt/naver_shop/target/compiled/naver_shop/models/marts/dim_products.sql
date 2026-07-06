-- dim_products.sql (v2)
-- ============================================================
-- marts 레이어: 에이전트의 RDBMS 조건 검색 대상 최종 상품 카탈로그.
--
-- [v2 변경사항]
--   제거된 컬럼:
--     - description      → S3 parquet에만 존재
--     - image_url        → S3 parquet에만 존재
--     - local_image_path → 불필요
--     - has_image        → image_url이 없으므로 판단 불가, 제거
--
--   유지된 비즈니스 로직:
--     - price_bucket: budget / mid / premium 구간화
--     - category_path: 카테고리 4단계를 하나의 경로 문자열로 결합
-- ============================================================

with stg as (

    select * from main."stg_products"

),

final as (

    select
        product_id,
        title,
        link,
        low_price,
        high_price,
        mall_name,
        maker,
        brand,

        -- 카테고리 4단계 경로 결합 (빈 하위 카테고리 자동 생략)
        trim(
            category_l1 ||
            case when category_l2 != '' then ' > ' || category_l2 else '' end ||
            case when category_l3 != '' then ' > ' || category_l3 else '' end ||
            case when category_l4 != '' then ' > ' || category_l4 else '' end
        ) as category_path,

        category_l1,
        category_l2,

        -- 가격 구간화 (에이전트의 모호한 가격 표현 처리용)
        case
            when low_price < 30000  then 'budget'   -- 3만원 미만
            when low_price < 100000 then 'mid'       -- 3만~10만원
            else                         'premium'   -- 10만원 이상
        end as price_bucket,

        search_keyword,
        collected_at

    from stg

)

select * from final