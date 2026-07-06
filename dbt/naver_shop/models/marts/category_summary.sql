-- category_summary.sql (v2)
-- ============================================================
-- marts 레이어: 카테고리별 수집 현황 요약.
--
-- [v2 변경사항]
--   제거: image_downloaded_count, image_coverage_pct
--   (has_image 컬럼이 dim_products에서 제거되어 집계 불가)
-- ============================================================

with products as (

    select * from {{ ref('dim_products') }}

),

summary as (

    select
        category_l1,
        count(*)                   as product_count,
        round(avg(low_price), 0)   as avg_low_price,
        min(low_price)             as min_price,
        max(low_price)             as max_price

    from products
    group by category_l1

)

select * from summary
order by product_count desc