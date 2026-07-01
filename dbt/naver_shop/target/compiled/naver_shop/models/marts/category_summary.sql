-- category_summary.sql
-- ============================================================
-- marts 레이어: 카테고리별 수집 현황을 요약하는 모델.
-- 분석 자체의 결과물이라기보다, 데이터 품질 점검(특정 카테고리가
-- 너무 적게 수집되지는 않았는지)과 추후 비용/사용량 대시보드의
-- "카테고리별 분포" 패널에 바로 활용할 수 있도록 미리 집계해둔다.
-- ============================================================

with products as (

    select * from main."dim_products"

),

summary as (

    select
        category_l1,
        count(*)                                   as product_count,
        round(avg(low_price), 0)                   as avg_low_price,
        min(low_price)                              as min_price,
        max(low_price)                              as max_price,
        sum(has_image)                               as image_downloaded_count,
        round(100.0 * sum(has_image) / count(*), 1) as image_coverage_pct

    from products
    group by category_l1

)

select * from summary
order by product_count desc