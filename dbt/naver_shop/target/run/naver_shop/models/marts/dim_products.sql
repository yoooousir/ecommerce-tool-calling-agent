
  
    
    
    create  table main."dim_products"
    as
        -- dim_products.sql
-- ============================================================
-- marts 레이어: stg_products를 기반으로 비즈니스 로직을 적용한
-- "최종 상품 카탈로그" 테이블. 쇼핑 에이전트의 RDBMS 조건 검색
-- (예: "10만원 이하 텐트")이 바로 이 테이블을 대상으로 쿼리하게 된다.
--
-- 추가하는 비즈니스 로직:
--   1) price_bucket: 가격대를 구간화 (저가/중가/고가) — 에이전트가
--      "저렴한 것 위주로 보여줘" 같은 모호한 표현을 처리할 때 활용 가능
--   2) has_image: 로컬에 이미지가 다운로드되어 있는지 여부 플래그
--      — 멀티모달(이미지) 검색 대상에 포함 가능한지 빠르게 필터링하는 용도
--   3) category_path: category_l1~l4를 사람이 읽기 쉬운 "대분류 > 중분류 > ..."
--      형태의 단일 문자열로 합쳐, 검색결과 표시/로깅에 바로 사용 가능하게 함
--
-- materialized: table (dbt_project.yml에서 marts 레이어 전체에 설정됨)
--   → 매 쿼리마다 재계산하지 않고, dbt run 시점에 한 번 계산해 디스크에 저장
--     (에이전트가 실시간으로 자주 조회하는 테이블이므로 응답속도를 위해 table로 고정)
-- ============================================================

with stg as (

    select * from main."stg_products"

),

final as (

    select
        product_id,
        title,
        link,
        image_url,
        local_image_path,
        description,
        low_price,
        high_price,
        mall_name,
        maker,
        brand,

        -- 카테고리 4단계를 사람이 읽기 좋은 하나의 경로 문자열로 결합
        -- (빈 값인 하위 카테고리는 자동으로 생략됨)
        trim(
            category_l1 ||
            case when category_l2 != '' then ' > ' || category_l2 else '' end ||
            case when category_l3 != '' then ' > ' || category_l3 else '' end ||
            case when category_l4 != '' then ' > ' || category_l4 else '' end
        ) as category_path,

        category_l1,
        category_l2,

        -- 가격 구간화: 에이전트의 search_by_text 도구가 "저렴한 것 위주" 같은
        -- 모호한 자연어 조건을 받았을 때, 정확한 숫자 대신 이 구간으로 필터링 가능
        case
            when low_price < 30000 then 'budget'       -- 저가
            when low_price < 100000 then 'mid'          -- 중가
            else 'premium'                                -- 고가
        end as price_bucket,

        -- 로컬에 이미지가 다운로드되어 있는지 여부
        -- (download_images Task 실행 전에는 모두 0으로 표시됨)
        case when local_image_path is not null then 1 else 0 end as has_image,

        search_keyword,
        collected_at

    from stg

)

select * from final

  