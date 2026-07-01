
    
    

with all_values as (

    select
        price_bucket as value_field,
        count(*) as n_records

    from main."dim_products"
    group by price_bucket

)

select *
from all_values
where value_field not in (
    'budget','mid','premium'
)


