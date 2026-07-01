
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test