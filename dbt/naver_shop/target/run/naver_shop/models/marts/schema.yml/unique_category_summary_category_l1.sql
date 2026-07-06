select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
    

select
    category_l1 as unique_field,
    count(*) as n_records

from main."category_summary"
where category_l1 is not null
group by category_l1
having count(*) > 1



      
    ) dbt_internal_test